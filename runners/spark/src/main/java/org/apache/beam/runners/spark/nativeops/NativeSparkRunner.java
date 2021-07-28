/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.runners.spark.nativeops;

import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;
import static org.apache.beam.runners.spark.SparkCommonPipelineOptions.prepareFilesToStage;
import static org.apache.beam.runners.spark.util.SparkCommon.startEventLoggingListener;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.resources.PipelineResources;
import org.apache.beam.runners.core.metrics.MetricsPusher;
import org.apache.beam.runners.spark.AbsSparkRunner;
import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkPipelineResult;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.SparkTransformOverrides;
import org.apache.beam.runners.spark.aggregators.AggregatorsAccumulator;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.runners.spark.translation.streaming.Checkpoint.CheckpointDir;
import org.apache.beam.runners.spark.translation.streaming.SparkRunnerStreamingContextFactory;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder.WatermarkAdvancingStreamingListener;
import org.apache.beam.runners.spark.util.SparkCompat;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.metrics.MetricsOptions;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.EventLoggingListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingListener;
import org.apache.spark.streaming.api.java.JavaStreamingListenerWrapper;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The NativeSparkRunner.
 */
public final class NativeSparkRunner extends AbsSparkRunner<NativeSparkPipelineResult>
      implements INativeSparkRunner {
  private static final Logger LOG = LoggerFactory.getLogger(NativeSparkRunner.class);

  /**
   * Creates and returns a new SparkRunner with default options. In particular, against a spark
   * instance running in local mode.
   *
   * @return A pipeline runner with default options.
   */
  public static NativeSparkRunner create() {
    SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(NativeSparkRunner.class);
    return new NativeSparkRunner(options);
  }

  /**
   * Creates and returns a new SparkRunner with specified options.
   *
   * @param options The PipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static NativeSparkRunner fromOptions(PipelineOptions options) {
    return new NativeSparkRunner(PipelineOptionsValidator.validate(SparkPipelineOptions.class, options));
  }

  /**
   * The run method for a regular pipeline as specified by the Runner interface.
   * Should not be used for native spark pipelines.
   *
   * @param pipeline the pipeline to run
   * @return nothing
   * @throws RuntimeException always
   */
  @Override
  public NativeSparkPipelineResult run(Pipeline pipeline) {
    throw new RuntimeException("Use run(NativeSparkPipeline, NativeSpark) instead.");
  }

  protected NativeSparkRunner(SparkPipelineOptions options) {
    super(options);
  }

  /**
   * The Run method for native spark pipeline. This should be used instead of
   * run(pipeline).
   */
  @Override
  @SuppressWarnings("CatchAndPrintStackTrace")
  public NativeSparkPipelineResult run(final NativeSparkPipeline pipeline,
                                       NativeSpark nativeSparkCode) {
    LOG.info("Executing pipeline using the SparkRunner.");

    final NativeSparkPipelineResult result;
    final Future<?> startPipeline;

    final SparkPipelineTranslator translator;

    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    MetricsEnvironment.setMetricsSupported(true);

    // visit the pipeline to determine the translation mode
    detectTranslationMode(pipeline);

    // Default to using the primitive versions of Read.Bounded and Read.Unbounded.
    // TODO(BEAM-10670): Use SDF read as default when we address performance issue.
    if (!ExperimentalOptions.hasExperiment(pipeline.getOptions(), "beam_fn_api")) {
      SplittableParDo.convertReadBasedSplittableDoFnsToPrimitiveReadsIfNecessary(pipeline);
    }

    pipeline.replaceAll(SparkTransformOverrides.getDefaultOverrides(pipelineOptions.isStreaming()));

    prepareFilesToStage(pipelineOptions);

    final long startTime = Instant.now().getMillis();
    EventLoggingListener eventLoggingListener = null;
    JavaSparkContext jsc = null;
    if (pipelineOptions.isStreaming()) {
      throw new RuntimeException("Cannot use streaming mode with NativeSparkRunner");
    } else {
      jsc = SparkContextFactory.getSparkContext(pipelineOptions);
      eventLoggingListener = startEventLoggingListener(jsc, pipelineOptions, startTime);
      final NativeSparkEvaluationContext evaluationContext =
            new NativeSparkEvaluationContext(jsc, pipeline, pipelineOptions);
      translator = new TransformTranslator.Translator();

      // update the cache candidates
      updateCacheCandidates(pipeline, translator, evaluationContext);

      initAccumulators(pipelineOptions, jsc);
      startPipeline =
            executorService.submit(
                  () -> {
                    pipeline.traverseTopologically(new Evaluator(translator, evaluationContext));
                    evaluationContext.computeOutputs(nativeSparkCode);
                    LOG.info("Batch pipeline execution complete.");
                  });
      executorService.shutdown();

      result = new NativeSparkPipelineResult(startPipeline, jsc,
                                                 evaluationContext.getOutputs());
    }

    if (pipelineOptions.getEnableSparkMetricSinks()) {
      registerMetricsSource(pipelineOptions.getAppName());
    }

    // it would have been better to create MetricsPusher from runner-core but we need
    // runner-specific
    // MetricsContainerStepMap
    MetricsPusher metricsPusher =
          new MetricsPusher(
                MetricsAccumulator.getInstance().value(),
                pipelineOptions.as(MetricsOptions.class),
                result);
    metricsPusher.start();

    if (eventLoggingListener != null && jsc != null) {
      eventLoggingListener.onApplicationStart(
            SparkCompat.buildSparkListenerApplicationStart(jsc, pipelineOptions, startTime, result));
      eventLoggingListener.onApplicationEnd(
            new SparkListenerApplicationEnd(Instant.now().getMillis()));
      eventLoggingListener.stop();
    }

    return result;

    /*

    LOG.info("Executing pipeline using the SparkRunner.");

    final NativeSparkPipelineResult result;
    final Future<?> startPipeline;

    final SparkPipelineTranslator translator;

    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    MetricsEnvironment.setMetricsSupported(true);

    // visit the pipeline to determine the translation mode
    detectTranslationMode(pipeline);

    if (pipelineOptions.isStreaming()) {
      throw new RuntimeException("Cannot use streaming mode with NativeSparkRunner");
    } else {
      // create the evaluation context
      PipelineResources.prepareFilesForStaging(pipelineOptions);

      final JavaSparkContext jsc = SparkContextFactory.getSparkContext(pipelineOptions);
      final NativeSparkEvaluationContext evaluationContext =
            new NativeSparkEvaluationContext(jsc, pipeline, pipelineOptions);

      translator = new TransformTranslator.Translator();

      // update the cache candidates
      updateCacheCandidates(pipeline, translator, evaluationContext);

      initAccumulators(pipelineOptions, jsc);

      startPipeline =
            executorService.submit(
                  () -> {
                    try {
                      LOG.info("Running executor");

                      pipeline.traverseTopologically(new Evaluator(translator,
                                                                   evaluationContext));
                      evaluationContext.computeOutputs(nativeSparkCode);
                      LOG.info("Batch pipeline execution complete.");
                    } catch (Throwable t) {
                      t.printStackTrace();
                    }
                  });
      executorService.shutdown();

      result = new NativeSparkPipelineResult(startPipeline, jsc,
                                             evaluationContext.getSparkOutputs());
    }

    if (pipelineOptions.getEnableSparkMetricSinks()) {
      registerMetricsSource(pipelineOptions.getAppName());
    }

    return result;
     */
  }
}