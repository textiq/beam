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

package org.apache.beam.runners.spark;

import static org.apache.beam.runners.core.construction.PipelineResources.detectClassPathResourcesToStage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.runners.spark.aggregators.AggregatorsAccumulator;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.runners.spark.translation.streaming.Checkpoint.CheckpointDir;
import org.apache.beam.runners.spark.translation.streaming.SparkRunnerStreamingContextFactory;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder.WatermarkAdvancingStreamingListener;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingListener;
import org.apache.spark.streaming.api.java.JavaStreamingListenerWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SparkRunner translate operations defined on a pipeline to a representation executable by
 * Spark, and then submitting the job to Spark to be executed. If we wanted to run a Beam pipeline
 * with the default options of a single threaded spark instance in local mode, we would do the
 * following:
 *
 * <p>{@code Pipeline p = [logic for pipeline creation] SparkPipelineResult result =
 * (SparkPipelineResult) p.run(); }
 *
 * <p>To create a pipeline runner to run against a different spark cluster, with a custom master url
 * we would do the following:
 *
 * <p>{@code Pipeline p = [logic for pipeline creation] SparkPipelineOptions options =
 * SparkPipelineOptionsFactory.create(); options.setSparkMaster("spark://host:port");
 * SparkPipelineResult result = (SparkPipelineResult) p.run(); }
 */
public final class SparkRunner extends AbsSparkRunner<SparkPipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRunner.class);

  /**
   * Creates and returns a new SparkRunner with default options. In particular, against a spark
   * instance running in local mode.
   *
   * @return A pipeline runner with default options.
   */
  public static SparkRunner create() {
    SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    return new SparkRunner(options);
  }

  /**
   * Creates and returns a new SparkRunner with specified options.
   *
   * @param options The SparkPipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SparkRunner create(SparkPipelineOptions options) {
    return new SparkRunner(options);
  }

  /**
   * Creates and returns a new SparkRunner with specified options.
   *
   * @param options The PipelineOptions to use when executing the job.
   * @return A pipeline runner that will execute with specified options.
   */
  public static SparkRunner fromOptions(PipelineOptions options) {
    SparkPipelineOptions sparkOptions =
        PipelineOptionsValidator.validate(SparkPipelineOptions.class, options);

    if (sparkOptions.getFilesToStage() == null) {
      sparkOptions.setFilesToStage(detectClassPathResourcesToStage(
          SparkRunner.class.getClassLoader()));
      LOG.info("PipelineOptions.filesToStage was not specified. "
              + "Defaulting to files from the classpath: will stage {} files. "
              + "Enable logging at DEBUG level to see which files will be staged.",
          sparkOptions.getFilesToStage().size());
      LOG.debug("Classpath elements: {}", sparkOptions.getFilesToStage());
    }

    return new SparkRunner(sparkOptions);
  }

  /**
   * No parameter constructor defaults to running this pipeline in Spark's local mode, in a single
   * thread.
   */
  protected SparkRunner(SparkPipelineOptions options) {
    super(options);
  }

  @Override
  public SparkPipelineResult run(final Pipeline pipeline) {
    LOG.info("Executing pipeline using the SparkRunner.");

    final SparkPipelineResult result;
    final Future<?> startPipeline;

    final SparkPipelineTranslator translator;

    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    MetricsEnvironment.setMetricsSupported(true);

    // visit the pipeline to determine the translation mode
    detectTranslationMode(pipeline);

    if (mOptions.isStreaming()) {
      CheckpointDir checkpointDir = new CheckpointDir(mOptions.getCheckpointDir());
      SparkRunnerStreamingContextFactory streamingContextFactory =
          new SparkRunnerStreamingContextFactory(pipeline, mOptions, checkpointDir);
      final JavaStreamingContext jssc =
          JavaStreamingContext.getOrCreate(
              checkpointDir.getSparkCheckpointDir().toString(), streamingContextFactory);

      // Checkpoint aggregator/metrics values
      jssc.addStreamingListener(
          new JavaStreamingListenerWrapper(
              new AggregatorsAccumulator.AccumulatorCheckpointingSparkListener()));
      jssc.addStreamingListener(
          new JavaStreamingListenerWrapper(
              new MetricsAccumulator.AccumulatorCheckpointingSparkListener()));

      // register user-defined listeners.
      for (JavaStreamingListener listener : mOptions.as(SparkContextOptions.class).getListeners()) {
        LOG.info("Registered listener {}." + listener.getClass().getSimpleName());
        jssc.addStreamingListener(new JavaStreamingListenerWrapper(listener));
      }

      // register Watermarks listener to broadcast the advanced WMs.
      jssc.addStreamingListener(
          new JavaStreamingListenerWrapper(new WatermarkAdvancingStreamingListener()));

      // The reason we call initAccumulators here even though it is called in
      // SparkRunnerStreamingContextFactory is because the factory is not called when resuming
      // from checkpoint (When not resuming from checkpoint initAccumulators will be called twice
      // but this is fine since it is idempotent).
      initAccumulators(mOptions, jssc.sparkContext());

      startPipeline =
          executorService.submit(
              () -> {
                LOG.info("Starting streaming pipeline execution.");
                jssc.start();
              });
      executorService.shutdown();

      result = new SparkPipelineResult.StreamingMode(startPipeline, jssc);
    } else {
      // create the evaluation context
      final JavaSparkContext jsc = SparkContextFactory.getSparkContext(mOptions);
      final EvaluationContext evaluationContext = new EvaluationContext(jsc, pipeline, mOptions);
      translator = new TransformTranslator.Translator();

      // update the cache candidates
      updateCacheCandidates(pipeline, translator, evaluationContext);

      initAccumulators(mOptions, jsc);

      startPipeline =
          executorService.submit(
              () -> {
                pipeline.traverseTopologically(new Evaluator(translator, evaluationContext));
                evaluationContext.computeOutputs();
                LOG.info("Batch pipeline execution complete.");
              });
      executorService.shutdown();

      result = new SparkPipelineResult.BatchMode(startPipeline, jsc);
    }

    if (mOptions.getEnableSparkMetricSinks()) {
      registerMetricsSource(mOptions.getAppName());
    }

    return result;
  }


  /** Init Metrics/Aggregators accumulators. This method is idempotent. */
  public static void initAccumulators(SparkPipelineOptions opts, JavaSparkContext jsc) {
    // Init metrics accumulators
    MetricsAccumulator.init(opts, jsc);
    AggregatorsAccumulator.init(opts, jsc);
  }
}
