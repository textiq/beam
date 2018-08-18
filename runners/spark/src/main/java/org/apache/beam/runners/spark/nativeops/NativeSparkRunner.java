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

import static org.apache.beam.runners.core.construction.PipelineResources.detectClassPathResourcesToStage;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.beam.runners.spark.AbsSparkRunner;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.runners.spark.translation.SparkContextFactory;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformTranslator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.spark.api.java.JavaSparkContext;
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
public final class NativeSparkRunner extends AbsSparkRunner<NativeSparkPipelineResult>
    implements INativeSparkRunner  {
    private static final Logger LOG = LoggerFactory.getLogger(SparkRunner.class);

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

        return new NativeSparkRunner(sparkOptions);
    }

    @Override
    public NativeSparkPipelineResult run(Pipeline pipeline) {
        throw new RuntimeException("Use run(NativeSparkPipeline, NativeSpark) instead.");
    }

    /**
     * No parameter constructor defaults to running this pipeline in Spark's local mode, in a single
     * thread.
     */
    protected NativeSparkRunner(SparkPipelineOptions options) {
        super(options);
    }

    @Override
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

        if (mOptions.isStreaming()) {
            throw new RuntimeException("Cannot use streaming mode with NativeSparkRunner");
        } else {
            // create the evaluation context
            final JavaSparkContext jsc = SparkContextFactory.getSparkContext(mOptions);
            final NativeSparkEvaluationContext evaluationContext =
                new NativeSparkEvaluationContext(jsc, pipeline, mOptions);

            translator = new TransformTranslator.Translator();

            // update the cache candidates
            updateCacheCandidates(pipeline, translator, evaluationContext);

            initAccumulators(mOptions, jsc);


            startPipeline =
                executorService.submit(
                    () -> {
                        try {
                            LOG.info("Running executor");

                            pipeline.traverseTopologically(new Evaluator(translator,
                                                                         evaluationContext));
                            evaluationContext.computeOutputs(TransformTranslator
                                                                 .getPValueToRddMap(),
                                                             nativeSparkCode);
                            LOG.info("Batch pipeline execution complete.");
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    });
            executorService.shutdown();

            result = new NativeSparkPipelineResult(startPipeline, jsc,
                                                   evaluationContext.getOutputs());
        }

        if (mOptions.getEnableSparkMetricSinks()) {
            registerMetricsSource(mOptions.getAppName());
        }

        return result;
    }
}
