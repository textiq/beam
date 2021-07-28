package org.apache.beam.runners.spark.nativeops;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.spark.TestSparkPipelineOptions;
import org.apache.beam.runners.spark.aggregators.AggregatorsAccumulator;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.stateful.SparkTimerInternals;
import org.apache.beam.runners.spark.util.GlobalWatermarkHolder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps an instance of NativeSpark with some utilities for testing.
 */
public final class TestNativeSparkRunner extends PipelineRunner<NativeSparkPipelineResult>
      implements INativeSparkRunner {


  private static final Logger LOG = LoggerFactory.getLogger(TestNativeSparkRunner.class);
  private final PipelineOptions options;
  private final NativeSparkRunner delegate;

  private TestNativeSparkRunner(PipelineOptions options) {
    this.delegate = NativeSparkRunner.fromOptions(options);
    this.options = options;
  }

  public static TestNativeSparkRunner fromOptions(PipelineOptions options) {
    return new TestNativeSparkRunner(options);
  }

  @Override
  public NativeSparkPipelineResult run(Pipeline pipeline) {
    throw new RuntimeException("Use run(NativeSparkPipeline, NativeSpark) instead.");
  }

  @Override
  @SuppressWarnings("Finally")
  public NativeSparkPipelineResult run(NativeSparkPipeline pipeline, NativeSpark nativeSparkCode) {
    // Default options suffice to set it up as a test runner
    TestSparkPipelineOptions testSparkOptions =
          PipelineOptionsValidator.validate(TestSparkPipelineOptions.class, options);

    boolean isForceStreaming = testSparkOptions.isForceStreaming();
    NativeSparkPipelineResult result = null;

    // clear state of Aggregators, Metrics and Watermarks if exists.
    AggregatorsAccumulator.clear();
    MetricsAccumulator.clear();
    GlobalWatermarkHolder.clear();

    LOG.info("About to run test pipeline " + options.getJobName());

    // if the pipeline was executed in streaming mode, validate aggregators.
    if (isForceStreaming) {
      throw new RuntimeException("Cannot use streaming mode with NativeSparkRunner");
    } else {
      // for batch test pipelines, run and block until done.
      result = delegate.run(pipeline, nativeSparkCode);
      result.waitUntilFinish();
      result.stop();
      PipelineResult.State finishState = result.getState();
      // assert finish state.
      assertThat(
            String.format("Finish state %s is not allowed.", finishState),
            finishState,
            is(PipelineResult.State.DONE));
      // assert via matchers.
      assertThat(result, testSparkOptions.getOnCreateMatcher());
      assertThat(result, testSparkOptions.getOnSuccessMatcher());
    }
    return result;
  }

  private static void awaitWatermarksOrTimeout(
        TestSparkPipelineOptions testSparkPipelineOptions, NativeSparkPipelineResult result) {
    Long timeoutMillis =
          Duration.standardSeconds(Preconditions.checkNotNull(testSparkPipelineOptions.getTestTimeoutSeconds()))
                .getMillis();
    Long batchDurationMillis = testSparkPipelineOptions.getBatchIntervalMillis();
    Instant stopPipelineWatermark =
          new Instant(testSparkPipelineOptions.getStopPipelineWatermark());
    // we poll for pipeline status in batch-intervals. while this is not in-sync with Spark's
    // execution clock, this is good enough.
    // we break on timeout or end-of-time WM, which ever comes first.
    Instant globalWatermark;
    result.waitUntilFinish(Duration.millis(batchDurationMillis));
    do {
      SparkTimerInternals sparkTimerInternals =
            SparkTimerInternals.global(GlobalWatermarkHolder.get(batchDurationMillis));
      sparkTimerInternals.advanceWatermark();
      globalWatermark = sparkTimerInternals.currentInputWatermarkTime();
      // let another batch-interval period of execution, just to reason about WM propagation.
      org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles
            .sleepUninterruptibly(batchDurationMillis, TimeUnit.MILLISECONDS);
    } while ((timeoutMillis -= batchDurationMillis) > 0
             && globalWatermark.isBefore(stopPipelineWatermark));
  }
}