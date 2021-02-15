package org.apache.beam.runners.spark.nativeops;

/**
 * Common run method for NativeSparkRunner and TestNativeSparkRunner.
 */
public interface INativeSparkRunner {
    NativeSparkPipelineResult run(NativeSparkPipeline pipeline,
                                  NativeSpark nativeSparkCode);
}
