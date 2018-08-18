package org.apache.beam.runners.spark.nativeops;

/**
 * Wrapper for native spark code.
 *
 */
public interface NativeSpark  {

    void expand(INativeSparkContext ctx);
}
