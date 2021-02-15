package org.apache.beam.runners.spark.nativeops;

import java.io.Serializable;

/**
 * Wrapper for native spark code.
 */
public interface NativeSpark extends Serializable {

    /**
     * The native spark code that should be executed.
     * @param ctx the context for fetching RDDs and outputting computed values
     */
    void expand(INativeSparkContext ctx);
}
