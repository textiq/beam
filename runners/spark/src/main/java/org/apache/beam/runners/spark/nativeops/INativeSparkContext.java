package org.apache.beam.runners.spark.nativeops;

import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.api.java.JavaRDD;

/**
 * Context for running native spark code.
 */
public interface INativeSparkContext {

    /**
     * Get the RDD implementing the given PCollection.
     * @param value the PCollection
     * @param <T> the type of object in the PCollection
     * @return the RDD representing this PCollection
     */
    <T> JavaRDD<T> get(PCollection<T> value);

    /**
     * Return an object from the NativeSparkCode.
     * @param key the name to store this object under
     * @param object the object to store
     */
    void output(String key, Object object);
}
