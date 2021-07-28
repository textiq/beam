package org.apache.beam.runners.spark.nativeops;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Context for running native spark code.
 */
public interface INativeSparkContext {

    /**
     * Get the RDD implementing the given PCollection.
     * @param pCollectionName the PCollection
     * @param <T> the type of object in the PCollection
     * @return the RDD representing this PCollection
     */
    <T> JavaRDD<T> get(String pCollectionName);

    /**
     * Return an object from the NativeSparkCode.
     * @param key the name to store this object under
     * @param object the object to store
     */
    void output(String key, Object object);

    /**
     * Get the Spark context created by beam.
     * @return the spark context
     */
    JavaSparkContext getSparkContext();
}