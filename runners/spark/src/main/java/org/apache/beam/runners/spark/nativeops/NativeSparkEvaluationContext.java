package org.apache.beam.runners.spark.nativeops;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * EvaluationContext for NativeSpark code.
 */
public class NativeSparkEvaluationContext extends EvaluationContext implements INativeSparkContext {
    private final Map<String, Object> outputData = new HashMap<String, Object>();
    private Map<String, JavaRDD<?>> pValueToRddMap;

    public NativeSparkEvaluationContext(JavaSparkContext jsc,
                                        Pipeline pipeline,
                                        PipelineOptions options) {
        super(jsc, pipeline, options);
    }

    /**
     * Run the beam pipeline, then execute the native spark code on
     * the resulting RDDs.
     *
     * @param pValueToRddMap the mapping of PValues to their RDD implementations,
     * as produced by the translator
     * @param nativeSparkCode the spark code to execute
     */
    public void computeOutputs(
        Map<String, JavaRDD<?>> pValueToRddMap,
        NativeSpark nativeSparkCode) {
        super.computeOutputs();
        this.pValueToRddMap = pValueToRddMap;
        nativeSparkCode.expand(this);
    }

    /**
     * Get the RDD that implements the given PCollection.
     *
     * @param pCollectionName the PCollection
     * @param <T> the type of elements in the PCollection / RDD
     * @return the RDD implementing the PCollection
     */
    @Override
    public <T> JavaRDD<T> get(String pCollectionName) {
        if (pValueToRddMap.containsKey(pCollectionName)) {
            JavaRDD<T> rdd = ((JavaRDD<WindowedValue<T>>) pValueToRddMap.get(pCollectionName))
                .map(wv -> wv.getValue());
            return rdd;
        }
        throw new IllegalStateException("Cannot resolve un-known PObject: " + pCollectionName);

    }

    /**
     * Store the given value for the given key.
     *
     * @param key the name to store this object under
     * @param object the object to store
     */
    @Override
    public void output(String key, Object object) {
        if (outputData.containsKey(key)) {
            throw new IllegalArgumentException("Variable '" + key
                                               + "' does not have a unique name.");
        }
        outputData.put(key, object);
    }

    /**
     * Get all the outputs produced.
     * @return the map of key-value pairs produced by the NativeSpark code.
     */
     Map<String, Object> getOutputs() {
        return outputData;
    }
}
