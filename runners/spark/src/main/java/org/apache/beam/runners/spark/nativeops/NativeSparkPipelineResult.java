package org.apache.beam.runners.spark.nativeops;

import java.util.Map;
import java.util.concurrent.Future;
import org.apache.beam.runners.spark.SparkPipelineResult;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * PipelineResult object returned in NativeSpark mode. Allows to access the
 * produced artifacts.
 */
public class NativeSparkPipelineResult extends SparkPipelineResult.BatchMode {
    final Map<String, Object> outputData;

    public NativeSparkPipelineResult(Future<?> pipelineExecution,
                                     JavaSparkContext javaSparkContext,
                                     Map<String, Object> outputs) {
        super(pipelineExecution, javaSparkContext);
        this.outputData = outputs;
    }

    /**
     * Returns a (named) value produced by the NativeSpark code.
     * @param variable the name of the variable
     * @param <T> the expected type
     * @return the value
     * @throws IllegalArgumentException when the key does not exist
     */
    public <T> T getValue(String variable) {
        T val = (T) outputData.get(variable);
        if (val == null) {
          throw new IllegalArgumentException("Variable '" + variable + "' not found.");
        }
        return (T) outputData.get(variable);
    }
}
