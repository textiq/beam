package org.apache.beam.runners.spark.nativeops;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * An output produced by spark.
 * @param <T> the type of output
 */
public class SparkOutput<T> implements POutput {
    private T value;
    private Pipeline pipeline;
    private boolean finishedSpecifying = false;

    public SparkOutput(Pipeline pipeline, T value) {
        this.pipeline = pipeline;
        this.value = value;

    }

    public T getValue() {
        return value;
    }

    boolean isFinishedSpecifying() {
        return finishedSpecifying;
    }

    @Override
    public Pipeline getPipeline() {
        return pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
        throw new RuntimeException("Cannot expand a SparkOutput");
    }

    @Override
    public void finishSpecifyingOutput(String transformName, PInput input,
                                       PTransform<?, ?> transform) {
        finishedSpecifying = true;
    }
}
