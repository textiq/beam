package org.apache.beam.runners.spark.nativeops;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.UserCodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pipeline including native spark code.
 */
public class NativeSparkPipeline extends Pipeline {
    private static final Logger LOG = LoggerFactory.getLogger(NativeSparkPipeline.class);

    public static NativeSparkPipeline createNativeSparkPipeline(PipelineOptions options) {
        PipelineRunner.fromOptions(options);

        NativeSparkPipeline pipeline = new NativeSparkPipeline(options);
        LOG.debug("Creating {}", pipeline);
        return pipeline;
    }

    protected NativeSparkPipeline(PipelineOptions options) {
        super(options);


        if (!INativeSparkRunner.class.isAssignableFrom(options.getRunner())) {
            throw new RuntimeException("NativeSparkPipeline requires a NativeSparkRunner.");
        }
    }

    /**
     * Runs this {@link Pipeline} using the given {@link PipelineOptions}, using the runner
     * specified by the options.
     */
    public NativeSparkPipelineResult run(NativeSpark spark) {
        INativeSparkRunner runner = (INativeSparkRunner) PipelineRunner.fromOptions(defaultOptions);
        // Ensure all of the nodes are fully specified before a PipelineRunner gets access to the
        // pipeline.
        LOG.debug("Running {} via {}", this, runner);
        try {
            validate(defaultOptions);
            return runner.run(this, spark);
        } catch (UserCodeException e) {
            // This serves to replace the stack with one that ends here and
            // is caused by the caught UserCodeException, thereby splicing
            // out all the stack frames in between the PipelineRunner itself
            // and where the worker calls into the user's code.
            throw new PipelineExecutionException(e.getCause());
        }
    }

}
