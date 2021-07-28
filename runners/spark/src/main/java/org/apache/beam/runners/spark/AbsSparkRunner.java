/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.spark;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.core.construction.TransformInputs;
import org.apache.beam.runners.spark.aggregators.AggregatorsAccumulator;
import org.apache.beam.runners.spark.metrics.AggregatorMetricSource;
import org.apache.beam.runners.spark.metrics.CompositeSource;
import org.apache.beam.runners.spark.metrics.MetricsAccumulator;
import org.apache.beam.runners.spark.metrics.SparkBeamMetricSource;
import org.apache.beam.runners.spark.translation.EvaluationContext;
import org.apache.beam.runners.spark.translation.SparkPipelineTranslator;
import org.apache.beam.runners.spark.translation.TransformEvaluator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy.Node;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.spark.SparkEnv$;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.metrics.MetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common code for both SparkRunner and NativeSparkRunner.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public abstract class AbsSparkRunner<T extends SparkPipelineResult> extends PipelineRunner<T> {
  /** Options used in this pipeline runner. */
  protected final SparkPipelineOptions pipelineOptions;

  protected AbsSparkRunner(SparkPipelineOptions options) {
    pipelineOptions = options;
  }

  protected void registerMetricsSource(String appName) {
    final MetricsSystem metricsSystem = SparkEnv$.MODULE$.get().metricsSystem();
    final AggregatorMetricSource aggregatorMetricSource =
        new AggregatorMetricSource(null, AggregatorsAccumulator.getInstance().value());
    final SparkBeamMetricSource metricsSource = new SparkBeamMetricSource(null);
    final CompositeSource compositeSource =
        new CompositeSource(
            appName + ".Beam",
            metricsSource.metricRegistry(),
            aggregatorMetricSource.metricRegistry());
    // re-register the metrics in case of context re-use
    metricsSystem.removeSource(compositeSource);
    metricsSystem.registerSource(compositeSource);
  }

  /** Init Metrics/Aggregators accumulators. This method is idempotent. */
  public static void initAccumulators(SparkPipelineOptions opts, JavaSparkContext jsc) {
    // Init metrics accumulators
    MetricsAccumulator.init(opts, jsc);
    AggregatorsAccumulator.init(opts, jsc);
  }

  /** Visit the pipeline to determine the translation mode (batch/streaming). */
  protected void detectTranslationMode(Pipeline pipeline) {
    TranslationModeDetector detector = new TranslationModeDetector();
    pipeline.traverseTopologically(detector);
    if (detector.getTranslationMode().equals(TranslationMode.STREAMING)) {
      // set streaming mode if it's a streaming pipeline
      this.pipelineOptions.setStreaming(true);
    }
  }

  /** Evaluator that update/populate the cache candidates. */
  public static void updateCacheCandidates(
      Pipeline pipeline, SparkPipelineTranslator translator, EvaluationContext evaluationContext) {
    CacheVisitor cacheVisitor = new CacheVisitor(translator, evaluationContext);
    pipeline.traverseTopologically(cacheVisitor);
  }

  /** The translation mode of the Beam Pipeline. */
  public enum TranslationMode {
    /** Uses the batch mode. */
    BATCH,
    /** Uses the streaming mode. */
    STREAMING
  }

  /** Traverses the Pipeline to determine the {@link TranslationMode} for this pipeline. */
  protected static class TranslationModeDetector extends Pipeline.PipelineVisitor.Defaults {
    private static final Logger LOG = LoggerFactory.getLogger(TranslationModeDetector.class);

    private TranslationMode translationMode;

    TranslationModeDetector(TranslationMode defaultMode) {
      this.translationMode = defaultMode;
    }

    TranslationModeDetector() {
      this(TranslationMode.BATCH);
    }

    TranslationMode getTranslationMode() {
      return translationMode;
    }

    @Override
    public void visitValue(PValue value, Node producer) {
      if (translationMode.equals(TranslationMode.BATCH)) {
        if (value instanceof PCollection
            && ((PCollection) value).isBounded() == IsBounded.UNBOUNDED) {
          LOG.info(
              "Found unbounded PCollection {}. Switching to streaming execution.", value.getName());
          translationMode = TranslationMode.STREAMING;
        }
      }
    }
  }

  /** Traverses the pipeline to populate the candidates for caching. */
  protected static class CacheVisitor extends Evaluator {

    CacheVisitor(SparkPipelineTranslator translator, EvaluationContext evaluationContext) {
      super(translator, evaluationContext);
    }

    @Override
    public void doVisitTransform(Node node) {
      // we populate cache candidates by updating the map with inputs of each node.
      // The goal is to detect the PCollections accessed more than one time, and so enable cache
      // on the underlying RDDs or DStreams.
      Map<TupleTag<?>, PValue> inputs = new HashMap<>(node.getInputs());
      for (TupleTag<?> tupleTag : node.getTransform().getAdditionalInputs().keySet()) {
        inputs.remove(tupleTag);
      }

      for (PValue value : inputs.values()) {
        if (value instanceof PCollection) {
          long count = 1L;
          if (ctxt.getCacheCandidates().get(value) != null) {
            count = ctxt.getCacheCandidates().get(value) + 1;
          }
          ctxt.getCacheCandidates().put((PCollection) value, count);
        }
      }
    }
  }

  /** Evaluator on the pipeline. */
  @SuppressWarnings("WeakerAccess")
  public static class Evaluator extends Pipeline.PipelineVisitor.Defaults {
    private static final Logger LOG = LoggerFactory.getLogger(Evaluator.class);

    protected final EvaluationContext ctxt;
    protected final SparkPipelineTranslator translator;

    public Evaluator(SparkPipelineTranslator translator, EvaluationContext ctxt) {
      this.translator = translator;
      this.ctxt = ctxt;
    }

    @Override
    public CompositeBehavior enterCompositeTransform(Node node) {
      PTransform<?, ?> transform = node.getTransform();
      if (transform != null) {
        if (translator.hasTranslation(transform) && !shouldDefer(node)) {
          LOG.info("Entering directly-translatable composite transform: '{}'", node.getFullName());
          LOG.debug("Composite transform class: '{}'", transform);
          doVisitTransform(node);
          return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
        }
      }
      return CompositeBehavior.ENTER_TRANSFORM;
    }

    protected boolean shouldDefer(Node node) {
      // if the input is not a PCollection, or it is but with non merging windows, don't defer.
      Collection<PValue> nonAdditionalInputs =
          TransformInputs.nonAdditionalInputs(node.toAppliedPTransform(getPipeline()));
      if (nonAdditionalInputs.size() != 1) {
        return false;
      }
      PValue input = Iterables.getOnlyElement(nonAdditionalInputs);
      if (!(input instanceof PCollection)
          || !((PCollection) input).getWindowingStrategy().needsMerge()) {
        return false;
      }
      // so far we know that the input is a PCollection with merging windows.
      // check for sideInput in case of a Combine transform.
      PTransform<?, ?> transform = node.getTransform();
      boolean hasSideInput = false;
      if (transform instanceof Combine.PerKey) {
        List<PCollectionView<?>> sideInputs = ((Combine.PerKey<?, ?, ?>) transform).getSideInputs();
        hasSideInput = sideInputs != null && !sideInputs.isEmpty();
      } else if (transform instanceof Combine.Globally) {
        List<PCollectionView<?>> sideInputs = ((Combine.Globally<?, ?>) transform).getSideInputs();
        hasSideInput = sideInputs != null && !sideInputs.isEmpty();
      }
      // defer if sideInputs are defined.
      if (hasSideInput) {
        LOG.info(
            "Deferring combine transformation {} for job {}",
            transform,
            ctxt.getOptions().getJobName());
        return true;
      }
      // default.
      return false;
    }

    @Override
    public void visitPrimitiveTransform(Node node) {
      doVisitTransform(node);
    }

    <TransformT extends PTransform<? super PInput, POutput>> void doVisitTransform(
        Node node) {
      @SuppressWarnings("unchecked")
      TransformT transform = (TransformT) node.getTransform();
      TransformEvaluator<TransformT> evaluator = translate(node, transform);
      LOG.info("Evaluating {}", transform);
      AppliedPTransform<?, ?, ?> appliedTransform = node.toAppliedPTransform(getPipeline());
      ctxt.setCurrentTransform(appliedTransform);
      evaluator.evaluate(transform, ctxt);
      ctxt.setCurrentTransform(null);
    }

    /**
     * Determine if this Node belongs to a Bounded branch of the pipeline, or Unbounded, and
     * translate with the proper translator.
     */
    protected <TransformT extends PTransform<? super PInput, POutput>>
        TransformEvaluator<TransformT> translate(
            Node node, TransformT transform) {
      // --- determine if node is bounded/unbounded.
      // usually, the input determines if the PCollection to apply the next transformation to
      // is BOUNDED or UNBOUNDED, meaning RDD/DStream.
      Map<TupleTag<?>, PCollection<?>> pValues;
      if (node.getInputs().isEmpty()) {
        // in case of a PBegin, it's the output.
        pValues = node.getOutputs();
      } else {
        pValues = node.getInputs();
      }
      IsBounded isNodeBounded = isBoundedCollection(pValues.values());
      // translate accordingly.
      LOG.debug("Translating {} as {}", transform, isNodeBounded);
      return isNodeBounded.equals(IsBounded.BOUNDED)
          ? translator.translateBounded(transform)
          : translator.translateUnbounded(transform);
    }

    protected IsBounded isBoundedCollection(Collection<PCollection<?>> pValues) {
      // anything that is not a PCollection, is BOUNDED.
      // For PCollections:
      // BOUNDED behaves as the Identity Element, BOUNDED + BOUNDED = BOUNDED
      // while BOUNDED + UNBOUNDED = UNBOUNDED.
      IsBounded isBounded = IsBounded.BOUNDED;
      for (PCollection<?> pValue : pValues) {
        isBounded = isBounded.and(pValue.isBounded());
      }
      return isBounded;
    }
  }
}
