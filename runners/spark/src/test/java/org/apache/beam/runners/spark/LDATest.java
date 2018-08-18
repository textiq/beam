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

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.runners.spark.nativeops.INativeSparkContext;
import org.apache.beam.runners.spark.nativeops.NativeSpark;
import org.apache.beam.runners.spark.nativeops.NativeSparkPipeline;
import org.apache.beam.runners.spark.nativeops.NativeSparkPipelineResult;
import org.apache.beam.runners.spark.nativeops.TestNativeSparkRunner;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.clustering.LDAModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

/**
 * Provided Spark Context tests.
 */
public class LDATest implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(LDATest.class);
    /**
     * Provide a context and call pipeline run.
     * @throws Exception
     */
    @Test
    public void testWithProvidedContext() throws Exception {
        SparkContextOptions options = getSparkContextOptions();

        NativeSparkPipeline p = NativeSparkPipeline.createNativeSparkPipeline(options);

        // https://github.com/apache/spark/blob/master/data/mllib/sample_lda_data.txt
        List<String> linesArray = Lists.newArrayList(
            "1 2 6 0 2 3 1 1 0 0 3",
            "1 3 0 1 3 0 0 2 0 0 1",
            "1 4 1 0 0 4 9 0 1 2 0",
            "2 1 0 3 0 0 5 0 2 3 9",
            "3 1 1 9 3 0 2 0 0 1 3",
            "4 2 0 3 4 5 1 1 1 4 0",
            "2 1 0 3 0 0 5 0 2 2 9",
            "1 1 1 9 2 1 2 0 0 1 3",
            "4 4 0 3 4 2 1 3 0 0 0",
            "2 8 2 0 3 0 2 0 2 7 2",
            "1 1 1 9 0 2 2 0 0 3 3",
            "4 1 0 0 4 5 1 3 0 1 0");

        // (1) Setup a small beam pipeline
        final PCollection<String> data = p.apply(Create.of(linesArray));
        final PCollection<Vector> parsedDataPCollection = data
            .apply(MapElements.via(new SimpleFunction<String, Vector>() {
                       @Override
                       public Vector apply(String input) {
                           String[] sarray = input.trim().split(" ");
                           double[] values = new double[sarray.length];
                           for (int i = 0; i < sarray.length; i++) {
                               values[i] = Double.parseDouble(sarray[i]);
                           }
                           return Vectors.dense(values);
                       }
                   })
            );
        data.apply(Count.globally());

        // (2) instruct spark to take the results of the beam pipeline,
        //     and run LDA on it
        NativeSparkPipelineResult result = p.run(
            new NativeSpark() {
                @Override
                public void expand(INativeSparkContext ctx) {
                    // Get the RDD that was created for the given PCollection
                    JavaRDD<Vector> parsedDataInSpark = ctx.get(parsedDataPCollection);

                    // Do a spark transformation
                    JavaPairRDD<Long, Vector> corpus =
                        JavaPairRDD.fromJavaRDD(parsedDataInSpark.zipWithIndex().map(Tuple2::swap));
                    corpus.cache();

                    // Run spark LDA
                    LDAModel model = new LDA().setK(3).run(corpus);

                    // output the result
                    ctx.output("ldaModel", model);
                }
            });

        // (3) Run the pipeline until completion
        Assert.assertEquals(State.DONE, result.waitUntilFinish());

        // (4) Once it's done, fetch the result
        LDAModel ldaModel = result.getValue("ldaModel");
        Assert.assertNotNull("Could not retrieve model", ldaModel);

        // (5) And do whatever else we want to do with it.
        System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
                           + " words):");
        Matrix topics = ldaModel.topicsMatrix();
        for (int topic = 0; topic < 3; topic++) {
            System.out.print("Topic " + topic + ":");
            for (int word = 0; word < ldaModel.vocabSize(); word++) {
                System.out.print(" " + topics.apply(word, topic));
            }
            System.out.println();
        }
    }

    private static SparkContextOptions getSparkContextOptions() {
        final SparkContextOptions options = PipelineOptionsFactory.as(SparkContextOptions.class);
        options.setRunner(TestNativeSparkRunner.class);
        return options;
    }
}
