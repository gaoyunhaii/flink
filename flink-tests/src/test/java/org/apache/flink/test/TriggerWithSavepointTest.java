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

package org.apache.flink.test;

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Test;
import org.scalactic.Chain;

import java.util.concurrent.CompletableFuture;

public class TriggerWithSavepointTest {

    @Test
    public void test() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT, "18081");
        MiniClusterConfiguration miniClusterConfiguration =
                new MiniClusterConfiguration.Builder().setConfiguration(configuration).build();

        String dir = "/tmp/savepoint";

        try (MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration)) {
            miniCluster.start();

            JobGraph jobGraph = createJobGraph();
            CompletableFuture<JobSubmissionResult> result = miniCluster.submitJob(jobGraph);
            result.get();

            Thread.sleep(2000);

            CompletableFuture<String> result2 =
                    miniCluster.stopWithSavepoint(jobGraph.getJobID(), dir, false);
            System.out.println(result2.get());
        }
    }

    private JobGraph createJobGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(
                        new SourceFunction<Integer>() {
                            @Override
                            public void run(SourceContext<Integer> ctx) throws Exception {
                                int i = 0;
                                while (true) {
                                    ctx.collect(i++);
                                    System.out.println("Emit " + i);
                                    Thread.sleep(1000);
                                }
                            }

                            @Override
                            public void cancel() {}
                        })
                .transform(
                        "head", BasicTypeInfo.INT_TYPE_INFO, new MyOperator(ChainingStrategy.HEAD))
                .transform(
                        "non-head",
                        BasicTypeInfo.INT_TYPE_INFO,
                        new MyOperator(ChainingStrategy.ALWAYS));

        return env.getStreamGraph().getJobGraph();
    }

    private static class MyOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer>, BoundedOneInput {

        public MyOperator(ChainingStrategy chainingStrategy) {
            this.chainingStrategy = chainingStrategy;
        }

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {
            output.collect(element);
        }

        @Override
        public void endInput() throws Exception {
            System.out.println(getOperatorID() + " received end of input");
        }
    }
}
