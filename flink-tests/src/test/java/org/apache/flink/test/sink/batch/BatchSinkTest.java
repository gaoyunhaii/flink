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

package org.apache.flink.test.sink.batch;

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.poc5.FileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.GlobalDataExchangeMode;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.PrintStream;

public class BatchSinkTest extends TestBaseUtils {

	@ClassRule
	public static MiniClusterWithClientResource miniClusterResource = new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(getConfig())
			.setNumberTaskManagers(1)
			.setNumberSlotsPerTaskManager(4)
			.build());

	private static Configuration getConfig() {
		Configuration config = new Configuration();
		config.set(TaskManagerOptions.SINK_BATCH_PERSISTENT_DIR, "file:///tmp/persistent");
		return config;
	}

	@Test
	public void test() throws JobExecutionException, InterruptedException {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		final FileSink<Tuple2<Integer, Integer>, String> sink = StreamingFileSink
			.forRowFormat(new Path("/tmp/test3"), (Encoder<Tuple2<Integer, Integer>>) (element, stream) -> {
				PrintStream out = new PrintStream(stream);
				out.println(element.f1);
			})
			.withBucketAssigner(new KeyBucketAssigner())
			.withRollingPolicy(OnCheckpointRollingPolicy.build())
			.buildFileSink();

		env.addSource(new RichParallelSourceFunction<Tuple2<Integer, Integer>>() {
			@Override
			public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
				for (int i = 0; i < 20; ++i) {
					ctx.collect(new Tuple2<>(i % 2, i));
				}
			}

			@Override
			public void cancel() {

			}
		}).rebalance().addUSinkBlocking(sink);

		StreamGraph graph = env.getStreamGraph("test");

		graph.setScheduleMode(ScheduleMode.LAZY_FROM_SOURCES);
		graph.setGlobalDataExchangeMode(GlobalDataExchangeMode.FORWARD_EDGES_PIPELINED);
		JobGraph jobGraph = graph.getJobGraph();

		MiniCluster miniCluster = miniClusterResource.getMiniCluster();
		miniCluster.executeJobBlocking(jobGraph);
	}

	public static final class KeyBucketAssigner implements BucketAssigner<Tuple2<Integer, Integer>, String> {

		private static final long serialVersionUID = 987325769970523326L;

		@Override
		public String getBucketId(final Tuple2<Integer, Integer> element, final Context context) {
			return String.valueOf(element.f0);
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}
}
