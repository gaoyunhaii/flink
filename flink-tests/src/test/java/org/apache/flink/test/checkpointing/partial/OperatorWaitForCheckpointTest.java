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

package org.apache.flink.test.checkpointing.partial;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class OperatorWaitForCheckpointTest {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Test
	public void test() throws Exception {
		final File checkpointDir = TEMPORARY_FOLDER.newFolder();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
		env.setStateBackend(new FsStateBackend(checkpointDir.toURI()));
		env.setParallelism(1);

		env.addSource(new CompletableSource()).map(new RichMapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer value) throws Exception {
				return value;
			}

			@Override
			public void close() throws Exception {
				System.out.println("Ordinary map finished");
			}
		}).map(new CompletableMap());

		StreamGraph streamGraph = env.getStreamGraph();
		System.out.println(streamGraph.getStreamingPlanAsJSON());
		streamGraph.setJobName("Test");
		JobGraph jobGraph = streamGraph.getJobGraph();

		assertEquals(1, jobGraph.getNumberOfVertices());

		final Configuration config = new Configuration();
		config.setString(RestOptions.BIND_PORT, "18081-19000");

		final MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setNumTaskManagers(1)
			.setNumSlotsPerTaskManager(1)
			.setConfiguration(config)
			.build();

		try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
			miniCluster.start();
			miniCluster.executeJobBlocking(jobGraph);
		}
	}

	public static class CompletableSource extends RichSourceFunction<Integer> implements CheckpointListener {

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {

		}

		@Override
		public void cancel() {

		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			System.out.println("Source Checkpoint complete: " + checkpointId);
		}

		@Override
		public void close() throws Exception {
			System.out.println("Source closing");
		}
	}

	public static class CompletableMap extends RichMapFunction<Integer, Integer> implements CheckpointListener {

		@Override
		public Integer map(Integer value) throws Exception {
			return value;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			System.out.println("Map Checkpoint complete: " + checkpointId);
		}

		@Override
		public void close() throws Exception {
			System.out.println("Map closing");
		}
	}
}
