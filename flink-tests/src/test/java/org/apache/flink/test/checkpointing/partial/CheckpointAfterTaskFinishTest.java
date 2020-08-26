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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.junit.Test;

import java.io.File;

public class CheckpointAfterTaskFinishTest extends AbstractPartialCheckpointTestBase {

	@Test(timeout = 60000)
	public void testCheckpointsAfterTasksFinished() throws Exception {
		final File checkpointDir = TEMPORARY_FOLDER.newFolder();

		final Configuration config = new Configuration();
		config.setString(RestOptions.BIND_PORT, "18081-19000");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
		env.setStateBackend((StateBackend) new FsStateBackend(checkpointDir.toURI()));
		env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

		createJobInEnvironment(env, false);

		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setJobName("Test");
		JobGraph jobGraph = streamGraph.getJobGraph();

		final MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setNumTaskManagers(5)
			.setNumSlotsPerTaskManager(2)
			.setRpcServiceSharing(RpcServiceSharing.SHARED)
			.setConfiguration(config)
			.build();

		try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
			miniCluster.start();
			miniCluster.executeJobBlocking(jobGraph);
		}
	}

	@Test(timeout = 60000)
	public void testCheckpointsAfterTasksFinishedWithFailover() throws Exception {
		final File checkpointDir = TEMPORARY_FOLDER.newFolder();

		final Configuration config = new Configuration();
		config.setString(RestOptions.BIND_PORT, "18081-19000");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
		env.setStateBackend((StateBackend) new FsStateBackend(checkpointDir.toURI()));
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.seconds(1)));

		createJobInEnvironment(env, true);

		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setJobName("Test");
		JobGraph jobGraph = streamGraph.getJobGraph();

		final MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
			.setNumTaskManagers(5)
			.setNumSlotsPerTaskManager(2)
			.setRpcServiceSharing(RpcServiceSharing.SHARED)
			.setConfiguration(config)
			.build();

		try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
			miniCluster.start();
			miniCluster.executeJobBlocking(jobGraph);
		}
	}
}
