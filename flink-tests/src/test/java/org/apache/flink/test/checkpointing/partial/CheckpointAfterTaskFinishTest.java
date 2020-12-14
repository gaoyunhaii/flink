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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.Collector;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;

/**
 * Tests doing checkpoints after some tasks finished.
 */
public class CheckpointAfterTaskFinishTest {
	private static final Map<String, ConcurrentHashMap<Integer, Integer>> RESULT = new HashMap<>();

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private static final int FAST_SOURCE_RECORDS = 2;
	private static final int SLOW_SOURCE_RECORDS = 8;
	private static final int FAILOVER_RECORDS = 4;

	@Test(timeout = 60000)
	public void testCheckpointsAfterTasksFinished() throws Exception {
		final File checkpointDir = TEMPORARY_FOLDER.newFolder();
		String resultId = UUID.randomUUID().toString();

		final Configuration config = new Configuration();
		config.setString(RestOptions.BIND_PORT, "18081-19000");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointTimeout(5000);
		env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
		env.setStateBackend(new FsStateBackend(checkpointDir.toURI()));
		env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

		createJobTopology(env, false, resultId);

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

		checkResult(resultId);
	}

	@Test(timeout = 60000)
	public void testCheckpointsAfterTasksFinishedWithFailover() throws Exception {
		final File checkpointDir = TEMPORARY_FOLDER.newFolder();
		String resultId = UUID.randomUUID().toString();

		final Configuration config = new Configuration();
		config.setString(RestOptions.BIND_PORT, "18081-19000");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointTimeout(5000);
		env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);
		env.setStateBackend(new FsStateBackend(checkpointDir.toURI()));
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.seconds(1)));

		createJobTopology(env, true, resultId);

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

		checkResult(resultId);
	}

	private void createJobTopology(StreamExecutionEnvironment env, boolean triggerFailover, String resultId) {
		DataStream<Integer> left = env
			.addSource(new CheckpointAfterTaskFinishTest.CheckpointAlignedSource(FAST_SOURCE_RECORDS, 0))
			.name("Source-0")
			.keyBy(f -> f)
			.map(new CheckpointAfterTaskFinishTest.StatefulMap(0))
			.name("Map-0");
		DataStream<Integer> right = env
			.addSource(new CheckpointAfterTaskFinishTest.CheckpointAlignedSource(SLOW_SOURCE_RECORDS, 1))
			.name("Source-1")
			.keyBy(f -> f)
			.map(new CheckpointAfterTaskFinishTest.StatefulMap(1))
			.name("Map-1");

		left.connect(right)
			.keyBy(f -> f, f -> f)
			.process(new CheckpointAfterTaskFinishTest.StatefulConnect(triggerFailover, FAILOVER_RECORDS, resultId));
	}

	private void checkResult(String resultId) {
		ConcurrentHashMap<Integer, Integer> result = RESULT.get(resultId);
		assertEquals(result.toString(), SLOW_SOURCE_RECORDS, result.size());
		for (int i = 0; i < SLOW_SOURCE_RECORDS; ++i) {
			assertEquals(result.toString(), i < FAST_SOURCE_RECORDS ? 2 : 1, result.getOrDefault(i, 0).intValue());
		}
	}

	private static class CheckpointAlignedSource
		extends RichParallelSourceFunction<Integer>
		implements CheckpointedFunction, CheckpointListener {

		private final int numRecords;
		private final int id;

		private volatile boolean isRunning = true;

		private ListState<Integer> state;
		private int nextValue;

		private volatile int numCheckpointsComplete;

		public CheckpointAlignedSource(int numRecords, int id) {
			this.numRecords = numRecords;
			this.id = id;
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("source-" + id, Integer.class));
			if (context.isRestored()) {
				nextValue = state.get().iterator().next();
				System.out.println(getRuntimeContext().getTaskName() + " restored " + nextValue);
			}
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			Object lock = ctx.getCheckpointLock();

			while (isRunning && nextValue < numRecords) {
				synchronized (lock) {
					ctx.collect(nextValue);
					nextValue++;
				}

				int checkpointsAwait = numCheckpointsComplete + 1;
				synchronized (lock) {
					while (isRunning && numCheckpointsComplete < checkpointsAwait) {
						lock.wait(10);
					}
				}
			}

			// An element marks end of the output, it won't be count in the output
			ctx.collect(Integer.MAX_VALUE);
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			state.update(Collections.singletonList(nextValue));
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			numCheckpointsComplete++;
		}
	}

	static class StatefulMap extends RichMapFunction<Integer, Integer> {

		private final int id;
		private ValueState<Integer> state;

		public StatefulMap(int id) {
			this.id = id;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("map-" + id, Integer.class));
		}

		@Override
		public Integer map(Integer value) throws Exception {
			if (value == Integer.MAX_VALUE) {
				Thread.sleep(2000);
				return value;
			} else {
				state.update(value);
				return value;
			}
		}
	}

	static class StatefulConnect
		extends CoProcessFunction<Integer, Integer, Integer>
		implements CheckpointedFunction, CheckpointListener {

		private final boolean triggerFailover;
		private final int numRecordsBeforeFailover;
		private final String resultId;

		private ListState<Integer> rightState;
		private int lastReceived;
		private boolean isRestored;

		public StatefulConnect(boolean triggerFailover, int numRecordsBeforeFailover, String resultId) {
			this.triggerFailover = triggerFailover;
			this.numRecordsBeforeFailover = numRecordsBeforeFailover;
			this.resultId = resultId;
			RESULT.computeIfAbsent(resultId, k -> new ConcurrentHashMap<>());
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			rightState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("right", Integer.class));
			if (context.isRestored()) {
				lastReceived = rightState.get().iterator().next();
				isRestored = true;
			} else {
				lastReceived = -1;
			}
		}

		@Override
		public void processElement1(Integer value, Context ctx, Collector<Integer> out) throws Exception {
			if (value == Integer.MAX_VALUE) {
				return;
			}

			RESULT.get(resultId).compute(value, (k, v) -> v == null ? 1 : v + 1);
		}

		@Override
		public void processElement2(Integer value, Context ctx, Collector<Integer> out) throws Exception {
			if (value == Integer.MAX_VALUE) {
				return;
			}

			checkState(
				value == lastReceived + 1,
				"The next value should be exactly the next number, but value is " + value + " lastReceived is " + lastReceived);
			lastReceived = value;
			RESULT.get(resultId).compute(value, (k, v) -> v == null ? 1 : v + 1);
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			rightState.update(Collections.singletonList(lastReceived));
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			if (triggerFailover && lastReceived >= numRecordsBeforeFailover && !isRestored) {
				throw new RuntimeException("Designed Exception");
			}
		}
	}
}
