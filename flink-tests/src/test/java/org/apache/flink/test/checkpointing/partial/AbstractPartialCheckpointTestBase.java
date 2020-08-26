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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;

import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkState;

public class AbstractPartialCheckpointTestBase extends AbstractTestBase {
	private static final int FAST_SOURCE_RECORDS = 2;
	private static final int SLOW_SOURCE_RECORDS = 8;
	private static final int FAILOVER_RECORDS = 4;

	protected void createJobInEnvironment(StreamExecutionEnvironment env, boolean triggerFailover) {
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
			.process(new CheckpointAfterTaskFinishTest.StatefulConnect(triggerFailover, FAILOVER_RECORDS));
	}

	public static class CheckpointAlignedSource
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

	public static class StatefulMap extends RichMapFunction<Integer, Integer> {

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

	public static class StatefulConnect
		extends CoProcessFunction<Integer, Integer, Integer>
		implements CheckpointedFunction, CheckpointListener {

		private final boolean triggerFailover;
		private final int numRecordsBeforeFailover;

		private ListState<Integer> rightState;
		private int lastReceived;
		private boolean isRestored;

		public StatefulConnect() {
			this(false, -1);
		}

		public StatefulConnect(boolean triggerFailover, int numRecordsBeforeFailover) {
			this.triggerFailover = triggerFailover;
			this.numRecordsBeforeFailover = numRecordsBeforeFailover;
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
			// do nothing
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
