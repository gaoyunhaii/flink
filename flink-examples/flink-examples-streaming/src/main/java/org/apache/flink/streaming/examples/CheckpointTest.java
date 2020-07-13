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

package org.apache.flink.streaming.examples;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;

public class CheckpointTest {

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 500_000_000L);
		configuration.set(TaskManagerOptions.NUM_TASK_SLOTS, 2);
		configuration.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.parse("1b"));
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, configuration);

		env.enableCheckpointing(180_000, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().setCheckpointTimeout(Long.MAX_VALUE);
		// env.setStateBackend(new FsStateBackend("file:///tmp/checkpoint"));
		env.setStateBackend(new RocksDBStateBackend("file:///tmp/checkpoint", true));
		env.setParallelism(1);

		env.addSource(new MySource()).slotSharingGroup("source").keyBy(new KeySelector<String, String>() {
			@Override
			public String getKey(String value) throws Exception {
				return value;
			}
		}).process(new KeyedProcessFunction<String, String, String>() {
			private ValueState<Integer> state = null;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);

				state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("test", TypeInformation.of(Integer.class)));
			}

			@Override
			public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
				state.update(value.length());
			}
		}).addSink(new SinkFunction<String>() {
			@Override
			public void invoke(String value, Context context) throws Exception {
				//System.out.println(value);
			}
		}).slotSharingGroup("sink");

		env.execute();
	}

	public static final class MySource extends RichParallelSourceFunction<String> implements CheckpointedFunction {
		private int next = 0;
		private ListState<Integer> count;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			for (int i = 0; i < 10; ++i) {
				int toSent;
				synchronized (ctx.getCheckpointLock()) {
					toSent = next++;
				}

				ctx.collect(toSent + "");
			}
		}

		@Override
		public void cancel() {

		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			count.update(Collections.singletonList(next));
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			count = context.getOperatorStateStore().getListState(new ListStateDescriptor<Integer>("count", Integer.class));

			if (context.isRestored()) {
				next = count.get().iterator().next();
			}
		}
	}
}
