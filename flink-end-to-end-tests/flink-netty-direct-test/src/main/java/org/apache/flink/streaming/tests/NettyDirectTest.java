/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.tests.udf.RandomNumberSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkState;

/**
 *
 */
public class NettyDirectTest {

	private static final Logger LOG = LoggerFactory.getLogger(NettyDirectTest.class);

	public static void main(String[] args) throws Exception {
		// parse the parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		final String applicationName = params.get("applicationName", "ZeroCopyTest");

		// Number generated per second
		final long rate = params.getLong("rate", 100L);
		// The largest number that can be generated, the rage is [0, max]
		final int max = params.getInt("max", 100);
		// Max count of records that can be generated, default value -1 means it's infinite
		final long maxCount = params.getLong("maxCount", -1L);
		// Seed of the generated random numbers
		final int seed = params.getInt("seed", 0);

		LOG.info("Number generating rate = " + rate + "/s, largest number = " + max);

		final int processParallelism = params.getInt("processParallelism", 1);
		final String stateBackend = params.get("stateBackend", "memory");

		final Boolean enableCheckpoint = params.getBoolean("enableCheckpoint", false);
		final long checkpointInterval = params.getLong("checkpointInterval", 300L);
		final String checkpointPath = params.get("checkpointPath");

		final int numberOfMaps = params.getInt("numberOfMaps", 1);

		final int mapParallelism = params.getInt("map.parallelism", 1);
		final int reduceParallelism = params.getInt("reduce.parallelism", 1);

		// This helps to generate desired state size for test need
		final int stateFatSize = params.getInt("stateFatSize", 0);

		final int flatMapCount = params.getInt("flatmap.explode.count", 1);
		final int strBytes = params.getInt("flatmap.str.bytes", 2048);

		LOG.info("Exploding rate = " + flatMapCount + ", strBytes = " + strBytes);
		final StringBuilder strBuilder = new StringBuilder();
		for (int i = 0; i < strBytes; ++i) {
			strBuilder.append("a");
		}
		final String str = strBuilder.toString();

		// set up the streaming execution environment
//        Configuration configuration = new Configuration();
//        configuration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 4);
//        final StreamExecutionEnvironment env = new LocalStreamEnvironment(configuration);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		if (enableCheckpoint) {
			env.enableCheckpointing(checkpointInterval, CheckpointingMode.AT_LEAST_ONCE);
		}

		KeySelector keySelector = new KeySelector<String, Long>() {
			private final AtomicLong counter = new AtomicLong(0);

			@Override
			public Long getKey(String value) throws Exception {
				return counter.incrementAndGet();
			}
		};

		DataStream<String> first =
			env.addSource(new RandomNumberSourceFunction(max, seed, rate / numberOfMaps, maxCount < 0 ? -1 : maxCount / numberOfMaps))
				.setParallelism(mapParallelism / numberOfMaps)
				.slotSharingGroup("a")
				.flatMap(new MyFlatMapFunction(flatMapCount, str, 0))
				.setParallelism(mapParallelism / numberOfMaps)
				.slotSharingGroup("a");
		for (int i = 0; i < numberOfMaps - 1; ++i) {
			first = first.union(
				env.addSource(new RandomNumberSourceFunction(max, seed, rate / numberOfMaps, maxCount < 0 ? -1 : maxCount / numberOfMaps))
					.setParallelism(mapParallelism / numberOfMaps)
					.slotSharingGroup("u_" + i)
					.flatMap(new MyFlatMapFunction(flatMapCount, str, 0))
					.setParallelism(mapParallelism / numberOfMaps)
					.slotSharingGroup("u_" + i));
		}

		first.rebalance()
			.addSink(new RichSinkFunction() {
				private NettyReporter reporter;

				@Override
				public void open(Configuration parameters) throws Exception {
					reporter = new NettyReporter();
					reporter.init();
				}

				@Override
				public void invoke(Object value, Context context) {
					reporter.test("2", getRuntimeContext().getIndexOfThisSubtask());
				}
			})
			.setParallelism(reduceParallelism)
			.slotSharingGroup("b");

		// execute program
		env.execute(applicationName);
	}

	public static class MyFlatMapFunction extends RichFlatMapFunction<Integer, String> {
		private final int flatMapCount;
		private final String str;
		private final int myIndex;

		public MyFlatMapFunction(int flatMapCount, String str, int myIndex) {
			this.flatMapCount = flatMapCount;
			this.str = str;
			this.myIndex = myIndex;
		}

		private NettyReporter reporter;


		@Override
		public void open(Configuration parameters) throws Exception {
			reporter = new NettyReporter();
			reporter.init();
		}

		@Override
		public void flatMap(Integer integer, Collector<String> collector) throws Exception {
			// for (int i = 0; i < flatMapCount; ++i) {
			collector.collect(str);
			//}

			reporter.test("m" + myIndex + "", getRuntimeContext().getIndexOfThisSubtask());
		}
	}

	private static class NettyReporter {
		private long lastSyncTime;

		private volatile long numDirectBuffers;
		private volatile long directCapacity;
		private volatile long directUsed;

		private volatile long numChunks;
		private volatile long numAssignedBytes;
		private volatile long maxAssignedBytes;

		private NettyBufferPool bufferPool;
		private Method assignBytesMethod;
		private Method allocatedBytesMethod;
		private Method maxAssignedBytesMethod;

		public void init() throws NoSuchFieldException, IllegalAccessException, NoSuchMethodException {
			lastSyncTime = System.nanoTime();

			// Now try load NettyBufferPool via reflection
			Field instanceField = NettyBufferPool.class.getField("INSTANCE");
			bufferPool = (NettyBufferPool) instanceField.get(null);

			assignBytesMethod = NettyBufferPool.class.getMethod("getNumberOfAssignedBytes");
			allocatedBytesMethod = NettyBufferPool.class.getMethod("getNumberOfAllocatedBytes");
			maxAssignedBytesMethod = NettyBufferPool.class.getMethod("getNumberOfMaxAssignedBytes");

			checkState(bufferPool != null, "Buffer Pool Object not found");
			checkState(assignBytesMethod != null, "assignBytesMethod not found");
			checkState(allocatedBytesMethod != null, "getNumberOfAllocatedBytes not found");
			checkState(maxAssignedBytesMethod != null, "maxAssignedBytesMethod not found");
		}

		public void test(String key, int index) {
			long now = System.nanoTime();

			if (now - lastSyncTime >= 2 * 1_000_000_000L) {
				// get all the values
				List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);

				boolean found = false;
				for (BufferPoolMXBean pool : pools) {
					if (pool.getName().equals("direct")) {
						numDirectBuffers = pool.getCount();
						directCapacity = pool.getTotalCapacity();
						directUsed = pool.getMemoryUsed();
						found = true;
						break;
					}
				}

				if (!found) {
					numDirectBuffers = -1;
					directCapacity = -1;
					directUsed = -1;
				}

				try {
					Option<Long> numAssignedBytesOption = (Option<Long>) assignBytesMethod.invoke(bufferPool);
					if (numAssignedBytesOption.nonEmpty()) {
						numAssignedBytes = numAssignedBytesOption.get();
					}

					Option<Long> numChunksOption = (Option<Long>) allocatedBytesMethod.invoke(bufferPool);
					if (numChunksOption.nonEmpty()) {
						numChunks = numChunksOption.get() / 4 / 1024 / 1024;
					}

					Option<Long> maxAssignedBytesOption = (Option<Long>) maxAssignedBytesMethod.invoke(bufferPool);
					if (maxAssignedBytesOption.nonEmpty()) {
						maxAssignedBytes = maxAssignedBytesOption.get();
					}

				} catch (Exception e) {
					e.printStackTrace();
				}

				LOG.info("[{}-{}] numDirectBuffers directCapacity directUsed numChunks numAssignedBytes maxAssignedBytes " +
						"{} {} {} {} {} {}",
					key, index, numDirectBuffers, directCapacity, directUsed, numChunks, numAssignedBytes, maxAssignedBytes);

				lastSyncTime = now;
			}
		}
	}
}
