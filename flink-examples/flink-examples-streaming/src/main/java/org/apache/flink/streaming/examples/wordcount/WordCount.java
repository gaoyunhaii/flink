/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkManager;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.FileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.splitsink.SplitCommitter;
import org.apache.flink.streaming.api.functions.splitsink.SplitSink;
import org.apache.flink.streaming.api.functions.splitsink.SplitWriter;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over text files in a streaming fashion.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>write a simple Flink Streaming program,
 * <li>use tuple data types,
 * <li>write and use user-defined functions.
 * </ul>
 */
public class WordCount {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(4);
		env.enableCheckpointing(5000L);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.of(10L, TimeUnit.SECONDS)));

		final FileSink<Tuple2<Integer, Integer>, String> sink = StreamingFileSink
			.forRowFormat(new Path("/tmp/test3"), (Encoder<Tuple2<Integer, Integer>>) (element, stream) -> {
				PrintStream out = new PrintStream(stream);
				out.println(element.f1);
			})
			.withBucketAssigner(new KeyBucketAssigner())
			.withRollingPolicy(OnCheckpointRollingPolicy.build())
			.buildFileSink();

		// generate data, shuffle, sinkSin
		env.addSource(new Generator(10, 1, 10))
			.keyBy(0)
			.addSink(sink);

		env.execute("StreamingFileSinkProgram");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}

	/**
	 * Use first field for buckets.
	 */
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

	/**
	 * Data-generating source function.
	 */
	public static final class Generator implements SourceFunction<Tuple2<Integer, Integer>>, CheckpointedFunction, CheckpointListener {

		private static final long serialVersionUID = -2819385275681175792L;

		private final int numKeys;
		private final int idlenessMs;
		private final int recordsToEmit;

		private volatile int numRecordsEmitted = 0;
		private volatile boolean canceled = false;

		private ListState<Integer> state = null;

		private boolean checkpointFinish = false;

		Generator(final int numKeys, final int idlenessMs, final int durationSeconds) {
			this.numKeys = numKeys;
			this.idlenessMs = idlenessMs;

			this.recordsToEmit = ((durationSeconds * 10) / idlenessMs) * numKeys;
		}

		@Override
		public void run(final SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			while (numRecordsEmitted < recordsToEmit) {
				synchronized (ctx.getCheckpointLock()) {
					for (int i = 0; i < numKeys; i++) {
						ctx.collect(Tuple2.of(i, numRecordsEmitted));
						numRecordsEmitted++;
					}
				}
				System.err.println("num send :" + numRecordsEmitted + " max :" + recordsToEmit);
				Thread.sleep(idlenessMs);
			}

			while (!canceled && checkpointFinish == false) {
				Thread.sleep(50);
			}

			while (true) {
				// wait for commit the file
				Thread.sleep(idlenessMs * 1000) ;

				synchronized (ctx.getCheckpointLock()) {
					for (int i = 0; i < numKeys; i++) {
						ctx.collect(Tuple2.of(i, numRecordsEmitted));
						numRecordsEmitted++;
					}
				}
				break;
			}

		}

		@Override
		public void cancel() {
			canceled = true;
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getOperatorStateStore().getListState(
				new ListStateDescriptor<Integer>("state", IntSerializer.INSTANCE));

			for (Integer i : state.get()) {
				numRecordsEmitted += i;
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			state.clear();
			state.add(numRecordsEmitted);
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			checkpointFinish = true;
		}
	}

}
