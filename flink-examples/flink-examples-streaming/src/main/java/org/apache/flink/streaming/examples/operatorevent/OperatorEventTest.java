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

package org.apache.flink.streaming.examples.operatorevent;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operatorevent.AbstractOperatorEvent;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;

public class OperatorEventTest {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source = env.addSource(new SourceFunction<Integer>() {
			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
				for (int i = 0; i < 100; ++i) {
					ctx.collect(i);
				}
			}

			@Override
			public void cancel() {

			}
		});

		source.transform("test", TypeInformation.of(Integer.class), new SumOperator())
				.setParallelism(1)
				.transform("test-2", TypeInformation.of(Integer.class), new DownStreamOperator())
				.setParallelism(2)
				.addSink(new SinkFunction<Integer>() {
					@Override
					public void invoke(Integer value, Context context) throws Exception {
						System.out.println(value);
					}
				});

		env.execute();
	}

	public static final class Barrier extends AbstractOperatorEvent {
		private int value;

		public Barrier() {
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeInt(value);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			value = in.readInt();
		}
	}

	private static class SumOperator extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<Integer, Integer> {

		@Override
		public void processElement(StreamRecord<Integer> element) throws Exception {
			if (element.getValue() % 50 == 0) {
				output.emitOperatorEvent(new Barrier());
			}

			output.collect(element);
		}
	}

	private static class DownStreamOperator extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<Integer, Integer> {
		private int sum = 0;

		@Override
		public void processElement(StreamRecord<Integer> element) throws Exception {
			sum += element.getValue();
		}

		@Override
		public void processOperatorEvent(AbstractOperatorEvent event) throws Exception {
			if (event instanceof Barrier) {
				output.collect(new StreamRecord<>(sum));
				sum = 0;
			}
		}
	}
}
