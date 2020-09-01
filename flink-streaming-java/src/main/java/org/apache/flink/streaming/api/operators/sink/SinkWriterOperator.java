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

package org.apache.flink.streaming.api.operators.sink;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.connector.sink.CleanUpUnmanagedCommittable;
import org.apache.flink.api.connector.sink.USink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * //TODO java doc.
 * @param <IN>
 * @param <CommittableT>
 */
public class SinkWriterOperator<IN, CommittableT> extends AbstractStreamOperator<CommittableT> implements OneInputStreamOperator<IN, CommittableT> , BoundedOneInput {

	private final USink<IN, CommittableT> uSink;

	private transient Writer<IN, CommittableT> writer = null;

	private transient CommittableCollector<CommittableT> collector;

	private boolean allManagedCommittablesHasDone = false;

	public SinkWriterOperator(USink<IN, CommittableT> uSink) {
		this.uSink = uSink;
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {

		super.initializeState(context);

		collector = new CommittableCollector<>(output);

		writer = uSink.createWriter(new USink.InitialContext() {
			@Override
			public boolean isRestored() {
				return context.isRestored();
			}

			@Override
			public int getSubtaskIndex() {
				return getRuntimeContext().getIndexOfThisSubtask();
			}

			@Override
			public int getParallelism() {
				return getRuntimeContext().getNumberOfParallelSubtasks();
			}

			@Override
			public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
				return context.getOperatorStateStore().getListState(stateDescriptor);
			}

			@Override
			public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
				return context.getOperatorStateStore().getUnionListState(stateDescriptor);
			}

			@Override
			public AbstractID getSessionId() {
				return null;
			}
		});
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		super.prepareSnapshotPreBarrier(checkpointId);
		writer.persistent(collector);
	}

	//@Override
//	public void snapshotState(StateSnapshotContext context) throws Exception {
//		super.snapshotState(context);
//		writer.persistent(collector);
//	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		writer.write(element.getValue(), new Writer.Context() {
			@Override
			public long currentProcessingTime() {
				throw new RuntimeException("TODO.");
			}

			@Override
			public long currentWatermark() {
				throw new RuntimeException("TODO.");
			}

			@Override
			public Long timestamp() {
				return element.getTimestamp();
			}
		}, collector);
	}

	@Override
	public void endInput() throws IOException {
		writer.flush(collector);
	}

	@Override
	public void close() throws Exception {
		super.close();
		writer.close();
		collector.close();
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		super.notifyCheckpointComplete(checkpointId);
		if (!allManagedCommittablesHasDone) {
			if (writer instanceof CleanUpUnmanagedCommittable) {
				final CleanUpUnmanagedCommittable cleanUpUnmangedCommittable = (CleanUpUnmanagedCommittable) writer;
				cleanUpUnmangedCommittable.cleanUp();
				allManagedCommittablesHasDone = true;
			}
		}
	}

	class CommittableCollector<CommittableT> implements Collector<CommittableT> {

		private final Output<StreamRecord<CommittableT>> output;

		private final StreamRecord<CommittableT> streamRecord;

		public CommittableCollector(Output<StreamRecord<CommittableT>> output) {
			this.output = output;
			this.streamRecord = new StreamRecord<>(null);
		}

		@Override
		public void collect(CommittableT committable) {
			output.collect(streamRecord.replace(committable));
		}

		@Override
		public void close() {

		}
	}
}
