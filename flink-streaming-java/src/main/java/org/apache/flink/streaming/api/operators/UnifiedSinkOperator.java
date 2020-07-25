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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkEvent;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.sink.event.SinkEventWrapper;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.splitsink.SplitSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

class UnifiedSinkOperator<IN> extends AbstractStreamOperator<Object> implements
	OneInputStreamOperator<IN, Object>, CheckpointListener, BoundedOneInput {

	private static final ListStateDescriptor<byte[]> SINK_EVENTS_STATE_DES =
		new ListStateDescriptor<>("sink-events", BytePrimitiveArraySerializer.INSTANCE);

	private final NavigableMap<Long, List<SinkEvent>> eventSentToSinkManager = new TreeMap<>();

	private final Sink<IN, ?> sink;

	private final OperatorEventGateway operatorEventGateway;

	private final SimpleVersionedSerializer<SinkEvent> sinkEventSimpleVersionedSerializer;

	private final ProcessingTimeService processingTimeService;

	private SinkWriter<IN> sinkWriter;

	private ListState<byte[]> sinkEventsState;

	private UnifiedSinkWriterContext unifiedSinkWriterContext;

	public UnifiedSinkOperator(
		Sink<IN, ?> sink,
		OperatorEventGateway operatorEventGateway,
		ProcessingTimeService processingTimeService,
		SimpleVersionedSerializer<SinkEvent> sinkEventSimpleVersionedSerializer) {
		this.sink = sink;
		this.processingTimeService = processingTimeService;
		this.operatorEventGateway = operatorEventGateway;
		this.sinkEventSimpleVersionedSerializer = sinkEventSimpleVersionedSerializer;
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		sinkEventsState = context.getOperatorStateStore().getListState(SINK_EVENTS_STATE_DES);
		for (byte[] es : sinkEventsState.get()) {
			final SinkEvent sinkEvent = SimpleVersionedSerialization.readVersionAndDeSerialize(sinkEventSimpleVersionedSerializer, es);
			operatorEventGateway.sendEventToCoordinator(new SinkEventWrapper(sinkEvent));
		}

		unifiedSinkWriterContext = new UnifiedSinkWriterContext(context.getOperatorStateStore(), operatorEventGateway);

		sinkWriter = sink.createWriter(unifiedSinkWriterContext);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		sinkWriter.write(element.getValue(), new SinkWriter.Context() {
			@Override
			public long currentProcessingTime() {
				return 0;
			}

			@Override
			public long currentWatermark() {
				return 0;
			}

			@Override
			public Long timestamp() {
				return element.getTimestamp();
			}
		});
	}

	@Override
	public void snapshotState(StateSnapshotContext stateSnapshotContext) throws Exception {
		sinkWriter.preCommit(stateSnapshotContext.getCheckpointId());


		eventSentToSinkManager.put(stateSnapshotContext.getCheckpointId(), unifiedSinkWriterContext.currentSinkEvents);
		unifiedSinkWriterContext.flush();
		unifiedSinkWriterContext.currentSinkEvents = new ArrayList<>();
		sinkEventsState.clear();
		for (List<SinkEvent> sinkEventList : eventSentToSinkManager.values()) {
//			for (SinkEvent sinkEvent : sinkEventList) {
//				sinkEventsState.add(sinkEventSimpleVersionedSerializer.serialize(sinkEvent));
//			}
		}
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws IOException {
		sinkWriter.commitUpTo(checkpointId);
		Iterator<Map.Entry<Long, List<SinkEvent>>> it = eventSentToSinkManager.headMap(checkpointId, false).entrySet().iterator();
		while (it.hasNext()) {
			it.next();
			it.remove();
		}
	}

	@Override
	public void endInput() throws Exception {
		//last we could not u
		sinkWriter.flush();

		unifiedSinkWriterContext.flush();
	}

	class UnifiedSinkWriterContext implements SplitSink.FileSinkWriterContext {

		List<SinkEvent> currentSinkEvents = new ArrayList<>();

		private final OperatorStateStore operatorStateStore;

		private final OperatorEventGateway operatorEventGateway;

		UnifiedSinkWriterContext(OperatorStateStore operatorStateStore, OperatorEventGateway operatorEventGateway) {
			this.operatorStateStore = operatorStateStore;
			this.operatorEventGateway = operatorEventGateway;
		}

		@Override
		public OperatorStateStore getOperatorStateStore() {
			return operatorStateStore;
		}

		@Override
		public boolean isRestored() {
			return false;
		}

		@Override
		public void sendSinkEventToSinkManager(SinkEvent sinkEvent) {
			currentSinkEvents.add(sinkEvent);
		}

		@Override
		public int getSubtaskIndex() {
			return getRuntimeContext().getIndexOfThisSubtask();
		}

		//this is just current implementation
		public void flush() {
			for (SinkEvent sinkEvent : currentSinkEvents) {
				operatorEventGateway.sendEventToCoordinator(new SinkEventWrapper(sinkEvent));
			}
		}

		@Override
		public ProcessingTimeService getProcessingTimeService() {
			return processingTimeService;
		}
	}
}
