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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.SourceOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.AbstractDataOutput;
import org.apache.flink.streaming.runtime.io.EmptyCheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTaskSourceInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A subclass of {@link StreamTask} for executing the {@link SourceOperator}.
 */
@Internal
public class SourceOperatorStreamTask<T> extends StreamTask<T, SourceOperator<T, ?>> {

	public SourceOperatorStreamTask(Environment env) throws Exception {
		super(env);
	}

	@Override
	public void init() {
		StreamTaskInput<T> input = new StreamTaskSourceInput<>(headOperator);
		DataOutput<T> output = new AsyncDataOutputToOutput<>(
			operatorChain.getChainEntryPoint(),
			getStreamStatusMaintainer());

		inputProcessor = new StreamOneInputProcessor<>(
			input,
			output,
			operatorChain,
			new EmptyCheckpointBarrierHandler(this));
	}

	@Override
	protected boolean triggerCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions, boolean advanceToEndOfEventTime) throws Exception {
		try {
			// No alignment if we inject a checkpoint
			CheckpointMetrics checkpointMetrics = new CheckpointMetrics().setAlignmentDurationNanos(0L);

			subtaskCheckpointCoordinator.initCheckpoint(checkpointMetaData.getCheckpointId(), checkpointOptions);

			boolean success = performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics, advanceToEndOfEventTime);
			if (!success) {
				declineCheckpoint(checkpointMetaData.getCheckpointId());
			}
			return success;
		} catch (Exception e) {
			// propagate exceptions only if the task is still in "running" state
			if (isRunning()) {
				throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() +
					" for operator " + getName() + '.', e);
			} else {
				LOG.debug("Could not perform checkpoint {} for operator {} while the " +
					"invokable was not in state running.", checkpointMetaData.getCheckpointId(), getName(), e);
				return false;
			}
		}
	}

	/**
	 * Implementation of {@link DataOutput} that wraps a specific {@link Output}.
	 */
	private static class AsyncDataOutputToOutput<T> extends AbstractDataOutput<T> {

		private final Output<StreamRecord<T>> output;

		AsyncDataOutputToOutput(
				Output<StreamRecord<T>> output,
				StreamStatusMaintainer streamStatusMaintainer) {
			super(streamStatusMaintainer);

			this.output = checkNotNull(output);
		}

		@Override
		public void emitRecord(StreamRecord<T> streamRecord) {
			output.collect(streamRecord);
		}

		@Override
		public void emitLatencyMarker(LatencyMarker latencyMarker) {
			output.emitLatencyMarker(latencyMarker);
		}

		@Override
		public void emitWatermark(Watermark watermark) {
			output.emitWatermark(watermark);
		}
	}
}
