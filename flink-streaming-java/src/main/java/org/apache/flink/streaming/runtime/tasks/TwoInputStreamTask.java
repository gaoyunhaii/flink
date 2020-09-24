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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.io.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.io.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.io.InputProcessorUtil;
import org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor;
import org.apache.flink.streaming.runtime.io.TwoInputSelectionHandler;

import javax.annotation.Nullable;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link StreamTask} for executing a {@link TwoInputStreamOperator} and supporting
 * the {@link TwoInputStreamOperator} to select input for reading.
 */
@Internal
public class TwoInputStreamTask<IN1, IN2, OUT> extends AbstractTwoInputStreamTask<IN1, IN2, OUT> {

	private CheckpointBarrierHandler checkpointBarrierHandler;

	public TwoInputStreamTask(Environment env) throws Exception {
		super(env);
	}

	@Override
	protected void createInputProcessor(
		List<IndexedInputGate> inputGates1,
		List<IndexedInputGate> inputGates2,
		TypeSerializer<IN1> inputDeserializer1,
		TypeSerializer<IN2> inputDeserializer2) {

		TwoInputSelectionHandler twoInputSelectionHandler = new TwoInputSelectionHandler(
			headOperator instanceof InputSelectable ? (InputSelectable) headOperator : null);

		checkpointBarrierHandler = InputProcessorUtil.createCheckpointBarrierHandler(
			getConfiguration(),
			getCheckpointCoordinator(),
			getTaskNameWithSubtaskAndId(),
			this,
			inputGates1,
			inputGates2);

		// create an input instance for each input
		CheckpointedInputGate[] checkpointedInputGates = InputProcessorUtil.createCheckpointedMultipleInputGate(
			getEnvironment().getMetricGroup().getIOMetricGroup(),
			checkpointBarrierHandler,
			inputGates1,
			inputGates2);
		checkState(checkpointedInputGates.length == 2);

		inputProcessor = new StreamTwoInputProcessor<>(
			checkpointedInputGates,
			inputDeserializer1,
			inputDeserializer2,
			getEnvironment().getIOManager(),
			getStreamStatusMaintainer(),
			headOperator,
			twoInputSelectionHandler,
			input1WatermarkGauge,
			input2WatermarkGauge,
			operatorChain,
			setupNumRecordsInCounter(headOperator));
	}

	@Override
	protected boolean triggerCheckpointSync(
		CheckpointMetaData checkpointMetaData,
		CheckpointOptions checkpointOptions,
		boolean advanceToEndOfEventTime) throws Exception {

		if (checkpointBarrierHandler != null) {
			checkpointBarrierHandler.onCheckpointTrigger(checkpointMetaData, checkpointOptions);
			return true;
		}

		return false;
	}
}
