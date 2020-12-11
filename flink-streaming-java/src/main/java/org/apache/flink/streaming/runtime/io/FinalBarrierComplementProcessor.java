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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.FinalizeBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Components insert barriers into the channel received EndOfPartition.
 */
public class FinalBarrierComplementProcessor {

	private boolean isEnableInsertingBarrier;

	private final CheckpointableInput[] inputs;

	private final Map<InputChannelInfo, Long> nextBarrierIds = new HashMap<>();

	private final TreeMap<Long, CheckpointBarrier> barriersToCheck = new TreeMap<>();

	public FinalBarrierComplementProcessor(CheckpointableInput... inputs) {
		this.inputs = inputs;

		this.isEnableInsertingBarrier = false;
	}

	public void enableInsertingBarrier() {
		this.isEnableInsertingBarrier = true;
	}

	public void processFinalBarrier(FinalizeBarrier finalizeBarrier, InputChannelInfo inputChannelInfo) throws IOException {
		checkState(!nextBarrierIds.containsKey(inputChannelInfo));

		nextBarrierIds.put(inputChannelInfo, finalizeBarrier.getNextBarrierId());
		insertBarriers(inputChannelInfo);
	}

	public void onEndOfPartition(InputChannelInfo inputChannelInfo) {
		nextBarrierIds.remove(inputChannelInfo);
	}

	public void onCheckpointAlignmentStart(CheckpointBarrier barrier) throws IOException {
		// There are two situations:
		//    1. Received barrier from one edge and trigger a new checkpoint
		//    2. Triggering checkpoint -> insert barrier -> received the inserted barrier
		// We should ignore the second case
		if (!barriersToCheck.containsKey(barrier.getId())) {
			barriersToCheck.put(barrier.getId(), barrier);
			for (InputChannelInfo inputChannelInfo : nextBarrierIds.keySet()) {
				insertBarriers(inputChannelInfo);
			}
		}
	}

	public void onCheckpointAlignmentStop(long checkpointId) {
		// Now all the checkpoints before the finished need not to be considered
		barriersToCheck.headMap(checkpointId, true).clear();
	}

	public void onTriggeringCheckpoint(CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) throws IOException {
		CheckpointBarrier checkpointBarrier = new CheckpointBarrier(
				checkpointMetaData.getCheckpointId(),
				checkpointMetaData.getTimestamp(),
				checkpointOptions);
		barriersToCheck.put(checkpointBarrier.getId(), checkpointBarrier);
		for (InputChannelInfo inputChannelInfo : nextBarrierIds.keySet()) {
			insertBarriers(inputChannelInfo);
		}
	}

	private void insertBarriers(InputChannelInfo inputChannelInfo) throws IOException {
		long nextBarrierId = nextBarrierIds.get(inputChannelInfo);

		NavigableMap<Long, CheckpointBarrier> barriersToInsert = barriersToCheck.tailMap(nextBarrierId, true);
		if (barriersToInsert.size() > 0) {
			for (Map.Entry<Long, CheckpointBarrier> entry : barriersToInsert.entrySet()) {

				if (isEnableInsertingBarrier) {
					inputs[inputChannelInfo.getGateIdx()].insertBarrierBeforeEndOfPartition(
						inputChannelInfo.getInputChannelIdx(),
						entry.getValue());
				}
			}

			nextBarrierIds.put(inputChannelInfo, barriersToInsert.lastEntry().getKey() + 1);
		}
	}
}
