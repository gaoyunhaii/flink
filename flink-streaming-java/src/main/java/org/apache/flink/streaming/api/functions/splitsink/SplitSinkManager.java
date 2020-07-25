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

package org.apache.flink.streaming.api.functions.splitsink;

import org.apache.flink.api.connector.sink.SinkEvent;
import org.apache.flink.api.connector.sink.SinkManager;
import org.apache.flink.runtime.sink.coordinator.SinkCoordinator;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitSinkManager<SplitT> implements SinkManager<List<SplitT>> {

	final Map<Integer, List<SplitT>> allFinalSplits = new HashMap<>();

	final SplitCommitter<SplitT> splitCommitter;

	public SplitSinkManager(SplitCommitter<SplitT> splitCommitter) {
		this.splitCommitter = splitCommitter;
	}

	@Override
	public void handleSinkEvent(int subtaskIndex, SinkEvent sinkEvent) {
		final FinalSplitsEvent<SplitT> finalSplitsEvent = (FinalSplitsEvent<SplitT>) sinkEvent;
		for (SplitT split : finalSplitsEvent.getSplits()) {
			System.err.println(split);
		}
//		try {
//			splitCommitter.commit(finalSplitsEvent.getSplits());
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		allFinalSplits.put(subtaskIndex, finalSplitsEvent.getSplits());

	}

	@Override
	public void failTask(int subtaskIndex) {
		allFinalSplits.remove(subtaskIndex);
	}

	@Override
	public List<SplitT> checkpoint(long checkpointId) {
		List<SplitT> allSplits = new ArrayList<>();
		for (List<SplitT> splitList : allFinalSplits.values()) {
			allSplits.addAll(splitList);
		}
		return allSplits;
	}

	@Override
	public void checkpointComplete(long checkpointId) throws IOException {
		LoggerFactory.getLogger(SinkCoordinator.class).info("Checkpoint complete: {}, split = {}", checkpointId, allFinalSplits);
		for (List<SplitT> splitList : allFinalSplits.values()) {
			splitCommitter.commit(splitList);
		}
	}
}
