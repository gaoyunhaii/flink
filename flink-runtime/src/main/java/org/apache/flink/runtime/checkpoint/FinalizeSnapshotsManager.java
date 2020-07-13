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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class FinalizeSnapshotsManager {
	private final Logger LOG = LoggerFactory.getLogger(FinalizeSnapshotsManager.class);

	private final ExecutionVertex[] tasksToCommitTo;

	private final ConcurrentHashMap<ExecutionVertexID, SnapshotAndLocation> finalSnapshots = new ConcurrentHashMap<>();

	public FinalizeSnapshotsManager(ExecutionVertex[] tasksToCommitTo) {
		this.tasksToCommitTo = tasksToCommitTo;
	}

	public boolean addSnapshot(ExecutionVertexID executionVertexId, TaskStateSnapshot snapshot, String location) {
		LOG.info("add snapshot {} for {}", executionVertexId, snapshot);
		finalSnapshots.put(executionVertexId, new SnapshotAndLocation(snapshot, location));

		return finalSnapshots.size() == tasksToCommitTo.length;
	}

	public TaskStateSnapshot getSnapshot(ExecutionVertexID id) {
		return finalSnapshots.get(id).snapshot;
	}

	public String getLocation(ExecutionVertexID id) {
		return finalSnapshots.get(id).location;
	}

	public boolean isAllReceived() {
		return finalSnapshots.size() == tasksToCommitTo.length;
	}

	private static class SnapshotAndLocation {
		final TaskStateSnapshot snapshot;
		final String location;

		public SnapshotAndLocation(TaskStateSnapshot snapshot, String location) {
			this.snapshot = snapshot;
			this.location = location;
		}
	}
}
