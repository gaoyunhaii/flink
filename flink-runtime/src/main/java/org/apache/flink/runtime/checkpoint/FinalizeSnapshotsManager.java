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
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class FinalizeSnapshotsManager {
	private final Logger LOG = LoggerFactory.getLogger(FinalizeSnapshotsManager.class);

	private final ExecutionVertex[] tasksToCommitTo;

	private final Map<ExecutionVertexID, TaskStateSnapshot> finalSnapshots = new HashMap<>();
	private final Map<ExecutionVertexID, String> locations = new HashMap<>();

	public FinalizeSnapshotsManager(ExecutionVertex[] tasksToCommitTo) {
		this.tasksToCommitTo = tasksToCommitTo;
	}

	public boolean addSnapshot(ExecutionVertexID executionVertexId, TaskStateSnapshot snapshot, String location) {
		LOG.info("add snapshot {} for {}", executionVertexId, snapshot);
		finalSnapshots.put(executionVertexId, snapshot);
		locations.put(executionVertexId, location);

		return finalSnapshots.size() == tasksToCommitTo.length;
	}

	public TaskStateSnapshot getSnapshot(ExecutionVertexID id) {
		return finalSnapshots.get(id);
	}

	public String getLocation(ExecutionVertexID id) {
		return locations.get(id);
	}

	public boolean isOk() {
		return finalSnapshots.size() == tasksToCommitTo.length;
	}
}
