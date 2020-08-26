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

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class FinalSnapshotManager {

	private final Logger LOG = LoggerFactory.getLogger(FinalSnapshotManager.class);

	private final Object lock = new Object();

	@GuardedBy("lock")
	private final Map<ExecutionVertexID, SnapshotAndLocation> finalSnapshots = new HashMap<>();

	private final Map<ExecutionVertexID, List<CompletableFuture<SnapshotAndLocation>>> pendingRetrieves = new HashMap<>();

	/**
	 * Registers the final snapshot for a execution vertex if not exists. We always keep the old one if exists.
	 * This is because the old one may have been used in some checkpoints.
	 *
	 * @param vertexId
	 * @param taskStateSnapshot
	 * @param location
	 * @throws Exception
	 */
	public void addFinalSnapshot(ExecutionVertexID vertexId, TaskStateSnapshot taskStateSnapshot, String location) {
		synchronized (lock) {
			SnapshotAndLocation existed = finalSnapshots.get(vertexId);
			if (existed == null) {
				finalSnapshots.put(vertexId, new SnapshotAndLocation(taskStateSnapshot, location));
			}
		}
	}

	Map<ExecutionVertexID, SnapshotAndLocation> getCurrentFinalSnapshots() {
		synchronized (lock) {
			Map<ExecutionVertexID, SnapshotAndLocation> clone = new HashMap<>();
			finalSnapshots.forEach((k, v) -> clone.put(k, v));
			return clone;
		}
	}

	CompletableFuture<SnapshotAndLocation> retrieveFinalSnapshot(ExecutionVertexID vertexId) {
		synchronized (lock) {
			SnapshotAndLocation existed = finalSnapshots.get(vertexId);
			if (existed != null) {
				return CompletableFuture.completedFuture(existed);
			}

			CompletableFuture<SnapshotAndLocation> result = new CompletableFuture<>();
			pendingRetrieves.compute(vertexId, (k, v) -> v == null ? new ArrayList<>() : v)
				.add(result);
			return result;
		}
	}

	public void onTaskRestarting(Set<ExecutionVertexID> verticesToRestart) throws Exception {
		synchronized (lock) {
			for (ExecutionVertexID executionVertexID : verticesToRestart) {
				SnapshotAndLocation snapshotAndLocation = finalSnapshots.remove(executionVertexID);

				if (snapshotAndLocation != null) {
					//TODO reference count
					snapshotAndLocation.snapshot.discardState();
				}

				if (pendingRetrieves.containsKey(executionVertexID)) {
					pendingRetrieves.get(executionVertexID)
						.forEach(future -> future.completeExceptionally(new Exception("Task restarted")));
				}
			}
		}
	}

	static class SnapshotAndLocation {
		final TaskStateSnapshot snapshot;
		final String location;

		public SnapshotAndLocation(TaskStateSnapshot snapshot, String location) {
			this.snapshot = snapshot;
			this.location = location;
		}
	}
}
