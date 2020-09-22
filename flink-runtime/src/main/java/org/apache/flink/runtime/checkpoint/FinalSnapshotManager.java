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

import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class FinalSnapshotManager {
	private final Logger LOG = LoggerFactory.getLogger(FinalSnapshotManager.class);

	private final Supplier<ScheduledExecutor> mainThreadExecutorSupplier;

	private final Runnable mainThreadChecker;

	private final Map<ExecutionVertexID, FinalSnapshot> activeFinalSnapshots = new HashMap<>();

	private final Map<ExecutionAttemptID, FinalSnapshot> waitReleasing = new HashMap<>();

	public FinalSnapshotManager(Supplier<ScheduledExecutor> mainThreadExecutorSupplier, Runnable mainThreadChecker) {
		this.mainThreadExecutorSupplier = checkNotNull(mainThreadExecutorSupplier);
		this.mainThreadChecker = checkNotNull(mainThreadChecker);
	}

	public void reportFinalSnapshot(
		ExecutionVertex executionVertex,
		ExecutionAttemptID attemptID,
		TaskStateSnapshot subtaskState,
		String taskManagerLocationInfo) {

		mainThreadChecker.run();

		LOG.info("Received final snapshot report for {}'s attempt {}",
			executionVertex.getTaskNameWithSubtaskIndex(),
			attemptID);

		FinalSnapshot finalSnapshot = new FinalSnapshot(
			executionVertex.getTaskNameWithSubtaskIndex(),
			attemptID,
			subtaskState,
			taskManagerLocationInfo);
		activeFinalSnapshots.put(executionVertex.getID(), finalSnapshot);
	}

	public void onTasksRestarting(Set<ExecutionVertexID> restartingTasks) throws Exception {
		mainThreadChecker.run();

		for (ExecutionVertexID vertexId : restartingTasks) {
			FinalSnapshot snapshot = activeFinalSnapshots.remove(vertexId);
			if (snapshot != null) {
				if (snapshot.referenceCount > 0) {
					waitReleasing.put(snapshot.attemptID, snapshot);
				} else {
					releaseFinalSnapshot(snapshot);
				}
			}
		}
	}

	public Map<ExecutionVertexID, FinalSnapshot> retrieveActiveFinalSnapshots() {
		activeFinalSnapshots.values().forEach(finalSnapshot -> finalSnapshot.referenceCount++);
		return new HashMap<>(activeFinalSnapshots);
	}

	private void dereferenceFinalSnapshot(FinalSnapshot finalSnapshot) {
		mainThreadExecutorSupplier.get().execute(() -> {
			finalSnapshot.referenceCount--;

			if (waitReleasing.containsKey(finalSnapshot.getAttemptID()) &&
				finalSnapshot.referenceCount <= 0) {

				try {
					releaseFinalSnapshot(finalSnapshot);
				} catch (Exception e) {
					LOG.warn("Failed to discard state for final snapshot {}", finalSnapshot.getAttemptID());
				}
			}
		});
	}

	private void releaseFinalSnapshot(FinalSnapshot finalSnapshot) throws Exception {
		finalSnapshot.subtaskState.discardState();
		waitReleasing.remove(finalSnapshot.getAttemptID());
	}

	public void shutdown() throws Exception {
		for (FinalSnapshot finalSnapshot : activeFinalSnapshots.values()) {
			releaseFinalSnapshot(finalSnapshot);
		}

		for (FinalSnapshot finalSnapshot : waitReleasing.values()) {
			releaseFinalSnapshot(finalSnapshot);
		}
	}

	public class FinalSnapshot {
		private final String taskNameWithIndex;
		private final ExecutionAttemptID attemptID;
		private final TaskStateSnapshot subtaskState;
		private final String taskManagerLocationInfo;

		private int referenceCount;

		public FinalSnapshot(
			String taskNameWithIndex,
			ExecutionAttemptID attemptID,
			TaskStateSnapshot subtaskState,
			String taskManagerLocationInfo) {

			this.taskNameWithIndex = taskNameWithIndex;
			this.attemptID = attemptID;
			this.subtaskState = subtaskState;
			this.taskManagerLocationInfo = taskManagerLocationInfo;
		}

		public ExecutionAttemptID getAttemptID() {
			return attemptID;
		}

		public TaskStateSnapshot getSubtaskState() {
			return subtaskState;
		}

		public String getTaskManagerLocationInfo() {
			return taskManagerLocationInfo;
		}

		public void release() {
			dereferenceFinalSnapshot(this);
		}

		@Override
		public String toString() {
			return "FinalSnapshot{" +
				"taskNameWithIndex='" + taskNameWithIndex + '\'' +
				'}';
		}
	}
}
