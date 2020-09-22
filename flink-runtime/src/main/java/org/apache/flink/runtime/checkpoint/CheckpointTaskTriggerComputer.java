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
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class CheckpointTaskTriggerComputer {
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointTaskTriggerComputer.class);

	private final Iterable<ExecutionJobVertex> verticesInTopologicalOrder;

	private final FinalSnapshotManager finalSnapshotManager;

	private final Supplier<ScheduledExecutor> mainThreadExecutorSupplier;

	public CheckpointTaskTriggerComputer(
		Iterable<ExecutionJobVertex> verticesInTopologicalOrder,
		FinalSnapshotManager finalSnapshotManager,
		Supplier<ScheduledExecutor> mainThreadExecutorSupplier) {

		this.verticesInTopologicalOrder = checkNotNull(verticesInTopologicalOrder);
		this.finalSnapshotManager = checkNotNull(finalSnapshotManager);
		this.mainThreadExecutorSupplier = checkNotNull(mainThreadExecutorSupplier);
	}

	public TaskTriggerResult computeTasksToTrigger() throws Exception {
		CompletableFuture<TaskTriggerResult> resultFuture = new CompletableFuture<>();

		mainThreadExecutorSupplier.get().execute(() -> {
			long start = System.nanoTime();

			Map<ExecutionVertexID, FinalSnapshotManager.FinalSnapshot> finalSnapshots =
				finalSnapshotManager.retrieveActiveFinalSnapshots();

			List<Execution> tasksToTrigger = new ArrayList<>();
			Map<JobVertexID, VertexTaskCheckingStatus> vertexStatus = new HashMap<>();
			verticesInTopologicalOrder.forEach(
				vertex -> vertexStatus.put(vertex.getJobVertexId(), new VertexTaskCheckingStatus()));

			for (ExecutionJobVertex vertex : verticesInTopologicalOrder) {
				VertexTaskCheckingStatus status = vertexStatus.get(vertex.getJobVertexId());

				List<ExecutionVertex> tasksToCheck;
				if (status.getType() == VertexTaskCheckingType.ALL_TASKS_TO_CHECK) {
					tasksToCheck = Arrays.asList(vertex.getTaskVertices());
				} else if (status.getType() == VertexTaskCheckingType.SOME_TASKS_TO_CHECK) {
					tasksToCheck = status.getTasksToCheck()
						.stream()
						.map(taskId -> vertex.getTaskVertices()[taskId.getSubtaskIndex()])
						.collect(Collectors.toList());
				} else {
					tasksToCheck = new ArrayList<>();
				}

				// Checks the tasks to see if they are finished
				List<ExecutionVertexID> tasksCheckedFinished = new ArrayList<>();
				for (ExecutionVertex task : tasksToCheck) {
					if (!finalSnapshots.containsKey(task.getID())) {
						tasksToTrigger.add(task.getCurrentExecutionAttempt());
					} else {
						tasksCheckedFinished.add(task.getID());
					}
				}

				// Find all the job edges
				List<JobEdge> jobEdges = vertex
					.getJobVertex()
					.getProducedDataSets()
					.stream()
					.flatMap(dataSet -> dataSet.getConsumers().stream())
					.collect(Collectors.toList());

				for (JobEdge jobEdge : jobEdges) {
					VertexTaskCheckingStatus targetStatus = vertexStatus.get(jobEdge.getTarget().getID());
					if (targetStatus.getType() == VertexTaskCheckingType.NO_TASKS_TO_CHECK) {
						continue;
					}

					switch (jobEdge.getDistributionPattern()) {
						case ALL_TO_ALL:
							if (tasksCheckedFinished.size() < vertex.getTaskVertices().length) {
								targetStatus.markNoTasksToCheck();
							}
							break;
						case POINTWISE:
							Set<ExecutionVertexID> targetTasksToCheck = tasksCheckedFinished.stream()
								.flatMap(taskId -> getDescendants(vertex.getTaskVertices()[taskId.getSubtaskIndex()]).stream())
								.map(ExecutionVertex::getID)
								.collect(Collectors.toSet());
							targetStatus.markSomeTasksToCheck(targetTasksToCheck);
							break;
						default:
							throw new UnsupportedOperationException("Not supported type");
					}
				}
			}

			LOG.info("Computes tasks to trigger used {} ms, final snapshots used is {}, executions to trigger is {}",
				(System.nanoTime() - start) / 1e6,
				finalSnapshots,
				tasksToTrigger);

			resultFuture.complete(new TaskTriggerResult(tasksToTrigger, finalSnapshots));
		});

		return resultFuture.get();
	}

	private List<ExecutionVertex> getDescendants(ExecutionVertex task) {
		return task.getProducedPartitions().values().stream().flatMap(partition ->
			partition.getConsumers()
				.stream()
				.flatMap(Collection::stream)
				.map(ExecutionEdge::getTarget))
			.collect(Collectors.toList());
	}

	private enum VertexTaskCheckingType {
		ALL_TASKS_TO_CHECK,
		SOME_TASKS_TO_CHECK,
		NO_TASKS_TO_CHECK
	}

	public static class TaskTriggerResult {
		private final List<Execution> tasksToTrigger;

		private final Map<ExecutionVertexID, FinalSnapshotManager.FinalSnapshot> finalSnapshotsUsed;

		public TaskTriggerResult(
			List<Execution> tasksToTrigger,
			Map<ExecutionVertexID, FinalSnapshotManager.FinalSnapshot> finalSnapshotsUsed) {

			this.tasksToTrigger = tasksToTrigger;
			this.finalSnapshotsUsed = finalSnapshotsUsed;
		}

		public List<Execution> getTasksToTrigger() {
			return tasksToTrigger;
		}

		public Map<ExecutionVertexID, FinalSnapshotManager.FinalSnapshot> getFinalSnapshotsUsed() {
			return finalSnapshotsUsed;
		}
	}

	private static class VertexTaskCheckingStatus {

		private VertexTaskCheckingType type;

		private Set<ExecutionVertexID> tasksToCheck;

		public VertexTaskCheckingStatus() {
			this.type = VertexTaskCheckingType.ALL_TASKS_TO_CHECK;
		}

		public void markNoTasksToCheck() {
			type = VertexTaskCheckingType.NO_TASKS_TO_CHECK;
			tasksToCheck = null;
		}

		public void markSomeTasksToCheck(Set<ExecutionVertexID> precedentResult) {
			checkState(type == VertexTaskCheckingType.ALL_TASKS_TO_CHECK ||
					type == VertexTaskCheckingType.SOME_TASKS_TO_CHECK,
				"The state could not change in reverse order.");

			if (type == VertexTaskCheckingType.ALL_TASKS_TO_CHECK) {
				type = VertexTaskCheckingType.NO_TASKS_TO_CHECK;
				tasksToCheck = precedentResult;
			} else if (type == VertexTaskCheckingType.SOME_TASKS_TO_CHECK) {
				checkState(tasksToCheck != null);
				tasksToCheck.removeIf(task -> !precedentResult.contains(task));
			}
		}

		public VertexTaskCheckingType getType() {
			return type;
		}

		public Set<ExecutionVertexID> getTasksToCheck() {
			return tasksToCheck;
		}
	}
}
