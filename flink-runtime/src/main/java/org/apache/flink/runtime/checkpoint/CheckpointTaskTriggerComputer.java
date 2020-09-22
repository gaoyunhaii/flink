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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

public class CheckpointTaskTriggerComputer {

	private final ExecutionGraph executionGraph;

	private final ScheduledExecutor mainThreadExecutor;

	public CheckpointTaskTriggerComputer(
		ExecutionGraph executionGraph,
		ScheduledExecutor mainThreadExecutor) {

		this.executionGraph = executionGraph;
		this.mainThreadExecutor = mainThreadExecutor;
	}

	public List<Execution> computeTasksToTrigger(Set<ExecutionVertexID> finishedTaskIDs) throws Exception {
		CompletableFuture<List<Execution>> resultFuture = new CompletableFuture<>();

		mainThreadExecutor.execute(() -> {
			List<Execution> tasksToTrigger = new ArrayList<>();
			Map<JobVertexID, VertexTaskCheckingStatus> vertexStatus = new HashMap<>();
			executionGraph.getVerticesTopologically().forEach(
				vertex -> vertexStatus.put(vertex.getJobVertexId(), new VertexTaskCheckingStatus()));

			for (ExecutionJobVertex vertex : executionGraph.getVerticesTopologically()) {
				VertexTaskCheckingStatus status = vertexStatus.get(vertex.getJobVertexId());

				List<ExecutionVertex> tasksToCheck = null;
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
					if (!finishedTaskIDs.contains(task.getID())) {
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

			resultFuture.complete(tasksToTrigger);
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
