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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Computes the tasks to trigger, ack or commit for each checkpoint.
 */
public class CheckpointBriefComputer {
	private static final Logger LOG = LoggerFactory.getLogger(CheckpointBriefComputer.class);

	private final JobID jobId;

	private final List<ExecutionJobVertex> jobVerticesInTopologyOrder = new ArrayList<>();

	private final List<ExecutionVertex> allTasks = new ArrayList<>();

	private final Supplier<ScheduledExecutor> mainThreadExecutorSupplier;

	public CheckpointBriefComputer(
		JobID jobId,
		Iterable<ExecutionJobVertex> jobVerticesInTopologyOrderIterable,
		Supplier<ScheduledExecutor> mainThreadExecutorSupplier) {

		this.jobId = jobId;
		jobVerticesInTopologyOrderIterable.forEach(jobVerticesInTopologyOrder::add);
		jobVerticesInTopologyOrder.forEach(vertex -> allTasks.addAll(Arrays.asList(vertex.getTaskVertices())));
		this.mainThreadExecutorSupplier = mainThreadExecutorSupplier;
	}

	public CompletableFuture<CheckpointBrief> computeCheckpointBrief() throws CheckpointException {
		CompletableFuture<CheckpointBrief> resultFuture = new CompletableFuture<>();

		mainThreadExecutorSupplier.get().execute(() -> {
			// Let's first compute which execution vertex is finished yet.
			// The ExecutionGraph might not be accurate, since for job like A -> B -> C,
			// It is possible that C first notify JM about finished.
			// Since if a vertex is finished, all its precedent tasks should be finished,
			// We use this logic to refine the finished state...
			Set<ExecutionVertexID> allFinishedVertices = findFinishedExecutionVertex();

			List<Execution> tasksToTrigger = new ArrayList<>();
			Map<ExecutionAttemptID, ExecutionVertex> tasksToAck = initTasksToAck();
			List<ExecutionVertex> finishedTasks = new ArrayList<>();
			Map<OperatorID, ExecutionJobVertex> fullyFinishedOperators = new HashMap<>();

			Map<JobVertexID, VertexTaskCheckingStatus> vertexStatus = new HashMap<>();
			jobVerticesInTopologyOrder.forEach(
				vertex -> vertexStatus.put(vertex.getJobVertexId(), new VertexTaskCheckingStatus()));

			for (ExecutionJobVertex vertex : jobVerticesInTopologyOrder) {
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
					tasksToCheck = Collections.emptyList();
				}

				// Checks the tasks to see if they are finished
				List<ExecutionVertex> tasksCheckedFinished = new ArrayList<>();
				for (ExecutionVertex task : tasksToCheck) {
					if (!allFinishedVertices.contains(task.getID())) {
						tasksToTrigger.add(task.getCurrentExecutionAttempt());
					} else {
						tasksCheckedFinished.add(task);
					}
				}

				// Now let's see if it violates the current limitation on sources
				// It is not very good that we have to check the source operator instead of the source task
				if (vertex.getJobVertex().isHasLegacySourceOperators()
					&& tasksCheckedFinished.size() > 0
					&& tasksCheckedFinished.size() < vertex.getTaskVertices().length) {

					checkState(vertex.getJobVertex().isInputVertex());

					resultFuture.completeExceptionally(new CheckpointException(
						CheckpointFailureReason.SOME_LEGACY_SOURCE_TASKS_PARTIALLY_FINISHED));
					return;
				} else if (vertex.getJobVertex().isInputVertex()
					&& tasksCheckedFinished.size() > 0
					&& tasksCheckedFinished.size() < vertex.getTaskVertices().length) {

					LOG.warn("Vertex {} {}/{} finished, be careful.", vertex.getName(), tasksCheckedFinished.size(), vertex.getTaskVertices().length);
				}

				// Update the results
				tasksCheckedFinished.forEach(task -> {
					finishedTasks.add(task);
					tasksToAck.remove(task.getCurrentExecutionAttempt().getAttemptId());
				});

				// If not all tasks are finished, then we could update the states for the descendant vertices
				if (tasksCheckedFinished.size() < vertex.getTaskVertices().length) {
					// Find all the job edges
					List<JobEdge> jobEdges = getAllJobEdges(vertex);
					for (JobEdge jobEdge : jobEdges) {
						VertexTaskCheckingStatus targetStatus = vertexStatus.get(jobEdge.getTarget().getID());
						if (targetStatus.getType() == VertexTaskCheckingType.NO_TASKS_TO_CHECK) {
							continue;
						}

						switch (jobEdge.getDistributionPattern()) {
							case ALL_TO_ALL:
								targetStatus.markNoTasksToCheck();
								break;
							case POINTWISE:
								Set<ExecutionVertexID> targetTasksToCheck = tasksCheckedFinished.stream()
									.flatMap(task -> getDescendants(task).stream())
									.map(ExecutionVertex::getID)
									.collect(Collectors.toSet());
								targetStatus.markSomeTasksToCheck(targetTasksToCheck);
								break;
							default:
								throw new UnsupportedOperationException("Not supported type");
						}
					}
				} else {
					for (OperatorIDPair idPair : vertex.getOperatorIDs()) {
						fullyFinishedOperators.put(idPair.getGeneratedOperatorID(), vertex);
					}
				}
			}

			resultFuture.complete(new CheckpointBrief(
				tasksToTrigger,
				tasksToAck,
				tasksToAck.size() == allTasks.size() ? allTasks : new ArrayList<>(tasksToAck.values()),
				finishedTasks,
				fullyFinishedOperators));
		});

		return resultFuture;
	}

	@VisibleForTesting
	Set<ExecutionVertexID> findFinishedExecutionVertex() {
		Set<ExecutionVertexID> allFinishedVertices = new HashSet<>();

		ListIterator<ExecutionJobVertex> jobVertexIterator = jobVerticesInTopologyOrder.listIterator(jobVerticesInTopologyOrder.size());
		while (jobVertexIterator.hasPrevious()) {
			ExecutionJobVertex jobVertex = jobVertexIterator.previous();

			// See if the job vertex has finished execution vertex
			List<ExecutionVertex> finishedVertex =
				Arrays.stream(jobVertex.getTaskVertices())
					.filter(v -> v.getCurrentExecutionAttempt().isFinished() || allFinishedVertices.contains(v.getID()))
					.collect(Collectors.toList());

			if (finishedVertex.size() == 0) {
				continue;
			}

			finishedVertex.forEach(v -> allFinishedVertices.add(v.getID()));

			for (JobEdge jobEdge : jobVertex.getJobVertex().getInputs()) {
				ExecutionJobVertex source = jobVertex.getGraph().getJobVertex(jobEdge.getSource().getProducer().getID());
				checkState(source != null);

				switch (jobEdge.getDistributionPattern()) {
					case ALL_TO_ALL:
						for (ExecutionVertex sourceTask : source.getTaskVertices()) {
							allFinishedVertices.add(sourceTask.getID());
						}
						break;
					case POINTWISE:
						finishedVertex.forEach(v -> getPrecedent(v).forEach(pv -> allFinishedVertices.add(pv.getID())));
						break;
					default:
						throw new UnsupportedOperationException("Not supported type");
				}
			}
		}

		return allFinishedVertices;
	}

	/**
	 * The brief of the situation for a checkpoint.
	 */
	public static class CheckpointBrief {

		private final List<Execution> tasksToTrigger;

		private final Map<ExecutionAttemptID, ExecutionVertex> tasksToAck;

		private final List<ExecutionVertex> runningTasks;

		private final List<ExecutionVertex> finishedTasks;

		private final Map<OperatorID, ExecutionJobVertex> fullyFinishedOperators;

		public CheckpointBrief(
			List<Execution> tasksToTrigger,
			Map<ExecutionAttemptID, ExecutionVertex> tasksToAck,
			List<ExecutionVertex> runningTasks,
			List<ExecutionVertex> finishedTasks,
			Map<OperatorID, ExecutionJobVertex> fullyFinishedOperators) {

			this.tasksToTrigger = checkNotNull(tasksToTrigger);
			this.tasksToAck = checkNotNull(tasksToAck);
			this.runningTasks = checkNotNull(runningTasks);
			this.finishedTasks = checkNotNull(finishedTasks);
			this.fullyFinishedOperators = checkNotNull(fullyFinishedOperators);
		}

		public List<Execution> getTasksToTrigger() {
			return tasksToTrigger;
		}

		public Map<ExecutionAttemptID, ExecutionVertex> getTasksToAck() {
			return tasksToAck;
		}

		public List<ExecutionVertex> getRunningTasks() {
			return runningTasks;
		}

		public List<ExecutionVertex> getFinishedTasks() {
			return finishedTasks;
		}

		public Map<OperatorID, ExecutionJobVertex> getFullyFinishedOperators() {
			return fullyFinishedOperators;
		}

		@Override
		public String toString() {
			return "CheckpointBrief{" +
				"tasksToTrigger=" + tasksToTrigger.size() +
				", tasksToAck=" + tasksToAck.size() +
				", runningTasks=" + runningTasks.size() +
				", finishedTasks=" + finishedTasks.size() +
				", fullyFinishedOperators =" + fullyFinishedOperators.size() +
				'}';
		}
	}

	private Map<ExecutionAttemptID, ExecutionVertex> initTasksToAck() {
		Map<ExecutionAttemptID, ExecutionVertex> tasksToAck = new HashMap<>(allTasks.size());
		allTasks.forEach(task -> tasksToAck.put(task.getCurrentExecutionAttempt().getAttemptId(), task));
		return tasksToAck;
	}

	private List<JobEdge> getAllJobEdges(ExecutionJobVertex vertex) {
		return vertex
			.getJobVertex()
			.getProducedDataSets()
			.stream()
			.flatMap(dataSet -> dataSet.getConsumers().stream())
			.collect(Collectors.toList());
	}

	private List<ExecutionVertex> getDescendants(ExecutionVertex task) {
		return task.getProducedPartitions().values().stream().flatMap(partition ->
			partition.getConsumers()
				.stream()
				.flatMap(Collection::stream)
				.map(ExecutionEdge::getTarget))
			.collect(Collectors.toList());
	}

	private List<ExecutionVertex> getPrecedent(ExecutionVertex task) {
		return Arrays.stream(task.getAllInputEdges())
			.flatMap(Arrays::stream)
			.map(edge -> edge.getSource().getProducer())
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
				type = VertexTaskCheckingType.SOME_TASKS_TO_CHECK;
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
