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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Computes the tasks to trigger, wait or commit for each checkpoint. */
public class CheckpointBriefComputer {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointBriefComputer.class);

    private final JobID jobId;

    private final CheckpointBriefComputerContext context;

    private final List<ExecutionJobVertex> jobVerticesInTopologyOrder = new ArrayList<>();

    private final List<ExecutionVertex> allTasks = new ArrayList<>();

    private final List<ExecutionVertex> sourceTasks = new ArrayList<>();

    public CheckpointBriefComputer(
            JobID jobId,
            CheckpointBriefComputerContext context,
            Iterable<ExecutionJobVertex> jobVerticesInTopologyOrderIterable) {

        this.jobId = jobId;
        this.context = checkNotNull(context);

        checkNotNull(jobVerticesInTopologyOrderIterable);
        jobVerticesInTopologyOrderIterable.forEach(
                jobVertex -> {
                    jobVerticesInTopologyOrder.add(jobVertex);
                    allTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));

                    if (jobVertex.getJobVertex().isInputVertex()) {
                        sourceTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));
                    }
                });
    }

    public CompletableFuture<CheckpointBrief> computeCheckpointBrief() {
        CompletableFuture<CheckpointBrief> resultFuture = new CompletableFuture<>();

        context.getMainExecutor()
                .execute(
                        () -> {
                            try {
                                if (!isAllExecutionAttemptsAreInitiated()) {
                                    throw new CheckpointException(
                                            CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
                                }

                                CheckpointBrief result;
                                if (!context.hasFinishedTasks()) {
                                    result = computeWithAllTasksRunning();
                                } else {
                                    result = computeAfterTasksFinished();
                                }

                                if (!isAllExecutionsToTriggerStarted(result.getTasksToTrigger())) {
                                    throw new CheckpointException(
                                            CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
                                }

                                resultFuture.complete(computeAfterTasksFinished());
                            } catch (Throwable throwable) {
                                resultFuture.completeExceptionally(throwable);
                            }
                        });

        return resultFuture;
    }

    private boolean isAllExecutionAttemptsAreInitiated() {
        for (ExecutionVertex task : allTasks) {
            if (task.getCurrentExecutionAttempt() == null) {
                LOG.info(
                        "task {} of job {} is not being executed at the moment. Aborting checkpoint.",
                        task.getTaskNameWithSubtaskIndex(),
                        jobId);
                return false;
            }
        }

        return true;
    }

    private boolean isAllExecutionsToTriggerStarted(List<Execution> toTrigger) {
        for (Execution execution : toTrigger) {
            if (execution.getState() == ExecutionState.CREATED
                    || execution.getState() == ExecutionState.SCHEDULED
                    || execution.getState() == ExecutionState.DEPLOYING) {

                LOG.info(
                        "Checkpoint triggering task {} of job {} has not being executed at the moment. "
                                + "Aborting checkpoint.",
                        execution.getVertex().getTaskNameWithSubtaskIndex(),
                        jobId);
                return false;
            }
        }

        return true;
    }

    /**
     * Computes the checkpoint brief when all tasks are running. It would simply marks all the
     * source tasks as need to trigger and all the tasks as need to wait and commit.
     *
     * @return The brief of this checkpoint.
     */
    private CheckpointBrief computeWithAllTasksRunning() {
        List<Execution> executionsToTrigger =
                sourceTasks.stream()
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toList());

        Map<ExecutionAttemptID, ExecutionVertex> ackTasks = createTaskToAck(allTasks);

        return new CheckpointBrief(
                Collections.unmodifiableList(executionsToTrigger),
                ackTasks,
                Collections.unmodifiableList(allTasks),
                Collections.emptyList(),
                Collections.emptyList());
    }

    /**
     * Computes the checkpoint brief after some tasks have finished. Due to the problem of the
     * report order, we have to first compute the accurate running tasks. Then we would iterate the
     * job graph in topological order, and for each job vertex, we would narrow down the set of
     * tasks need to trigger by removing the descendant tasks connected from running tasks of this
     * vertex.
     *
     * @return The brief of this checkpoint.
     */
    private CheckpointBrief computeAfterTasksFinished() {
        Map<JobVertexID, JobVertexTaskSet> runningTasks = computeRunningTasks();

        List<Execution> tasksToTrigger = new ArrayList<>();
        // We would first have a full set of tasks to ack and then remove the finished tasks
        // during the iteration.
        Map<ExecutionAttemptID, ExecutionVertex> tasksToAck = createTaskToAck(allTasks);
        List<ExecutionVertex> finishedTasks = new ArrayList<>();
        List<ExecutionJobVertex> fullyFinishedJobVertex = new ArrayList<>();

        Map<JobVertexID, JobVertexTaskSet> possibleTasksToTrigger = createTaskSetsWithAllTasks();

        for (ExecutionJobVertex jobVertex : jobVerticesInTopologyOrder) {
            JobVertexTaskSet jobVertexRunningTasks = runningTasks.get(jobVertex.getJobVertexId());

            JobVertexTaskSet jobVertexPossibleTasksToTrigger =
                    possibleTasksToTrigger.get(jobVertex.getJobVertexId());
            List<ExecutionVertex> jobVertexFinishedTask = new ArrayList<>();
            jobVertexPossibleTasksToTrigger.forEachTask(
                    task -> {
                        if (jobVertexRunningTasks.contains(task.getID())) {
                            tasksToTrigger.add(task.getCurrentExecutionAttempt());
                        } else {
                            jobVertexFinishedTask.add(task);
                            finishedTasks.add(task);
                            tasksToAck.remove(task.getCurrentExecutionAttempt().getAttemptId());
                        }
                    });

            // If not all tasks of this tasks are finished, then we could narrow down the possible
            // tasks to trigger for the following job vertex:
            // 1. For the job vertices connected with ALL_TO_ALL edges, none of their tasks need
            //    to trigger since they have precedent running tasks in the current job vertex.
            // 2. For the job vertices connected with POINTWISE edges, only the descendant tasks
            //    connected from the finished tasks of this job vertex might need to trigger.
            if (jobVertexFinishedTask.size() < jobVertex.getTaskVertices().length) {
                List<JobEdge> jobEdges = getOutputJobEdges(jobVertex);
                for (JobEdge jobEdge : jobEdges) {
                    JobVertexTaskSet targetVertexTasksToCheck =
                            possibleTasksToTrigger.get(jobEdge.getTarget().getID());

                    if (targetVertexTasksToCheck.containsNoTasks()) {
                        continue;
                    }

                    switch (jobEdge.getDistributionPattern()) {
                        case ALL_TO_ALL:
                            targetVertexTasksToCheck.marksNoTasks();
                            break;
                        case POINTWISE:
                            Set<ExecutionVertexID> targetTasksToCheck =
                                    jobVertexFinishedTask.stream()
                                            .flatMap(
                                                    task ->
                                                            getDescendantTasks(task, jobEdge)
                                                                    .stream())
                                            .map(ExecutionVertex::getID)
                                            .collect(Collectors.toSet());
                            targetVertexTasksToCheck.intersect(targetTasksToCheck);
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    "Not supported distribution pattern");
                    }
                }
            } else {
                fullyFinishedJobVertex.add(jobVertex);
            }
        }

        return new CheckpointBrief(
                Collections.unmodifiableList(tasksToTrigger),
                tasksToAck,
                Collections.unmodifiableList(
                        tasksToAck.size() == allTasks.size()
                                ? allTasks
                                : new ArrayList<>(tasksToAck.values())),
                Collections.unmodifiableList(finishedTasks),
                Collections.unmodifiableList(fullyFinishedJobVertex));
    }

    /**
     * Compute the accurate running tasks for each job vertex. Currently if multiple tasks all
     * finished in short period, the order of their reports of FINISHED is nondeterministic, and
     * some tasks may report FINISHED before all its precedent tasks have.
     *
     * <p>To overcome this issue we would iterates the graph first to acquire the accurate running
     * tasks. We would iterate the job graph in reverse topological order, and for each job vertex,
     * we would remove those precedent tasks that connected to finished tasks of this job vertex
     * from possibly running tasks.
     *
     * @return An accurate set of running tasks for each job vertex.
     */
    @VisibleForTesting
    Map<JobVertexID, JobVertexTaskSet> computeRunningTasks() {
        Map<JobVertexID, JobVertexTaskSet> possibleRunningTasks = createTaskSetsWithAllTasks();

        ListIterator<ExecutionJobVertex> jobVertexIterator =
                jobVerticesInTopologyOrder.listIterator(jobVerticesInTopologyOrder.size());
        while (jobVertexIterator.hasPrevious()) {
            ExecutionJobVertex jobVertex = jobVertexIterator.previous();
            JobVertexTaskSet jobVertexPossibleRunningTasks =
                    possibleRunningTasks.get(jobVertex.getJobVertexId());

            // Checks the actual task state and update the result into the task set.
            Set<ExecutionVertexID> jobVertexRunningTasks = new HashSet<>();
            jobVertexPossibleRunningTasks.forEachTask(
                    task -> {
                        if (!task.getCurrentExecutionAttempt().isFinished()) {
                            jobVertexRunningTasks.add(task.getID());
                        }
                    });
            jobVertexPossibleRunningTasks.intersect(jobVertexRunningTasks);

            // If not all tasks of this vertex are running, then we could narrow
            // down the set of possible running tasks of its precedent job vertices:
            // 1. For precedent ALL_TO_ALL edges, the precedent job vertices must contains only
            //    finished tasks.
            // 2. For precedent POINTWISE edges, only the precedent tasks connected to the running
            //    tasks of this job vertex are possibly still running.
            if (!jobVertexPossibleRunningTasks.containsAllTasks()) {
                for (int index = 0; index < jobVertex.getJobVertex().getInputs().size(); ++index) {
                    JobEdge jobEdge = jobVertex.getJobVertex().getInputs().get(index);
                    JobVertexTaskSet precedentVertexPossibleRunningTasks =
                            possibleRunningTasks.get(jobEdge.getSource().getProducer().getID());

                    if (precedentVertexPossibleRunningTasks.containsNoTasks()) {
                        continue;
                    }

                    switch (jobEdge.getDistributionPattern()) {
                        case ALL_TO_ALL:
                            precedentVertexPossibleRunningTasks.marksNoTasks();
                            break;
                        case POINTWISE:
                            int finalIndex = index;
                            Set<ExecutionVertexID> targetPossibleRunningTasks =
                                    jobVertexRunningTasks.stream()
                                            .map(
                                                    id ->
                                                            jobVertex
                                                                    .getTaskVertices()[
                                                                    id.getSubtaskIndex()])
                                            .flatMap(
                                                    task ->
                                                            getPrecedentTasks(task, finalIndex)
                                                                    .stream())
                                            .map(ExecutionVertex::getID)
                                            .collect(Collectors.toSet());

                            precedentVertexPossibleRunningTasks.intersect(
                                    targetPossibleRunningTasks);
                            break;
                        default:
                            throw new UnsupportedOperationException(
                                    "Not supported distribution pattern");
                    }
                }
            }
        }

        return possibleRunningTasks;
    }

    private Map<JobVertexID, JobVertexTaskSet> createTaskSetsWithAllTasks() {
        Map<JobVertexID, JobVertexTaskSet> taskSets = new HashMap<>();
        jobVerticesInTopologyOrder.forEach(
                jobVertex ->
                        taskSets.put(jobVertex.getJobVertexId(), new JobVertexTaskSet(jobVertex)));
        return taskSets;
    }

    private List<JobEdge> getOutputJobEdges(ExecutionJobVertex vertex) {
        return vertex.getJobVertex().getProducedDataSets().stream()
                .flatMap(dataSet -> dataSet.getConsumers().stream())
                .collect(Collectors.toList());
    }

    private List<ExecutionVertex> getDescendantTasks(ExecutionVertex task, JobEdge jobEdge) {
        return task.getProducedPartitions()
                .get(
                        new IntermediateResultPartitionID(
                                jobEdge.getSourceId(), task.getParallelSubtaskIndex()))
                .getConsumers().stream()
                .flatMap(Collection::stream)
                .map(ExecutionEdge::getTarget)
                .collect(Collectors.toList());
    }

    private List<ExecutionVertex> getPrecedentTasks(ExecutionVertex task, int index) {
        return Arrays.stream(task.getInputEdges(index))
                .map(edge -> edge.getSource().getProducer())
                .collect(Collectors.toList());
    }

    private Map<ExecutionAttemptID, ExecutionVertex> createTaskToAck(List<ExecutionVertex> tasks) {
        Map<ExecutionAttemptID, ExecutionVertex> tasksToAck = new HashMap<>(tasks.size());
        tasks.forEach(
                task -> tasksToAck.put(task.getCurrentExecutionAttempt().getAttemptId(), task));
        return tasksToAck;
    }

    /**
     * An optimized representation for a set of tasks belonging to a single job vertex and need to
     * check during iteration of execution graph for some purpose. If all tasks or no tasks are in
     * this set, it would only stores a type flag instead of the detailed list of tasks.
     */
    @VisibleForTesting
    static class JobVertexTaskSet {

        private final ExecutionJobVertex jobVertex;

        private TaskSetType type;

        private Set<ExecutionVertexID> tasks;

        public JobVertexTaskSet(ExecutionJobVertex jobVertex) {
            this.jobVertex = checkNotNull(jobVertex);
            type = TaskSetType.ALL_TASKS;
        }

        public void intersect(Set<ExecutionVertexID> otherTasks) {
            otherTasks.forEach(
                    taskId -> {
                        checkState(taskId.getJobVertexId().equals(jobVertex.getJobVertexId()));
                    });

            if (type == TaskSetType.NO_TASKS) {
                return;
            } else if (type == TaskSetType.ALL_TASKS
                    && otherTasks.size() < jobVertex.getTaskVertices().length) {
                type = TaskSetType.SOME_TASKS;
                tasks = new HashSet<>(otherTasks);
            } else if (type == TaskSetType.SOME_TASKS) {
                checkState(tasks != null);
                tasks.removeIf(task -> !otherTasks.contains(task));
            }

            if (type == TaskSetType.SOME_TASKS && tasks.size() == 0) {
                marksNoTasks();
            }
        }

        public void marksNoTasks() {
            type = TaskSetType.NO_TASKS;
            tasks = null;
        }

        public boolean contains(ExecutionVertexID taskId) {
            if (!taskId.getJobVertexId().equals(jobVertex.getJobVertexId())) {
                return false;
            }

            return type == TaskSetType.ALL_TASKS
                    || (type == TaskSetType.SOME_TASKS && tasks.contains(taskId));
        }

        public void forEachTask(Consumer<ExecutionVertex> consumer) {
            if (type == TaskSetType.ALL_TASKS) {
                Arrays.stream(jobVertex.getTaskVertices()).forEach(consumer);
            } else if (type == TaskSetType.SOME_TASKS) {
                tasks.stream()
                        .map(id -> jobVertex.getTaskVertices()[id.getSubtaskIndex()])
                        .forEach(consumer);
            }
        }

        public boolean containsAllTasks() {
            return type == TaskSetType.ALL_TASKS;
        }

        public boolean containsNoTasks() {
            return type == TaskSetType.NO_TASKS;
        }

        @VisibleForTesting
        public ExecutionJobVertex getJobVertex() {
            return jobVertex;
        }
    }

    private enum TaskSetType {
        ALL_TASKS,
        SOME_TASKS,
        NO_TASKS
    }
}
