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
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Computes the tasks to trigger, wait or commit for each checkpoint. */
public class CheckpointPlanCalculator {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointPlanCalculator.class);

    private final JobID jobId;

    private final CheckpointPlanCalculatorContext context;

    private final ArrayList<ExecutionJobVertex> jobVerticesInTopologyOrder = new ArrayList<>();

    private final List<ExecutionVertex> allTasks = new ArrayList<>();

    private final List<ExecutionVertex> sourceTasks = new ArrayList<>();

    private final Map<JobVertexID, VertexOutEdgeIndex> outEdgeIndex = new HashMap<>();

    public CheckpointPlanCalculator(
            JobID jobId,
            CheckpointPlanCalculatorContext context,
            Iterable<ExecutionJobVertex> jobVerticesInTopologyOrderIterable) {

        this.jobId = checkNotNull(jobId);
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

        jobVerticesInTopologyOrder.forEach(
                vertex -> {
                    outEdgeIndex.put(vertex.getJobVertexId(), new VertexOutEdgeIndex(vertex));
                });
    }

    public CompletableFuture<CheckpointPlan> calculateCheckpointPlan() {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        checkAllTasksInitiated();

                        CheckpointPlan result =
                                context.hasFinishedTasks()
                                        ? calculateAfterTasksFinished()
                                        : calculateWithAllTasksRunning();

                        checkTasksStarted(result.getTasksToTrigger());

                        return result;
                    } catch (Throwable throwable) {
                        throw new CompletionException(throwable);
                    }
                },
                context.getMainExecutor());
    }

    /**
     * Checks if all tasks are attached with the current Execution already. This method should be
     * called from JobMaster main thread executor.
     *
     * @throws CheckpointException if some tasks do not have attached Execution.
     */
    private void checkAllTasksInitiated() throws CheckpointException {
        for (ExecutionVertex task : allTasks) {
            if (task.getCurrentExecutionAttempt() == null) {
                throw new CheckpointException(
                        String.format(
                                "task %s of job %s is not being executed at the moment. Aborting checkpoint.",
                                task.getTaskNameWithSubtaskIndex(), jobId),
                        CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
            }
        }
    }

    /**
     * Checks if all tasks to trigger have already been in RUNNING state. This method should be
     * called from JobMaster main thread executor.
     *
     * @throws CheckpointException if some tasks to trigger have not turned into RUNNING yet.
     */
    private void checkTasksStarted(List<Execution> toTrigger) throws CheckpointException {
        for (Execution execution : toTrigger) {
            if (execution.getState() == ExecutionState.CREATED
                    || execution.getState() == ExecutionState.SCHEDULED
                    || execution.getState() == ExecutionState.DEPLOYING) {

                throw new CheckpointException(
                        String.format(
                                "Checkpoint triggering task %s of job %s has not being executed at the moment. "
                                        + "Aborting checkpoint.",
                                execution.getVertex().getTaskNameWithSubtaskIndex(), jobId),
                        CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
            }
        }
    }

    /**
     * Computes the checkpoint plan when all tasks are running. It would simply marks all the source
     * tasks as need to trigger and all the tasks as need to wait and commit.
     *
     * @return The plan of this checkpoint.
     */
    private CheckpointPlan calculateWithAllTasksRunning() {
        List<Execution> executionsToTrigger =
                sourceTasks.stream()
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toList());

        Map<ExecutionAttemptID, ExecutionVertex> ackTasks = createTaskToAck(allTasks);

        return new CheckpointPlan(
                Collections.unmodifiableList(executionsToTrigger),
                Collections.unmodifiableMap(ackTasks),
                Collections.unmodifiableList(allTasks),
                Collections.emptyList(),
                Collections.emptyList());
    }

    /**
     * Computes the checkpoint plan after some tasks have finished. Due to the problem of the order
     * of reporting FINISHED is nondeterministic, we have to first compute the accurate running
     * tasks. Then we would iterate the job graph to find the task that is still running, but do not
     * has precedent running tasks.
     *
     * @return The plan of this checkpoint.
     */
    private CheckpointPlan calculateAfterTasksFinished() {
        Map<JobVertexID, JobVertexTaskSet> runningTasksByVertex = calculateRunningTasks();

        List<Execution> tasksToTrigger = new ArrayList<>();

        Map<ExecutionAttemptID, ExecutionVertex> tasksToAck = new HashMap<>();
        List<Execution> finishedTasks = new ArrayList<>();
        List<ExecutionJobVertex> fullyFinishedJobVertex = new ArrayList<>();

        for (ExecutionJobVertex jobVertex : jobVerticesInTopologyOrder) {
            JobVertexTaskSet runningTasks = runningTasksByVertex.get(jobVertex.getJobVertexId());

            if (runningTasks.containsNoTasks()) {
                fullyFinishedJobVertex.add(jobVertex);
                Arrays.stream(jobVertex.getTaskVertices())
                        .forEach(task -> finishedTasks.add(task.getCurrentExecutionAttempt()));
                continue;
            }

            List<JobEdge> prevJobEdges = jobVertex.getJobVertex().getInputs();

            // this is an optimization: we determine at the JobVertex level if some tasks can even
            // be eligible for being in the "triggerTo" set
            boolean someTasksMustBeTriggered =
                    someTasksMustBeTriggered(runningTasksByVertex, runningTasks, prevJobEdges);

            for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
                if (runningTasks.contains(vertex.getID())) {
                    tasksToAck.put(vertex.getCurrentExecutionAttempt().getAttemptId(), vertex);

                    if (someTasksMustBeTriggered) {
                        boolean hasRunningPrecedentTasks =
                                hasRunningPrecedentTasks(
                                        vertex, runningTasksByVertex, prevJobEdges);

                        if (!hasRunningPrecedentTasks) {
                            tasksToTrigger.add(vertex.getCurrentExecutionAttempt());
                        }
                    }
                } else {
                    finishedTasks.add(vertex.getCurrentExecutionAttempt());
                }
            }
        }

        return new CheckpointPlan(
                Collections.unmodifiableList(tasksToTrigger),
                Collections.unmodifiableMap(tasksToAck),
                Collections.unmodifiableList(
                        tasksToAck.size() == allTasks.size()
                                ? allTasks
                                : new ArrayList<>(tasksToAck.values())),
                Collections.unmodifiableList(finishedTasks),
                Collections.unmodifiableList(fullyFinishedJobVertex));
    }

    private boolean hasRunningPrecedentTasks(
            ExecutionVertex task,
            Map<JobVertexID, JobVertexTaskSet> runningTasksByVertex,
            List<JobEdge> prevJobEdges) {

        for (int i = 0; i < prevJobEdges.size(); ++i) {
            if (prevJobEdges.get(i).getDistributionPattern() == DistributionPattern.POINTWISE) {
                JobVertexTaskSet sourceRunningTasks =
                        runningTasksByVertex.get(
                                prevJobEdges.get(i).getSource().getProducer().getID());
                if (hasRunningPrecedentTasksViaEdge(task, i, sourceRunningTasks)) {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean hasRunningPrecedentTasksViaEdge(
            ExecutionVertex task, int index, JobVertexTaskSet sourceRunningTasks) {
        for (ExecutionEdge edge : task.getInputEdges(index)) {
            if (sourceRunningTasks.contains(edge.getSource().getProducer().getID())) {
                return true;
            }
        }

        return false;
    }

    private boolean someTasksMustBeTriggered(
            Map<JobVertexID, JobVertexTaskSet> runningTasksByVertex,
            JobVertexTaskSet runningTasks,
            List<JobEdge> prevJobEdges) {
        if (runningTasks.containsNoTasks()) {
            // if this task doesn't have any running tasks, we cannot trigger anything
            return false;
        }
        for (JobEdge jobEdge : prevJobEdges) {
            DistributionPattern distributionPattern = jobEdge.getDistributionPattern();
            JobVertexTaskSet upstreamRunningTasks =
                    runningTasksByVertex.get(jobEdge.getSource().getProducer().getID());

            if (hasActiveUpstream(distributionPattern, upstreamRunningTasks)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Every task must have active upstream tasks if
     *
     * <ol>
     *   <li>ALL_TO_ALL connection and some predecessors are still running.
     *   <li>POINTWISE connection and all predecessors are still running.
     * </ol>
     *
     * @param distribution The distribution pattern between the upstream vertex and the current
     *     vertex.
     * @param upstream The set of running tasks of the upstream vertex.
     * @return Whether every task of the current vertex is connected to some active predecessors.
     */
    private boolean hasActiveUpstream(DistributionPattern distribution, JobVertexTaskSet upstream) {
        return (distribution == DistributionPattern.ALL_TO_ALL && !upstream.containsNoTasks())
                || (distribution == DistributionPattern.POINTWISE && upstream.containsAllTasks());
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
    Map<JobVertexID, JobVertexTaskSet> calculateRunningTasks() {
        Map<JobVertexID, JobVertexTaskSet> runningTasksByVertex = new HashMap<>();

        for (int i = jobVerticesInTopologyOrder.size() - 1; i >= 0; --i) {
            ExecutionJobVertex jobVertex = jobVerticesInTopologyOrder.get(i);

            List<JobEdge> outputJobEdges = getOutputJobEdges(jobVertex);

            // we're lucky if this is true
            if (isFinishedAccordingToDescendants(runningTasksByVertex, outputJobEdges)) {
                runningTasksByVertex.put(
                        jobVertex.getJobVertexId(), JobVertexTaskSet.noTasks(jobVertex));
                continue;
            }

            // not lucky, need to determine which of our tasks can still be running
            BitSet runningTasks = getRunningTasks(runningTasksByVertex, jobVertex, outputJobEdges);

            runningTasksByVertex.put(
                    jobVertex.getJobVertexId(),
                    JobVertexTaskSet.someTasks(jobVertex, runningTasks));
        }

        return runningTasksByVertex;
    }

    /**
     * Determines the {@link ExecutionVertexID ExecutionVertexIDs} of those subtasks that are still
     * running.
     */
    private BitSet getRunningTasks(
            Map<JobVertexID, JobVertexTaskSet> runningTasksByVertex,
            ExecutionJobVertex jobVertex,
            List<JobEdge> outputJobEdges) {

        BitSet runningTasks = new BitSet(jobVertex.getTaskVertices().length);
        VertexOutEdgeIndex taskEdgeIndex = outEdgeIndex.get(jobVertex.getJobVertexId());

        for (ExecutionVertex task : jobVertex.getTaskVertices()) {
            if (task.getCurrentExecutionAttempt().isFinished()) {
                continue;
            }

            boolean hasFinishedDescendants = false;
            ExecutionVertexID[] targets = taskEdgeIndex.getOutputEdges(task.getID());
            for (ExecutionVertexID target : targets) {
                JobVertexTaskSet targetVertexSet =
                        runningTasksByVertex.get(target.getJobVertexId());
                if (!targetVertexSet.contains(target)) {
                    hasFinishedDescendants = true;
                    break;
                }
            }

            if (!hasFinishedDescendants) {
                runningTasks.set(task.getID().getSubtaskIndex());
            }
        }

        return runningTasks;
    }

    /**
     * A fast way to determine if a task can be in RUNNING state. A task cannot be in RUNNING state
     * if it has finished descendants.
     */
    private boolean isFinishedAccordingToDescendants(
            Map<JobVertexID, JobVertexTaskSet> runningTasksByVertex, List<JobEdge> jobEdges) {

        for (JobEdge jobEdge : jobEdges) {
            DistributionPattern distributionPattern = jobEdge.getDistributionPattern();
            JobVertexTaskSet downstreamRunningTasks =
                    runningTasksByVertex.get(jobEdge.getTarget().getID());

            if (hasInactiveDownstream(distributionPattern, downstreamRunningTasks)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Every task must have inactive downstream tasks if
     *
     * <ol>
     *   <li>ALL_TO_ALL connection and some descendants are finished.
     *   <li>POINTWISE connection and all descendants are still running.
     * </ol>
     *
     * @param distribution The distribution pattern between the current vertex and the downstream
     *     vertex.
     * @param downstream The set of running tasks of the downstream vertex.
     * @return Whether every task of the current vertex is connected to some inactive descendants.
     */
    private boolean hasInactiveDownstream(
            DistributionPattern distribution, JobVertexTaskSet downstream) {
        return (distribution == DistributionPattern.ALL_TO_ALL && !downstream.containsAllTasks())
                || (distribution == DistributionPattern.POINTWISE && downstream.containsNoTasks());
    }

    private List<JobEdge> getOutputJobEdges(ExecutionJobVertex vertex) {
        return vertex.getJobVertex().getProducedDataSets().stream()
                .flatMap(dataSet -> dataSet.getConsumers().stream())
                .collect(Collectors.toList());
    }

    private boolean hasFinishedDescendantTasks(
            ExecutionVertex task, JobEdge jobEdge, JobVertexTaskSet targetRunningTasks) {

        List<List<ExecutionEdge>> edges =
                task.getProducedPartitions()
                        .get(
                                new IntermediateResultPartitionID(
                                        jobEdge.getSourceId(), task.getParallelSubtaskIndex()))
                        .getConsumers();
        for (int i = 0; i < edges.size(); ++i) {
            for (int j = 0; j < edges.get(i).size(); ++j) {
                ExecutionVertex target = edges.get(i).get(j).getTarget();
                if (!targetRunningTasks.contains(target.getID())) {
                    return true;
                }
            }
        }

        return false;
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

        private final TaskSetType type;

        @Nullable private final BitSet tasks;

        public static JobVertexTaskSet allTasks(ExecutionJobVertex jobVertex) {
            return new JobVertexTaskSet(jobVertex, TaskSetType.ALL_TASKS, null);
        }

        public static JobVertexTaskSet noTasks(ExecutionJobVertex jobVertex) {
            return new JobVertexTaskSet(jobVertex, TaskSetType.NO_TASKS, null);
        }

        public static JobVertexTaskSet someTasks(ExecutionJobVertex jobVertex, BitSet tasks) {
            checkNotNull(tasks);

            if (tasks.cardinality() == jobVertex.getTaskVertices().length) {
                return allTasks(jobVertex);
            } else if (tasks.cardinality() == 0) {
                return noTasks(jobVertex);
            } else {
                return new JobVertexTaskSet(jobVertex, TaskSetType.SOME_TASKS, tasks);
            }
        }

        private JobVertexTaskSet(ExecutionJobVertex jobVertex, TaskSetType type, BitSet tasks) {
            this.jobVertex = checkNotNull(jobVertex);
            this.type = type;
            this.tasks = tasks;
        }

        public boolean contains(ExecutionVertexID taskId) {
            checkState(taskId.getJobVertexId().equals(jobVertex.getJobVertexId()));
            return type == TaskSetType.ALL_TASKS
                    || (type == TaskSetType.SOME_TASKS && tasks.get(taskId.getSubtaskIndex()));
        }

        public boolean containsAllTasks() {
            return type == TaskSetType.ALL_TASKS;
        }

        public boolean containsNoTasks() {
            return type == TaskSetType.NO_TASKS;
        }

        @VisibleForTesting
        ExecutionJobVertex getJobVertex() {
            return jobVertex;
        }
    }

    private enum TaskSetType {
        ALL_TASKS,
        SOME_TASKS,
        NO_TASKS
    }

    private class VertexOutEdgeIndex {

        private final ExecutionVertexID[][] outEdges;

        public VertexOutEdgeIndex(ExecutionJobVertex jobVertex) {
            outEdges = new ExecutionVertexID[jobVertex.getTaskVertices().length][];

            List<JobEdge> pointWiseEdges = new ArrayList<>();
            for (JobEdge jobEdge : getOutputJobEdges(jobVertex)) {
                if (jobEdge.getDistributionPattern() == DistributionPattern.POINTWISE) {
                    pointWiseEdges.add(jobEdge);
                }
            }

            for (int i = 0; i < jobVertex.getTaskVertices().length; ++i) {
                List<ExecutionVertexID> outTaskIds = new ArrayList<>();
                ExecutionVertex task = jobVertex.getTaskVertices()[i];

                for (JobEdge jobEdge : pointWiseEdges) {
                    List<List<ExecutionEdge>> edges =
                            task.getProducedPartitions()
                                    .get(
                                            new IntermediateResultPartitionID(
                                                    jobEdge.getSourceId(),
                                                    task.getParallelSubtaskIndex()))
                                    .getConsumers();

                    for (int t = 0; t < edges.size(); ++t) {
                        for (int j = 0; j < edges.get(t).size(); ++j) {
                            ExecutionVertex target = edges.get(t).get(j).getTarget();
                            outTaskIds.add(target.getID());
                        }
                    }
                }

                outEdges[i] = outTaskIds.toArray(new ExecutionVertexID[0]);
            }
        }

        public ExecutionVertexID[] getOutputEdges(ExecutionVertexID source) {
            return outEdges[source.getSubtaskIndex()];
        }
    }
}
