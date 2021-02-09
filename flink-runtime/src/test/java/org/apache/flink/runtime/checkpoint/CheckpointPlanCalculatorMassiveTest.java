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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphCheckpointPlanCalculatorContext;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class CheckpointPlanCalculatorMassiveTest {

    @Test
    public void testMassive() throws Exception {
        for (DistributionPattern pattern :
                Arrays.asList(
                        DistributionPattern.POINTWISE /*,  DistributionPattern.ALL_TO_ALL*/)) {
            for (int level = 10; level <= 20; level += 2) {
                for (int numEachLevel : Arrays.asList(/*1000, 2000, 4000, */ 8000, 16000)) {
                    Tuple2<List<Double>, List<Double>> result =
                            testLineGraph(
                                    level,
                                    numEachLevel,
                                    i -> pattern,
                                    Arrays.asList(
                                            0,
                                            1,
                                            10,
                                            numEachLevel / 2,
                                            numEachLevel - 10,
                                            numEachLevel));
                    System.out.print(
                            "@Find Running "
                                    + pattern
                                    + "\tL"
                                    + level
                                    + " N"
                                    + numEachLevel
                                    + ": ");
                    result.f0.forEach(time -> System.out.printf("%.2f ", time / 1e6));
                    System.out.println();
                    System.out.print(
                            "@CalculateALL "
                                    + pattern
                                    + "\tL"
                                    + level
                                    + " N"
                                    + numEachLevel
                                    + ": ");
                    result.f1.forEach(time -> System.out.printf("%.2f\t", time / 1e6));
                    System.out.println("\n@");
                }
            }
        }
    }

    private Tuple2<List<Double>, List<Double>> testLineGraph(
            int level,
            int numEachLevel,
            Function<Integer, DistributionPattern> patternFunction,
            List<Integer> numFinishedEachLevels)
            throws Exception {
        Random random = new Random(System.currentTimeMillis());
        List<Double> findAllRunningResult = new ArrayList<>();
        List<Double> allResult = new ArrayList<>();
        for (int numFinishedEachLevel : numFinishedEachLevels) {
            // First change vertex status
            Set<Integer> finishedIndex = new HashSet<>();
            while (finishedIndex.size() < numFinishedEachLevel) {
                finishedIndex.add(random.nextInt(numEachLevel));
            }

            ExecutionGraph graph =
                    createLineGraph(level, numEachLevel, patternFunction, finishedIndex);
            CheckpointPlanCalculator planCalculator = createCheckpointPlanCalculator(graph);

            List<Long> findAllRunningTasksTime = new ArrayList<>();
            for (int i = 0; i < 10; ++i) {
                long used = -System.nanoTime();
                Map<JobVertexID, CheckpointPlanCalculator.JobVertexTaskSet> runningTasks =
                        planCalculator.calculateRunningTasks();
                used += System.nanoTime();

                System.out.print(runningTasks.size());
                findAllRunningTasksTime.add(used);
            }
            findAllRunningResult.add(
                    (double) findAllRunningTasksTime.stream().reduce(0L, Long::sum)
                            / findAllRunningTasksTime.size());

            List<Long> allTime = new ArrayList<>();
            for (int i = 0; i < 10; ++i) {
                long used = -System.nanoTime();
                CheckpointPlan checkpointPlan = planCalculator.calculateCheckpointPlan().get();
                used += System.nanoTime();

                System.out.print(checkpointPlan.getFinishedTasks().size());
                allTime.add(used);
            }
            allResult.add((double) allTime.stream().reduce(0L, Long::sum) / allTime.size());
        }
        System.out.println();

        return new Tuple2<>(findAllRunningResult, allResult);
    }

    private ExecutionGraph createLineGraph(
            int level,
            int numEachLevel,
            Function<Integer, DistributionPattern> patternFunction,
            Set<Integer> finishedTasks)
            throws Exception {
        List<VertexDeclaration> vertexDeclarations = new ArrayList<>();
        List<EdgeDeclaration> edgeDeclarations = new ArrayList<>();

        for (int i = 0; i < level; ++i) {
            vertexDeclarations.add(
                    new VertexDeclaration(
                            numEachLevel, i == level - 1 ? finishedTasks : Collections.emptySet()));
        }

        for (int i = 0; i < level; ++i) {
            for (int j = i + 1; j < level; ++j) {
                edgeDeclarations.add(new EdgeDeclaration(i, j, patternFunction.apply(i)));
            }
        }

        ExecutionGraph graph = createExecutionGraph(vertexDeclarations, edgeDeclarations);
        return graph;
    }

    // ------------------------- Utility methods ---------------------------------------

    private ExecutionGraph createExecutionGraph(
            List<VertexDeclaration> vertexDeclarations, List<EdgeDeclaration> edgeDeclarations)
            throws Exception {

        JobVertex[] jobVertices = new JobVertex[vertexDeclarations.size()];
        for (int i = 0; i < vertexDeclarations.size(); ++i) {
            jobVertices[i] =
                    ExecutionGraphTestUtils.createJobVertex(
                            vertexName(i),
                            vertexDeclarations.get(i).parallelism,
                            NoOpInvokable.class);
        }

        for (EdgeDeclaration edgeDeclaration : edgeDeclarations) {
            jobVertices[edgeDeclaration.target].connectNewDataSetAsInput(
                    jobVertices[edgeDeclaration.source],
                    edgeDeclaration.distributionPattern,
                    ResultPartitionType.PIPELINED);
        }

        ExecutionGraph graph = ExecutionGraphTestUtils.createSimpleTestGraph(jobVertices);
        graph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
        graph.transitionToRunning();
        graph.getAllExecutionVertices()
                .forEach(
                        task ->
                                task.getCurrentExecutionAttempt()
                                        .transitionState(ExecutionState.RUNNING));

        for (int i = 0; i < vertexDeclarations.size(); ++i) {
            JobVertexID jobVertexId = jobVertices[i].getID();
            vertexDeclarations
                    .get(i)
                    .finishedSubtaskIndices
                    .forEach(
                            index -> {
                                graph.getJobVertex(jobVertexId)
                                        .getTaskVertices()[index]
                                        .getCurrentExecutionAttempt()
                                        .markFinished();
                            });
        }

        return graph;
    }

    private CheckpointPlanCalculator createCheckpointPlanCalculator(ExecutionGraph graph) {
        return new CheckpointPlanCalculator(
                graph.getJobID(),
                new ExecutionGraphCheckpointPlanCalculatorContext(graph),
                graph.getVerticesTopologically());
    }

    private List<ExecutionVertex> collectToList(
            Map<JobVertexID, CheckpointPlanCalculator.JobVertexTaskSet> taskSets) {

        List<ExecutionVertex> tasks = new ArrayList<>();
        taskSets.forEach(
                (jobVertexID, jobVertexTaskSet) ->
                        Arrays.stream(jobVertexTaskSet.getJobVertex().getTaskVertices())
                                .filter(task -> jobVertexTaskSet.contains(task.getID()))
                                .forEach(tasks::add));
        return tasks;
    }

    private void checkCheckpointPlan(
            List<ExecutionVertex> expectedToTrigger,
            List<ExecutionVertex> expectedRunning,
            List<Execution> expectedFinished,
            List<ExecutionJobVertex> expectedFullyFinished,
            CheckpointPlan plan) {

        // Compares tasks to trigger
        List<Execution> expectedTriggeredExecutions =
                expectedToTrigger.stream()
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toList());
        assertSameInstancesWithoutOrder(
                "The computed tasks to trigger is different from expected",
                expectedTriggeredExecutions,
                plan.getTasksToTrigger());

        // Compares running tasks
        assertSameInstancesWithoutOrder(
                "The computed running tasks is different from expected",
                expectedRunning,
                plan.getTasksToCommitTo());

        // Compares finished tasks
        assertSameInstancesWithoutOrder(
                "The computed finished tasks is different from expected",
                expectedFinished,
                plan.getFinishedTasks());

        // Compares fully finished job vertices
        assertSameInstancesWithoutOrder(
                "The computed fully finished JobVertex is different from expected",
                expectedFullyFinished,
                plan.getFullyFinishedJobVertex());

        // Compares tasks to ack
        assertEquals(
                "The computed tasks to ack is different from expected",
                expectedRunning.stream()
                        .map(vertex -> vertex.getCurrentExecutionAttempt().getAttemptId())
                        .collect(Collectors.toSet()),
                plan.getTasksToWaitFor().keySet());
        plan.getTasksToWaitFor()
                .forEach(
                        (attemptID, executionVertex) -> {
                            assertEquals(
                                    attemptID,
                                    executionVertex.getCurrentExecutionAttempt().getAttemptId());
                        });
    }

    private <T> void assertSameInstancesWithoutOrder(
            String comment, Collection<T> expected, Collection<T> actual) {
        assertThat(
                comment,
                expected,
                containsInAnyOrder(
                        actual.stream()
                                .map(CoreMatchers::sameInstance)
                                .collect(Collectors.toList())));
    }

    private List<ExecutionVertex> chooseTasks(
            ExecutionGraph graph, TaskDeclaration... chosenDeclarations) {
        List<ExecutionVertex> tasks = new ArrayList<>();

        for (TaskDeclaration chosenDeclaration : chosenDeclarations) {
            ExecutionJobVertex jobVertex = chooseJobVertex(graph, chosenDeclaration.vertexIndex);
            chosenDeclaration.subtaskIndices.forEach(
                    index -> tasks.add(jobVertex.getTaskVertices()[index]));
        }

        return tasks;
    }

    private ExecutionJobVertex chooseJobVertex(ExecutionGraph graph, int vertexIndex) {
        String name = vertexName(vertexIndex);
        Optional<ExecutionJobVertex> foundVertex =
                graph.getAllVertices().values().stream()
                        .filter(jobVertex -> jobVertex.getName().equals(name))
                        .findFirst();

        if (!foundVertex.isPresent()) {
            throw new RuntimeException("Vertex not found with index " + vertexIndex);
        }

        return foundVertex.get();
    }

    private String vertexName(int index) {
        return "vertex_" + index;
    }

    private Set<Integer> range(int start, int end) {
        return IntStream.range(start, end).boxed().collect(Collectors.toSet());
    }

    private Set<Integer> of(Integer... index) {
        return new HashSet<>(Arrays.asList(index));
    }

    private Set<Integer> minus(Set<Integer> all, Set<Integer> toMinus) {
        return all.stream().filter(e -> !toMinus.contains(e)).collect(Collectors.toSet());
    }

    // ------------------------- Utility helper classes ---------------------------------------

    private static class VertexDeclaration {
        final int parallelism;
        final Set<Integer> finishedSubtaskIndices;

        public VertexDeclaration(int parallelism, Set<Integer> finishedSubtaskIndices) {
            this.parallelism = parallelism;
            this.finishedSubtaskIndices = finishedSubtaskIndices;
        }
    }

    private static class EdgeDeclaration {
        final int source;
        final int target;
        final DistributionPattern distributionPattern;

        public EdgeDeclaration(int source, int target, DistributionPattern distributionPattern) {
            this.source = source;
            this.target = target;
            this.distributionPattern = distributionPattern;
        }
    }

    private static class TaskDeclaration {
        final int vertexIndex;

        final Set<Integer> subtaskIndices;

        public TaskDeclaration(int vertexIndex, Set<Integer> subtaskIndices) {
            this.vertexIndex = vertexIndex;
            this.subtaskIndices = subtaskIndices;
        }
    }
}
