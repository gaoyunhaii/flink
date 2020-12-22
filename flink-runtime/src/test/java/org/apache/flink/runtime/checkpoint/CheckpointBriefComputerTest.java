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

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests the logic for the brief computer.
 */
public class CheckpointBriefComputerTest {

	@Test
	public void testFindAllFinishedVertices() throws Exception {
		JobVertex first = ExecutionGraphTestUtils.createJobVertex("first", 10, NoOpInvokable.class);
		JobVertex second = ExecutionGraphTestUtils.createJobVertex("second", 10, NoOpInvokable.class);
		JobVertex third = ExecutionGraphTestUtils.createJobVertex("third", 10, NoOpInvokable.class);
		second.connectNewDataSetAsInput(first, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		third.connectNewDataSetAsInput(second, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		ExecutionGraph simpleGraph = ExecutionGraphTestUtils.createSimpleTestGraph(first, second, third);
		simpleGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

		ExecutionVertex[] firstVertices = simpleGraph.getJobVertex(first.getID()).getTaskVertices();
		ExecutionVertex[] secondVertices = simpleGraph.getJobVertex(second.getID()).getTaskVertices();
		ExecutionVertex[] thirdVertices = simpleGraph.getJobVertex(third.getID()).getTaskVertices();
		for (int i = 0; i < thirdVertices.length; ++i) {
			if (i % 2 == 0) {
				thirdVertices[i].getCurrentExecutionAttempt().transitionState(ExecutionState.FINISHED);
			}
		}

		CheckpointBriefComputer computer = new CheckpointBriefComputer(
			simpleGraph.getJobID(),
			simpleGraph.getVerticesTopologically(),
			simpleGraph::getJobMasterMainThreadExecutor);

		Set<ExecutionVertexID> finished = computer.findFinishedExecutionVertex();

		assertEquals(20, finished.size());
		for (int i = 0; i < firstVertices.length; ++i) {
			assertTrue(finished.contains(firstVertices[i].getID()));
		}

		for (int i = 0; i < firstVertices.length; i += 2) {
			assertTrue(finished.contains(secondVertices[i].getID()));
		}

		for (int i = 0; i < firstVertices.length; i += 2) {
			assertTrue(finished.contains(thirdVertices[i].getID()));
		}
	}

	@Ignore
	@Test
	public void testNotAllSourceRunning() throws Exception {
		JobVertex first = ExecutionGraphTestUtils.createJobVertex("first", 1, NoOpInvokable.class);
		JobVertex second = ExecutionGraphTestUtils.createJobVertex("second", 1, NoOpInvokable.class);
		second.connectNewDataSetAsInput(first, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		ExecutionGraph simpleGraph = ExecutionGraphTestUtils.createSimpleTestGraph(first, second);
		simpleGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

		CheckpointBriefComputer computer = new CheckpointBriefComputer(
			simpleGraph.getJobID(),
			simpleGraph.getVerticesTopologically(),
			simpleGraph::getJobMasterMainThreadExecutor);

		try {
			computer.computeCheckpointBrief();
			fail("Should throw exception due to not all tasks are running");
		} catch (CheckpointException e) {
			assertEquals(CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING, e.getCheckpointFailureReason());
		}
	}

	@Ignore
	@Test
	public void manuallyPerformanceTest() throws Exception {
		// 10 job vertices each with 1000 tasks
		JobVertex[] vertices = new JobVertex[10];
		for (int i = 0; i < 10; ++i) {
			vertices[i] = ExecutionGraphTestUtils.createJobVertex(i + "", 1000, NoOpInvokable.class);
		}

		vertices[1].connectNewDataSetAsInput(vertices[0], DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertices[2].connectNewDataSetAsInput(vertices[0], DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertices[3].connectNewDataSetAsInput(vertices[1], DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		vertices[3].connectNewDataSetAsInput(vertices[2], DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		vertices[7].connectNewDataSetAsInput(vertices[5], DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertices[7].connectNewDataSetAsInput(vertices[6], DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertices[8].connectNewDataSetAsInput(vertices[5], DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		vertices[8].connectNewDataSetAsInput(vertices[6], DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		vertices[9].connectNewDataSetAsInput(vertices[5], DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		vertices[9].connectNewDataSetAsInput(vertices[6], DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		ExecutionGraph simpleGraph = ExecutionGraphTestUtils.createSimpleTestGraph(vertices);
		simpleGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());
		simpleGraph.transitionToRunning();
		simpleGraph.getAllExecutionVertices().forEach(v -> v.getCurrentExecutionAttempt().transitionState(ExecutionState.RUNNING));
		Arrays.stream(simpleGraph.getJobVertex(vertices[0].getID()).getTaskVertices()).forEach(
			v -> v.getCurrentExecutionAttempt().transitionState(ExecutionState.FINISHED));
		Arrays.stream(simpleGraph.getJobVertex(vertices[1].getID()).getTaskVertices()).forEach(
			v -> v.getCurrentExecutionAttempt().transitionState(ExecutionState.FINISHED));
		Arrays.stream(simpleGraph.getJobVertex(vertices[5].getID()).getTaskVertices()).forEach(
			v -> v.getCurrentExecutionAttempt().transitionState(ExecutionState.FINISHED));
		Arrays.stream(simpleGraph.getJobVertex(vertices[6].getID()).getTaskVertices()).forEach(
			v -> v.getCurrentExecutionAttempt().transitionState(ExecutionState.FINISHED));

		long start = System.nanoTime();
		CheckpointBriefComputer computer = new CheckpointBriefComputer(
			simpleGraph.getJobID(),
			simpleGraph.getVerticesTopologically(),
			simpleGraph::getJobMasterMainThreadExecutor);
		CheckpointBriefComputer.CheckpointBrief brief = computer.computeCheckpointBrief().get();
		long end = System.nanoTime();
		System.out.println("Usage: " + (end - start) / 1e6 + " ms");

		System.out.println(brief);
	}
}
