/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.jobgraph.OperatorID;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Set;

/**
 * This class encapsulates the data from the job manager to restore a task.
 */
public class JobManagerTaskRestore implements Serializable {

	private static final long serialVersionUID = 1L;

	/** The id of the checkpoint from which we restore. */
	private final long restoreCheckpointId;

	/** The state for this task to restore. */
	private final TaskStateSnapshot taskStateSnapshot;

	private final Set<OperatorID> fullyFinishedOperators;

	@VisibleForTesting
	public JobManagerTaskRestore(
		@Nonnegative long restoreCheckpointId,
		@Nonnull TaskStateSnapshot taskStateSnapshot) {

		this(restoreCheckpointId, taskStateSnapshot, null);
	}

	public JobManagerTaskRestore(
		@Nonnegative long restoreCheckpointId,
		@Nonnull TaskStateSnapshot taskStateSnapshot,
		@Nullable Set<OperatorID> fullyFinishedOperators) {

		this.restoreCheckpointId = restoreCheckpointId;
		this.taskStateSnapshot = taskStateSnapshot;
		this.fullyFinishedOperators = fullyFinishedOperators;
	}

	public long getRestoreCheckpointId() {
		return restoreCheckpointId;
	}

	@Nonnull
	public TaskStateSnapshot getTaskStateSnapshot() {
		return taskStateSnapshot;
	}

	@Nullable
	public Set<OperatorID> getFullyFinishedOperators() {
		return fullyFinishedOperators;
	}

	@Override
	public String toString() {
		return "JobManagerTaskRestore{" +
			"restoreCheckpointId=" + restoreCheckpointId +
			", taskStateSnapshot=" + taskStateSnapshot +
			", fullyFinishedOperators=" + fullyFinishedOperators +
			'}';
	}
}
