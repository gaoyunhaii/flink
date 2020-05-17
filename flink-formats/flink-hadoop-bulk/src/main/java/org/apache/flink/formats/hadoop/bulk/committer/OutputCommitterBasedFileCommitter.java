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
package org.apache.flink.formats.hadoop.bulk.committer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.formats.hadoop.bulk.HadoopFileCommitter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 *
 */
public class OutputCommitterBasedFileCommitter implements HadoopFileCommitter {
	private final Path targetFilePath;

	private final Path inProgressFilePath;

	private final Job job;

	private final TaskAttemptContext taskAttemptContext;

	private final OutputCommitter outputCommitter;

	public OutputCommitterBasedFileCommitter(
		Path targetFilePath,
		Path inProgressFilePath,
		Job job,
		TaskAttemptContext taskAttemptContext,
		OutputCommitter outputCommitter) {

		this.targetFilePath = targetFilePath;
		this.inProgressFilePath = inProgressFilePath;
		this.job = job;
		this.taskAttemptContext = taskAttemptContext;
		this.outputCommitter = outputCommitter;
	}

	@Override
	public Path getTargetFilePath() {
		return targetFilePath;
	}

	@Override
	public Path getInProgressFilePath() {
		return inProgressFilePath;
	}

	@Override
	public void preCommit() throws IOException {
		outputCommitter.commitTask(taskAttemptContext);
	}

	@Override
	public void commit() throws IOException {
		outputCommitter.commitJob(job);
		cleanupUniqueName();
	}

	@Override
	public void commitAfterRecovery() throws IOException {
		outputCommitter.commitJob(job);
		cleanupUniqueName();
	}

	private void cleanupUniqueName() throws IOException {
		FileSystem fileSystem = FileSystem.get(targetFilePath.toUri(), job.getConfiguration());
		fileSystem.delete(new Path(targetFilePath.getParent(), "." + targetFilePath.getName()), true);
	}

	@VisibleForTesting
	public OutputCommitter getOutputCommitter() {
		return outputCommitter;
	}
}
