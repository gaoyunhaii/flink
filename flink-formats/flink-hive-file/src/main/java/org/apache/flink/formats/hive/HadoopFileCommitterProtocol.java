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

package org.apache.flink.formats.hive;

import org.apache.flink.util.ExceptionUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;

/**
 *
 */
public class HadoopFileCommitterProtocol {

	protected final Path path;

	protected final Job job;

	protected final TaskAttemptContext taskAttemptContext;

	protected OutputCommitter outputCommitter;

	public HadoopFileCommitterProtocol(Configuration configuration, Path path) throws IOException {
		this.path = path;
		this.job = Job.getInstance(configuration);

		FileOutputFormat.setOutputPath(job, path.getParent());
		job.getConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

		TaskAttemptID taskAttemptID = createTaskId(path);
		this.taskAttemptContext = new TaskAttemptContextImpl(job.getConfiguration(), taskAttemptID);

		createCommitter();
	}

	protected void createCommitter() {
		try {
			OutputFormat<?, ?> outputFormat = job
				.getOutputFormatClass()
				.getConstructor()
				.newInstance();

			if (outputFormat instanceof Configurable) {
				((Configurable) outputFormat).setConf(job.getConfiguration());
			}

			outputCommitter = outputFormat.getOutputCommitter(taskAttemptContext);
		} catch (Exception e) {
			ExceptionUtils.rethrow(e);
		}
	}

	public void setup() throws IOException {
		outputCommitter.setupJob(job);
		outputCommitter.setupTask(taskAttemptContext);
	}

	public Path getTaskAttemptPath() {
		String fileName = path.getName();

		Path stagingDir = path.getParent();
		if (outputCommitter instanceof FileOutputCommitter) {
			stagingDir = ((FileOutputCommitter) outputCommitter).getTaskAttemptPath(taskAttemptContext);
		}

		return new Path(stagingDir + "/" + fileName);
	}

	public void commitTask() throws IOException {
		if (outputCommitter.needsTaskCommit(taskAttemptContext)) {
			outputCommitter.commitTask(taskAttemptContext);
		}
	}

	public void commitJob() throws IOException {
		outputCommitter.commitJob(job);
	}

	public Path getPath() {
		return path;
	}

	private TaskAttemptID createTaskId(Path path) {
		return new TaskAttemptID(
			path.getName(),
			0,
			TaskType.REDUCE,
			0,
			0);
	}
}
