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

package org.apache.flink.formats.hadoop.bulk;

import org.apache.flink.formats.hadoop.bulk.committer.HadoopRenameFileCommitter;

import org.apache.flink.formats.hadoop.bulk.committer.OutputCommitterBasedFileCommitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import java.io.IOException;

/**
 * The default hadoop file committer factory which always use {@link HadoopRenameFileCommitter}.
 */
public class DefaultHadoopFileCommitterFactory implements HadoopFileCommitterFactory {

	private static final long serialVersionUID = 1L;

	private final OutputFormatUtilsV3 outputCommitterUtils = new OutputFormatUtilsV3();

	@Override
	public HadoopFileCommitter create(
		Configuration configuration,
		Path targetFilePath,
		int maxParallelism,
		int subtaskIndex,
		long partCount) throws IOException {

		configuration = new Configuration(configuration);
		configuration.set("targetFilePath", targetFilePath.toUri().toString());

		// 1. compute the job attempt number
		int interval = Integer.MAX_VALUE / maxParallelism;
		int jobAttemptNumber = (int) (subtaskIndex * interval + partCount % interval);

		// 2. Set the corresponding configurations
		configuration.set(MRJobConfig.APPLICATION_ATTEMPT_ID, String.valueOf(jobAttemptNumber));
		configuration.set(FileOutputFormat.OUTDIR, targetFilePath.getParent().toUri().toString());
		configuration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false");

		Job job = Job.getInstance(configuration);
		TaskAttemptID id = new TaskAttemptID(
			targetFilePath.getName(),
			0,
			TaskType.REDUCE,
			0,
			0);
		TaskAttemptContext context = new TaskAttemptContextImpl(job.getConfiguration(), id);

		// 3. create the output committer
		try {
			OutputFormat<?, ?> outputFormat = TextOutputFormat.class.newInstance();
			OutputCommitter outputCommitter = outputFormat.getOutputCommitter(context);
			outputCommitter = outputCommitterUtils.verifyAndEnhanceOutputCommitter(
				outputCommitter,
				targetFilePath,
				context);

			outputCommitter.setupJob(job);
			outputCommitter.setupTask(context);

			// 4. Acquire the attempt path.
			Path taskWorkerPath = outputCommitterUtils.getTaskAttemptPath(outputCommitter, targetFilePath);
			return new OutputCommitterBasedFileCommitter(
				targetFilePath,
				new Path(taskWorkerPath, targetFilePath.getName()),
				job,
				context,
				outputCommitter);

		} catch (IOException e) {
			throw e;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
