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

package org.apache.flink.streaming.api.operators.sink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;

import static org.apache.flink.streaming.api.operators.sink.BatchSinkWriterOperator.BATCH_PERSISTENT_MARK_FILE;

public class BatchSinkBarrierOperator<CommittableT>
	extends AbstractStreamOperator<CommittableT>
	implements OneInputStreamOperator<CommittableT, CommittableT>, BoundedOneInput {

	private Path persistentDir;

	private FileSystem fileSystem;

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<CommittableT>> output) {
		super.setup(containingTask, config, output);

		JobID jobID = containingTask.getEnvironment().getJobID();
		persistentDir = new Path(containingTask
				.getEnvironment()
				.getTaskManagerInfo()
				.getConfiguration()
				.get(TaskManagerOptions.SINK_BATCH_PERSISTENT_DIR),
			jobID.toString());

		try {
			fileSystem = FileSystem.get(persistentDir.toUri());
		} catch (IOException e) {
			ExceptionUtils.rethrow(e);
		}
	}

	@Override
	public void processElement(StreamRecord<CommittableT> element) throws Exception {
		throw new RuntimeException("We should not receive any records");
	}

	@Override
	public void endInput() throws Exception {
		FSDataOutputStream outputStream = fileSystem.create(new Path(
				persistentDir,
				BATCH_PERSISTENT_MARK_FILE),
			FileSystem.WriteMode.NO_OVERWRITE);

		// TODO: Magic number
		outputStream.write(new byte[]{1, 2, 3, 4});

		outputStream.close();
	}
}
