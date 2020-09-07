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
import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.ExceptionUtils;

import java.io.EOFException;
import java.io.IOException;
import java.util.UUID;

public class BatchSinkCommitOperator<CommittableT, Void>
	extends AbstractStreamOperator<Void>
	implements OneInputStreamOperator<CommittableT, Void>, BoundedOneInput {

	private final CommitFunction<CommittableT> commitFunction;

	private final UUID sinkId;

	private Path persistentDir;

	private FileSystem fileSystem;

	private TypeSerializer<CommittableT> serializer;

	public BatchSinkCommitOperator(
		CommitFunction<CommittableT> commitFunction,
		UUID sinkId) {

		this.commitFunction = commitFunction;
		this.sinkId = sinkId;
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Void>> output) {
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

		this.serializer = config.getTypeSerializerIn(0, getClass().getClassLoader());
	}

	@Override
	public void processElement(StreamRecord<CommittableT> element) throws Exception {
		throw new RuntimeException("We should not receive any records");
	}

	@Override
	public void endInput() throws Exception {
		// Now we do the commit
		Path targetFile = new Path(persistentDir, sinkId.toString() + getRuntimeContext().getIndexOfThisSubtask());

		try (FSDataInputStream input = fileSystem.open(targetFile)) {
			DataInputView inputView = new DataInputViewStreamWrapper(input);

			while (true) {
				CommittableT committable = serializer.deserialize(inputView);
				LOG.info("Will commit " + committable);

				if (committable != null) {
					commitFunction.commit(committable);
				}
			}
		} catch (EOFException e) {
			LOG.info("Finish processing all the committable of " + targetFile);
		} catch (IOException e) {
			LOG.warn("Failed to finish reading the input, it should be ok.");
		}
	}
}
