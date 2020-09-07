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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink.USink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class BatchSinkWriterOperator<IN, CommittableT>
	extends AbstractStreamOperator<CommittableT>
	implements OneInputStreamOperator<IN, CommittableT>, BoundedOneInput {

	public static final String BATCH_PERSISTENT_MARK_FILE = "_success";

	private final USink<IN, CommittableT> uSink;

	private final UUID sinkId;

	private Path persistentDir;

	private FileSystem fileSystem;

	private boolean ignoreAllInputs;

	private Writer<IN, CommittableT> writer;

	private TypeSerializer<CommittableT> committableSerializer;

	private PersistentStorageCollector collector;

	public BatchSinkWriterOperator(USink<IN, CommittableT> uSink, UUID sinkId) {
		this.uSink = uSink;
		this.sinkId = sinkId;
	}

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

			if (fileSystem.exists(new Path(persistentDir, BATCH_PERSISTENT_MARK_FILE))) {
				ignoreAllInputs = true;
			}

			collector = new PersistentStorageCollector();
		} catch (IOException e) {
			ExceptionUtils.rethrow(e);
		}

		committableSerializer = config.getTypeSerializerOut(getClass().getClassLoader());
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		if (!ignoreAllInputs) {
			writer = uSink.createWriter(new USink.InitialContext() {
				@Override
				public boolean isRestored() {
					return context.isRestored();
				}

				@Override
				public int getSubtaskIndex() {
					return getRuntimeContext().getIndexOfThisSubtask();
				}

				@Override
				public int getParallelism() {
					return getRuntimeContext().getNumberOfParallelSubtasks();
				}

				@Override
				public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
					return context.getOperatorStateStore().getListState(stateDescriptor);
				}

				@Override
				public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
					return context.getOperatorStateStore().getUnionListState(stateDescriptor);
				}

				@Override
				public AbstractID getSessionId() {
					return null;
				}
			});
		}
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		if (!ignoreAllInputs) {
			writer.write(element.getValue(), new Writer.Context() {
				@Override
				public long currentProcessingTime() {
					throw new RuntimeException("TODO.");
				}

				@Override
				public long currentWatermark() {
					throw new RuntimeException("TODO.");
				}

				@Override
				public Long timestamp() {
					return element.getTimestamp();
				}
			}, collector);
		}
	}

	@Override
	public void endInput() throws Exception {
		if (!ignoreAllInputs) {
			writer.flush(collector);
		}
	}

	@Override
	public void close() throws Exception {
		super.close();

		if (!ignoreAllInputs) {
			writer.close();
		}

		collector.close();
	}

	public class PersistentStorageCollector implements Collector<CommittableT> {
		private final FSDataOutputStream outputStream;

		private PersistentStorageCollector() throws IOException {
			outputStream = fileSystem.create(new Path(
					persistentDir,
					sinkId.toString() + getRuntimeContext().getIndexOfThisSubtask()),
				FileSystem.WriteMode.OVERWRITE);
		}

		@Override
		public void collect(CommittableT record) {
			checkNotNull(record, "record should not be null");

			DataOutputSerializer buffer = new DataOutputSerializer(1024);
			try {
				committableSerializer.serialize(record, buffer);
				outputStream.write(buffer.getSharedBuffer(), 0, buffer.length());
				outputStream.flush();
				outputStream.sync();
			} catch (IOException e) {
				ExceptionUtils.rethrow(e);
			}
		}

		@Override
		public void close() {
			try {
				outputStream.close();
			} catch (IOException e) {
				ExceptionUtils.rethrow(e);
			}
		}
	}
}
