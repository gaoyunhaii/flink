/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * A {@link StreamOperator} for executing a {@link org.apache.flink.api.dag.CommitTransformation}.
 */
@Internal
public class StreamingCommitOperator<CommitT>
		extends AbstractStreamOperator<Void>
		implements OneInputStreamOperator<CommitT, Void> {

	private static final long serialVersionUID = 1L;

	private final CommitFunction<CommitT> commitFunction;

	private final TypeSerializer<CommitT> commitSerializer;

	private transient ListState<CommitT> commits;

	private final NavigableMap<Long, List<CommitT>> committablesPerCheckpoint = new TreeMap<>();

	private List<CommitT> currentCommittables;

	public StreamingCommitOperator(
			CommitFunction<CommitT> commitFunction,
			TypeSerializer<CommitT> commitSerializer) {
		this.commitFunction = commitFunction;
		this.commitSerializer = commitSerializer;
		this.currentCommittables = new ArrayList<>();
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		ListStateDescriptor<CommitT> commitStateDescriptor = new ListStateDescriptor<>(
				"commits",
				commitSerializer);

		this.commits = context.getOperatorStateStore().getListState(commitStateDescriptor);

		if (context.isRestored()) {
			for (CommitT commit : commits.get()) {
				LOG.info("[USK] restore ................ :" + commit);
				commitFunction.commit(commit);
			}
		}
	}

	@Override
	public void processElement(StreamRecord<CommitT> element) throws Exception {
		currentCommittables.add(element.getValue());
		LOG.info("[USK] Process ................ :" + element.getValue());
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);

		committablesPerCheckpoint.put(context.getCheckpointId(), currentCommittables);
		currentCommittables = new ArrayList<>();
		commits.clear();

		Iterator<Map.Entry<Long, List<CommitT>>> it = committablesPerCheckpoint.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<Long, List<CommitT>> item = it.next();
			for (CommitT commit : item.getValue()) {
				commits.add(commit);
				LOG.info("[USK] checkpoint ................ :" + commit);

			}
		}
		return;
	}

	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		super.notifyCheckpointComplete(checkpointId);
		Iterator<Map.Entry<Long, List<CommitT>>> it =
			committablesPerCheckpoint.headMap(checkpointId, true).entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry<Long, List<CommitT>> item = it.next();
			for (CommitT commit : item.getValue()) {
				LOG.info("[USK] commit ................ :" + commit);
				commitFunction.commit(commit);
			}
			it.remove();
		}
	}
}
