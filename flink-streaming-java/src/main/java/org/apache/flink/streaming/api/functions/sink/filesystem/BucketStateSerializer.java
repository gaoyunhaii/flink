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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A {@code SimpleVersionedSerializer} used to serialize the {@link BucketState BucketState}.
 */
@Internal
class BucketStateSerializer<BucketID> implements SimpleVersionedSerializer<BucketState<BucketID>> {

	private static final int MAGIC_NUMBER = 0x1e764b79;

	private final SimpleVersionedSerializer<PartFileWriter.InProgressFileSnapshot> inProgressFileSnapshotSerializer;

	private final SimpleVersionedSerializer<PartFileWriter.PendingFileSnapshot> pendingFileSnapshotSerializer;

	private final SimpleVersionedSerializer<BucketID> bucketIdSerializer;

	BucketStateSerializer(
			final SimpleVersionedSerializer<PartFileWriter.InProgressFileSnapshot> inProgressFileSnapshotSerializer,
			final SimpleVersionedSerializer<PartFileWriter.PendingFileSnapshot> pendingFileSnapshotSerializer,
			final SimpleVersionedSerializer<BucketID> bucketIdSerializer
	) {
		this.inProgressFileSnapshotSerializer = Preconditions.checkNotNull(inProgressFileSnapshotSerializer);
		this.pendingFileSnapshotSerializer = Preconditions.checkNotNull(pendingFileSnapshotSerializer);
		this.bucketIdSerializer = Preconditions.checkNotNull(bucketIdSerializer);
	}

	@Override
	public int getVersion() {
		return 2;
	}

	@Override
	public byte[] serialize(BucketState<BucketID> state) throws IOException {
		DataOutputSerializer out = new DataOutputSerializer(256);
		out.writeInt(MAGIC_NUMBER);
		serializeV2(state, out);
		return out.getCopyOfBuffer();
	}

	@Override
	public BucketState<BucketID> deserialize(int version, byte[] serialized) throws IOException {
		final DataInputDeserializer in = new DataInputDeserializer(serialized);

		switch (version) {
			case 1:
				validateMagicNumber(in);
				return deserializeV1(in);
			case 2:
				validateMagicNumber(in);
				return deserializeV2(in);
			default:
				throw new IOException("Unrecognized version or corrupt state: " + version);
		}
	}

	@VisibleForTesting
	void serializeV1(BucketState<BucketID> state, DataOutputView out) throws IOException {

		final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> commitableSerializer = getCommitableSerializer();
		final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> resumableSerializer = getResumableSerializer();

		SimpleVersionedSerialization.writeVersionAndSerialize(bucketIdSerializer, state.getBucketId(), out);
		out.writeUTF(state.getBucketPath().toString());
		out.writeLong(state.getInProgressFileCreationTime());

		// put the current open part file
		if (state.hasInProgressResumableFile()) {
			final RecoverableWriter.ResumeRecoverable resumable =
				((OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressSnapshot) state.getInProgressFileSnapshot()).getResumeRecoverable();
			out.writeBoolean(true);
			SimpleVersionedSerialization.writeVersionAndSerialize(resumableSerializer, resumable, out);
		}
		else {
			out.writeBoolean(false);
		}

		// put the map of pending files per checkpoint
		final Map<Long, List<RecoverableWriter.CommitRecoverable>> pendingCommitters = getCommittablesPerCheckpoint(state);

		// manually keep the version here to safe some bytes
		out.writeInt(commitableSerializer.getVersion());

		out.writeInt(pendingCommitters.size());
		for (Entry<Long, List<RecoverableWriter.CommitRecoverable>> resumablesForCheckpoint : pendingCommitters.entrySet()) {
			List<RecoverableWriter.CommitRecoverable> resumables = resumablesForCheckpoint.getValue();

			out.writeLong(resumablesForCheckpoint.getKey());
			out.writeInt(resumables.size());

			for (RecoverableWriter.CommitRecoverable resumable : resumables) {
				byte[] serialized = commitableSerializer.serialize(resumable);
				out.writeInt(serialized.length);
				out.write(serialized);
			}
		}
	}

	void serializeV2(BucketState<BucketID> state, DataOutputView dataOutputView) throws IOException {
		SimpleVersionedSerialization.writeVersionAndSerialize(bucketIdSerializer, state.getBucketId(), dataOutputView);
		dataOutputView.writeUTF(state.getBucketPath().toString());
		dataOutputView.writeLong(state.getInProgressFileCreationTime());

		// put the current open part file
		if (state.hasInProgressResumableFile()) {
			final PartFileWriter.InProgressFileSnapshot inProgressFileSnapshot = state.getInProgressFileSnapshot();
			dataOutputView.writeBoolean(true);
			SimpleVersionedSerialization.writeVersionAndSerialize(inProgressFileSnapshotSerializer, inProgressFileSnapshot, dataOutputView);
		} else {
			dataOutputView.writeBoolean(false);
		}

		// put the map of pending files per checkpoint
		final Map<Long, List<PartFileWriter.PendingFileSnapshot>> pendingFileSnapshots = state.getPendingFileSnapshots();

		dataOutputView.writeInt(pendingFileSnapshotSerializer.getVersion());

		dataOutputView.writeInt(pendingFileSnapshots.size());

		for (Entry<Long, List<PartFileWriter.PendingFileSnapshot>> pendingFilesForCheckpoint : pendingFileSnapshots.entrySet()) {
			final List<PartFileWriter.PendingFileSnapshot> pendingFileSnapshotList = pendingFilesForCheckpoint.getValue();

			dataOutputView.writeLong(pendingFilesForCheckpoint.getKey());
			dataOutputView.writeInt(pendingFileSnapshotList.size());

			for (PartFileWriter.PendingFileSnapshot pendingFileSnapshot : pendingFileSnapshotList) {
				byte[] serialized = pendingFileSnapshotSerializer.serialize(pendingFileSnapshot);
				dataOutputView.writeInt(serialized.length);
				dataOutputView.write(serialized);
			}
		}
	}

	@VisibleForTesting
	BucketState<BucketID> deserializeV1(DataInputView in) throws IOException {

		final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> commitableSerializer = getCommitableSerializer();
		final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> resumableSerializer = getResumableSerializer();

		final BucketID bucketId = SimpleVersionedSerialization.readVersionAndDeSerialize(bucketIdSerializer, in);
		final String bucketPathStr = in.readUTF();
		final long creationTime = in.readLong();

		// then get the current resumable stream
		PartFileWriter.InProgressFileSnapshot current = null;
		if (in.readBoolean()) {
			current =
				new OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressSnapshot(
					SimpleVersionedSerialization.readVersionAndDeSerialize(resumableSerializer, in));
		}

		final int committableVersion = in.readInt();
		final int numCheckpoints = in.readInt();
		final HashMap<Long, List<PartFileWriter.PendingFileSnapshot>> pendingFileSnapshotPerCheckpoint = new HashMap<>(numCheckpoints);

		for (int i = 0; i < numCheckpoints; i++) {
			final long checkpointId = in.readLong();
			final int noOfResumables = in.readInt();

			final List<PartFileWriter.PendingFileSnapshot> pendingFileSnapshots = new ArrayList<>(noOfResumables);
			for (int j = 0; j < noOfResumables; j++) {
				final byte[] bytes = new byte[in.readInt()];
				in.readFully(bytes);
				pendingFileSnapshots.add(
					new OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileSnapshot(commitableSerializer.deserialize(committableVersion, bytes)));
			}
			pendingFileSnapshotPerCheckpoint.put(checkpointId, pendingFileSnapshots);
		}

		return new BucketState<>(
			bucketId,
			new Path(bucketPathStr),
			creationTime,
			current,
			pendingFileSnapshotPerCheckpoint);
	}

	BucketState<BucketID> deserializeV2(DataInputView dataInputView) throws IOException {
		final BucketID bucketId = SimpleVersionedSerialization.readVersionAndDeSerialize(bucketIdSerializer, dataInputView);
		final String bucketPathStr = dataInputView.readUTF();
		final long creationTime = dataInputView.readLong();

		// then get the current resumable stream
		PartFileWriter.InProgressFileSnapshot current = null;
		if (dataInputView.readBoolean()) {
			current = SimpleVersionedSerialization.readVersionAndDeSerialize(inProgressFileSnapshotSerializer, dataInputView);
		}

		final int pendingFileSnapshotSerializerVersion = dataInputView.readInt();
		final int numCheckpoints = dataInputView.readInt();
		final HashMap<Long, List<PartFileWriter.PendingFileSnapshot>> pendingFileSnapshotsPerCheckpoint = new HashMap<>(numCheckpoints);

		for (int i = 0; i < numCheckpoints; i++) {
			final long checkpointId = dataInputView.readLong();
			final int numOfPendingFileSnapshots = dataInputView.readInt();

			final List<PartFileWriter.PendingFileSnapshot> pendingFileSnapshots = new ArrayList<>(numOfPendingFileSnapshots);
			for (int j = 0; j < numOfPendingFileSnapshots; j++) {
				final byte[] bytes = new byte[dataInputView.readInt()];
				dataInputView.readFully(bytes);
				pendingFileSnapshots.add(pendingFileSnapshotSerializer.deserialize(pendingFileSnapshotSerializerVersion, bytes));
			}
			pendingFileSnapshotsPerCheckpoint.put(checkpointId, pendingFileSnapshots);
		}

		return new BucketState(bucketId, new Path(bucketPathStr), creationTime, current, pendingFileSnapshotsPerCheckpoint);
	}

	private SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> getResumableSerializer() {
		final OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileSnapshotSerializer
			outputStreamBasedInProgressFileSnapshotSerializer =
			(OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileSnapshotSerializer) inProgressFileSnapshotSerializer;
		return outputStreamBasedInProgressFileSnapshotSerializer.getResumeSerializer();
	}

	private SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> getCommitableSerializer() {
		final OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileSnapshotSerializer
			outputStreamBasedPendingFileSnapshotSerializer =
			(OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileSnapshotSerializer) pendingFileSnapshotSerializer;
		return outputStreamBasedPendingFileSnapshotSerializer.getCommitSerializer();
	}

	private Map<Long, List<RecoverableWriter.CommitRecoverable>> getCommittablesPerCheckpoint(BucketState<BucketID> bucketState) {
		final Map<Long, List<RecoverableWriter.CommitRecoverable>> committablesPerCheckpoint = new HashMap<>();
		for (Entry<Long, List<PartFileWriter.PendingFileSnapshot>> pendingFilesForCheckpoint : bucketState.getPendingFileSnapshots().entrySet()) {
			final List<PartFileWriter.PendingFileSnapshot> pendingFileSnapshotList = pendingFilesForCheckpoint.getValue();
			final List<RecoverableWriter.CommitRecoverable> commitRecoverableList = new ArrayList<>(pendingFileSnapshotList.size());
			committablesPerCheckpoint.put(pendingFilesForCheckpoint.getKey(), commitRecoverableList);
			for (PartFileWriter.PendingFileSnapshot pendingFileSnapshot : pendingFileSnapshotList) {
				OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileSnapshot outputStreamBasedPendingFileSnapshot =
					(OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileSnapshot) pendingFileSnapshot;
				commitRecoverableList.add(outputStreamBasedPendingFileSnapshot.getCommitRecoverable());
			}
		}
		return committablesPerCheckpoint;
	}

	private static void validateMagicNumber(DataInputView in) throws IOException {
		final int magicNumber = in.readInt();
		if (magicNumber != MAGIC_NUMBER) {
			throw new IOException(String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
		}
	}
}
