package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.IOUtils;

import java.io.IOException;

/**
 * The base class for all the part file writer that use {@link org.apache.flink.core.fs.RecoverableFsDataOutputStream}.
 * @param <IN> the element type
 * @param <BucketID> the bucket type
 */
public abstract class OutputStreamBasedPartFileWriter<IN, BucketID> implements PartFileWriter<IN, BucketID>{

	private final BucketID bucketID;

	private final long creationTime;

	protected final RecoverableFsDataOutputStream currentPartStream;

	private long lastUpdateTime;

	public OutputStreamBasedPartFileWriter(
		final BucketID bucketID,
		final RecoverableFsDataOutputStream recoverableFsDataOutputStream,
		final long createTime) {

		this.bucketID = bucketID;
		this.creationTime = createTime;
		this.currentPartStream = recoverableFsDataOutputStream;
		this.lastUpdateTime = createTime;
	}

	@Override
	public InProgressFileSnapshot persist() throws IOException {
		return new OutputStreamBasedInProgressSnapshot(currentPartStream.persist());
	}

	@Override
	public PendingFileSnapshot closeForCommit() throws IOException {
		return new OutputStreamBasedPendingFileSnapshot(currentPartStream.closeForCommit().getRecoverable());
	}

	@Override
	public void dispose() {
		// we can suppress exceptions here, because we do not rely on close() to
		// flush or persist any data
		IOUtils.closeQuietly(currentPartStream);
	}

	@Override
	public BucketID getBucketId() {
		return bucketID;
	}

	@Override
	public long getCreationTime() {
		return creationTime;
	}

	@Override
	public long getSize() throws IOException {
		return currentPartStream.getPos();
	}

	@Override
	public long getLastUpdateTime() {
		return lastUpdateTime;
	}

	void markWrite(long now) {
		this.lastUpdateTime = now;
	}

	abstract static class OutputStreamBasedPartFileFactory<IN, BucketID> implements PartFileFactory<IN, BucketID> {

		private final RecoverableWriter recoverableWriter;

		public OutputStreamBasedPartFileFactory(final RecoverableWriter recoverableWriter) throws IOException {
			this.recoverableWriter = recoverableWriter;
		}

		@Override
		public PartFileWriter<IN, BucketID> openNew(final BucketID bucketID, final Path path, final long creationTime) throws IOException {
			return openNew(bucketID, recoverableWriter.open(path), path, creationTime);
		}

		@Override
		public PartFileWriter<IN, BucketID> resumeFrom(final BucketID bucketID, final InProgressFileSnapshot inProgressFileSnapshot, final long creationTime) throws IOException {
			final OutputStreamBasedInProgressSnapshot outputStreamBasedInProgressSnapshot = (OutputStreamBasedInProgressSnapshot) inProgressFileSnapshot;
			return resumeFrom(
				bucketID,
				recoverableWriter.recover(outputStreamBasedInProgressSnapshot.getResumeRecoverable()),
				outputStreamBasedInProgressSnapshot.getResumeRecoverable(),
				creationTime);
		}

		@Override
		public void commitPendingFile(final PendingFileSnapshot pendingFileSnapshot) throws IOException {
			final RecoverableWriter.CommitRecoverable commitRecoverable =
				((OutputStreamBasedPendingFileSnapshot) pendingFileSnapshot).getCommitRecoverable();
			recoverableWriter.recoverForCommit(commitRecoverable);
		}

		@Override
		public boolean requiresCleanupOfInProgressFileSnapshot() {
			return recoverableWriter.requiresCleanupOfRecoverableState();
		}

		@Override
		public boolean cleanupInProgressFileSnapshot(InProgressFileSnapshot inProgressFileSnapshot) throws IOException {
			final RecoverableWriter.ResumeRecoverable resumeRecoverable =
				((OutputStreamBasedInProgressSnapshot) inProgressFileSnapshot).getResumeRecoverable();
			return recoverableWriter.cleanupRecoverableState(resumeRecoverable);
		}

		@Override
		public SimpleVersionedSerializer<PendingFileSnapshot> getPendingFileSnapshotSerializer() {
			return new OutputStreamBasedPendingFileSnapshotSerializer(recoverableWriter.getCommitRecoverableSerializer());
		}

		@Override
		public SimpleVersionedSerializer<InProgressFileSnapshot> getInProgressFileSnapshotSerializer() {
			return new OutputStreamBasedInProgressFileSnapshotSerializer(recoverableWriter.getResumeRecoverableSerializer());
		}

		@Override
		public boolean supportsResume() {
			return recoverableWriter.supportsResume();
		}

		public abstract PartFileWriter<IN, BucketID> openNew(
			final BucketID bucketId,
			final RecoverableFsDataOutputStream stream,
			final Path path,
			final long creationTime) throws IOException;

		public abstract PartFileWriter<IN, BucketID> resumeFrom(
			final BucketID bucketId,
			final RecoverableFsDataOutputStream stream,
			final RecoverableWriter.ResumeRecoverable resumable,
			final long creationTime) throws IOException;
	}

	static final class OutputStreamBasedPendingFileSnapshot implements PendingFileSnapshot {

		private final RecoverableWriter.CommitRecoverable commitRecoverable;

		public OutputStreamBasedPendingFileSnapshot(final RecoverableWriter.CommitRecoverable commitRecoverable) {
			this.commitRecoverable = commitRecoverable;
		}

		public RecoverableWriter.CommitRecoverable getCommitRecoverable() {
			return commitRecoverable;
		}
	}

	static final class OutputStreamBasedInProgressSnapshot implements InProgressFileSnapshot {

		private final RecoverableWriter.ResumeRecoverable resumeRecoverable;

		public OutputStreamBasedInProgressSnapshot(final RecoverableWriter.ResumeRecoverable resumeRecoverable) {
			this.resumeRecoverable = resumeRecoverable;
		}

		public RecoverableWriter.ResumeRecoverable getResumeRecoverable() {
			return resumeRecoverable;
		}
	}

	static class OutputStreamBasedInProgressFileSnapshotSerializer implements SimpleVersionedSerializer<InProgressFileSnapshot> {

		private static final int MAGIC_NUMBER = 0xb3a4073d;

		private final SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> resumeSerializer;

		public OutputStreamBasedInProgressFileSnapshotSerializer(SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> resumeSerializer) {
			this.resumeSerializer = resumeSerializer;
		}

		@Override
		public int getVersion() {
			return 1;
		}

		@Override
		public byte[] serialize(InProgressFileSnapshot inProgressSnapshot) throws IOException {
			OutputStreamBasedInProgressSnapshot outputStreamBasedInProgressSnapshot = (OutputStreamBasedInProgressSnapshot) inProgressSnapshot;
			DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(256);
			dataOutputSerializer.writeInt(MAGIC_NUMBER);
			serializeV1(outputStreamBasedInProgressSnapshot, dataOutputSerializer);
			return dataOutputSerializer.getCopyOfBuffer();
		}

		@Override
		public InProgressFileSnapshot deserialize(int version, byte[] serialized) throws IOException {
			switch (version) {
				case 1:
					DataInputView dataInputView = new DataInputDeserializer(serialized);
					validateMagicNumber(dataInputView);
					return deserializeV1(dataInputView);
				default:
					throw new IOException("Unrecognized version or corrupt state: " + version);
			}
		}

		public SimpleVersionedSerializer<RecoverableWriter.ResumeRecoverable> getResumeSerializer() {
			return resumeSerializer;
		}

		private void serializeV1(final OutputStreamBasedInProgressSnapshot outputStreamBasedInProgressSnapshot, final DataOutputView dataOutputView) throws IOException {
			SimpleVersionedSerialization.writeVersionAndSerialize(resumeSerializer, outputStreamBasedInProgressSnapshot.getResumeRecoverable(), dataOutputView);
		}

		private OutputStreamBasedInProgressSnapshot deserializeV1(final DataInputView dataInputView) throws IOException {
			return new OutputStreamBasedInProgressSnapshot(SimpleVersionedSerialization.readVersionAndDeSerialize(resumeSerializer, dataInputView));
		}

		private static void validateMagicNumber(final DataInputView dataInputView) throws IOException {
			final int magicNumber = dataInputView.readInt();
			if (magicNumber != MAGIC_NUMBER) {
				throw new IOException(String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
			}
		}
	}

	static class OutputStreamBasedPendingFileSnapshotSerializer implements SimpleVersionedSerializer<PendingFileSnapshot> {

		private static final int MAGIC_NUMBER = 0x2c853c89;

		private final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> commitSerializer;

		public OutputStreamBasedPendingFileSnapshotSerializer(final SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> commitSerializer) {
			this.commitSerializer = commitSerializer;
		}

		@Override
		public int getVersion() {
			return 1;
		}

		@Override
		public byte[] serialize(PendingFileSnapshot pendingFileSnapshot) throws IOException {
			OutputStreamBasedPendingFileSnapshot outputStreamBasedPendingFileSnapshot = (OutputStreamBasedPendingFileSnapshot) pendingFileSnapshot;
			DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(256);
			dataOutputSerializer.writeInt(MAGIC_NUMBER);
			serializeV1(outputStreamBasedPendingFileSnapshot, dataOutputSerializer);
			return dataOutputSerializer.getCopyOfBuffer();
		}

		@Override
		public PendingFileSnapshot deserialize(int version, byte[] serialized) throws IOException {
			switch (version) {
				case 1:
					DataInputDeserializer in = new DataInputDeserializer(serialized);
					validateMagicNumber(in);
					return deserializeV1(in);

				default:
					throw new IOException("Unrecognized version or corrupt state: " + version);
			}
		}

		public SimpleVersionedSerializer<RecoverableWriter.CommitRecoverable> getCommitSerializer() {
			return this.commitSerializer;
		}

		private void serializeV1(final OutputStreamBasedPendingFileSnapshot outputStreamBasedPendingFileSnapshot, final DataOutputView dataOutputView) throws IOException {
			SimpleVersionedSerialization.writeVersionAndSerialize(commitSerializer, outputStreamBasedPendingFileSnapshot.getCommitRecoverable(), dataOutputView);
		}

		private OutputStreamBasedPendingFileSnapshot deserializeV1(final DataInputView dataInputView) throws IOException {
			return new OutputStreamBasedPendingFileSnapshot(SimpleVersionedSerialization.readVersionAndDeSerialize(commitSerializer, dataInputView));
		}

		private static void validateMagicNumber(final DataInputView dataInputView) throws IOException {
			final int magicNumber = dataInputView.readInt();
			if (magicNumber != MAGIC_NUMBER) {
				throw new IOException(String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
			}
		}
	}

}
