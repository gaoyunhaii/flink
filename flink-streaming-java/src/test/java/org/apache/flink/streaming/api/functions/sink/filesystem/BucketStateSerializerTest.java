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

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for the {@link BucketStateSerializer}.
 */
public class BucketStateSerializerTest {

	private static final String IN_PROGRESS_CONTENT = "writing";
	private static final String PENDING_CONTENT = "wrote";

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testSerializationEmpty() throws IOException {
		final File testFolder = tempFolder.newFolder();
		final FileSystem fs = FileSystem.get(testFolder.toURI());
		final RecoverableWriter writer = fs.createRecoverableWriter();

		final Path testBucket = new Path(testFolder.getPath(), "test");

		final BucketState<String> bucketState = new BucketState<>(
				"test", testBucket, Long.MAX_VALUE, null, new HashMap<>());

		final SimpleVersionedSerializer<BucketState<String>> serializer =
				new BucketStateSerializer<>(
						new OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileSnapshotSerializer(writer.getResumeRecoverableSerializer()),
						new OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileSnapshotSerializer(writer.getCommitRecoverableSerializer()),
						SimpleVersionedStringSerializer.INSTANCE
				);

		byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializer, bucketState);
		final BucketState<String> recoveredState =  SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);

		Assert.assertEquals(testBucket, recoveredState.getBucketPath());
		Assert.assertNull(recoveredState.getInProgressFileSnapshot());
		Assert.assertTrue(recoveredState.getPendingFileSnapshots().isEmpty());
	}

	@Test
	public void testSerializationOnlyInProgress() throws IOException {
		final File testFolder = tempFolder.newFolder();
		final FileSystem fs = FileSystem.get(testFolder.toURI());

		final Path testBucket = new Path(testFolder.getPath(), "test");

		final RecoverableWriter writer = fs.createRecoverableWriter();
		final RecoverableFsDataOutputStream stream = writer.open(testBucket);
		stream.write(IN_PROGRESS_CONTENT.getBytes(Charset.forName("UTF-8")));

		final RecoverableWriter.ResumeRecoverable current = stream.persist();

		final BucketState<String> bucketState = new BucketState<>(
				"test", testBucket, Long.MAX_VALUE, new OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressSnapshot(current), new HashMap<>());

		final SimpleVersionedSerializer<BucketState<String>> serializer =
				new BucketStateSerializer<>(
					new OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileSnapshotSerializer(writer.getResumeRecoverableSerializer()),
					new OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileSnapshotSerializer(writer.getCommitRecoverableSerializer()),
						SimpleVersionedStringSerializer.INSTANCE
				);

		final byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializer, bucketState);

		// to simulate that everything is over for file.
		stream.close();

		final BucketState<String> recoveredState =  SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);

		Assert.assertEquals(testBucket, recoveredState.getBucketPath());

		FileStatus[] statuses = fs.listStatus(testBucket.getParent());
		Assert.assertEquals(1L, statuses.length);
		Assert.assertTrue(
				statuses[0].getPath().getPath().startsWith(
						(new Path(testBucket.getParent(), ".test.inprogress")).getPath())
		);
	}

	@Test
	public void testSerializationFull() throws IOException {
		final int noOfTasks = 5;

		final File testFolder = tempFolder.newFolder();
		final FileSystem fs = FileSystem.get(testFolder.toURI());
		final RecoverableWriter writer = fs.createRecoverableWriter();
		final PartFileWriter.PartFileFactory partFileFactory = new RowWisePartWriter.Factory(writer, new SimpleStringEncoder());

		final Path bucketPath = new Path(testFolder.getPath());

		// pending for checkpoints
		final Map<Long, List<PartFileWriter.PendingFileSnapshot>> pendingFileSnapshotList = new HashMap<>();
		for (int i = 0; i < noOfTasks; i++) {
			final List<PartFileWriter.PendingFileSnapshot> pendingFileSnapshots = new ArrayList<>();
			for (int j = 0; j < 2 + i; j++) {
				final Path part = new Path(bucketPath, "part-" + i + '-' + j);

				final RecoverableFsDataOutputStream stream = writer.open(part);
				stream.write((PENDING_CONTENT + '-' + j).getBytes(Charset.forName("UTF-8")));
				pendingFileSnapshots.add(new OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileSnapshot(stream.closeForCommit().getRecoverable()));
			}
			pendingFileSnapshotList.put((long) i, pendingFileSnapshots);
		}

		// in-progress
		final Path testBucket = new Path(bucketPath, "test-2");
		final RecoverableFsDataOutputStream stream = writer.open(testBucket);
		stream.write(IN_PROGRESS_CONTENT.getBytes(Charset.forName("UTF-8")));

		final RecoverableWriter.ResumeRecoverable current = stream.persist();

		final BucketState<String> bucketState = new BucketState<>(
				"test-2",
			bucketPath,
			Long.MAX_VALUE,
			new OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressSnapshot(current),
			pendingFileSnapshotList);
		final SimpleVersionedSerializer<BucketState<String>> serializer =
				new BucketStateSerializer<>(
						new OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileSnapshotSerializer(writer.getResumeRecoverableSerializer()),
						new OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileSnapshotSerializer(writer.getCommitRecoverableSerializer()),
						SimpleVersionedStringSerializer.INSTANCE
				);
		stream.close();

		byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializer, bucketState);

		final BucketState<String> recoveredState =  SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);

		Assert.assertEquals(bucketPath, recoveredState.getBucketPath());

		final Map<Long, List<PartFileWriter.PendingFileSnapshot>> recoveredRecoverables = recoveredState.getPendingFileSnapshots();
		Assert.assertEquals(5L, recoveredRecoverables.size());

		// recover and commit
		for (Map.Entry<Long, List<PartFileWriter.PendingFileSnapshot>> entry: recoveredRecoverables.entrySet()) {
			for (PartFileWriter.PendingFileSnapshot pendingFileSnapshot: entry.getValue()) {
				partFileFactory.commitPendingFile(pendingFileSnapshot);
			}
		}

		FileStatus[] filestatuses = fs.listStatus(bucketPath);
		Set<String> paths = new HashSet<>(filestatuses.length);
		for (FileStatus filestatus : filestatuses) {
			paths.add(filestatus.getPath().getPath());
		}

		for (int i = 0; i < noOfTasks; i++) {
			for (int j = 0; j < 2 + i; j++) {
				final String part = new Path(bucketPath, "part-" + i + '-' + j).getPath();
				Assert.assertTrue(paths.contains(part));
				paths.remove(part);
			}
		}

		// only the in-progress must be left
		Assert.assertEquals(1L, paths.size());

		// verify that the in-progress file is still there
		Assert.assertTrue(paths.iterator().next().startsWith(
				(new Path(testBucket.getParent(), ".test-2.inprogress").getPath())));
	}

	@Test
	public void testSerializationNullInProgress() throws IOException {
		final int noOfTasks = 5;

		final File testFolder = tempFolder.newFolder();
		final FileSystem fs = FileSystem.get(testFolder.toURI());
		final RecoverableWriter writer = fs.createRecoverableWriter();
		final PartFileWriter.PartFileFactory partFileFactory = new RowWisePartWriter.Factory(writer, new SimpleStringEncoder());

		final Path bucketPath = new Path(testFolder.getPath());

		// pending for checkpoints
		final Map<Long, List<PartFileWriter.PendingFileSnapshot>> pendingFileSnapshotsList = new HashMap<>();
		for (int i = 0; i < noOfTasks; i++) {
			final List<PartFileWriter.PendingFileSnapshot> pendingFileSnapshots = new ArrayList<>();
			for (int j = 0; j < 2 + i; j++) {
				final Path part = new Path(bucketPath, "test-" + i + '-' + j);

				final RecoverableFsDataOutputStream stream = writer.open(part);
				stream.write((PENDING_CONTENT + '-' + j).getBytes(Charset.forName("UTF-8")));
				pendingFileSnapshots.add(new OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileSnapshot(stream.closeForCommit().getRecoverable()));
			}
			pendingFileSnapshotsList.put((long) i, pendingFileSnapshots);
		}

		final RecoverableWriter.ResumeRecoverable current = null;

		final BucketState<String> bucketState = new BucketState<>(
				"", bucketPath, Long.MAX_VALUE, new OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressSnapshot(current), pendingFileSnapshotsList);

		final SimpleVersionedSerializer<BucketState<String>> serializer = new BucketStateSerializer<>(
				new OutputStreamBasedPartFileWriter.OutputStreamBasedInProgressFileSnapshotSerializer(writer.getResumeRecoverableSerializer()),
				new OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileSnapshotSerializer(writer.getCommitRecoverableSerializer()),
				SimpleVersionedStringSerializer.INSTANCE
		);

		byte[] bytes = SimpleVersionedSerialization.writeVersionAndSerialize(serializer, bucketState);

		final BucketState<String> recoveredState =  SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, bytes);

		Assert.assertEquals(bucketPath, recoveredState.getBucketPath());
		Assert.assertNull(recoveredState.getInProgressFileSnapshot());

		final Map<Long, List<PartFileWriter.PendingFileSnapshot>> recoveredRecoverables = recoveredState.getPendingFileSnapshots();
		Assert.assertEquals(5L, recoveredRecoverables.size());

		// recover and commit
		for (Map.Entry<Long, List<PartFileWriter.PendingFileSnapshot>> entry: recoveredRecoverables.entrySet()) {
			for (PartFileWriter.PendingFileSnapshot pendingFileSnapshot: entry.getValue()) {
				partFileFactory.commitPendingFile(pendingFileSnapshot);
			}
		}

		FileStatus[] filestatuses = fs.listStatus(bucketPath);
		Set<String> paths = new HashSet<>(filestatuses.length);
		for (FileStatus filestatus : filestatuses) {
			paths.add(filestatus.getPath().getPath());
		}

		for (int i = 0; i < noOfTasks; i++) {
			for (int j = 0; j < 2 + i; j++) {
				final String part = new Path(bucketPath, "test-" + i + '-' + j).getPath();
				Assert.assertTrue(paths.contains(part));
				paths.remove(part);
			}
		}

		// only the in-progress must be left
		Assert.assertTrue(paths.isEmpty());
	}
}
