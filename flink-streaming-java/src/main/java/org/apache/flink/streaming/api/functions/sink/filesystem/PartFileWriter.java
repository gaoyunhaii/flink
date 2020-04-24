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
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * The {@link Bucket} uses the {@link PartFileWriter} to write element to a part file.
 */
@Internal
interface PartFileWriter<IN, BucketID> extends PartFileInfo<BucketID> {

	/**
	 * Write a element to the part file.
	 * @param element the element to be written
	 * @param currentTime the writing time
	 * @throws IOException
	 */
	void write(final IN element, final long currentTime) throws IOException;

	/**
	 * @return the state of the current part file.
	 * @throws IOException
	 */
	InProgressFileSnapshot persist() throws IOException;


	/**
	 * @return The state of the pending part file. {@link Bucket} uses this to commit the pending file.
	 * @throws IOException
	 */
	PendingFileSnapshot closeForCommit() throws IOException;

	/**
	 * TODO:: why there is a dispose method for the writer?
	 */
	void dispose();

	// ------------------------------------------------------------------------

	/**
	 * An interface for factories that create the different {@link PartFileWriter writers}.
	 */
	interface PartFileFactory<IN, BucketID> {

		/**
		 * Used to create a new {@link PartFileWriter}.
		 * @param bucketID the id of the bucket this writer is writing to.
		 * @param path the path this writer will write to.
		 * @param creationTime the creation time of the file.
		 * @return the new {@link PartFileWriter}
		 */
		PartFileWriter<IN, BucketID> openNew(
			final BucketID bucketID,
			final Path path,
			final long creationTime) throws IOException;

		/**
		 * Used to resume a {@link PartFileWriter} from a {@link InProgressFileSnapshot}.
		 * @param bucketID the id of the bucket this writer is writing to.
		 * @param inProgressFileSnapshot the state of the part file.
		 * @param creationTime the creation time of the file.
		 * @return the resumed {@link PartFileWriter}
		 */
		PartFileWriter<IN, BucketID> resumeFrom(
			final BucketID bucketID,
			final InProgressFileSnapshot inProgressFileSnapshot,
			final long creationTime) throws IOException;

		/**
		 * Used to commit the pending file.
		 * @param pendingFileSnapshot the file needed to be committed
		 */
		void commitPendingFile(final PendingFileSnapshot pendingFileSnapshot) throws IOException;

		/**
		 * Marks if requiring to do any additional cleanup/freeing of resources occupied
		 * as part of a {@link InProgressFileSnapshot}.
		 *
		 * <p>In case cleanup is required, then {@link #cleanupInProgressFileSnapshot(InProgressFileSnapshot)} should
		 * be called.
		 *
		 * @return {@code true} if cleanup is required, {@code false} otherwise.
		 */
		boolean requiresCleanupOfInProgressFileSnapshot();

		/**
		 * Frees up any resources that were previously occupied in order to be able to
		 * recover from a (potential) failure.
		 *
		 * <p><b>NOTE:</b> This operation should not throw an exception if the {@link InProgressFileSnapshot} has already
		 * been cleaned up and the resources have been freed. But the contract is that it will throw
		 * an {@link UnsupportedOperationException} if it is called for a {@link PartFileFactory}
		 * whose {@link #requiresCleanupOfInProgressFileSnapshot()} returns {@code false}.
		 *
		 * @param inProgressFileSnapshot the {@link InProgressFileSnapshot} whose state we want to clean-up.
		 * @return {@code true} if the resources were successfully freed, {@code false} otherwise
		 * (e.g. the file to be deleted was not there for any reason - already deleted or never created).
		 */
		boolean cleanupInProgressFileSnapshot(final InProgressFileSnapshot inProgressFileSnapshot) throws IOException;


		/**
		 * @return the serializer for the {@link PendingFileSnapshot}.
		 */
		SimpleVersionedSerializer<? extends PendingFileSnapshot> getPendingFileSnapshotSerializer();

		/**
		 * @return the serializer for the {@link InProgressFileSnapshot}.
		 */
		SimpleVersionedSerializer<? extends InProgressFileSnapshot> getInProgressFileSnapshotSerializer();

		/**
		 * Checks whether the {@link PartFileWriter} supports resuming (appending to) files after
		 * recovery (via the {@link #resumeFrom(Object, InProgressFileSnapshot, long)} method).
		 *
		 * <p>If true, then this writer supports the {@link #resumeFrom(Object, InProgressFileSnapshot, long)} method.
		 * If false, then that method may not be supported and file can only be recovered via
		 * {@link #commitPendingFile(PendingFileSnapshot)}.
		 * TODO:: why we needs this? if the
		 */
		boolean supportsResume();
	}

	/**
	 * This represents the state of the in-progress file and we could use it to recover the writer of the in-progress file.
	 */
	interface InProgressFileSnapshot extends PendingFileSnapshot {}

	/**
	 * This represents the file that can not write any data to.
	 */
	interface PendingFileSnapshot {}
}
