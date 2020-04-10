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

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;

import java.io.IOException;

/**
 *
 */
public interface PartFileWriterInterface<IN, BucketID> extends PartFileInfo<BucketID>  {

	void write(IN element, long currentTime) throws IOException;

	PendingFileStatus closeForCommit() throws IOException;

	void dispose();

	void markWrite(long now);

	interface PendingFileCommitter {

		/**
		 * Commits the file, making it visible. The file will contain the exact data
		 * as when the committer was created.
		 *
		 * @throws IOException Thrown if committing fails.
		 */
		void commit() throws IOException;

		/**
		 * Commits the file, making it visible. The file will contain the exact data
		 * as when the committer was created.
		 *
		 * <p>This method tolerates situations where the file was already committed and
		 * will not raise an exception in that case. This is important for idempotent
		 * commit retries as they need to happen after recovery.
		 *
		 * @throws IOException Thrown if committing fails.
		 */
		void commitAfterRecovery() throws IOException;

		/**
		 * Gets a recoverable object to recover the committer. The recovered committer
		 * will commit the file with the exact same data as this committer would commit
		 * it.
		 */
		RecoverableWriter.CommitRecoverable getRecoverable();
	}

	interface PendingFileStatus {

	}

	/**
	 * An interface for factories that create the different {@link PartFileWriter writers}.
	 */
	interface PartFileFactory<IN, BucketID> {

		/**
		 * Used upon recovery from a failure to recover a {@link PartFileWriter writer}.
		 * @param bucketId the id of the bucket this writer is writing to.
		 * @param resumable the state of the stream we are resurrecting.
		 * @param creationTime the creation time of the stream.
		 * @return the recovered {@link PartFileWriter writer}.
		 * @throws IOException
		 */
		PartFileWriterInterface<IN, BucketID> resumeFrom(
			final BucketID bucketId,
			final FileSystem fileSystem,
			final RecoverableWriter recoverableWriter,
			final RecoverableWriter.ResumeRecoverable resumable,
			final long creationTime) throws IOException;

		/**
		 * Used to create a new {@link PartFileWriter writer}.
		 * @param bucketId the id of the bucket this writer is writing to.
		 * @param path the part this writer will write to.
		 * @param creationTime the creation time of the stream.
		 * @return the new {@link PartFileWriter writer}.
		 * @throws IOException
		 */
		PartFileWriterInterface<IN, BucketID> openNew(
			final BucketID bucketId,
			final FileSystem fileSystem,
			final RecoverableWriter recoverableWriter,
			final Path path,
			final long creationTime) throws IOException;

		void commitRecoveredFile(final BucketID bucketId,
											  final FileSystem fileSystem,
											  final RecoverableWriter recoverableWriter,
											  final PendingFileStatus status) throws IOException;
	}

}
