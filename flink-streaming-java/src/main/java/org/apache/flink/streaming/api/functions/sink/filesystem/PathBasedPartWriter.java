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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;

import java.io.IOException;

public class PathBasedPartWriter<IN, BucketID> implements PartFileWriterInterface<IN, BucketID> {

	@Override
	public void write(IN element, long currentTime) throws IOException {

	}

	@Override
	public PendingFileStatus closeForCommit() throws IOException {
		return null;
	}

	@Override
	public void dispose() {

	}

	@Override
	public void markWrite(long now) {

	}

	@Override
	public BucketID getBucketId() {
		return null;
	}

	@Override
	public long getCreationTime() {
		return 0;
	}

	@Override
	public long getSize() throws IOException {
		return 0;
	}

	@Override
	public long getLastUpdateTime() {
		return 0;
	}

	/**
	 * A factory that creates {@link BulkPartWriter BulkPartWriters}.
	 * @param <IN> The type of input elements.
	 * @param <BucketID> The type of ids for the buckets, as returned by the {@link BucketAssigner}.
	 */
	static class Factory<IN, BucketID> implements PartFileWriter.PartFileFactory<IN, BucketID> {

		private final BulkWriter.PathBasedFactory<IN> writerFactory;

		Factory(BulkWriter.PathBasedFactory<IN> writerFactory) {
			this.writerFactory = writerFactory;
		}

		@Override
		public PathBasedPartWriter<IN, BucketID> resumeFrom(
			BucketID bucketId,
			FileSystem fileSystem,
			RecoverableWriter recoverableWriter,
			RecoverableWriter.ResumeRecoverable resumable,
			long creationTime) throws IOException {

			throw new IllegalStateException("Should not get here");
		}

		@Override
		public PathBasedPartWriter<IN, BucketID> openNew(
			BucketID bucketId,
			FileSystem fileSystem,
			RecoverableWriter recoverableWriter,
			Path path,
			long creationTime) throws IOException {

			return null;
		}
	}
}
