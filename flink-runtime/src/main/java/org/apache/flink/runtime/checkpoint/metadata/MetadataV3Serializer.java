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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.annotation.Internal;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * (De)serializer for checkpoint metadata format version 3.
 * This format was introduced with Apache Flink 1.11.0.
 *
 * <p>Compared to format version 2, this drops some unused fields and introduces
 * operator coordinator state.
 *
 * <p>See {@link MetadataV2V3SerializerBase} for a description of the format layout.
 */
@Internal
public class MetadataV3Serializer extends MetadataV3v4SerializerBase implements MetadataSerializer {

	/** The metadata format version. */
	public static final int VERSION = 3;

	/** The singleton instance of the serializer. */
	public static final MetadataV3Serializer INSTANCE = new MetadataV3Serializer();

	/** Singleton, not meant to be instantiated. */
	private MetadataV3Serializer() {}

	@Override
	public int getVersion() {
		return VERSION;
	}

	// ------------------------------------------------------------------------
	//  (De)serialization entry points
	// ------------------------------------------------------------------------

	public static void serialize(CheckpointMetadata checkpointMetadata, DataOutputStream dos) throws IOException {
		INSTANCE.serializeMetadata(checkpointMetadata, dos);
	}

	@Override
	public CheckpointMetadata deserialize(DataInputStream dis, ClassLoader classLoader, String externalPointer) throws IOException {
		return deserializeMetadata(dis, externalPointer);
	}
}
