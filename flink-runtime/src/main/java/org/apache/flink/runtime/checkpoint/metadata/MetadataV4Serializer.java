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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A new version of checkpoint metadata serializer that stores whether an operator is finished.
 */
@Internal
public class MetadataV4Serializer extends MetadataV3v4SerializerBase implements MetadataSerializer {

	/** The metadata format version. */
	public static final int VERSION = 4;

	/** The singleton instance of the serializer. */
	public static final MetadataV4Serializer INSTANCE = new MetadataV4Serializer();

	/** Singleton, not meant to be instantiated. */
	private MetadataV4Serializer() {}

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
	public CheckpointMetadata deserialize(DataInputStream dis, ClassLoader userCodeClassLoader, String externalPointer) throws IOException {
		return deserializeMetadata(dis, externalPointer);
	}

	// ------------------------------------------------------------------------
	//  version-specific serialization formats
	// ------------------------------------------------------------------------

	@Override
	protected void serializeOperatorState(OperatorState operatorState, DataOutputStream dos) throws IOException {
		super.serializeOperatorState(operatorState, dos);
		dos.writeBoolean(operatorState.isFinished());
	}

	@Override
	protected OperatorState deserializeOperatorState(DataInputStream dis, @Nullable DeserializationContext context) throws IOException {
		OperatorState operatorState = super.deserializeOperatorState(dis, context);
		operatorState.setFinished(dis.readBoolean());
		return operatorState;
	}

	// ------------------------------------------------------------------------
	//  exposed static methods for test cases
	//
	//  NOTE: The fact that certain tests directly call these lower level
	//        serialization methods is a problem, because that way the tests
	//        bypass the versioning scheme. Especially tests that test for
	//        cross-version compatibility need to version themselves if we
	//        ever break the format of these low level state types.
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public static void serializeStreamStateHandle(StreamStateHandle stateHandle, DataOutputStream dos) throws IOException {
		MetadataV2V3SerializerBase.serializeStreamStateHandle(stateHandle, dos);
	}

	@VisibleForTesting
	public static StreamStateHandle deserializeStreamStateHandle(DataInputStream dis) throws IOException {
		return MetadataV2V3SerializerBase.deserializeStreamStateHandle(dis, null);
	}

	@VisibleForTesting
	public static void serializeOperatorStateHandleUtil(OperatorStateHandle stateHandle, DataOutputStream dos) throws IOException {
		INSTANCE.serializeOperatorStateHandle(stateHandle, dos);
	}

	@VisibleForTesting
	public static OperatorStateHandle deserializeOperatorStateHandleUtil(DataInputStream dis) throws IOException {
		return INSTANCE.deserializeOperatorStateHandle(dis, null);
	}

	@VisibleForTesting
	public static void serializeKeyedStateHandleUtil(
		KeyedStateHandle stateHandle,
		DataOutputStream dos) throws IOException {
		INSTANCE.serializeKeyedStateHandle(stateHandle, dos);
	}

	@VisibleForTesting
	public static KeyedStateHandle deserializeKeyedStateHandleUtil(DataInputStream dis) throws IOException {
		return INSTANCE.deserializeKeyedStateHandle(dis, null);
	}

	@VisibleForTesting
	public static StateObjectCollection<InputChannelStateHandle> deserializeInputChannelStateHandle(
		DataInputStream dis) throws IOException {
		return INSTANCE.deserializeInputChannelStateHandle(dis, null);
	}

	@VisibleForTesting
	public StateObjectCollection<ResultSubpartitionStateHandle> deserializeResultSubpartitionStateHandle(
		DataInputStream dis) throws IOException {
		return INSTANCE.deserializeResultSubpartitionStateHandle(dis, null);
	}
}
