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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Special {@link RemoteInputChannel} implementation that correspond to buffer request.
 */
public class BufferProviderRemoteInputChannel extends RemoteInputChannel {
	private final int maxNumberOfBuffers;
	private final int bufferSize;

	private int allocatedBuffers;

	public BufferProviderRemoteInputChannel(
		SingleInputGate inputGate,
		int maxNumberOfBuffers,
		int bufferSize) {

		super(
			inputGate,
			0,
			new ResultPartitionID(),
			InputChannelBuilder.STUB_CONNECTION_ID,
			new LocalConnectionManager(),
			0,
			0,
			InputChannelTestUtils.newUnregisteredInputChannelMetrics(),
			InputChannelTestUtils.StubMemorySegmentProvider.getInstance());

		inputGate.setInputChannel(new IntermediateResultPartitionID(), this);

		this.maxNumberOfBuffers = maxNumberOfBuffers;
		this.bufferSize = bufferSize;
	}

	@Nullable
	@Override
	public Buffer requestBuffer() {
		if (isReleased()) {
			return null;
		}

		checkState(allocatedBuffers < maxNumberOfBuffers,
			String.format("The number of allocated buffers %d have reached the maximum allowed %d.", allocatedBuffers, maxNumberOfBuffers));
		allocatedBuffers++;

		MemorySegment segment = MemorySegmentFactory.allocateUnpooledOffHeapMemory(bufferSize);
		return new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
	}
}
