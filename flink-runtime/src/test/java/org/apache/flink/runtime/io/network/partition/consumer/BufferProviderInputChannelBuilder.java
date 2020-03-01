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

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Builder for special {@link RemoteInputChannel} that correspond to buffer request, allow users to
 * set InputChannelId and release state.
 */
public class BufferProviderInputChannelBuilder {
	private SingleInputGate inputGate = new SingleInputGateBuilder().build();
	private InputChannelID id = new InputChannelID();
	private int maxNumberOfBuffers = Integer.MAX_VALUE;
	private int bufferSize = 32 * 1024;
	private boolean isReleased = false;

	public BufferProviderInputChannelBuilder setInputGate(SingleInputGate inputGate) {
		this.inputGate = inputGate;
		return this;
	}

	public BufferProviderInputChannelBuilder setId(InputChannelID id) {
		this.id = id;
		return this;
	}

	public BufferProviderInputChannelBuilder setMaxNumberOfBuffers(int maxNumberOfBuffers) {
		this.maxNumberOfBuffers = maxNumberOfBuffers;
		return this;
	}

	public BufferProviderInputChannelBuilder setBufferSize(int bufferSize) {
		this.bufferSize = bufferSize;
		return this;
	}

	public BufferProviderInputChannelBuilder setReleased(boolean isReleased) {
		this.isReleased = isReleased;
		return this;
	}

	public RemoteInputChannel build() {
		return new BufferProviderRemoteInputChannel(inputGate, id, maxNumberOfBuffers, bufferSize, isReleased);
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * Special {@link RemoteInputChannel} implementation.
	 */
	private static class BufferProviderRemoteInputChannel extends RemoteInputChannel {
		private final InputChannelID id;
		private final int maxNumberOfBuffers;
		private final int bufferSize;
		private final boolean isReleased;

		private int allocatedBuffers;

		BufferProviderRemoteInputChannel(
			SingleInputGate inputGate,
			InputChannelID id,
			int maxNumberOfBuffers,
			int bufferSize,
			boolean isReleased) {

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

			this.id = id;
			this.maxNumberOfBuffers = maxNumberOfBuffers;
			this.bufferSize = bufferSize;
			this.isReleased = isReleased;
		}

		@Override
		public InputChannelID getInputChannelId() {
			return id;
		}

		@Override
		public boolean isReleased() {
			return isReleased;
		}

		@Nullable
		@Override
		public Buffer requestBuffer() {
			if (isReleased) {
				return null;
			}

			checkState(allocatedBuffers < maxNumberOfBuffers,
				String.format("The number of allocated buffers %d have reached the maximum allowed %d.", allocatedBuffers, maxNumberOfBuffers));
			allocatedBuffers++;

			MemorySegment segment = MemorySegmentFactory.allocateUnpooledOffHeapMemory(bufferSize);
			return new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
		}
	}
}
