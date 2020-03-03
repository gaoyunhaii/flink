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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferProviderRemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.verifyBufferResponseHeader;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.verifyErrorResponse;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests the client side message decoder.
 */
public class NettyMessageClientDecoderDelegateTest {

	private static final int BUFFER_SIZE = 1024;

	private static final NettyBufferPool ALLOCATOR = new NettyBufferPool(1);

	/**
	 * Verifies that the client side decoder works well for unreleased input channels.
	 */
	@Test
	public void testDownstreamMessageDecode() throws Exception {
		int totalBufferRequired = 3;

		SingleInputGate inputGate = createSingleInputGate(1);
		RemoteInputChannel normalInputChannel = new BufferProviderRemoteInputChannel(inputGate, totalBufferRequired, BUFFER_SIZE);

		CreditBasedPartitionRequestClientHandler handler = new CreditBasedPartitionRequestClientHandler();
		handler.addInputChannel(normalInputChannel);

		EmbeddedChannel channel = new EmbeddedChannel(new NettyMessageClientDecoderDelegate(handler));

		testRepartitionMessagesAndDecode(
			channel,
			false,
			false,
			false,
			normalInputChannel.getInputChannelId(),
			null);
	}

	/**
	 * Verifies that the client side decoder works well for empty buffers. Empty buffers should not
	 * consume data buffers of the input channels.
	 */
	@Test
	public void testDownstreamMessageDecodeWithEmptyBuffers() throws Exception {
		int totalBufferRequired = 3;

		SingleInputGate inputGate = createSingleInputGate(1);
		RemoteInputChannel normalInputChannel = new BufferProviderRemoteInputChannel(inputGate, totalBufferRequired, BUFFER_SIZE);

		CreditBasedPartitionRequestClientHandler handler = new CreditBasedPartitionRequestClientHandler();
		handler.addInputChannel(normalInputChannel);

		EmbeddedChannel channel = new EmbeddedChannel(new NettyMessageClientDecoderDelegate(handler));

		testRepartitionMessagesAndDecode(
			channel,
			true,
			false,
			false,
			normalInputChannel.getInputChannelId(),
			null);
	}

	/**
	 * Verifies that NettyMessageDecoder works well with buffers sent to a released input channel. The data buffer
	 * part should be discarded before reading the next message.
	 */
	@Test
	public void testDownstreamMessageDecodeWithReleasedInputChannel() throws Exception {
		int totalBufferRequired = 3;

		SingleInputGate normalInputGate = createSingleInputGate(1);
		RemoteInputChannel normalInputChannel = new BufferProviderRemoteInputChannel(normalInputGate, totalBufferRequired, BUFFER_SIZE);

		SingleInputGate releasedInputGate = createSingleInputGate(1);
		RemoteInputChannel releasedInputChannel = new BufferProviderRemoteInputChannel(releasedInputGate, 0, BUFFER_SIZE);
		releasedInputGate.close();

		CreditBasedPartitionRequestClientHandler handler = new CreditBasedPartitionRequestClientHandler();
		handler.addInputChannel(normalInputChannel);
		handler.addInputChannel(releasedInputChannel);

		EmbeddedChannel channel = new EmbeddedChannel(new NettyMessageClientDecoderDelegate(handler));

		testRepartitionMessagesAndDecode(
			channel,
			false,
			true,
			false,
			normalInputChannel.getInputChannelId(),
			releasedInputChannel.getInputChannelId());
	}

	/**
	 * Verifies that NettyMessageDecoder works well with buffers sent to a removed input channel. The data buffer
	 * part should be discarded before reading the next message.
	 */
	@Test
	public void testDownstreamMessageDecodeWithRemovedInputChannel() throws Exception {
		int totalBufferRequired = 3;

		SingleInputGate normalInputGate = createSingleInputGate(1);
		RemoteInputChannel normalInputChannel = new BufferProviderRemoteInputChannel(normalInputGate, totalBufferRequired, BUFFER_SIZE);

		CreditBasedPartitionRequestClientHandler handler = new CreditBasedPartitionRequestClientHandler();
		handler.addInputChannel(normalInputChannel);

		EmbeddedChannel channel = new EmbeddedChannel(new NettyMessageClientDecoderDelegate(handler));

		testRepartitionMessagesAndDecode(
			channel,
			false,
			false,
			true,
			normalInputChannel.getInputChannelId(),
			null);
	}

	//------------------------------------------------------------------------------------------------------------------

	private void testRepartitionMessagesAndDecode(
		EmbeddedChannel channel,
		boolean hasEmptyBuffer,
		boolean hasBufferForReleasedChannel,
		boolean hasBufferForRemovedChannel,
		InputChannelID normalInputChannelId,
		InputChannelID releasedInputChannelId) throws Exception {

		List<NettyMessage> messages = createMessageList(
			hasEmptyBuffer,
			hasBufferForReleasedChannel,
			hasBufferForRemovedChannel,
			normalInputChannelId,
			releasedInputChannelId);

		ByteBuf[] serializedBuffers = null;
		ByteBuf mergedBuffer = null;

		try {
			serializedBuffers = serializeMessages(messages);
			int[] sizes = getBufferSizes(serializedBuffers);
			mergedBuffer = mergeBuffers(serializedBuffers);

			List<ByteBuf> partitionedBuffers = partitionBuffer(mergedBuffer, sizes);
			List<NettyMessage> decodedMessages = decodedMessages(channel, partitionedBuffers);

			verifyDecodedMessages(messages, decodedMessages, normalInputChannelId);
		} finally {
			if (serializedBuffers != null) {
				releaseBuffers(serializedBuffers);
			}

			if (mergedBuffer != null) {
				mergedBuffer.release();
			}

			channel.close();
		}
	}

	private List<NettyMessage> createMessageList(
		boolean hasEmptyBuffer,
		boolean hasBufferForReleasedChannel,
		boolean hasBufferForRemovedChannel,
		InputChannelID normalInputChannelId,
		InputChannelID releasedInputChannelId) {

		List<NettyMessage> messages = new ArrayList<>();

		Buffer event = createDataBuffer(32);
		event.tagAsEvent();

		messages.add(new NettyMessage.BufferResponse(createDataBuffer(128), 0, normalInputChannelId, 4));
		messages.add(new NettyMessage.BufferResponse(createDataBuffer(256), 1, normalInputChannelId, 3));

		if (hasEmptyBuffer) {
			messages.add(new NettyMessage.BufferResponse(createDataBuffer(0), 1, normalInputChannelId, 2));
		}

		if (hasBufferForReleasedChannel) {
			messages.add(new NettyMessage.BufferResponse(createDataBuffer(256), 1, releasedInputChannelId, 3));
		}

		if (hasBufferForRemovedChannel) {
			messages.add(new NettyMessage.BufferResponse(createDataBuffer(256), 1, new InputChannelID(), 3));
		}

		messages.add(new NettyMessage.BufferResponse(event, 2, normalInputChannelId, 4));
		messages.add(new NettyMessage.ErrorResponse(new RuntimeException("test"), normalInputChannelId));
		messages.add(new NettyMessage.BufferResponse(createDataBuffer(56), 3, normalInputChannelId, 4));

		return messages;
	}

	private ByteBuf[] serializeMessages(List<NettyMessage> messages) throws Exception {
		ByteBuf[] serializedBuffers = new ByteBuf[messages.size()];
		for (int i = 0; i < messages.size(); ++i) {
			serializedBuffers[i] = messages.get(i).write(ALLOCATOR);
		}

		return serializedBuffers;
	}

	private int[] getBufferSizes(ByteBuf[] buffers) {
		int[] sizes = new int[buffers.length];
		for (int i = 0; i < sizes.length; ++i) {
			sizes[i] = buffers[i].readableBytes();
		}

		return sizes;
	}

	private ByteBuf mergeBuffers(ByteBuf[] buffers) {
		ByteBuf allData = ALLOCATOR.buffer();
		for (ByteBuf buffer : buffers) {
			allData.writeBytes(buffer);
		}

		return allData;
	}

	private List<ByteBuf> partitionBuffer(ByteBuf buffer, int[] sizes) {
		// For each group of three messages [A, B, C], we repartition them as three buffers:
		//  1. First half of A.
		//  2. Second half of A + B + first half of C.
		//  3. Second half of C.
		List<Integer> partitionPositions = new ArrayList<>();

		int position = 0;
		partitionPositions.add(position);

		for (int i = 0; i < sizes.length / 3 * 3; i += 3) {
			position += sizes[i] / 2;
			partitionPositions.add(position);

			position += (sizes[i] / 2 + sizes[i + 1] + sizes[i + 2] / 2);
			partitionPositions.add(position);

			position += sizes[i + 2] / 2;
			partitionPositions.add(position);
		}

		// Partitions the remaining data into three parts averagely.
		int remainingBytes = buffer.readableBytes() - position;

		position += (remainingBytes / 3);
		partitionPositions.add(position);

		position += (remainingBytes / 3 * 2);
		partitionPositions.add(position);

		partitionPositions.add(buffer.readableBytes());

		// Partitions the buffers according to the selected partition positions.
		List<ByteBuf> result = new ArrayList<>();
		for (int i = 1; i < partitionPositions.size(); ++i) {
			ByteBuf partitionedBuffer = ALLOCATOR.buffer();
			partitionedBuffer.writeBytes(
				buffer,
				partitionPositions.get(i - 1),
				partitionPositions.get(i) - partitionPositions.get(i - 1));
			result.add(partitionedBuffer);
		}

		return result;
	}

	private List<NettyMessage> decodedMessages(EmbeddedChannel channel, List<ByteBuf> inputBuffers) {
		for (ByteBuf buffer : inputBuffers) {
			channel.writeInbound(buffer);
		}

		channel.runPendingTasks();

		List<NettyMessage> decodedMessages = new ArrayList<>();
		Object input;
		while ((input = channel.readInbound()) != null) {
			assertTrue(input instanceof NettyMessage);
			decodedMessages.add((NettyMessage) input);
		}

		return decodedMessages;
	}

	private void verifyDecodedMessages(
		List<NettyMessage> expectedMessages,
		List<NettyMessage> decodedMessages,
		InputChannelID normalChannelId) {

		assertEquals(expectedMessages.size(), decodedMessages.size());
		for (int i = 0; i < expectedMessages.size(); ++i) {
			assertEquals(expectedMessages.get(i).getClass(), decodedMessages.get(i).getClass());

			if (expectedMessages.get(i) instanceof NettyMessage.BufferResponse) {
				BufferResponse expected = (BufferResponse) expectedMessages.get(i);
				BufferResponse actual = (BufferResponse) decodedMessages.get(i);

				verifyBufferResponseHeader(expected, actual);
				if (expected.bufferSize == 0 || !expected.receiverId.equals(normalChannelId)) {
					assertNull(actual.getBuffer());
				} else {
					assertEquals(expected.getBuffer(), actual.getBuffer());
				}

			} else if (expectedMessages.get(i) instanceof NettyMessage.ErrorResponse) {
				verifyErrorResponse((ErrorResponse) expectedMessages.get(i), (ErrorResponse) decodedMessages.get(i));
			} else {
				fail("Unsupported message type");
			}
		}
	}

	private void releaseBuffers(ByteBuf[] buffers) {
		for (ByteBuf buffer : buffers) {
			buffer.release();
		}
	}

	private Buffer createDataBuffer(int size) {
		MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(size);
		NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
		for (int i = 0; i < size / 4; ++i) {
			buffer.writeInt(i);
		}

		return buffer;
	}
}
