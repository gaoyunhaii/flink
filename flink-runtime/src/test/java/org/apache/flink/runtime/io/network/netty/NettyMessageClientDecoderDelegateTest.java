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
import org.apache.flink.runtime.io.network.partition.consumer.BufferProviderInputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests the client side message decoder.
 */
public class NettyMessageClientDecoderDelegateTest {
	private static final NettyBufferPool ALLOCATOR = new NettyBufferPool(1);

	private static final InputChannelID NORMAL_CHANNEL_ID = new InputChannelID();

	private static final InputChannelID RELEASED_CHANNEL_ID = new InputChannelID();

	private static final InputChannelID REMOVED_CHANNEL_ID = new InputChannelID();

	/**
	 * Verifies that the client side decoder works well for unreleased input channels.
	 */
	@Test
	public void testDownstreamMessageDecode() throws Exception {
		// 3 buffers required.
		testRepartitionMessagesAndDecode(3, false, false, false);
	}

	/**
	 * Verifies that the client side decoder works well for empty buffers. Empty buffers should not
	 * consume data buffers of the input channels.
	 */
	@Test
	public void testDownstreamMessageDecodeWithEmptyBuffers() throws Exception {
		// 4 buffers required.
		testRepartitionMessagesAndDecode(4, true, false, false);
	}

	/**
	 * Verifies that NettyMessageDecoder works well with buffers sent to a released  and removed input channels.
	 * For such channels, no Buffer is available to receive the data buffer in the message, and the data buffer
	 * part should be discarded before reading the next message.
	 */
	@Test
	public void testDownstreamMessageDecodeWithReleasedAndRemovedInputChannel() throws Exception {
		// 3 buffers required.
		testRepartitionMessagesAndDecode(3, false, true, true);
	}

	//------------------------------------------------------------------------------------------------------------------

	private void testRepartitionMessagesAndDecode(
		int numberOfBuffersInNormalChannel,
		boolean hasEmptyBuffer,
		boolean hasBufferForReleasedChannel,
		boolean hasBufferForRemovedChannel) throws Exception {

		EmbeddedChannel channel = createPartitionRequestClientHandler(numberOfBuffersInNormalChannel);

		try {
			List<NettyMessage> messages = createMessageList(hasEmptyBuffer, hasBufferForReleasedChannel, hasBufferForRemovedChannel);
			repartitionMessagesAndVerifyDecoding(channel, messages);
		} finally {
			channel.close();
		}
	}

	private List<NettyMessage> createMessageList(
		boolean hasEmptyBuffer,
		boolean hasBufferForReleasedChannel,
		boolean hasBufferForRemovedChannel) {

		List<NettyMessage> messages = new ArrayList<>();

		Buffer event = createDataBuffer(32);
		event.tagAsEvent();

		messages.add(new NettyMessage.BufferResponse(createDataBuffer(128), 0, NORMAL_CHANNEL_ID, 4));
		messages.add(new NettyMessage.BufferResponse(createDataBuffer(256), 1, NORMAL_CHANNEL_ID, 3));

		if (hasEmptyBuffer) {
			messages.add(new NettyMessage.BufferResponse(createDataBuffer(0), 1, NORMAL_CHANNEL_ID, 2));
		}

		if (hasBufferForReleasedChannel) {
			messages.add(new NettyMessage.BufferResponse(createDataBuffer(256), 1, RELEASED_CHANNEL_ID, 3));
		}

		if (hasBufferForRemovedChannel) {
			messages.add(new NettyMessage.BufferResponse(createDataBuffer(256), 1, REMOVED_CHANNEL_ID, 3));
		}

		messages.add(new NettyMessage.BufferResponse(event, 2, NORMAL_CHANNEL_ID, 4));
		messages.add(new NettyMessage.ErrorResponse(new RuntimeException("test"), NORMAL_CHANNEL_ID));
		messages.add(new NettyMessage.BufferResponse(createDataBuffer(56), 3, NORMAL_CHANNEL_ID, 4));

		return messages;
	}

	private void repartitionMessagesAndVerifyDecoding(
		EmbeddedChannel channel,
		List<NettyMessage> messages) throws Exception {

		ByteBuf[] serializedBuffers = null;
		ByteBuf mergedBuffer = null;

		try {
			serializedBuffers = serializeMessages(messages);
			int[] sizes = getBufferSizes(serializedBuffers);
			mergedBuffer = mergeBuffers(serializedBuffers);

			List<ByteBuf> partitionedBuffers = partitionBuffer(mergedBuffer, sizes);
			List<NettyMessage> decodedMessages = decodedMessages(channel, partitionedBuffers);

			verifyDecodedMessages(messages, decodedMessages);
		} finally {
			if (serializedBuffers != null) {
				releaseBuffers(serializedBuffers);
			}

			if (mergedBuffer != null) {
				mergedBuffer.release();
			}
		}
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

	private void verifyDecodedMessages(List<NettyMessage> expected, List<NettyMessage> decodedMessages) {
		assertEquals(expected.size(), decodedMessages.size());
		for (int i = 0; i < expected.size(); ++i) {
			assertEquals(expected.get(i).getClass(), decodedMessages.get(i).getClass());

			if (expected.get(i) instanceof NettyMessage.BufferResponse) {
				BufferResponse expectedBufferResponse = (BufferResponse) expected.get(i);
				BufferResponse decodedBufferResponse = (BufferResponse) decodedMessages.get(i);

				assertEquals(expectedBufferResponse.backlog, decodedBufferResponse.backlog);
				assertEquals(expectedBufferResponse.sequenceNumber, decodedBufferResponse.sequenceNumber);
				assertEquals(expectedBufferResponse.bufferSize, decodedBufferResponse.bufferSize);
				assertEquals(expectedBufferResponse.receiverId, decodedBufferResponse.receiverId);

				if (expectedBufferResponse.bufferSize == 0 ||
					expectedBufferResponse.receiverId.equals(RELEASED_CHANNEL_ID) ||
					expectedBufferResponse.receiverId.equals(REMOVED_CHANNEL_ID)) {

					assertNull(decodedBufferResponse.getBuffer());
				} else if (expectedBufferResponse.receiverId.equals(NORMAL_CHANNEL_ID)) {
					assertEquals(expectedBufferResponse.getBuffer(), decodedBufferResponse.getBuffer());
				} else {
					fail("The decoded buffer response was sent to unknown input channel.");
				}
			} else if (expected.get(i) instanceof NettyMessage.ErrorResponse) {
				ErrorResponse expectedErrorResponse = (ErrorResponse) expected.get(i);
				ErrorResponse decodedErrorResponse = (ErrorResponse) decodedMessages.get(i);

				assertEquals(expectedErrorResponse.receiverId, decodedErrorResponse.receiverId);
				assertEquals(expectedErrorResponse.cause.getClass(), decodedErrorResponse.cause.getClass());
				assertEquals(expectedErrorResponse.cause.getMessage(), decodedErrorResponse.cause.getMessage());
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

	private EmbeddedChannel createPartitionRequestClientHandler(int numberOfBuffersInNormalChannel) throws IOException, InterruptedException {
		RemoteInputChannel normalInputChannel = new BufferProviderInputChannelBuilder()
			.setId(NORMAL_CHANNEL_ID)
			.setMaxNumberOfBuffers(numberOfBuffersInNormalChannel)
			.build();

		RemoteInputChannel releasedInputChannel = new BufferProviderInputChannelBuilder()
			.setId(RELEASED_CHANNEL_ID)
			.setMaxNumberOfBuffers(0)
			.setReleased(true)
			.build();

		CreditBasedPartitionRequestClientHandler handler = new CreditBasedPartitionRequestClientHandler();
		handler.addInputChannel(normalInputChannel);
		handler.addInputChannel(releasedInputChannel);

		return new EmbeddedChannel(new NettyMessageClientDecoderDelegate(handler));
	}
}
