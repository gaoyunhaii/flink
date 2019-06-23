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
import org.apache.flink.runtime.io.network.TestingPartitionRequestClient;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.verifyBufferResponseHeader;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.verifyErrorResponse;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createRemoteInputChannel;
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
		testNettyMessageClientDecoding(false, false, false);
	}

	/**
	 * Verifies that the client side decoder works well for empty buffers. Empty buffers should not
	 * consume data buffers of the input channels.
	 */
	@Test
	public void testDownstreamMessageDecodeWithEmptyBuffers() throws Exception {
		testNettyMessageClientDecoding(true, false, false);
	}

	/**
	 * Verifies that NettyMessageDecoder works well with buffers sent to a released input channel. The data buffer
	 * part should be discarded before reading the next message.
	 */
	@Test
	public void testDownstreamMessageDecodeWithReleasedInputChannel() throws Exception {
		testNettyMessageClientDecoding(false, true, false);
	}

	/**
	 * Verifies that NettyMessageDecoder works well with buffers sent to a removed input channel. The data buffer
	 * part should be discarded before reading the next message.
	 */
	@Test
	public void testDownstreamMessageDecodeWithRemovedInputChannel() throws Exception {
		testNettyMessageClientDecoding(false, false, true);
	}

	//------------------------------------------------------------------------------------------------------------------

	private void testNettyMessageClientDecoding(
		boolean hasEmptyBuffer,
		boolean hasBufferForReleasedChannel,
		boolean hasBufferForRemovedChannel) throws Exception {

		int numberOfNormalBufferResponse = 5;

		CreditBasedPartitionRequestClientHandler handler = new CreditBasedPartitionRequestClientHandler();
		NetworkBufferPool networkBufferPool = new NetworkBufferPool(numberOfNormalBufferResponse, BUFFER_SIZE, numberOfNormalBufferResponse);

		SingleInputGate normalInputGate = createSingleInputGate(1);
		RemoteInputChannel normalInputChannel = createRemoteInputChannel(
			normalInputGate,
			new TestingPartitionRequestClient(),
			networkBufferPool);
		normalInputGate.assignExclusiveSegments();
		normalInputChannel.requestSubpartition(0);
		handler.addInputChannel(normalInputChannel);

		RemoteInputChannel releasedInputChannel = null;
		if (hasBufferForReleasedChannel) {
			SingleInputGate releasedInputGate = createSingleInputGate(1);
			releasedInputChannel = new InputChannelBuilder()
				.setMemorySegmentProvider(networkBufferPool)
				.buildRemoteAndSetToGate(normalInputGate);
			releasedInputGate.close();
			handler.addInputChannel(releasedInputChannel);
		}

		EmbeddedChannel channel = new EmbeddedChannel(new NettyMessageClientDecoderDelegate(handler));

		List<NettyMessage> messages = createMessageList(
			numberOfNormalBufferResponse,
			hasEmptyBuffer,
			hasBufferForReleasedChannel,
			hasBufferForRemovedChannel,
			normalInputChannel.getInputChannelId(),
			releasedInputChannel == null ? null : releasedInputChannel.getInputChannelId());

		ByteBuf[] encodedMessages = encodeMessages(messages);
		List<NettyMessage> decodedMessages = null;
		try {
			List<ByteBuf> partitionedBuffers = repartitionMessages(encodedMessages);
			decodedMessages = decodeMessages(channel, partitionedBuffers);
			verifyDecodedMessages(messages, decodedMessages, normalInputChannel.getInputChannelId());
		} finally {
			if (decodedMessages != null) {
				for (NettyMessage nettyMessage : decodedMessages) {
					if (nettyMessage instanceof BufferResponse) {
						((BufferResponse) nettyMessage).releaseBuffer();
					}
				}
			}
			normalInputGate.close();
			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();

			releaseBuffers(encodedMessages);
			channel.close();
		}
	}

	private List<NettyMessage> createMessageList(
		int numberOfNormalBufferResponse,
		boolean hasEmptyBuffer,
		boolean hasBufferForReleasedChannel,
		boolean hasBufferForRemovedChannel,
		InputChannelID normalInputChannelId,
		InputChannelID releasedInputChannelId) {

		int nextNormalInputChannelSeqNumber = 1;

		List<NettyMessage> messages = new ArrayList<>();
		for (int i = 0; i < numberOfNormalBufferResponse - 1; i++) {
			messages.add(new NettyMessage.BufferResponse(createDataBuffer(BUFFER_SIZE), nextNormalInputChannelSeqNumber++, normalInputChannelId, 1));
		}

		if (hasEmptyBuffer) {
			messages.add(new NettyMessage.BufferResponse(createDataBuffer(0), nextNormalInputChannelSeqNumber++, normalInputChannelId, 1));
		}

		if (hasBufferForReleasedChannel) {
			messages.add(new NettyMessage.BufferResponse(createDataBuffer(BUFFER_SIZE), 1, releasedInputChannelId, 1));
		}

		if (hasBufferForRemovedChannel) {
			messages.add(new NettyMessage.BufferResponse(createDataBuffer(BUFFER_SIZE), 1, new InputChannelID(), 1));
		}

		Buffer event = createDataBuffer(32);
		event.tagAsEvent();
		messages.add(new NettyMessage.BufferResponse(event, nextNormalInputChannelSeqNumber++, normalInputChannelId, 1));

		messages.add(new NettyMessage.BufferResponse(createDataBuffer(BUFFER_SIZE), nextNormalInputChannelSeqNumber, normalInputChannelId, 1));
		messages.add(new NettyMessage.ErrorResponse(new RuntimeException("test"), normalInputChannelId));

		return messages;
	}

	private ByteBuf[] encodeMessages(List<NettyMessage> messages) throws Exception {
		ByteBuf[] serializedBuffers = new ByteBuf[messages.size()];
		for (int i = 0; i < messages.size(); ++i) {
			serializedBuffers[i] = messages.get(i).write(ALLOCATOR);
		}

		return serializedBuffers;
	}

	private List<ByteBuf> repartitionMessages(ByteBuf[] encodedMessages) {
		List<ByteBuf> result = new ArrayList<>();

		ByteBuf firstHalfMergedBuffer = null;
		ByteBuf secondHalfMergedBuffer = null;

		try {
			firstHalfMergedBuffer = mergeBuffers(
				Arrays.copyOfRange(encodedMessages, 0, encodedMessages.length / 2));
			result.addAll(partitionBuffer(firstHalfMergedBuffer, BUFFER_SIZE * 2));

			secondHalfMergedBuffer = mergeBuffers(
				Arrays.copyOfRange(encodedMessages, encodedMessages.length / 2, encodedMessages.length));
			result.addAll(partitionBuffer(secondHalfMergedBuffer, BUFFER_SIZE / 4));

			return result;
		} finally {
			if (firstHalfMergedBuffer != null) {
				firstHalfMergedBuffer.release();
			}

			if (secondHalfMergedBuffer != null) {
				secondHalfMergedBuffer.release();
			}
		}
	}

	private ByteBuf mergeBuffers(ByteBuf[] buffers) {
		ByteBuf mergedBuffer = ALLOCATOR.buffer();
		for (ByteBuf buffer : buffers) {
			mergedBuffer.writeBytes(buffer);
		}

		return mergedBuffer;
	}

	private List<ByteBuf> partitionBuffer(ByteBuf buffer, int eachPartitionSize) {
		List<ByteBuf> result = new ArrayList<>();

		int bufferSize = buffer.readableBytes();
		for (int position = 0; position < bufferSize; position += eachPartitionSize) {
			int endPosition = Math.min(position + eachPartitionSize, bufferSize);
			ByteBuf partitionedBuffer = ALLOCATOR.buffer(endPosition - position);
			partitionedBuffer.writeBytes(buffer, position, endPosition - position);
			result.add(partitionedBuffer);
		}

		return result;
	}

	private List<NettyMessage> decodeMessages(EmbeddedChannel channel, List<ByteBuf> inputBuffers) {
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
