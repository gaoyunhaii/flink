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

import javax.annotation.Nullable;
import java.util.ArrayList;
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

	private static final int NUMBER_OF_BUFFER_RESPONSES = 5;

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
	 * Verifies that NettyMessageDecoder works well with buffers sent to a released input channel.
	 * The data buffer part should be discarded before reading the next message.
	 */
	@Test
	public void testDownstreamMessageDecodeWithReleasedInputChannel() throws Exception {
		testNettyMessageClientDecoding(false, true, false);
	}

	/**
	 * Verifies that NettyMessageDecoder works well with buffers sent to a removed input channel.
	 * The data buffer part should be discarded before reading the next message.
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

		CreditBasedPartitionRequestClientHandler handler = new CreditBasedPartitionRequestClientHandler();
		NetworkBufferPool networkBufferPool = new NetworkBufferPool(
			NUMBER_OF_BUFFER_RESPONSES,
			BUFFER_SIZE,
			NUMBER_OF_BUFFER_RESPONSES);
		EmbeddedChannel channel = new EmbeddedChannel(new NettyMessageClientDecoderDelegate(handler));

		SingleInputGate inputGate = createSingleInputGate(1);
		RemoteInputChannel inputChannel = createRemoteInputChannel(
			inputGate,
			new TestingPartitionRequestClient(),
			networkBufferPool);
		inputGate.assignExclusiveSegments();
		inputChannel.requestSubpartition(0);
		handler.addInputChannel(inputChannel);

		RemoteInputChannel releasedInputChannel = null;
		if (hasBufferForReleasedChannel) {
			SingleInputGate releasedInputGate = createSingleInputGate(1);
			releasedInputChannel = new InputChannelBuilder()
				.setMemorySegmentProvider(networkBufferPool)
				.buildRemoteAndSetToGate(inputGate);
			releasedInputGate.close();
			handler.addInputChannel(releasedInputChannel);
		}

		ByteBuf[] encodedMessages = null;
		List<NettyMessage> decodedMessages = null;
		try {
			List<NettyMessage> messages = createMessageList(
				hasEmptyBuffer,
				hasBufferForRemovedChannel,
				inputChannel.getInputChannelId(),
				releasedInputChannel == null ? null : releasedInputChannel.getInputChannelId());

			encodedMessages = encodeMessages(messages);

			List<ByteBuf> partitionedBuffers = repartitionMessages(encodedMessages);
			decodedMessages = decodeMessages(channel, partitionedBuffers);
			verifyDecodedMessages(messages, decodedMessages, inputChannel.getInputChannelId());
		} finally {
			if (decodedMessages != null) {
				for (NettyMessage nettyMessage : decodedMessages) {
					if (nettyMessage instanceof BufferResponse) {
						((BufferResponse) nettyMessage).releaseBuffer();
					}
				}
			}
			inputGate.close();
			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();

			if (encodedMessages != null) {
				releaseBuffers(encodedMessages);
			}
			channel.close();
		}
	}

	private List<NettyMessage> createMessageList(
		boolean hasEmptyBuffer,
		boolean hasBufferForRemovedChannel,
		InputChannelID inputChannelId,
		@Nullable InputChannelID releasedInputChannelId) {

		int seqNumber = 1;
		List<NettyMessage> messages = new ArrayList<>();

		for (int i = 0; i < NUMBER_OF_BUFFER_RESPONSES - 1; i++) {
			addBufferResponse(messages, inputChannelId, true, BUFFER_SIZE, seqNumber++);
		}

		if (hasEmptyBuffer) {
			addBufferResponse(messages, inputChannelId, true, 0, seqNumber++);
		}
		if (releasedInputChannelId != null) {
			addBufferResponse(messages, releasedInputChannelId, true, BUFFER_SIZE, seqNumber++);
		}
		if (hasBufferForRemovedChannel) {
			addBufferResponse(messages, new InputChannelID(), true, BUFFER_SIZE, seqNumber++);
		}

		addBufferResponse(messages, inputChannelId, false, 32, seqNumber++);
		addBufferResponse(messages, inputChannelId, true, BUFFER_SIZE, seqNumber);
		messages.add(new NettyMessage.ErrorResponse(new RuntimeException("test"), inputChannelId));

		return messages;
	}

	private void addBufferResponse(
		List<NettyMessage> messages,
		InputChannelID inputChannelId,
		boolean isBuffer,
		int bufferSize,
		int seqNumber) {

		Buffer buffer = createDataBuffer(bufferSize);
		if (!isBuffer) {
			buffer.tagAsEvent();
		}
		messages.add(new BufferResponse(buffer, seqNumber, inputChannelId, 1));
	}

	private ByteBuf[] encodeMessages(List<NettyMessage> messages) throws Exception {
		ByteBuf[] encodedMessages = new ByteBuf[messages.size()];
		for (int i = 0; i < messages.size(); ++i) {
			encodedMessages[i] = messages.get(i).write(ALLOCATOR);
		}

		return encodedMessages;
	}

	private List<ByteBuf> repartitionMessages(ByteBuf[] encodedMessages) {
		List<ByteBuf> result = new ArrayList<>();

		ByteBuf mergedBuffer1 = null;
		ByteBuf mergedBuffer2 = null;

		try {
			mergedBuffer1 = mergeBuffers(encodedMessages, 0, encodedMessages.length / 2);
			mergedBuffer2 = mergeBuffers(
				encodedMessages,
				encodedMessages.length / 2,
				encodedMessages.length);

			result.addAll(partitionBuffer(mergedBuffer1, BUFFER_SIZE * 2));
			result.addAll(partitionBuffer(mergedBuffer2, BUFFER_SIZE / 4));

			return result;
		} finally {
			releaseBuffers(mergedBuffer1, mergedBuffer2);
		}
	}

	private ByteBuf mergeBuffers(ByteBuf[] buffers, int start, int end) {
		ByteBuf mergedBuffer = ALLOCATOR.buffer();
		for (int i = start; i < end; ++i) {
			mergedBuffer.writeBytes(buffers[i]);
		}

		return mergedBuffer;
	}

	private List<ByteBuf> partitionBuffer(ByteBuf buffer, int partitionSize) {
		List<ByteBuf> result = new ArrayList<>();

		int bufferSize = buffer.readableBytes();
		for (int position = 0; position < bufferSize; position += partitionSize) {
			int endPosition = Math.min(position + partitionSize, bufferSize);
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
		InputChannelID inputChannelId) {

		assertEquals(expectedMessages.size(), decodedMessages.size());
		for (int i = 0; i < expectedMessages.size(); ++i) {
			assertEquals(expectedMessages.get(i).getClass(), decodedMessages.get(i).getClass());

			if (expectedMessages.get(i) instanceof NettyMessage.BufferResponse) {
				BufferResponse expected = (BufferResponse) expectedMessages.get(i);
				BufferResponse actual = (BufferResponse) decodedMessages.get(i);

				verifyBufferResponseHeader(expected, actual);
				if (expected.bufferSize == 0 || !expected.receiverId.equals(inputChannelId)) {
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

	private void releaseBuffers(ByteBuf... buffers) {
		for (ByteBuf buffer : buffers) {
			if (buffer != null) {
				buffer.release();
			}
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
