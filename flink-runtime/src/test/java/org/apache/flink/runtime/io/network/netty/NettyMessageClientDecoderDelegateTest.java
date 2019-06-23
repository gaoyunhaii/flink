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
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createRemoteInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Tests the client side message decoder.
 */
public class NettyMessageClientDecoderDelegateTest {
	private static final NettyBufferPool ALLOCATOR = new NettyBufferPool(1);

	/**
	 * Verifies that the client side decoder works well for unreleased input channels.
	 */
	@Test
	public void testDownstreamMessageDecode() throws Exception {
		// 6 buffers required for running 2 rounds and 3 buffers each round.
		NettyChannelAndInputChannelIds context = createPartitionRequestClientHandler(6);

		Supplier<NettyMessage[]> messagesSupplier = () -> {
			Buffer event = createDataBuffer(32);
			event.tagAsEvent();

			return new NettyMessage[]{
				new NettyMessage.BufferResponse(createDataBuffer(128), 0, context.getNormalChannelId(), 4),
				new NettyMessage.BufferResponse(createDataBuffer(256), 1, context.getNormalChannelId(), 3),
				new NettyMessage.BufferResponse(event, 2, context.getNormalChannelId(), 4),
				new NettyMessage.ErrorResponse(new RuntimeException("test"), context.getNormalChannelId()),
				new NettyMessage.BufferResponse(createDataBuffer(56), 3, context.getNormalChannelId(), 4)
			};
		};

		repartitionMessagesAndVerifyDecoding(
			context,
			messagesSupplier,
			(int[] sizes) -> new int[]{
				sizes[0] / 3,
				sizes[0] + sizes[1] + sizes[2] / 3,
				sizes[0] + sizes[1] + sizes[2] + sizes[3] / 3 * 2,
				sizes[0] + sizes[1] + sizes[2] + sizes[3] + sizes[4] / 3 * 2
			});

		repartitionMessagesAndVerifyDecoding(
			context,
			messagesSupplier,
			(int[] sizes) -> new int[]{
				sizes[0] / 3,
				sizes[0] + sizes[1] / 3,
				sizes[0] + sizes[1] + sizes[2] / 3,
				sizes[0] + sizes[1] + sizes[2] + sizes[3],
				sizes[0] + sizes[1] + sizes[2] + sizes[3] + sizes[4] / 3 * 2
			});
	}

	/**
	 * Verifies that the client side decoder works well for empty buffers. Empty buffers should not
	 * consume data buffers of the input channels.
	 */
	@Test
	public void testDownstreamMessageDecodeWithEmptyBuffers() throws Exception {
		// 4 buffers required for running 2 rounds and 2 buffers each round.
		NettyChannelAndInputChannelIds context = createPartitionRequestClientHandler(4);

		Supplier<NettyMessage[]> messagesSupplier = () -> {
			Buffer event = createDataBuffer(32);
			event.tagAsEvent();

			return new NettyMessage[]{
				new NettyMessage.BufferResponse(createDataBuffer(128), 0, context.getNormalChannelId(), 4),
				new NettyMessage.BufferResponse(createDataBuffer(0), 1, context.getNormalChannelId(), 3),
				new NettyMessage.BufferResponse(event, 2, context.getNormalChannelId(), 4),
				new NettyMessage.ErrorResponse(new RuntimeException("test"), context.getNormalChannelId()),
				new NettyMessage.BufferResponse(createDataBuffer(56), 3, context.getNormalChannelId(), 4)
			};
		};

		repartitionMessagesAndVerifyDecoding(
			context,
			messagesSupplier,
			(int[] sizes) -> new int[]{
				sizes[0] / 3,
				sizes[0] + sizes[1] + sizes[2] / 3 * 2,
				sizes[0] + sizes[1] + sizes[2] + sizes[3] / 3,
				sizes[0] + sizes[1] + sizes[2] + sizes[3] + sizes[4]
			});

		repartitionMessagesAndVerifyDecoding(
			context,
			messagesSupplier,
			(int[] sizes) -> new int[]{
				sizes[0] / 3,
				sizes[0] + sizes[1] / 3,
				sizes[0] + sizes[1] + sizes[2] / 3,
				sizes[0] + sizes[1] + sizes[2] + sizes[3],
				sizes[0] + sizes[1] + sizes[2] + sizes[3] + sizes[4] / 3 * 2
			});
	}

	/**
	 * Verifies that NettyMessageDecoder works well with buffers sent to a released  and removed input channels.
	 * For such channels, no Buffer is available to receive the data buffer in the message, and the data buffer
	 * part should be discarded before reading the next message.
	 */
	@Test
	public void testDownstreamMessageDecodeWithReleasedAndRemovedInputChannel() throws Exception {
		// 6 buffers required for running 2 rounds and 3 buffers each round.
		NettyChannelAndInputChannelIds context = createPartitionRequestClientHandler(6);

		Supplier<NettyMessage[]> messagesSupplier = () -> {
			Buffer event = createDataBuffer(32);
			event.tagAsEvent();

			Buffer event2 = createDataBuffer(128);
			event.tagAsEvent();

			return new NettyMessage[]{
				new NettyMessage.BufferResponse(createDataBuffer(128), 0, context.getNormalChannelId(), 4),
				new NettyMessage.BufferResponse(createDataBuffer(256), 1, context.getReleasedChannelId(), 3),
				new NettyMessage.BufferResponse(event, 2, context.getNormalChannelId(), 4),
				new NettyMessage.BufferResponse(createDataBuffer(256), 1, context.getRemovedChannelId(), 3),
				new NettyMessage.ErrorResponse(new RuntimeException("test"), context.getReleasedChannelId()),
				new NettyMessage.BufferResponse(event2, 2, context.getNormalChannelId(), 4),
				new NettyMessage.ErrorResponse(new RuntimeException("test"), context.getRemovedChannelId()),
				new NettyMessage.BufferResponse(createDataBuffer(64), 3, context.getNormalChannelId(), 4),
			};
		};

		repartitionMessagesAndVerifyDecoding(
			context,
			messagesSupplier,
			(int[] sizes) -> new int[]{
				sizes[0] / 3,
				sizes[0] + sizes[1] / 3,
				sizes[0] + sizes[1] + sizes[2] / 3,
				sizes[0] + sizes[1] + sizes[2],
				sizes[0] + sizes[1] + sizes[2] + sizes[3],
				sizes[0] + sizes[1] + sizes[2] + sizes[3] + sizes[4] / 3 * 2,
				sizes[0] + sizes[1] + sizes[2] + sizes[3] + sizes[4] + sizes[5] + sizes[6] / 2
			});

		repartitionMessagesAndVerifyDecoding(
			context,
			messagesSupplier,
			(int[] sizes) -> new int[]{
				sizes[0] / 3,
				sizes[0],
				sizes[0] + sizes[1] / 3,
				sizes[0] + sizes[1] / 3 * 2,
				sizes[0] + sizes[1] + sizes[2] / 3,
				sizes[0] + sizes[1] + sizes[2] + sizes[3] + sizes[4] / 3,
				sizes[0] + sizes[1] + sizes[2] + sizes[3] + sizes[4] + sizes[5] + sizes[6]
			});
	}

	//------------------------------------------------------------------------------------------------------------------

	private void repartitionMessagesAndVerifyDecoding(
		NettyChannelAndInputChannelIds context,
		Supplier<NettyMessage[]> messagesSupplier,
		Function<int[], int[]> rePartitioner) throws Exception {

		NettyMessage[] messages = messagesSupplier.get();

		ByteBuf[] serializedBuffers = null;
		ByteBuf mergedBuffer = null;

		try {
			serializedBuffers = serializeMessages(messages);
			int[] sizes = getBufferSizes(serializedBuffers);
			mergedBuffer = mergeBuffers(serializedBuffers);

			ByteBuf[] partitionedBuffers = partitionBuffer(mergedBuffer, rePartitioner.apply(sizes));
			List<NettyMessage> decodedMessages = decodedMessages(context, partitionedBuffers);

			verifyDecodedMessages(context, messages, decodedMessages);
		} finally {
			if (serializedBuffers != null) {
				releaseBuffers(serializedBuffers);
			}

			if (mergedBuffer != null) {
				mergedBuffer.release();
			}
		}
	}

	private ByteBuf[] serializeMessages(NettyMessage[] messages) throws Exception {
		ByteBuf[] serializedBuffers = new ByteBuf[messages.length];
		for (int i = 0; i < messages.length; ++i) {
			serializedBuffers[i] = messages[i].write(ALLOCATOR);
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

	private ByteBuf[] partitionBuffer(ByteBuf buffer, int[] partitionPositions) {
		ByteBuf[] result = new ByteBuf[partitionPositions.length + 1];
		for (int i = 0; i < result.length; ++i) {
			int startPos = (i == 0 ? 0 : partitionPositions[i - 1]);
			int endPos = (i < partitionPositions.length ? partitionPositions[i] : buffer.readableBytes());

			result[i] = ALLOCATOR.buffer();
			result[i].writeBytes(buffer, startPos, endPos - startPos);
		}

		return result;
	}

	private List<NettyMessage> decodedMessages(NettyChannelAndInputChannelIds context, ByteBuf[] inputBuffers) {
		EmbeddedChannel channel = context.getChannel();

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

	private void verifyDecodedMessages(NettyChannelAndInputChannelIds context, NettyMessage[] expected, List<NettyMessage> decodedMessages) {
		assertEquals(expected.length, decodedMessages.size());
		for (int i = 0; i < expected.length; ++i) {
			assertEquals(expected[i].getClass(), decodedMessages.get(i).getClass());

			if (expected[i] instanceof NettyMessage.BufferResponse) {
				BufferResponse expectedBufferResponse = (BufferResponse) expected[i];
				BufferResponse decodedBufferResponse = (BufferResponse) decodedMessages.get(i);

				assertEquals(expectedBufferResponse.backlog, decodedBufferResponse.backlog);
				assertEquals(expectedBufferResponse.sequenceNumber, decodedBufferResponse.sequenceNumber);
				assertEquals(expectedBufferResponse.bufferSize, decodedBufferResponse.bufferSize);
				assertEquals(expectedBufferResponse.receiverId, decodedBufferResponse.receiverId);

				if (expectedBufferResponse.bufferSize == 0 ||
					expectedBufferResponse.receiverId.equals(context.getReleasedChannelId()) ||
					expectedBufferResponse.receiverId.equals(context.getRemovedChannelId())) {

					assertNull(decodedBufferResponse.getBuffer());
				} else if (expectedBufferResponse.receiverId.equals(context.getNormalChannelId())) {
					assertEquals(expectedBufferResponse.getBuffer(), decodedBufferResponse.getBuffer());
				} else {
					fail("The decoded buffer response was sent to unknown input channel.");
				}
			} else if (expected[i] instanceof NettyMessage.ErrorResponse) {
				ErrorResponse expectedErrorResponse = (ErrorResponse) expected[i];
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

	private NettyChannelAndInputChannelIds createPartitionRequestClientHandler(int numberOfBuffersInNormalChannel) throws IOException, InterruptedException {
		NetworkBufferPool networkBufferPool = new NetworkBufferPool(
			numberOfBuffersInNormalChannel,
			32 * 1024,
			numberOfBuffersInNormalChannel);

		SingleInputGate normalInputGate = createSingleInputGate(1);
		RemoteInputChannel normalInputChannel = createRemoteInputChannel(
			normalInputGate,
			mock(PartitionRequestClient.class),
			networkBufferPool);
		normalInputGate.assignExclusiveSegments();
		normalInputChannel.requestSubpartition(0);

		SingleInputGate releasedInputGate = createSingleInputGate(1);
		RemoteInputChannel releasedInputChannel = createRemoteInputChannel(
			releasedInputGate,
			mock(PartitionRequestClient.class),
			networkBufferPool);
		releasedInputGate.close();

		CreditBasedPartitionRequestClientHandler handler = new CreditBasedPartitionRequestClientHandler();
		handler.addInputChannel(normalInputChannel);
		handler.addInputChannel(releasedInputChannel);

		EmbeddedChannel channel = new EmbeddedChannel(new NettyMessageClientDecoderDelegate(handler));
		return new NettyChannelAndInputChannelIds(
			channel,
			normalInputChannel.getInputChannelId(),
			releasedInputChannel.getInputChannelId(),
			new InputChannelID());
	}

	// ------------------------------------------------------------------------

	/**
	 * The context information for the tests.
	 */
	private static class NettyChannelAndInputChannelIds {
		private final EmbeddedChannel channel;
		private final InputChannelID normalChannelId;
		private final InputChannelID releasedChannelId;
		private final InputChannelID removedChannelId;

		public NettyChannelAndInputChannelIds(EmbeddedChannel channel, InputChannelID normalChannelId, InputChannelID releasedChannelId, InputChannelID removedChannelId) {
			this.channel = channel;
			this.normalChannelId = normalChannelId;
			this.releasedChannelId = releasedChannelId;
			this.removedChannelId = removedChannelId;
		}

		public EmbeddedChannel getChannel() {
			return channel;
		}

		public InputChannelID getNormalChannelId() {
			return normalChannelId;
		}

		public InputChannelID getReleasedChannelId() {
			return releasedChannelId;
		}

		public InputChannelID getRemovedChannelId() {
			return removedChannelId;
		}
	}
}
