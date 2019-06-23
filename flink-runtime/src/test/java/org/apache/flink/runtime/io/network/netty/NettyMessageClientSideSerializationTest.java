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
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.encodeAndDecode;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createRemoteInputChannel;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * Tests for the serialization and deserialization of the various {@link NettyMessage} sub-classes.
 */
@RunWith(Parameterized.class)
public class NettyMessageClientSideSerializationTest {

	private static final int BUFFER_SIZE = 1024;

	private static final BufferCompressor COMPRESSOR = new BufferCompressor(BUFFER_SIZE, "LZ4");

	private static final BufferDecompressor DECOMPRESSOR = new BufferDecompressor(BUFFER_SIZE, "LZ4");

	private final Random random = new Random();

	private NetworkBufferPool networkBufferPool;

	private SingleInputGate inputGate;

	private RemoteInputChannel inputChannel;

	private EmbeddedChannel channel;

	// ------------------------------------------------------------------------
	//  parameters
	// ------------------------------------------------------------------------

	private final boolean testReadOnlyBuffer;

	private final boolean testCompressedBuffer;

	@Parameterized.Parameters(name = "testReadOnlyBuffer = {0}, testCompressedBuffer = {1}")
	public static Collection<Object[]> testReadOnlyBuffer() {
		return Arrays.asList(new Object[][] {
			{false, false},
			{true, false},
			{false, true},
			{true, true}
		});
	}

	public NettyMessageClientSideSerializationTest(boolean testReadOnlyBuffer, boolean testCompressedBuffer) {
		this.testReadOnlyBuffer = testReadOnlyBuffer;
		this.testCompressedBuffer = testCompressedBuffer;
	}

	@Before
	public void setup() throws IOException, InterruptedException {
		networkBufferPool = new NetworkBufferPool(10, 1024, 2);
		BufferPool bufferPool = networkBufferPool.createBufferPool(8, 8);

		inputGate = new SingleInputGateBuilder()
			.setNumberOfChannels(1)
			.setBufferPoolFactory(bufferPool)
			.build();
		inputChannel = createRemoteInputChannel(
			inputGate,
			mock(PartitionRequestClient.class),
			networkBufferPool);
		inputGate.assignExclusiveSegments();
		inputChannel.requestSubpartition(0);

		CreditBasedPartitionRequestClientHandler handler = new CreditBasedPartitionRequestClientHandler();
		handler.addInputChannel(inputChannel);

		channel = new EmbeddedChannel(
			new NettyMessage.NettyMessageEncoder(), // outbound messages
			new NettyMessageClientDecoderDelegate(handler)); // inbound messages
	}

	@After
	public void tearDown() throws IOException {
		if (inputGate != null) {
			inputGate.close();
		}

		if (networkBufferPool != null) {
			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();
		}
	}

	@Test
	public void testEncodeDecode() {
		testEncodeDecodeBuffer(testReadOnlyBuffer, testCompressedBuffer);

		{
			{
				IllegalStateException expectedError = new IllegalStateException();
				InputChannelID receiverId = new InputChannelID();

				NettyMessage.ErrorResponse expected = new NettyMessage.ErrorResponse(expectedError, receiverId);
				NettyMessage.ErrorResponse actual = encodeAndDecode(expected, channel);

				assertEquals(expected.cause.getClass(), actual.cause.getClass());
				assertEquals(expected.cause.getMessage(), actual.cause.getMessage());
				assertEquals(receiverId, actual.receiverId);
			}

			{
				IllegalStateException expectedError = new IllegalStateException("Illegal illegal illegal");
				InputChannelID receiverId = new InputChannelID();

				NettyMessage.ErrorResponse expected = new NettyMessage.ErrorResponse(expectedError, receiverId);
				NettyMessage.ErrorResponse actual = encodeAndDecode(expected, channel);

				assertEquals(expected.cause.getClass(), actual.cause.getClass());
				assertEquals(expected.cause.getMessage(), actual.cause.getMessage());
				assertEquals(receiverId, actual.receiverId);
			}

			{
				IllegalStateException expectedError = new IllegalStateException("Illegal illegal illegal");

				NettyMessage.ErrorResponse expected = new NettyMessage.ErrorResponse(expectedError);
				NettyMessage.ErrorResponse actual = encodeAndDecode(expected, channel);

				assertEquals(expected.cause.getClass(), actual.cause.getClass());
				assertEquals(expected.cause.getMessage(), actual.cause.getMessage());
				assertNull(actual.receiverId);
				assertTrue(actual.isFatalError());
			}
		}
	}

	private void testEncodeDecodeBuffer(boolean testReadOnlyBuffer, boolean testCompressedBuffer) {
		NetworkBuffer buffer = new NetworkBuffer(MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE), FreeingBufferRecycler.INSTANCE);

		for (int i = 0; i < BUFFER_SIZE; i += 8) {
			buffer.writeLong(i);
		}

		Buffer testBuffer = testReadOnlyBuffer ? buffer.readOnlySlice() : buffer;
		if (testCompressedBuffer) {
			testBuffer = COMPRESSOR.compressToOriginalBuffer(buffer);
		}

		NettyMessage.BufferResponse expected = new NettyMessage.BufferResponse(
			testBuffer, random.nextInt(), inputChannel.getInputChannelId(), random.nextInt());
		NettyMessage.BufferResponse actual = encodeAndDecode(expected, channel);

		// Netty 4.1 is not copying the messages, but retaining slices of them. BufferResponse actual is in this case
		// holding a reference to the buffer. Buffer will be recycled only once "actual" will be released.
		assertTrue(buffer.isRecycled());
		assertTrue(testBuffer.isRecycled());

		assertNotNull(
			"The request input channel should always have available buffers in this test.",
			actual.getBuffer());
		ByteBuf retainedSlice = actual.getBuffer().asByteBuf();
		if (testCompressedBuffer) {
			assertTrue(actual.isCompressed);
			retainedSlice = decompress(retainedSlice);
		}

		// Ensure not recycled and same size as original buffer
		assertEquals(1, retainedSlice.refCnt());
		assertEquals(BUFFER_SIZE, retainedSlice.readableBytes());

		for (int i = 0; i < BUFFER_SIZE; i += 8) {
			assertEquals(i, retainedSlice.readLong());
		}

		// Release the retained slice
		actual.releaseBuffer();
		if (testCompressedBuffer) {
			retainedSlice.release();
		}
		assertEquals(0, retainedSlice.refCnt());
		assertTrue(buffer.isRecycled());
		assertTrue(testBuffer.isRecycled());

		assertEquals(expected.sequenceNumber, actual.sequenceNumber);
		assertEquals(expected.receiverId, actual.receiverId);
		assertEquals(expected.backlog, actual.backlog);
	}

	private ByteBuf decompress(ByteBuf buffer) {
		MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
		Buffer compressedBuffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
		buffer.readBytes(compressedBuffer.asByteBuf(), buffer.readableBytes());
		compressedBuffer.setCompressed(true);
		return DECOMPRESSOR.decompressToOriginalBuffer(compressedBuffer).asByteBuf();
	}
}
