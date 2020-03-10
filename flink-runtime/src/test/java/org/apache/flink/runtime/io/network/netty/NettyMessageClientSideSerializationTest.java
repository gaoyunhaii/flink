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
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.ErrorResponse;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.NettyMessageEncoder;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.encodeAndDecode;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.verifyBufferResponseHeader;
import static org.apache.flink.runtime.io.network.netty.NettyTestUtil.verifyErrorResponse;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createRemoteInputChannel;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createSingleInputGate;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the serialization and deserialization of the various {@link NettyMessage} sub-classes
 * sent from server side to client side.
 */
@RunWith(Enclosed.class)
public class NettyMessageClientSideSerializationTest {

	/**
	 * Test the serialization of {@link BufferResponse}.
	 */
	@RunWith(Parameterized.class)
	public static class BufferResponseTest extends AbstractClientSideSerializationTest {

		private static final BufferCompressor COMPRESSOR = new BufferCompressor(BUFFER_SIZE, "LZ4");

		private static final BufferDecompressor DECOMPRESSOR = new BufferDecompressor(BUFFER_SIZE, "LZ4");

		private final Random random = new Random();

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

		public BufferResponseTest(boolean testReadOnlyBuffer, boolean testCompressedBuffer) {
			this.testReadOnlyBuffer = testReadOnlyBuffer;
			this.testCompressedBuffer = testCompressedBuffer;
		}

		@Test
		public void testBufferResponse() {
			NetworkBuffer buffer = new NetworkBuffer(
				MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE),
				FreeingBufferRecycler.INSTANCE);

			for (int i = 0; i < BUFFER_SIZE; i += 8) {
				buffer.writeLong(i);
			}

			Buffer testBuffer = testReadOnlyBuffer ? buffer.readOnlySlice() : buffer;
			if (testCompressedBuffer) {
				testBuffer = COMPRESSOR.compressToOriginalBuffer(buffer);
			}

			BufferResponse expected = new BufferResponse(
				testBuffer,
				random.nextInt(),
				inputChannelId,
				random.nextInt());
			BufferResponse actual = encodeAndDecode(expected, channel);

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

			// Release the received message.
			actual.releaseBuffer();
			if (testCompressedBuffer) {
				retainedSlice.release();
			}
			assertEquals(0, retainedSlice.refCnt());

			verifyBufferResponseHeader(expected, actual);
		}

		private ByteBuf decompress(ByteBuf buffer) {
			MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(BUFFER_SIZE);
			Buffer compressedBuffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
			buffer.readBytes(compressedBuffer.asByteBuf(), buffer.readableBytes());
			compressedBuffer.setCompressed(true);
			return DECOMPRESSOR.decompressToOriginalBuffer(compressedBuffer).asByteBuf();
		}
	}

	/**
	 * Test the serialization of messages other than BufferResponse.
	 */
	public static class NonBufferResponseTest extends AbstractClientSideSerializationTest {

		@Test
		public void testErrorResponseWithoutErrorMessage() {
			testErrorResponse(new ErrorResponse(new IllegalStateException(), new InputChannelID()));
		}

		@Test
		public void testErrorResponseWithErrorMessage() {
			testErrorResponse(new ErrorResponse(
				new IllegalStateException("Illegal illegal illegal"),
				new InputChannelID()));
		}

		@Test
		public void testErrorResponseWithFatalError() {
			testErrorResponse(new ErrorResponse(new IllegalStateException("Illegal illegal illegal")));
		}

		private void testErrorResponse(ErrorResponse expect) {
			ErrorResponse actual = encodeAndDecode(expect, channel);
			verifyErrorResponse(expect, actual);
		}
	}

	private static class AbstractClientSideSerializationTest {
		protected static final int BUFFER_SIZE = 1024;

		protected EmbeddedChannel channel;

		protected NetworkBufferPool networkBufferPool;

		protected SingleInputGate inputGate;

		protected InputChannelID inputChannelId;

		@Before
		public void setup() throws IOException, InterruptedException {
			networkBufferPool = new NetworkBufferPool(8, BUFFER_SIZE, 8);
			inputGate = createSingleInputGate(1);
			RemoteInputChannel inputChannel = createRemoteInputChannel(
				inputGate,
				new TestingPartitionRequestClient(),
				networkBufferPool);
			inputChannel.requestSubpartition(0);
			inputGate.assignExclusiveSegments();

			CreditBasedPartitionRequestClientHandler handler = new CreditBasedPartitionRequestClientHandler();
			handler.addInputChannel(inputChannel);

			channel = new EmbeddedChannel(
				new NettyMessageEncoder(), // For outbound messages
				new NettyMessageClientDecoderDelegate(handler)); // For inbound messages

			inputChannelId = inputChannel.getInputChannelId();
		}

		@After
		public void tearDown() throws IOException {
			inputGate.close();

			networkBufferPool.destroyAllBufferPools();
			networkBufferPool.destroy();

			channel.close();
		}
	}
}
