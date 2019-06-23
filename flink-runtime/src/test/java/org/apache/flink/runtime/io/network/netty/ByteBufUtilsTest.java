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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

/**
 * Tests the methods in {@link ByteBufUtils}.
 */
public class ByteBufUtilsTest {

	@Test
	public void testAccumulateWithoutCopy() {
		final int sourceLength = 128;
		final int sourceStartPosition = 32;
		final int expectedAccumulationSize = 16;

		ByteBuf src = createSourceBuffer(sourceLength, sourceStartPosition);

		ByteBuf target = Unpooled.buffer(expectedAccumulationSize);

		// If src has enough data and no data has been copied yet, src will be returned without modification.
		ByteBuf accumulated = ByteBufUtils.accumulate(target, src, expectedAccumulationSize, target.readableBytes());

		assertSame(src, accumulated);
		assertEquals(sourceStartPosition, src.readerIndex());

		verifyBufferContent(src, sourceStartPosition, sourceLength - sourceStartPosition, sourceStartPosition);
	}

	@Test
	public void testAccumulateWithCopy() {
		final int firstSourceLength = 128;
		final int firstSourceStartPosition = 32;
		final int secondSourceLength = 64;
		final int secondSourceStartPosition = 0;
		final int expectedAccumulationSize = 128;

		final int firstCopyLength = firstSourceLength - firstSourceStartPosition;
		final int secondCopyLength = expectedAccumulationSize - firstCopyLength;

		ByteBuf firstSource = createSourceBuffer(firstSourceLength, firstSourceStartPosition);
		ByteBuf secondSource = createSourceBuffer(secondSourceLength, secondSourceStartPosition);

		ByteBuf target = Unpooled.buffer(expectedAccumulationSize);

		// If src does not have enough data, src will be copied into target and null will be returned.
		ByteBuf accumulated = ByteBufUtils.accumulate(
			target,
			firstSource,
			expectedAccumulationSize,
			target.readableBytes());
		assertNull(accumulated);
		assertEquals(firstSourceLength, firstSource.readerIndex());
		assertEquals(firstCopyLength, target.readableBytes());

		// The remaining data will be copied from the second buffer, and the target buffer will be returned
		// after all data is accumulated.
		accumulated = ByteBufUtils.accumulate(
			target,
			secondSource,
			expectedAccumulationSize,
			target.readableBytes());
		assertSame(target, accumulated);
		assertEquals(secondSourceStartPosition + secondCopyLength, secondSource.readerIndex());
		assertEquals(expectedAccumulationSize, target.readableBytes());

		verifyBufferContent(accumulated, 0, firstCopyLength, firstSourceStartPosition);
		verifyBufferContent(accumulated, firstCopyLength, secondCopyLength, secondSourceStartPosition);
	}

	private ByteBuf createSourceBuffer(int size, int readerIndex) {
		ByteBuf buf = Unpooled.buffer(size);
		for (int i = 0; i < size; ++i) {
			buf.writeByte((byte) i);
		}

		buf.readerIndex(readerIndex);

		return buf;
	}

	private void verifyBufferContent(ByteBuf buf, int start, int length, int startValue) {
		for (int i = 0; i < length; ++i) {
			byte b = buf.getByte(start + i);
			assertEquals((byte) (startValue + i), b);
		}
	}
}
