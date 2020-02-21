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
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse.MESSAGE_HEADER_LENGTH;

/**
 * The parser for {@link org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse}.
 */
class BufferResponseDecoder extends NettyMessageDecoder {

	/** The Flink Buffer allocator. */
	private final NetworkBufferAllocator allocator;

	/** The cumulation buffer of message header. */
	private ByteBuf messageHeaderBuffer;

	/**
	 * The current BufferResponse message that are process the buffer part.
	 * If it is null, we are still processing the message header part, otherwise
	 * we are processing the buffer part.
	 */
	private BufferResponse currentResponse;

	/** How much bytes have been received or discarded for the buffer part. */
	private int decodedBytesOfBuffer;

	BufferResponseDecoder(NetworkBufferAllocator allocator) {
		this.allocator = allocator;
	}

	@Override
	public void onChannelActive(ChannelHandlerContext ctx) {
		messageHeaderBuffer = ctx.alloc().directBuffer(MESSAGE_HEADER_LENGTH);
	}

	@Override
	public ParseResult onChannelRead(ByteBuf data) throws Exception {
		if (currentResponse == null) {
			ByteBuf toDecode = ByteBufUtils.cumulate(messageHeaderBuffer, data, MESSAGE_HEADER_LENGTH);

			if (toDecode != null) {
				currentResponse = BufferResponse.readFrom(toDecode, allocator);
			}
		}

		if (currentResponse != null) {
			int remainingBufferSize = currentResponse.bufferSize - decodedBytesOfBuffer;
			int actualBytesToDecode = Math.min(data.readableBytes(), remainingBufferSize);

			if (actualBytesToDecode > 0) {
				if (currentResponse.isReleased) {
					data.readerIndex(data.readerIndex() + actualBytesToDecode);
				} else {
					currentResponse.getBuffer().asByteBuf().writeBytes(data, actualBytesToDecode);
				}

				decodedBytesOfBuffer += actualBytesToDecode;
			}

			if (decodedBytesOfBuffer == currentResponse.bufferSize) {
				BufferResponse result = currentResponse;
				clearState();
				return ParseResult.finishedWith(result);
			}
		}

		return ParseResult.notFinished();
	}

	private void clearState() {
		currentResponse = null;
		decodedBytesOfBuffer = 0;

		messageHeaderBuffer.clear();
	}

	@Override
	public void close() {
		if (currentResponse != null && currentResponse.getBuffer() != null) {
			currentResponse.getBuffer().recycleBuffer();
		}

		messageHeaderBuffer.release();
	}
}
