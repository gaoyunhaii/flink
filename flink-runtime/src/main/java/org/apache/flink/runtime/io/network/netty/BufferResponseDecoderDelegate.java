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

import org.apache.flink.runtime.io.network.NetworkClientHandler;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBufAllocator;

/**
 * The parser for {@link org.apache.flink.runtime.io.network.netty.NettyMessage.BufferResponse}.
 */
public class BufferResponseDecoderDelegate implements NettyMessageDecoderDelegate {

	/** The network client handler of current channel. */
	private final NetworkClientHandler networkClientHandler;

	/** The Flink Buffer allocator. */
	private final NetworkBufferAllocator allocator;

	/** The cumulation buffer of message header. */
	private ByteBuf messageHeaderCumulationBuffer;

	/**
	 * The current BufferResponse message that are process the buffer part.
	 * If it is null, we are still processing the message header part, otherwise
	 * we are processing the buffer part.
	 */
	private NettyMessage.BufferResponse currentResponse;

	/** How much bytes have been received or discarded for the buffer part. */
	private int decodedBytesOfBuffer;

	public BufferResponseDecoderDelegate(NetworkClientHandler networkClientHandler) {
		this.networkClientHandler = networkClientHandler;
		this.allocator = new NetworkBufferAllocator(networkClientHandler);
	}

	@Override
	public void onChannelActive(ByteBufAllocator alloc) {
		messageHeaderCumulationBuffer = alloc.directBuffer(NettyMessage.BufferResponse.MESSAGE_HEADER_LENGTH);
	}

	@Override
	public void startParsingMessage(int msgId, int messageLength) {
		currentResponse = null;
		decodedBytesOfBuffer = 0;

		messageHeaderCumulationBuffer.clear();
	}

	@Override
	public ParseResult onChannelRead(ByteBuf data) throws Exception {
		if (currentResponse == null) {
			ByteBuf toDecode = ByteBufUtils.cumulate(messageHeaderCumulationBuffer, data, NettyMessage.BufferResponse.MESSAGE_HEADER_LENGTH);

			if (toDecode != null) {
				currentResponse = NettyMessage.BufferResponse.readFrom(toDecode, allocator);

				if (currentResponse.bufferSize == 0) {
					return ParseResult.finishedWith(currentResponse);
				}
			}
		}

		if (currentResponse != null) {
			boolean isDiscarding = allocator.isPlaceHolderBuffer(currentResponse.getBuffer());
			int remainingBufferSize = currentResponse.bufferSize - decodedBytesOfBuffer;
			int actualBytesToDecode = Math.min(data.readableBytes(), remainingBufferSize);

			if (isDiscarding) {
				data.readerIndex(data.readerIndex() + actualBytesToDecode);
			} else {
				currentResponse.getBuffer().asByteBuf().writeBytes(data, actualBytesToDecode);
			}

			decodedBytesOfBuffer += actualBytesToDecode;

			if (decodedBytesOfBuffer == currentResponse.bufferSize) {
				if (isDiscarding) {
					networkClientHandler.cancelRequestFor(currentResponse.receiverId);
					return ParseResult.finishedWith(null);
				} else {
					return ParseResult.finishedWith(currentResponse);
				}
			}
		}

		return ParseResult.notFinished();
	}

	@Override
	public void release() {
		if (currentResponse != null && currentResponse.getBuffer() != null) {
			currentResponse.getBuffer().recycleBuffer();
		}

		messageHeaderCumulationBuffer.release();
	}
}
