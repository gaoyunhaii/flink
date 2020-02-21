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

import java.net.ProtocolException;

/**
 * The parser for messages without specific parser. It receives the whole
 * messages and then delegate the parsing to the targeted messages.
 */
class NonBufferResponseDecoder extends NettyMessageDecoder {

	/** The initial size of the message header cumulator buffer. */
	private static final int INITIAL_MESSAGE_HEADER_BUFFER_LENGTH = 128;

	/** The cumulation buffer of message header. */
	private ByteBuf messageBuffer;

	@Override
	public void onChannelActive(ChannelHandlerContext ctx) {
		messageBuffer = ctx.alloc().directBuffer(INITIAL_MESSAGE_HEADER_BUFFER_LENGTH);
	}

	@Override
	public ParseResult onChannelRead(ByteBuf data) throws Exception {
		ensureBufferCapacityIfNewMessage();

		ByteBuf toDecode = ByteBufUtils.cumulate(messageBuffer, data, messageLength);

		if (toDecode == null) {
			return ParseResult.notFinished();
		}

		NettyMessage nettyMessage;
		switch (msgId) {
			case NettyMessage.ErrorResponse.ID:
				nettyMessage = NettyMessage.ErrorResponse.readFrom(toDecode);
				break;
			default:
				throw new ProtocolException("Received unknown message from producer: " + msgId);
		}

		messageBuffer.clear();
		return ParseResult.finishedWith(nettyMessage);
	}

	private void ensureBufferCapacityIfNewMessage() {
		if (messageBuffer.writerIndex() == 0 && messageBuffer.capacity() < messageLength) {
			messageBuffer.capacity(messageLength);
		}
	}

	@Override
	public void close() {
		messageBuffer.release();
	}
}
