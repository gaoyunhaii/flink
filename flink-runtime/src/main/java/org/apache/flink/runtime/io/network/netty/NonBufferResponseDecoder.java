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
public class NonBufferResponseDecoder implements NettyMessageDecoder {

	/** The initial size of the message header cumulator buffer. */
	private static final int INITIAL_MESSAGE_HEADER_BUFFER_LENGTH = 128;

	/** The cumulation buffer of message header. */
	private ByteBuf messageBuffer;

	/** The type of messages under processing. */
	private int msgId = -1;

	/** The length of messages under processing. */
	private int messageLength;

	@Override
	public void onChannelActive(ChannelHandlerContext ctx) {
		messageBuffer = ctx.alloc().directBuffer(INITIAL_MESSAGE_HEADER_BUFFER_LENGTH);
	}

	@Override
	public void startParsingMessage(int msgId, int messageLength) {
		this.msgId = msgId;
		this.messageLength = messageLength;

		messageBuffer.clear();
		messageBuffer.capacity(messageLength);
	}

	@Override
	public ParseResult onChannelRead(ByteBuf data) throws Exception {
		ByteBuf toDecode = ByteBufUtils.cumulate(messageBuffer, data, messageLength);

		if (toDecode == null) {
			return ParseResult.notFinished();
		}

		switch (msgId) {
			case NettyMessage.ErrorResponse.ID:
				return ParseResult.finishedWith(NettyMessage.ErrorResponse.readFrom(toDecode));
			default:
				throw new ProtocolException("Received unknown message from producer: " + msgId);
		}
	}

	@Override
	public void close() {
		messageBuffer.release();
	}
}
