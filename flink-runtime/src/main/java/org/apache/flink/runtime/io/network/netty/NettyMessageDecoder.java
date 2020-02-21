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

/**
 * Parsers for specified netty messages.
 */
abstract class NettyMessageDecoder implements AutoCloseable {

	/** ID of message currently under decoding. */
	protected int msgId;

	/** Length of message currently under decoding */
	protected int messageLength;

	/**
	 * The result of message parsing with the provided data.
	 */
	static class ParseResult {
		private final static ParseResult NOT_FINISHED = new ParseResult(false, null);

		static ParseResult notFinished() {
			return NOT_FINISHED;
		}

		static ParseResult finishedWith(NettyMessage message) {
			return new ParseResult(true, message);
		}

		final boolean finished;

		final NettyMessage message;

		private ParseResult(boolean finished, NettyMessage message) {
			this.finished = finished;
			this.message = message;
		}
	}

	/**
	 * Notifies the underlying channel become active.
	 *
	 * @param ctx The context for the callback.
	 */
	abstract void onChannelActive(ChannelHandlerContext ctx);

	/**
	 * Notifies a new message is to be parsed.
	 *
	 * @param msgId The type of the message to be parsed.
	 * @param messageLength The length of the message to be parsed.
	 */
	void onNewMessage(int msgId, int messageLength) {
		this.msgId = msgId;
		this.messageLength = messageLength;
	}

	/**
	 * Notifies we have received more data for the current message.
	 *
	 * @param data The data received.
	 * @return The result of current pass of parsing.
	 */
	abstract ParseResult onChannelRead(ByteBuf data) throws Exception;
}
