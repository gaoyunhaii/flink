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

import javax.annotation.Nullable;

/**
 * Parsers for specified netty messages.
 */
public interface NettyMessageDecoder extends AutoCloseable {

	/**
	 * The result of message parsing with the provided data.
	 */
	class ParseResult {
		private final static ParseResult NOT_FINISHED = new ParseResult(false, null);

		static ParseResult notFinished() {
			return NOT_FINISHED;
		}

		static ParseResult finishedWith(@Nullable NettyMessage message) {
			return new ParseResult(true, message);
		}

		final boolean finished;

		@Nullable
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
	void onChannelActive(ChannelHandlerContext ctx);

	/**
	 * Notifies a new message is to be parsed.
	 *
	 * @param msgId The type of the message to be parsed.
	 * @param messageLength The length of the message to be parsed.
	 */
	void startParsingMessage(int msgId, int messageLength);

	/**
	 * Notifies we have received more data for the current message.
	 *
	 * @param data The data received.
	 * @return The result of current pass of parsing.
	 */
	ParseResult onChannelRead(ByteBuf data) throws Exception;
}
