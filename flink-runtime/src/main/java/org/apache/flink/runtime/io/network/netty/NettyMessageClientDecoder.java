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
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;

import static org.apache.flink.runtime.io.network.netty.NettyMessage.FRAME_HEADER_LENGTH;
import static org.apache.flink.runtime.io.network.netty.NettyMessage.MAGIC_NUMBER;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Decodes messages from the fragmentary netty buffers. This decoder assumes the
 * messages have the following format:
 * +-----------------------------------+--------------------------------+
 * | FRAME_HEADER ||  MESSAGE_HEADER   |     DATA BUFFER (Optional)     |
 * +-----------------------------------+--------------------------------+
 *
 * This decoder decodes the frame header and delegates the other following work
 * to corresponding message parsers according to the message type. During the process
 * of decoding, the decoder and parsers try best to eliminate copying. For the frame
 * header and message header, it only cumulates data when they span multiple input buffers.
 * For the buffer part, it copies directly to the input channels to avoid future copying.
 *
 * The format of the frame header is
 * +------------------+------------------+--------+
 * | FRAME LENGTH (4) | MAGIC NUMBER (4) | ID (1) |
 * +------------------+------------------+--------+
 */
public class NettyMessageClientDecoder extends ChannelInboundHandlerAdapter {

	/** The message parser for buffer response. */
    private final NettyMessageDecoderDelegate bufferResponseDecoderDelegate;

    /** The message parser for other messages other than buffer response. */
	private final NettyMessageDecoderDelegate nonBufferResponseDecoderDelegate;

	/** The cumulation buffer for the frame header part. */
	private ByteBuf frameHeaderBuffer;

	/**
	 * The chosen message parser for the current message. If it is null, then
	 * we are decoding the frame header part, otherwise we are decoding the actual
	 * message.
	 */
	private NettyMessageDecoderDelegate currentDecoderDelegate;

    NettyMessageClientDecoder(NetworkClientHandler networkClientHandler) {
        this.bufferResponseDecoderDelegate = new BufferResponseDecoderDelegate(networkClientHandler);
        this.nonBufferResponseDecoderDelegate = new NonBufferResponseDecoderDelegate();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        bufferResponseDecoderDelegate.onChannelActive(ctx.alloc());
        nonBufferResponseDecoderDelegate.onChannelActive(ctx.alloc());

		frameHeaderBuffer = ctx.alloc().directBuffer(FRAME_HEADER_LENGTH);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        super.channelInactive(ctx);

		bufferResponseDecoderDelegate.release();
		nonBufferResponseDecoderDelegate.release();

		frameHeaderBuffer.release();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof ByteBuf)) {
            ctx.fireChannelRead(msg);
            return;
        }

        ByteBuf data = (ByteBuf) msg;

        try {
            while (data.isReadable()) {
				if (currentDecoderDelegate == null) {
					ByteBuf toDecode = ByteBufUtils.cumulate(frameHeaderBuffer, data, FRAME_HEADER_LENGTH);

            		if (toDecode != null) {
						int messageAndFrameLength = toDecode.readInt();
						checkState(messageAndFrameLength >= 0, "The length field of current message must be non-negative");

						int magicNumber = toDecode.readInt();
						checkState(magicNumber == MAGIC_NUMBER, "Network stream corrupted: received incorrect magic number.");

						int msgId = toDecode.readByte();

						if (msgId == NettyMessage.BufferResponse.ID) {
							currentDecoderDelegate = bufferResponseDecoderDelegate;
						} else {
							currentDecoderDelegate = nonBufferResponseDecoderDelegate;
						}

						currentDecoderDelegate.startParsingMessage(msgId, messageAndFrameLength - FRAME_HEADER_LENGTH);
					}
				}

				if (data.isReadable() && currentDecoderDelegate != null) {
					NettyMessageDecoderDelegate.ParseResult result = currentDecoderDelegate.onChannelRead(data);

					if (result.finished) {
						if (result.message != null) {
							ctx.fireChannelRead(result.message);
						}

						currentDecoderDelegate = null;
						frameHeaderBuffer.clear();
					}
				}
            }

            checkState(!data.isReadable(), "Not all data of the received buffer consumed.");
        } finally {
            data.release();
        }
    }
}
