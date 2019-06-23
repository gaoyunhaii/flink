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

import javax.annotation.Nullable;

/**
 * Utility routines to process Netty ByteBuf.
 */
public class ByteBufUtils {

	/**
	 * Accumulates data from the source buffer to the target buffer. If no data has been accumulated yet and the source
	 * buffer has enough data, source buffer will be returned directly. Otherwise, data will be copied into the target
	 * buffer. If the size of data copied after this operation has reached the expected total size, the target buffer
	 * will be returned for read, otherwise <tt>null</tt> will be return to indicate more data is required.
	 *
	 * @param target The target buffer.
	 * @param src The source buffer.
	 * @param totalSize The total length to accumulate.
	 * @param accumulatedSize The size of data accumulated so far.
	 *
	 * @return The ByteBuf containing accumulated data. If not enough data has been accumulated,
	 * 		<tt>null</tt> will be returned.
	 */
	@Nullable
	public static ByteBuf accumulate(ByteBuf target, ByteBuf src, int totalSize, int accumulatedSize) {
		if (accumulatedSize == 0 && src.readableBytes() >= totalSize) {
			return src;
		}

		int copyLength = Math.min(src.readableBytes(), totalSize - accumulatedSize);
		if (copyLength > 0) {
			target.writeBytes(src, copyLength);
		}

		if (accumulatedSize + copyLength == totalSize) {
			return target;
		}

		return null;
	}

}
