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

/**
 * Utility routines to process Netty ByteBuf.
 */
public class ByteBufUtils {

	/**
	 * Cumulates data from the source buffer to the target buffer.
	 *
	 * @param cumulationBuf The target buffer.
	 * @param src The source buffer.
	 * @param expectedSize The expected length to cumulate.
	 *
	 * @return The ByteBuf containing cumulated data or null if not enough data has been cumulated.
	 */
	public static ByteBuf cumulate(ByteBuf cumulationBuf, ByteBuf src, int expectedSize) {
		// If the cumulation buffer is empty and src has enought bytes,
		// user could read from src directly without cumulation.
		if (cumulationBuf.readerIndex() == 0
			&& cumulationBuf.writerIndex() == 0
			&& src.readableBytes() >= expectedSize) {

			return src;
		}

		int copyLength = Math.min(src.readableBytes(), expectedSize - cumulationBuf.readableBytes());

		if (copyLength > 0) {
			cumulationBuf.writeBytes(src, copyLength);
		}

		if (cumulationBuf.readableBytes() == expectedSize) {
			return cumulationBuf;
		}

		return null;
	}

}
