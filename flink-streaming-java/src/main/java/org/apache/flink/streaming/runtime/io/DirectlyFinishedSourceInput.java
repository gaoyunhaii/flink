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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * I'm java doc.
 */
public class DirectlyFinishedSourceInput<T> implements StreamTaskInput<T> {

	private final int inputIndex;
	private final AvailabilityHelper isBlockedAvailability = new AvailabilityHelper();

	public DirectlyFinishedSourceInput(int inputIndex) {
		this.inputIndex = inputIndex;

		isBlockedAvailability.resetAvailable();
	}

	@Override
	public int getInputIndex() {
		return inputIndex;
	}

	@Override
	public InputStatus emitNext(DataOutput<T> output) throws Exception {
		isBlockedAvailability.resetUnavailable();
		return InputStatus.END_OF_INPUT;
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return isBlockedAvailability.getAvailableFuture();
	}

	@Override
	public CompletableFuture<Void> prepareSnapshot(ChannelStateWriter channelStateWriter, long checkpointId) throws IOException {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public void close() throws IOException {
		// Nothing to do.
	}
}
