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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.streaming.api.operators.MailboxExecutor;

import java.util.function.Supplier;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Allow the caller to wait till a checkpoint is completed before continuing.
 * The caller is expected to run in the same mailbox-thread.
 */
public class CheckpointLatchImpl implements CheckpointLatch {
	private final MailboxExecutor mailboxExecutor;

	private final Supplier<Boolean> isRunningInMailboxThread;

	private boolean checkpointTriggered;

	private boolean checkpointCompleted;

	public CheckpointLatchImpl(MailboxExecutor mailboxExecutor, Supplier<Boolean> isRunningInMailboxThread) {
		this.mailboxExecutor = checkNotNull(mailboxExecutor);
		this.isRunningInMailboxThread = checkNotNull(isRunningInMailboxThread);
	}

	@Override
	public void waitForCheckpointComplete() throws InterruptedException {
		checkState(isRunningInMailboxThread.get(), "Wait should only happen in mailbox thread");

		checkpointTriggered = false;
		checkpointCompleted = false;

		while (!checkpointCompleted) {
			mailboxExecutor.yield();
		}
	}

	@Override
	public void onCheckpointTriggered() {
		checkState(isRunningInMailboxThread.get(), "Wait should only happen in mailbox thread");

		checkpointTriggered = true;
	}

	@Override
	public void onCheckpointCompleted() {
		checkState(isRunningInMailboxThread.get(), "Wait should only happen in mailbox thread");

		if (checkpointTriggered) {
			checkpointCompleted = true;
		}
	}
}
