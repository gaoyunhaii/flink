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

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.io.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * Tmp.
 */
public abstract class AbstractNonSourceStreamTask<OUT, OP extends StreamOperator<OUT>>
	extends StreamTask<OUT, OP> {

	protected AbstractNonSourceStreamTask(Environment env) throws Exception {
		super(env);
	}

	protected AbstractNonSourceStreamTask(
		Environment env,
		@Nullable TimerService timerService) throws Exception {
		super(env, timerService);
	}

	protected AbstractNonSourceStreamTask(
		Environment environment,
		@Nullable TimerService timerService,
		Thread.UncaughtExceptionHandler uncaughtExceptionHandler) throws Exception {
		super(environment, timerService, uncaughtExceptionHandler);
	}

	protected AbstractNonSourceStreamTask(
		Environment environment,
		@Nullable TimerService timerService,
		Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
		StreamTaskActionExecutor actionExecutor) throws Exception {
		super(environment, timerService, uncaughtExceptionHandler, actionExecutor);
	}

	protected AbstractNonSourceStreamTask(
		Environment environment,
		@Nullable TimerService timerService,
		Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
		StreamTaskActionExecutor actionExecutor,
		TaskMailbox mailbox) throws Exception {
		super(environment, timerService, uncaughtExceptionHandler, actionExecutor, mailbox);
	}

	@Nullable
	protected abstract CheckpointBarrierHandler getCheckpointBarrierHandler();

	@Override
	protected void triggerCheckpoint(
		CheckpointMetaData checkpointMetaData,
		CheckpointOptions checkpointOptions,
		boolean advanceToEndOfEventTime,
		CompletableFuture<Boolean> resultFuture) throws Exception {

		CheckpointBarrierHandler checkpointBarrierHandler = getCheckpointBarrierHandler();
		if (checkpointBarrierHandler == null) {
			resultFuture.complete(false);
			return;
		}

		try {
			boolean success = checkpointBarrierHandler.triggerCheckpoint(checkpointMetaData, checkpointOptions);
			if (!success) {
				declineCheckpoint(checkpointMetaData.getCheckpointId());
			}
			resultFuture.complete(success);
		} catch (Exception e) {
			if (isRunning) {
				Exception exception = new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() +
					" for operator " + getName() + '.', e);
				resultFuture.completeExceptionally(exception);
				throw exception;
			} else {
				LOG.debug("Could not perform checkpoint {} for operator {} while the " +
					"invokable was not in state running.", checkpointMetaData.getCheckpointId(), getName(), e);
				resultFuture.complete(false);
			}
		}
	}
}
