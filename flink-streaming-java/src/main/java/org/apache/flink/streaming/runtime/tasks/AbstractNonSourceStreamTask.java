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
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkState;

/** Base class for non-source tasks which need to trigger {@link CheckpointBarrierHandler}. */
public abstract class AbstractNonSourceStreamTask<OUT, OP extends StreamOperator<OUT>>
        extends StreamTask<OUT, OP> {

    protected AbstractNonSourceStreamTask(Environment env) throws Exception {
        super(env);
    }

    protected AbstractNonSourceStreamTask(Environment env, @Nullable TimerService timerService)
            throws Exception {
        super(env, timerService);
    }

    protected AbstractNonSourceStreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler)
            throws Exception {
        super(environment, timerService, uncaughtExceptionHandler);
    }

    protected AbstractNonSourceStreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            StreamTaskActionExecutor actionExecutor)
            throws Exception {
        super(environment, timerService, uncaughtExceptionHandler, actionExecutor);
    }

    protected AbstractNonSourceStreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            StreamTaskActionExecutor actionExecutor,
            TaskMailbox mailbox)
            throws Exception {
        super(environment, timerService, uncaughtExceptionHandler, actionExecutor, mailbox);
    }

    @Nullable
    protected abstract CheckpointBarrierHandler getCheckpointBarrierHandler();

    @Override
    protected boolean internalTriggerCheckpoint(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws Exception {
        CheckpointBarrierHandler checkpointBarrierHandler = getCheckpointBarrierHandler();
        checkState(checkpointBarrierHandler != null, "checkpointBarrierHandler does not exists");

        return checkpointBarrierHandler.triggerCheckpoint(checkpointMetaData, checkpointOptions);
    }
}
