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

package org.apache.flink.streaming.api.operators.sink;

import org.apache.flink.api.common.functions.CommitFunction;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.util.UUID;

public class BatchSinkCommitOperatorFactory<T> implements StreamOperatorFactory<Void> {

	private final CommitFunction<T> commitFunction;

	private final UUID sinkId;

	private ChainingStrategy chainingStrategy;

	public BatchSinkCommitOperatorFactory(CommitFunction<T> commitFunction, UUID sinkId) {
		this.commitFunction = commitFunction;
		this.sinkId = sinkId;
	}

	@Override
	public <T1 extends StreamOperator<Void>> T1 createStreamOperator(StreamOperatorParameters<Void> parameters) {
		BatchSinkCommitOperator<T, Void> batchSinkCommitOperator = new BatchSinkCommitOperator<>(commitFunction, sinkId);
		batchSinkCommitOperator.setup(
			parameters.getContainingTask(),
			parameters.getStreamConfig(),
			parameters.getOutput());
		return (T1) batchSinkCommitOperator;
	}

	@Override
	public void setChainingStrategy(ChainingStrategy strategy) {
		this.chainingStrategy = strategy;
	}

	@Override
	public ChainingStrategy getChainingStrategy() {
		return chainingStrategy;
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return BatchSinkCommitOperator.class;
	}
}
