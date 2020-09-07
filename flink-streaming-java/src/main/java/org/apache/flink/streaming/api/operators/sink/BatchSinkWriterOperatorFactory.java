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

import org.apache.flink.api.connector.sink.USink;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

import java.util.UUID;

public class BatchSinkWriterOperatorFactory<IN, SplitT> implements StreamOperatorFactory<SplitT> {

	private final USink<IN, SplitT> uSink;

	private final UUID sinkId;

	private ChainingStrategy chainingStrategy;

	public BatchSinkWriterOperatorFactory(USink<IN, SplitT> uSink, UUID sinkId) {
		this.uSink = uSink;
		this.sinkId = sinkId;
		this.chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public <T extends StreamOperator<SplitT>> T createStreamOperator(StreamOperatorParameters<SplitT> parameters) {
		BatchSinkWriterOperator<IN, SplitT> splitSinkOperator = new BatchSinkWriterOperator<>(uSink, sinkId);
		splitSinkOperator.setup(parameters.getContainingTask(), parameters.getStreamConfig(), parameters.getOutput());

		return (T) splitSinkOperator;
	}

	@Override
	public void setChainingStrategy(ChainingStrategy strategy) {
		this.chainingStrategy = chainingStrategy;
	}

	@Override
	public ChainingStrategy getChainingStrategy() {
		return this.chainingStrategy;
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return BatchSinkWriterOperator.class;
	}
}
