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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import java.util.Collection;
import java.util.List;

public class SingletonPlaceHolderTransformation<IN, OUT> extends Transformation<OUT> {

	private final Transformation<IN> input;

	private final String uniqueId;

	private final StreamOperatorFactory<OUT> operatorFactory;

	public SingletonPlaceHolderTransformation(
		Transformation<IN> input,
		String name,
		String uniqueId,
		StreamOperatorFactory<OUT> operatorFactory,
		TypeInformation<OUT> outputType) {

		super(name, outputType, 1);

		this.input = input;
		this.uniqueId = uniqueId;
		this.operatorFactory = operatorFactory;
	}

	public Transformation<IN> getInput() {
		return input;
	}

	public String getUniqueId() {
		return uniqueId;
	}

	public StreamOperatorFactory<OUT> getOperatorFactory() {
		return operatorFactory;
	}

	@Override
	public Collection<Transformation<?>> getTransitivePredecessors() {
		List<Transformation<?>> result = Lists.newArrayList();
		result.add(this);
		result.addAll(input.getTransitivePredecessors());
		return result;
	}
}
