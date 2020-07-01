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

package org.apache.flink.api.java.typeutils.javaruntime;

import org.apache.flink.api.java.typeutils.types.AbstractParameterizedType;
import org.apache.flink.api.java.typeutils.types.AbstractType;
import org.apache.flink.api.java.typeutils.types.AbstractTypeClass;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;

public class JavaParameterizedType implements AbstractParameterizedType, JavaTypeProvider {

	private final ParameterizedType parameterizedType;

	public JavaParameterizedType(ParameterizedType parameterizedType) {
		this.parameterizedType = parameterizedType;
	}

	@Override
	public AbstractTypeClass getRawType() {
		return (AbstractTypeClass) JavaTypeConversion.convertToAbstractType(parameterizedType.getRawType());
	}

	@Override
	public AbstractType[] getActualArguments() {
		return (AbstractType[]) Arrays.stream(parameterizedType.getActualTypeArguments())
			.map(JavaTypeConversion::convertToAbstractType)
			.toArray();
	}

	@Override
	public String getTypeName() {
		return parameterizedType.getTypeName();
	}

	@Override
	public Type getType() {
		return parameterizedType;
	}
}
