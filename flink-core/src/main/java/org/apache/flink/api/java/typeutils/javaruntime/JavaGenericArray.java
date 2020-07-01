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

import org.apache.flink.api.java.typeutils.types.AbstractGenericArray;
import org.apache.flink.api.java.typeutils.types.AbstractType;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;

public class JavaGenericArray implements AbstractGenericArray, JavaTypeProvider {

	private final GenericArrayType raw;

	public JavaGenericArray(GenericArrayType raw) {
		this.raw = raw;
	}

	@Override
	public AbstractType getComponentType() {
		return JavaTypeConversion.convertToAbstractType(raw.getGenericComponentType());
	}

	@Override
	public String getTypeName() {
		return "[" + raw.getTypeName();
	}

	@Override
	public Type getType() {
		return raw;
	}
}
