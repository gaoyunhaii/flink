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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.api.java.typeutils.javaruntime.AbstractTypeClassFactory;
import org.apache.flink.api.java.typeutils.javaruntime.JavaGenericArray;
import org.apache.flink.api.java.typeutils.javaruntime.JavaParameterizedType;
import org.apache.flink.api.java.typeutils.javaruntime.JavaTypeVariable;
import org.apache.flink.api.java.typeutils.types.AbstractType;
import org.apache.flink.api.java.typeutils.types.AbstractTypeClass;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

class TypeDescriptionContext implements TypeInformationExtractor.Context {

	private final List<Class<?>> extractingClasses;
	private final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings;

	public TypeDescriptionContext(final List<Class<?>> extractingClasses, final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings) {
		this.extractingClasses = extractingClasses;
		this.typeVariableBindings = typeVariableBindings;
	}

	@Override
	public List<Class<?>> getExtractingClasses() {
		return extractingClasses;
	}

	@Override
	public Map<TypeVariable<?>, TypeInformation<?>> getTypeVariableBindings() {
		return this.typeVariableBindings;
	}

	@Override
	public TypeDescription resolve(final Type type) {
		final List<Class<?>> currentExtractingClasses;
		if (isClassType(type)) {
			currentExtractingClasses =
				ImmutableList.<Class<?>>builder().addAll(extractingClasses).add(typeToClass(type)).build();
		} else {
			currentExtractingClasses = extractingClasses;
		}
		return TypeExtractionUtils.resolve(
			type,
			new TypeDescriptionContext(currentExtractingClasses, typeVariableBindings));
	}
}
