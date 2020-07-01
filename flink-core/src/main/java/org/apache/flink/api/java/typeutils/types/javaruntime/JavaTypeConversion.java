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

package org.apache.flink.api.java.typeutils.types.javaruntime;

import org.apache.flink.api.java.typeutils.types.AbstractGenericType;
import org.apache.flink.api.java.typeutils.types.AbstractParameterizedType;
import org.apache.flink.api.java.typeutils.types.AbstractType;
import org.apache.flink.api.java.typeutils.types.AbstractTypeClass;
import org.apache.flink.api.java.typeutils.types.AbstractTypeClassFactory;
import org.apache.flink.api.java.typeutils.types.AbstractTypeVariable;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;

public class JavaTypeConversion {

	public static AbstractType convert(List<AbstractTypeClassFactory> factories, Type type) {
		if (type == null) {
			return null;
		} else if (type instanceof Class<?>) {
			for (AbstractTypeClassFactory factory : factories) {
				AbstractTypeClass current = factory.forName(((Class<?>) type).getName());

				if (current != null) {
					return current;
				}
			}
		} else if (type instanceof ParameterizedType) {
			AbstractType[] actualArguments = new AbstractType[((ParameterizedType) type).getActualTypeArguments().length];
			for (int i = 0; i < ((ParameterizedType) type).getActualTypeArguments().length; ++i) {
				actualArguments[i] = convert(factories, ((ParameterizedType) type).getActualTypeArguments()[i]);
			}

			return new AbstractParameterizedType(
				convert(factories, ((ParameterizedType) type).getRawType()),
				actualArguments,
				type.getTypeName());
		} else if (type instanceof GenericArrayType) {
			return new AbstractGenericType(convert(factories, ((GenericArrayType) type).getGenericComponentType()));
		} else if (type instanceof TypeVariable<?>) {
			Class<?> clazz = (Class<?>) ((TypeVariable<?>) type).getGenericDeclaration();
			return new AbstractTypeVariable(((TypeVariable<?>) type).getName(), convert(factories, clazz));
		} else {
			throw new UnsupportedOperationException("Unsupported java type: " + type.getTypeName() + " of type " + type.getClass());
		}

		return null;
	}
}
