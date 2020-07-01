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

import com.sun.javafx.tools.packager.Param;
import org.apache.flink.api.java.typeutils.types.AbstractType;
import org.apache.flink.api.java.typeutils.types.AbstractTypeClass;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;

public class JavaTypeConversion {

	public static AbstractType convertToAbstractType(Type type) {
		if (type == null) {
			return null;
		} else if (type instanceof Class) {
			return TypeClassFactoryFinder.forName(((Class<?>) type).getName());
		} else if (type instanceof GenericArrayType) {
			return new JavaGenericArray((GenericArrayType) type);
		} else if (type instanceof ParameterizedType) {
			return new JavaParameterizedType((ParameterizedType) type);
		} else if (type instanceof TypeVariable<?>) {
			return new JavaTypeVariable((TypeVariable<?>) type);
		}

		throw new UnsupportedOperationException("unknow " + type);
	}

	public static Type convertFromAbstractType(AbstractType type) {
		return ((JavaTypeProvider) type).getType();
	}
}
