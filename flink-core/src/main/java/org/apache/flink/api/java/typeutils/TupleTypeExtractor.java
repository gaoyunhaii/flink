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

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeExtractor.getForObject;
import static org.apache.flink.api.java.typeutils.TypeHierarchyBuilder.buildParameterizedTypeHierarchy;
import static org.apache.flink.api.java.typeutils.TypeResolver.resolveTypeFromTypeHierarchy;

class TupleTypeExtractor {

	/**
	 * Extract the {@link TypeInformation} for the {@link Tuple}.
	 * @param type the type needed to extract {@link TypeInformation}
	 * @param typeVariableBindings contains mapping relation between {@link TypeVariable} and {@link TypeInformation}. This
	 *                             is used to extract the {@link TypeInformation} for {@link TypeVariable}.
	 * @param extractingClasses contains the classes that type extractor stack is extracting for {@link TypeInformation}.
	 *                             This is used to check whether there is a recursive type.
	 * @return the {@link TypeInformation} of the type or {@code null} if the type information of the generic parameter of
	 * the {@link Tuple} could not be extracted
	 * @throws InvalidTypesException if the immediate child of {@link Tuple} is not a generic class or if the child of {@link Tuple}
	 * is not a generic class or if the type equals {@link Tuple}
	 */
	@Nullable
	@SuppressWarnings("unchecked")
	static TypeInformation<?> extract(
		final Type type,
		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		final List<Class<?>> extractingClasses) {

		if (!(isClassType(type) && Tuple.class.isAssignableFrom(typeToClass(type)))) {
			return null;
		}

		//do not allow usage of Tuple as type
		if (typeToClass(type).equals(Tuple.class)) {
			throw new InvalidTypesException(
				"Usage of class Tuple as a type is not allowed. Use a concrete subclass (e.g. Tuple1, Tuple2, etc.) instead.");
		}

		if (Tuple0.class.isAssignableFrom(typeToClass(type))) {
			return new TupleTypeInfo(Tuple0.class);
		}

		final List<ParameterizedType> typeHierarchy = buildParameterizedTypeHierarchy(type, Tuple.class);

		if (typeHierarchy.size() < 1) {
			throw new InvalidTypesException("Tuple needs to be parameterized by using generics.");
		}

		// check if immediate child of Tuple has generics
		if (typeToClass(typeHierarchy.get(typeHierarchy.size() - 1)).getSuperclass() != Tuple.class) {
			throw new InvalidTypesException("Tuple needs to be parameterized by using generics.");
		}

		final ParameterizedType curT = typeHierarchy.get(typeHierarchy.size() - 1);
		final ParameterizedType resolvedType = (ParameterizedType) resolveTypeFromTypeHierarchy(curT, typeHierarchy, true);
		final int typeArgumentsLength = resolvedType.getActualTypeArguments().length;

		// check the origin type contains additional fields.
		if (countFieldsInClass(typeToClass(type)) > typeArgumentsLength) {
			return null;
		}

		// create the type information for the subtypes
		final TypeInformation<?>[] subTypesInfo = new TypeInformation<?>[typeArgumentsLength];

		for (int i = 0; i < typeArgumentsLength; i++) {
			subTypesInfo[i] = TypeExtractor.extract(resolvedType.getActualTypeArguments()[i], typeVariableBindings, extractingClasses);
		}
		// return tuple info
		return new TupleTypeInfo(typeToClass(type), subTypesInfo);
	}

	/**
	 * Infer the {@link TypeInformation} of the {@link TypeVariable} from the given {@link TypeInformation} that is {@link TupleTypeInfo}.
	 * @param type the resolved type
	 * @param typeInformation the type information of the given type
	 * @return the mapping relation between {@link TypeVariable} and {@link TypeInformation} or {@code null} if the typeInformation
	 * is not {@link TupleTypeInfo}.
	 */
	@Nullable
	static Map<TypeVariable<?>, TypeInformation<?>> bindTypeVariables(final Type type, final TypeInformation<?> typeInformation) {

		if (typeInformation instanceof TupleTypeInfo && isClassType(type) && Tuple.class.isAssignableFrom(typeToClass(type))) {
			final List<ParameterizedType> typeHierarchy = buildParameterizedTypeHierarchy(type, Tuple.class);

			if (typeHierarchy.size() > 0) {
				final ParameterizedType tupleBaseClass = typeHierarchy.get(typeHierarchy.size() - 1);
				final ParameterizedType resolvedTupleBaseClass = (ParameterizedType) resolveTypeFromTypeHierarchy(tupleBaseClass, typeHierarchy, true);
				return TypeVariableBinder.bindTypeVariableFromGenericParameters(resolvedTupleBaseClass, typeInformation);
			}
			return Collections.emptyMap();
		}
		return null;
	}

	/**
	 * Extract the {@link TypeInformation} for the Tuple object.
	 * @param value the object needed to extract {@link TypeInformation}
	 * @return the {@link TypeInformation} of the type or {@code null} if the value is not the Tuple type.
	 */
	@SuppressWarnings("unchecked")
	@Nullable
	static TypeInformation<?> extract(Object value) {

		if (!(value instanceof Tuple)) {
			return null;
		}

		final Tuple t = (Tuple) value;
		final int numFields = t.getArity();

		if (numFields != countFieldsInClass(value.getClass())) {
			// not a tuple since it has more fields.
			// we immediately call analyze Pojo here, because there is currently no other type that can handle such a class.
			//TODO:: use createType??
			return PojoTypeExtractor.extract(
				value.getClass(),
				Collections.emptyMap(),
				Collections.emptyList());
		}

		final TypeInformation<?>[] typeInformations = new TypeInformation[numFields];
		for (int i = 0; i < numFields; i++) {
			Object field = t.getField(i);

			if (field == null) {
				throw new InvalidTypesException("Automatic type extraction is not possible on candidates with null values. "
					+ "Please specify the types directly.");
			}

			typeInformations[i] = getForObject(field);
		}
		return new TupleTypeInfo(value.getClass(), typeInformations);
	}

	private static int countFieldsInClass(Class<?> clazz) {
		int fieldCount = 0;
		for (Field field : clazz.getFields()) { // get all fields
			if (!Modifier.isStatic(field.getModifiers()) &&
				!Modifier.isTransient(field.getModifiers())
			) {
				fieldCount++;
			}
		}
		return fieldCount;
	}
}
