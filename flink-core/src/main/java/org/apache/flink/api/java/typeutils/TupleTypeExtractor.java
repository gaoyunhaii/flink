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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeExtractor.countFieldsInClass;
import static org.apache.flink.api.java.typeutils.TypeExtractor.createTypeInfo;
import static org.apache.flink.api.java.typeutils.TypeResolve.resolveTypeFromTypeHierarchy;

class TupleTypeExtractor {

	/**
	 * Extract the {@link TypeInformation} for the {@link Tuple}.
	 * @param type the type needed to extract {@link TypeInformation}
	 * @param typeVariableBindings contains mapping relation between {@link TypeVariable} and {@link TypeInformation}
	 * @param extractingClasses the classes that the type is nested into.
	 * @return the {@link TypeInformation} of the type or {@code null} if the type information of the generic parameter of
	 * the {@link Tuple} could not be extracted
	 * @throws InvalidTypesException if the type is sub type of {@link Tuple} but not a generic class or if the type equals {@link Tuple}.
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

		final List<ParameterizedType> typeHierarchy = new ArrayList<>();

		Type curT = type;

		// do not allow usage of Tuple as type
		if (typeToClass(type).equals(Tuple.class)) {
			throw new InvalidTypesException(
				"Usage of class Tuple as a type is not allowed. Use a concrete subclass (e.g. Tuple1, Tuple2, etc.) instead.");
		}

		// go up the hierarchy until we reach immediate child of Tuple (with or without generics)
		// collect the types while moving up for a later top-down
		while (!(isClassType(curT) && typeToClass(curT).getSuperclass().equals(Tuple.class))) {
			if (curT instanceof ParameterizedType) {
				typeHierarchy.add((ParameterizedType) curT);
			}
			curT = typeToClass(curT).getGenericSuperclass();
		}

		if (curT == Tuple0.class) {
			return new TupleTypeInfo(Tuple0.class);
		}

		// check if immediate child of Tuple has generics
		if (curT instanceof Class<?>) {
			throw new InvalidTypesException("Tuple needs to be parameterized by using generics.");
		}

		if (curT instanceof ParameterizedType) {
			typeHierarchy.add((ParameterizedType) curT);
		}

		curT = resolveTypeFromTypeHierarchy(curT, typeHierarchy, true);

		// create the type information for the subtypes
		final TypeInformation<?>[] subTypesInfo =
			createSubTypesInfo(type, (ParameterizedType) curT, typeVariableBindings, extractingClasses);

		if (subTypesInfo == null) {
			return null;
		}
		// return tuple info
		return new TupleTypeInfo(typeToClass(type), subTypesInfo);
	}

	/**
	 * Bind the {@link TypeVariable} with {@link TypeInformation} from the generic type.
	 *
	 * @param type the type that has {@link TypeVariable}
	 * @param typeInformation the {@link TypeInformation} that stores the mapping relations between the generic parameters
	 *                        and {@link TypeInformation}.
	 * @return the mapping relation between {@link TypeVariable} and {@link TypeInformation}
	 */
	static Map<TypeVariable<?>, TypeInformation<?>> bindTypeVariable(
		final Type type,
		final TypeInformation<?> typeInformation) {

		final List<ParameterizedType> typeHierarchy = new ArrayList<>();
		Type curType = type;
		// get tuple from possible tuple subclass
		while (!(isClassType(curType) && typeToClass(curType).getSuperclass().equals(Tuple.class))) {
			if (curType instanceof ParameterizedType) {
				typeHierarchy.add((ParameterizedType) curType);
			}
			curType = typeToClass(curType).getGenericSuperclass();
		}
		if (curType instanceof ParameterizedType) {
			typeHierarchy.add((ParameterizedType) curType);
		}
		final Type tupleBaseClass = resolveTypeFromTypeHierarchy(curType, typeHierarchy, true);
		if (tupleBaseClass instanceof ParameterizedType) {
			return TypeExtractor.bindTypeVariableFromGenericParameters((ParameterizedType) tupleBaseClass, typeInformation);
		}
		return Collections.emptyMap();
	}

	/**
	 * Creates the TypeInformation for all generic type of {@link Tuple}.
	 *
	 * @param originalType most concrete subclass
	 * @param definingType type that defines the number of subtypes (e.g. Tuple2 -> 2 subtypes)
	 * @param typeVariableBindings the mapping relation between the type variable and the typeinformation
	 * @param extractingClasses the classes that the type is nested into.
	 * @return array containing TypeInformation of sub types or null if definingType contains
	 *     more subtypes (fields) that defined
	 */
	private static TypeInformation<?>[] createSubTypesInfo(
		final Type originalType,
		final ParameterizedType definingType,
		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		final List<Class<?>> extractingClasses) {

		Preconditions.checkArgument(isClassType(originalType), "originalType has an unexpected type");

		final int typeArgumentsLength = definingType.getActualTypeArguments().length;
		// check the origin type contains additional fields.
		final int fieldCount = countFieldsInClass(typeToClass(originalType));
		if (fieldCount > typeArgumentsLength) {
			return null;
		}

		final TypeInformation<?>[] subTypesInfo = new TypeInformation<?>[typeArgumentsLength];

		for (int i = 0; i < typeArgumentsLength; i++) {
			subTypesInfo[i] = createTypeInfo(definingType.getActualTypeArguments()[i], typeVariableBindings, extractingClasses);
		}
		return subTypesInfo;
	}
}
