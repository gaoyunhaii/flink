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

import org.apache.flink.annotation.VisibleForTesting;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.sameTypeVars;

/**
 * This class is used to resolve the type from the type hierarchy.
 */
class TypeResolver {

	/**
	 * Resolve all {@link TypeVariable}s of the type from the type hierarchy.
	 * @param type the type needed to be resolved
	 * @param typeHierarchy the set of types which the {@link TypeVariable} could be resolved from.
	 * @param resolveGenericArray whether to resolve the {@code GenericArrayType} or not. This is for compatible.
	 *                               (Some code path resolves the component type of a GenericArrayType. Some code path
	 *                               does not resolve the component type of a GenericArray. A example case is
	 *                               {@code testParameterizedArrays()})
	 * @return resolved type
	 */
	static Type resolveTypeFromTypeHierarchy(
		final Type type,
		final List<ParameterizedType> typeHierarchy,
		final boolean resolveGenericArray) {

		Type resolvedType = type;

		if (type instanceof TypeVariable) {
			resolvedType = materializeTypeVariable(typeHierarchy, (TypeVariable) type);
		}

		if (resolvedType instanceof ParameterizedType) {
			return resolveParameterizedType((ParameterizedType) resolvedType, typeHierarchy, resolveGenericArray);
		} else if (resolveGenericArray && resolvedType instanceof GenericArrayType) {
			return resolveGenericArrayType((GenericArrayType) resolvedType, typeHierarchy);
		}

		return resolvedType;
	}

	/**
	 * Resolve all {@link TypeVariable}s of a {@link ParameterizedType}.
	 * @param parameterizedType the {@link ParameterizedType} needed to be resolved.
	 * @param typeHierarchy the set of types which the {@link TypeVariable}s could be resolved from.
	 * @param resolveGenericArray whether to resolve the {@code GenericArrayType} or not. This is for compatible.
	 * @return resolved {@link ParameterizedType}
	 */
	private static Type resolveParameterizedType(
		final ParameterizedType parameterizedType,
		final List<ParameterizedType> typeHierarchy,
		final boolean resolveGenericArray) {

		final Type[] actualTypeArguments = new Type[parameterizedType.getActualTypeArguments().length];

		int i = 0;
		for (Type type : parameterizedType.getActualTypeArguments()) {
			actualTypeArguments[i] = resolveTypeFromTypeHierarchy(type, typeHierarchy, resolveGenericArray);
			i++;
		}

		return new ResolvedParameterizedType(parameterizedType.getRawType(),
			parameterizedType.getOwnerType(),
			actualTypeArguments,
			parameterizedType.getTypeName());
	}

	/**
	 * Resolve the component type of {@link GenericArrayType}.
	 * @param genericArrayType the {@link GenericArrayType} needed to be resolved.
	 * @param typeHierarchy the set of types which the {@link TypeVariable}s could be resolved from.
	 * @return resolved {@link GenericArrayType}
	 */
	private static Type resolveGenericArrayType(final GenericArrayType genericArrayType, final List<ParameterizedType> typeHierarchy) {

		final Type resolvedComponentType =
			resolveTypeFromTypeHierarchy(genericArrayType.getGenericComponentType(), typeHierarchy, true);

		return new ResolvedGenericArrayType(genericArrayType.getTypeName(), resolvedComponentType);
	}

	/**
	 * Tries to find a concrete value (Class, ParameterizedType etc. ) for a TypeVariable by traversing the type
	 * hierarchy downwards.
	 *
	 * @param typeHierarchy the type hierarchy
	 * @param typeVar the type variable needed to be concreted
	 * @return the concrete value or
	 * 		   the most bottom type variable in the hierarchy
	 */
	@VisibleForTesting
	static Type materializeTypeVariable(List<ParameterizedType> typeHierarchy, TypeVariable<?> typeVar) {
		TypeVariable<?> inTypeTypeVar = typeVar;
		// iterate thru hierarchy from top to bottom until type variable gets a class assigned
		for (int i = typeHierarchy.size() - 1; i >= 0; i--) {
			ParameterizedType curT = typeHierarchy.get(i);
			Class<?> rawType = (Class<?>) curT.getRawType();

			for (int paramIndex = 0; paramIndex < rawType.getTypeParameters().length; paramIndex++) {

				TypeVariable<?> curVarOfCurT = rawType.getTypeParameters()[paramIndex];

				// check if variable names match
				if (sameTypeVars(curVarOfCurT, inTypeTypeVar)) {
					Type curVarType = curT.getActualTypeArguments()[paramIndex];

					// another type variable level
					if (curVarType instanceof TypeVariable<?>) {
						inTypeTypeVar = (TypeVariable<?>) curVarType;
					} else {
						return curVarType;
					}
				}
			}
		}
		// can not be materialized, most likely due to type erasure
		// return the type variable of the deepest level
		return inTypeTypeVar;
	}

	private static class ResolvedGenericArrayType implements GenericArrayType {

		private final Type componentType;

		private final String typeName;

		ResolvedGenericArrayType(String typeName, Type componentType) {
			this.componentType = componentType;
			this.typeName = typeName;
		}

		@Override
		public Type getGenericComponentType() {
			return componentType;
		}

		public String getTypeName() {
			return typeName;
		}
	}

	private static class ResolvedParameterizedType implements ParameterizedType {

		private final Type rawType;

		private final Type ownerType;

		private final Type[] actualTypeArguments;

		private final String typeName;

		ResolvedParameterizedType(Type rawType, Type ownerType, Type[] actualTypeArguments, String typeName) {
			this.rawType = rawType;
			this.ownerType = ownerType;
			this.actualTypeArguments = actualTypeArguments;
			this.typeName = typeName;
		}

		@Override
		public Type[] getActualTypeArguments() {
			return actualTypeArguments;
		}

		@Override
		public Type getRawType() {
			return rawType;
		}

		@Override
		public Type getOwnerType() {
			return ownerType;
		}

		public String getTypeName() {
			return typeName;
		}
	}

}
