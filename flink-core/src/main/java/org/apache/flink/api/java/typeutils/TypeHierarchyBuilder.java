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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

/**
 * This class is used to build the type hierarchy.
 */
class TypeHierarchyBuilder {

	/**
	 * Traverse the type hierarchy of the given {@code type} and record all the parameterized types until the base class.
	 * The traversal skips the interface type.
	 * @param type the begin type of the type hierarchy
	 * @param baseClass the end type of the type hierarchy
	 * @return the parameterized type hierarchy.
	 */
	static List<ParameterizedType> buildParameterizedTypeHierarchy(final Type type, final Class<?> baseClass) {
		if (isClassType(type)) {
			final List<ParameterizedType> typeHierarchy = new ArrayList<>();
			if (type instanceof ParameterizedType) {
				typeHierarchy.add((ParameterizedType) type);
			}
			typeHierarchy.addAll(
				buildParameterizedTypeHierarchy(
					typeToClass(type),
					isSameClass(baseClass).or(assignTo(baseClass).negate()), assignTo(baseClass),
					false)
			);
			return typeHierarchy.size() == 0 ? Collections.emptyList() : typeHierarchy;
		}

		return Collections.emptyList();
	}

	/**
	 * Traverse the type hierarchy of the given {@code type} until meeting the type that satisfies the stop condition.
	 * Record all the parameterized types that satisfy the matcher. The traversal skips the interface type.
	 * @param type the begin type of the type hierarchy
	 * @param stopCondition stop traversing the hierarchy when the condition is satisfied
	 * @param matcher add the parameterized type to the result if the type satisfied the matcher.
	 * @return the parameterized type hierarchy.
	 */
	static List<ParameterizedType> buildParameterizedTypeHierarchy(
		final Type type,
		final Predicate<Class> stopCondition,
		final Predicate<Class> matcher) {

		if (isClassType(type)) {
			final List<ParameterizedType> typeHierarchy = new ArrayList<>();
			if (matcher.test(typeToClass(type))) {
				if (type instanceof ParameterizedType) {
					typeHierarchy.add((ParameterizedType) type);
				}
				if (stopCondition.test(typeToClass(type))) {
					return typeHierarchy;
				} else {
					typeHierarchy.addAll(
						buildParameterizedTypeHierarchy(typeToClass(type), stopCondition, matcher, false));
					return typeHierarchy.size() == 0 ? Collections.emptyList() : typeHierarchy;
				}
			} else {
				return Collections.emptyList();
			}
		}

		return Collections.emptyList();
	}

	/**
	 * Traverse the type hierarchy of the given sub class and record all the parameterized types until the base class.
	 * The traversal includes the interface type.
	 * @param subClass the begin type of the type hierarchy
	 * @param baseClass the end type of the type hierarchy
	 * @return the parameterized type hierarchy.
	 */
	static List<ParameterizedType> buildParameterizedTypeHierarchy(final Class<?> subClass, final Class<?> baseClass) {
		return buildParameterizedTypeHierarchy(
			subClass,
			isSameClass(baseClass).or(assignTo(baseClass).negate()), assignTo(baseClass),
			true);
	}

	/**
	 * Build the parameterized type hierarchy during traverse the {@code clazz}'s type hierarchy.
	 * @param clazz the begin class of the type hierarchy
	 * @param stopCondition stop traversing the hierarchy when the condition is satisfied
	 * @param matcher add the parameterized type to the result if the type satisfied the matcher.
	 * @return the parameterized type hierarchy.
	 */
	private static List<ParameterizedType> buildParameterizedTypeHierarchy(
		final Class<?> clazz,
		final Predicate<Class> stopCondition,
		final Predicate<Class> matcher,
		final boolean traverseInterface) {

		final List<ParameterizedType> typeHierarchy = new ArrayList<>();

		if (traverseInterface) {
			final Type[] interfaceTypes = clazz.getGenericInterfaces();

			for (Type type : interfaceTypes) {
				if (matcher.test(typeToClass(type))) {
					if (type instanceof ParameterizedType) {
						typeHierarchy.add((ParameterizedType) type);
					}
					if (stopCondition.test(typeToClass(type))) {
						return typeHierarchy;
					} else {
						final List<ParameterizedType> subTypeHierarchy =
							buildParameterizedTypeHierarchy(typeToClass(type), stopCondition, matcher, true);
						typeHierarchy.addAll(subTypeHierarchy);
						return typeHierarchy;
					}
				}
			}
		}

		final Type type = clazz.getGenericSuperclass();
		if (type != null && matcher.test(typeToClass(type))) {
			if (type instanceof ParameterizedType) {
				typeHierarchy.add((ParameterizedType) type);
			}
			if (stopCondition.test(typeToClass(type))) {
				return typeHierarchy;
			} else {
				final List<ParameterizedType> subTypeHierarchy =
					buildParameterizedTypeHierarchy(typeToClass(type), stopCondition, matcher, traverseInterface);
				typeHierarchy.addAll(subTypeHierarchy);
				return typeHierarchy;
			}
		}

		return Collections.emptyList();
	}

	private static Predicate<Class> isSameClass(final Class<?> baseClass) {
		return clazz -> clazz.equals(baseClass);
	}

	private static Predicate<Class> assignTo(final Class<?> baseClass) {
		return baseClass::isAssignableFrom;
	}
}
