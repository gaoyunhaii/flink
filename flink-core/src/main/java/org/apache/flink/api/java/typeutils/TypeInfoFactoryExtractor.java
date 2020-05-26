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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeExtractor.createTypeInfo;
import static org.apache.flink.api.java.typeutils.TypeResolver.resolveTypeFromTypeHierarchy;
import static org.apache.flink.api.java.typeutils.TypeVariableBinder.bindTypeVariableFromGenericParameters;

/**
 * This class is used to extract the {@link org.apache.flink.api.common.typeinfo.TypeInformation} of the class that has
 * the {@link org.apache.flink.api.common.typeinfo.TypeInfo} annotation.
 */
public class TypeInfoFactoryExtractor {

	/**
	 * Extract {@link TypeInformation} for the type that has {@link TypeInfo} annotation.
	 * @param type  the type needed to extract {@link TypeInformation}
	 * @param typeVariableBindings contains mapping relation between {@link TypeVariable} and {@link TypeInformation}.
	 * @param extractingClasses the classes that the type is nested into.
	 * @return the {@link TypeInformation} of the given type or {@code null} if the type does not have the annotation
	 * @throws InvalidTypesException if the factory does not create a valid {@link TypeInformation} or if creating generic type failed
	 */
	@Nullable
	static TypeInformation<?> extract(
		final Type type,
		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings,
		final List<Class<?>> extractingClasses) {

		final Tuple3<Type, List<ParameterizedType>, TypeInfoFactory<?>> result = buildTypeHierarchy(type);

		if (result == null) {
			return null;
		}

		final Type resolvedFactoryDefiningType = resolveTypeFromTypeHierarchy(result.f0, result.f1, true);

		// infer possible type parameters from input
		final Map<String, TypeInformation<?>> genericParams;

		if (resolvedFactoryDefiningType instanceof ParameterizedType) {
			genericParams = new HashMap<>();
			final Type[] genericTypes = ((ParameterizedType) resolvedFactoryDefiningType).getActualTypeArguments();
			final Type[] args = typeToClass(resolvedFactoryDefiningType).getTypeParameters();
			for (int i = 0; i < genericTypes.length; i++) {
				try {
					genericParams.put(
						args[i].toString(),
						createTypeInfo(genericTypes[i], typeVariableBindings, extractingClasses));
				} catch (InvalidTypesException e) {
					genericParams.put(args[i].toString(), null);
				}
			}
		} else {
			genericParams = Collections.emptyMap();
		}

		final TypeInformation<?> createdTypeInfo = result.f2.createTypeInfo(type, genericParams);
		if (createdTypeInfo == null) {
			throw new InvalidTypesException("TypeInfoFactory returned invalid TypeInformation 'null'");
		}
		return createdTypeInfo;
	}

	/**
	 * Bind the {@link TypeVariable} with the {@link TypeInformation} from {@link TypeInformation} that is created from {@link TypeInfoFactory}.
	 * @param type the resolved type
	 * @param typeInformation the type information of the given type
	 * @return the mapping relation between the {@link TypeVariable} and {@link TypeInformation} or
	 * 		   {@code null} if the typeInformation is not created from the {@link TypeInfoFactory}
	 */
	static Map<TypeVariable<?>, TypeInformation<?>> bindTypeVariables(final Type type, final TypeInformation<?> typeInformation) {

		final Tuple3<Type, List<ParameterizedType>, TypeInfoFactory<?>> result = buildTypeHierarchy(type);

		if (result == null) {
			return null;
		}
		if (result.f0 instanceof ParameterizedType) {
			final ParameterizedType resolvedFactoryDefiningType =
				(ParameterizedType) resolveTypeFromTypeHierarchy(result.f0, result.f1, true);
			return bindTypeVariableFromGenericParameters(resolvedFactoryDefiningType, typeInformation);
		} else {
			return Collections.emptyMap();
		}
	}

	/**
	 * Returns the type information factory for a type using the factory registry or annotations.
	 */
	@Internal
	@SuppressWarnings("unchecked")
	public static <X> TypeInfoFactory<X> getTypeInfoFactory(Type t) {
		final Class<?> factoryClass;

		if (!isClassType(t) || !typeToClass(t).isAnnotationPresent(TypeInfo.class)) {
			return null;
		}
		final TypeInfo typeInfoAnnotation = typeToClass(t).getAnnotation(TypeInfo.class);
		factoryClass = typeInfoAnnotation.value();
		// check for valid factory class
		if (!TypeInfoFactory.class.isAssignableFrom(factoryClass)) {
			throw new InvalidTypesException("TypeInfo annotation does not specify a valid TypeInfoFactory.");
		}
		// instantiate
		return (TypeInfoFactory<X>) InstantiationUtil.instantiate(factoryClass);
	}

	private static Tuple3<Type, List<ParameterizedType>, TypeInfoFactory<?>> buildTypeHierarchy(final Type type) {

		if (!isClassType(type)) {
			return null;
		}
		final List<ParameterizedType> factoryHierarchy = TypeHierarchyBuilder.buildParameterizedTypeHierarchy(
			type,
			clazz -> clazz.equals(Object.class) || getTypeInfoFactory(clazz) != null,
			clazz -> true);

		final TypeInfoFactory<?> factory;
		final Type factoryDefiningType;

		if (factoryHierarchy.size() > 0) {
			factoryDefiningType = factoryHierarchy.get(factoryHierarchy.size() - 1);
			factory = getTypeInfoFactory(factoryDefiningType);
		} else {
			factoryDefiningType = type;
			factory = getTypeInfoFactory(type);
		}

		if (factory == null) {
			return null;
		}

		return Tuple3.of(factoryDefiningType, factoryHierarchy, factory);
	}

}
