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
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeExtractor.bindTypeVariableFromGenericParameters;
import static org.apache.flink.api.java.typeutils.TypeExtractor.createTypeInfo;
import static org.apache.flink.api.java.typeutils.TypeResolve.resolveTypeFromTypeHierarchy;

/**
 * This class is used to extract the {@link org.apache.flink.api.common.typeinfo.TypeInformation} of the class that has
 * the {@link org.apache.flink.api.common.typeinfo.TypeInfo} annotation.
 */
class TypeInfoFactoryExtractor {

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

		final List<ParameterizedType> factoryHierarchy = new ArrayList<>();
		final TypeInfoFactory<?> factory = getClosestFactory(factoryHierarchy, type);

		if (factory == null) {
			return null;
		}

		final Type factoryDefiningType = factoryHierarchy.size() < 1 ? type :
			resolveTypeFromTypeHierarchy(factoryHierarchy.get(factoryHierarchy.size() - 1), factoryHierarchy, true);

		// infer possible type parameters from input
		final Map<String, TypeInformation<?>> genericParams;

		if (factoryDefiningType instanceof ParameterizedType) {
			genericParams = new HashMap<>();
			final Type[] genericTypes = ((ParameterizedType) factoryDefiningType).getActualTypeArguments();
			final Type[] args = typeToClass(factoryDefiningType).getTypeParameters();
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

		final TypeInformation<?> createdTypeInfo = factory.createTypeInfo(type, genericParams);
		if (createdTypeInfo == null) {
			//TODO:: has a test??
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
	static Map<TypeVariable<?>, TypeInformation<?>> bindTypeVariable(final Type type, final TypeInformation<?> typeInformation) {

		final List<ParameterizedType> factoryHierarchy = new ArrayList<>();
		final TypeInfoFactory<?> factory = getClosestFactory(factoryHierarchy, type);

		if (factory != null) {
			final Type factoryDefiningType = factoryHierarchy.size() < 1 ? type :
				resolveTypeFromTypeHierarchy(factoryHierarchy.get(factoryHierarchy.size() - 1), factoryHierarchy, true);
			if (factoryDefiningType instanceof ParameterizedType) {
				return bindTypeVariableFromGenericParameters((ParameterizedType) factoryDefiningType, typeInformation);
			}
		}

		return null;
	}

	/**
	 * Traverses the type hierarchy up until a type information factory can be found.
	 * @param typeHierarchy hierarchy to be filled while traversing up
	 * @param t type for which a factory needs to be found
	 * @return closest type information factory or null if there is no factory in the type hierarchy
	 */
	private static TypeInfoFactory<?> getClosestFactory(List<ParameterizedType> typeHierarchy, Type t) {
		TypeInfoFactory factory = null;
		while (factory == null && isClassType(t) && !(typeToClass(t).equals(Object.class))) {
			if (t instanceof ParameterizedType) {
				typeHierarchy.add((ParameterizedType) t);
			}
			factory = getTypeInfoFactory(t);
			t = typeToClass(t).getGenericSuperclass();

			if (t == null) {
				break;
			}
		}
		return factory;
	}

	/**
	 * Returns the type information factory for a type using the factory registry or annotations.
	 */
	private static TypeInfoFactory<?> getTypeInfoFactory(Type t) {
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
		return (TypeInfoFactory<?>) InstantiationUtil.instantiate(factoryClass);
	}

}
