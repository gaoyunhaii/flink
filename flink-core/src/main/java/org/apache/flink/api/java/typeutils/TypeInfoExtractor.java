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

import javax.annotation.Nullable;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.getClosestFactory;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeExtractor.createTypeInfo;
import static org.apache.flink.api.java.typeutils.TypeResolve.resolveTypeFromTypeHierarchy;

/**
 * This class is used to extract the {@link org.apache.flink.api.common.typeinfo.TypeInformation} of the class that has
 * the {@link org.apache.flink.api.common.typeinfo.TypeInfo} annotation.
 */
class TypeInfoExtractor {

	/**
	 * Extract {@link TypeInformation} for the type that has {@link TypeInfo} annotation.
	 * @param type  the type needed to extract {@link TypeInformation}
	 * @param typeVariableBindings contains mapping relation between {@link TypeVariable} and {@link TypeInformation}.
	 * @param extractingClasses the classes that the type is nested into.
	 * @return the {@link TypeInformation} of the given type or {@code null} if the type does not have the annotation
	 * @throws InvalidTypesException if the factory does not create a valid {@link TypeInformation} or if creating generic type failed
	 */
	@Nullable
	static TypeInformation<?> extractTypeInformationForTypeFactory(
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

}
