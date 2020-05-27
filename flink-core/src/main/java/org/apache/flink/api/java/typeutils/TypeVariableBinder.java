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

import javax.annotation.Nonnull;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

/**
 * This class is used to infer the {@link TypeInformation} of the {@link TypeVariable}s in the given resolved type from
 * the given {@link TypeInformation}.
 */
class TypeVariableBinder {

	/**
	 * Infer all the {@link TypeInformation} of the {@link TypeVariable}s in the given resolved type from the given {@link TypeInformation}.
	 * @param type the resolved type
	 * @param typeInformation the {@link TypeInformation} of the given resolved type
	 * @return the mapping relation between {@link TypeVariable} and {@link TypeInformation}
	 */
	@Nonnull
	//TODO:: make this more general and decouple it from specific extractor
	static Map<TypeVariable<?>, TypeInformation<?>> bindTypeVariables(final Type type, final TypeInformation<?> typeInformation) {

		Map<TypeVariable<?>, TypeInformation<?>> result;

		if ((result = TypeInfoFactoryExtractor.bindTypeVariables(type, typeInformation)) != null) {
			return result;
		}

		if ((result = ArrayTypeExtractor.bindTypeVariables(type, typeInformation)) != null) {
			return result;
		}

		if ((result = TupleTypeExtractor.bindTypeVariables(type, typeInformation)) != null) {
			return result;
		}

		if ((result = PojoTypeExtractor.bindTypeVariables(type, typeInformation)) != null) {
			return result;
		}

		if (type instanceof TypeVariable) {
			final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings = new HashMap<>();

			typeVariableBindings.put((TypeVariable<?>) type, typeInformation);
			return typeVariableBindings;
		}

		return Collections.emptyMap();
	}

	/**
	 * Infer all the {@link TypeInformation} of the {@link TypeVariable}s in the given resolved from the given {@link TypeInformation}.
	 *
	 * @param type the type that has {@link TypeVariable}
	 * @param typeInformation the {@link TypeInformation} that stores the mapping relations between the generic parameters
	 *                        and {@link TypeInformation}.
	 * @return the mapping relation between {@link TypeVariable} and {@link TypeInformation}
	 */
	static Map<TypeVariable<?>, TypeInformation<?>> bindTypeVariableFromGenericParameters(
		final ParameterizedType type,
		final TypeInformation<?> typeInformation) {

		final Map<TypeVariable<?>, TypeInformation<?>> typeVariableBindings = new HashMap<>();
		final Type[] typeParams = typeToClass(type).getTypeParameters();
		final Type[] actualParams = type.getActualTypeArguments();
		for (int i = 0; i < actualParams.length; i++) {
			final Map<String, TypeInformation<?>> componentInfo = typeInformation.getGenericParameters();
			final String typeParamName = typeParams[i].toString();
			if (!componentInfo.containsKey(typeParamName) || componentInfo.get(typeParamName) == null) {
				throw new InvalidTypesException("TypeInformation '" + typeInformation.getClass().getSimpleName() +
					"' does not supply a mapping of TypeVariable '" + typeParamName + "' to corresponding TypeInformation. " +
					"Input type inference can only produce a result with this information. " +
					"Please implement method 'TypeInformation.getGenericParameters()' for this.");
			}
			final Map<TypeVariable<?>, TypeInformation<?>> sub =
				bindTypeVariables(actualParams[i], componentInfo.get(typeParamName));
			typeVariableBindings.putAll(sub);
		}
		return typeVariableBindings.isEmpty() ? Collections.emptyMap() : typeVariableBindings;
	}
}
