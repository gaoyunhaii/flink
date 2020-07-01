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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.types.AbstractTypeClass;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The class that implements this interface tells the type extractor stack which classes it could extract the
 * {@link TypeInformationExtractor} corresponding to. The type extractor stack automatically loads the implementation through the
 * java service loader mechanism.
 */
@Internal
public interface TypeInformationExtractor {

	/**
	 * Resolve the {@link TypeDescription} for the given type.
	 * @param type the type that is needed to compute {@link TypeDescription}.
	 * @param context used to resolve the {@link TypeDescription} for the generic parameters or components and contains
	 *                some information of resolving process.
	 * @return {@link TypeInformation} of the given type or {@link Optional#empty()} if could not handle this type
	 * @throws InvalidTypesException if error occurs during the resolving
	 */
	Optional<TypeDescription> resolve(final Type type, final Context context);

	/**
	 * @return the classes that the extractor could extract the{@link TypeInformationExtractor} corresponding to.
	 */
	List<Class<?>> getClasses();

	default List<String> getClassNames() {
		return getClasses().stream().map(Class::getName).collect(Collectors.toList());
	}

	/**
	 * The extractor can use the interface to extract {@link TypeDescription} for the given type and get the information
	 * of extracting process.
	 */
	interface Context {

		/**
		 * @return the mapping relation between {@link TypeVariable} and {@link TypeInformation}. The extractor can try to
		 * get the {@link TypeInformation} for a {@link TypeVariable}.
		 */
		Map<TypeVariable<?>, TypeInformation<?>> getTypeVariableBindings();

		/**
		 * @return the classes that type extractor stack is extracting for {@link TypeInformation}. This is used to check
		 * whether there is a recursive type extracting process.
		 */
		List<Class<?>> getExtractingClasses();

		/**
		 * @param type the type that is needed to resolve {@link TypeDescription}.
		 * @return {@link TypeDescription} of the given type.
		 * @throws InvalidTypesException if error occurs during resolving the {@link TypeDescription}.
		 */
		TypeDescription resolve(final Type type);
	}
}
