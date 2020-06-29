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

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * The class that implements this interface tells the type extractor stack which classes it could extract the
 * {@link TypeInformationExtractor} corresponding to. The type extractor stack automatically loads the implementation through the
 * java service loader mechanism.
 */
@Internal
public interface TypeInformationExtractor {

	interface ResolveContext {

		List<Class<?>> getExtractingClasses();

		Type resolve(final Type type);
	}

	Optional<Type> resolve(final Type type, final ResolveContext context);

	/**
	 * @return the classes that the extractor could extract the{@link TypeInformationExtractor} corresponding to.
	 */
	List<Class<?>> getClasses();

	/**
	 * Extract the {@link TypeInformation} of given type.
	 * @param type the type that is needed to extract {@link TypeInformation}
	 * @param context used to extract the {@link TypeInformation} for the generic parameters or components and contains some
	 *                information of extracting process.
	 * @return {@link TypeInformation} of the given type or {@link Optional#empty()} if the extractor could not handle this type
	 * @throws InvalidTypesException if error occurs during extracting the {@link TypeInformation}
	 */
	Optional<TypeInformation<?>> extract(final Type type, final Context context);

	/**
	 * The extractor can use the interface to extract {@link TypeInformation} for the given type and get the information
	 * of extracting process.
	 */
	interface Context {
		/**
		 * @param type the type that is needed to extract {@link TypeInformation}
		 * @return {@link TypeInformation} of the given type
		 * @throws InvalidTypesException if error occurs during extracting the {@link TypeInformation} or can not handle the type.
		 */
		TypeInformation<?> extract(final Type type);

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
	}
}
