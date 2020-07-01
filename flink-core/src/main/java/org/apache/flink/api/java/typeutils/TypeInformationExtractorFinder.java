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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.javaruntime.TypeClassFactoryFinder;
import org.apache.flink.api.java.typeutils.types.AbstractType;
import org.apache.flink.api.java.typeutils.types.AbstractTypeClass;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.TreeMap;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isAvroType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isHadoopWritable;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

/**
 * This class responses for finding extractor candidates.
 */
class TypeInformationExtractorFinder {

	/**
	 * 1. The registered classes are sorted as following :
	 * 	1.1 The child type is "greater" than the parent type. This is because we assume that the child type's extractor
	 *      is more suitable for a type.
	 * 	1.2 The sibling types uses name to compare.
	 * 2. Sorting by class is because that the finder returns a list candidates extractors that could
	 *    extract the given type's {@link TypeInformation}. We want want list has a deterministic order.
	 *
	 */
	private static final NavigableMap<AbstractTypeClass, TypeInformationExtractor> EXTRACTORS = new TreeMap<>((o1, o2) -> {
		Preconditions.checkArgument(o1 != null);
		Preconditions.checkArgument(o2 != null);
		boolean o2greater = o1.isAssignableFrom(o2);
		boolean o1greater = o2.isAssignableFrom(o1);

		if (o2greater && !o1greater) {
			return -1;
		} else if (o1greater && !o2greater) {
			return 1;
		}
		return o1.getName().compareTo(o2.getName());
	});


	/**
	 * Return a ordered list of {@link TypeInfoFactoryExtractor}s that might extract the given type's {@link TypeInformation}.
	 *
	 * @param type the given type
	 * @return {@link TypeInformationExtractor} or {@link Optional#empty()} if could not find any one.
	 * @throws RuntimeException if can not find {@link TypeInformationExtractor} for the Hadoop writable or the Avro class.
	 */
	static List<TypeInformationExtractor> findTypeInfoExtractor(final AbstractType type) {
		loadExtractors();

		final List<TypeInformationExtractor> typeInfoExtractors = new ArrayList<>();

		typeInfoExtractors.add(TypeInfoFactoryExtractor.INSTANCE);

		if (isClassType(type)) {
			final AbstractTypeClass clazz = typeToClass(type);
			EXTRACTORS.descendingKeySet()
				.stream()
				.filter(c -> c.isAssignableFrom(clazz))
				.map(EXTRACTORS::get)
				.filter(x -> x != null) //TODO:: why x is null ?
				.forEachOrdered(typeInfoExtractors::add);

			if (typeInfoExtractors.size() == 1) {
				if (isHadoopWritable(clazz)) {
					throw new RuntimeException("You may be missing the 'flink-hadoop-compatibility' dependency.");
				}

				if (isAvroType(clazz)) {
					throw new RuntimeException("You may be missing the 'flink-avro' dependency.");
				}
			}
		}

		typeInfoExtractors.add(ArrayTypeInfoExtractor.INSTANCE);
		typeInfoExtractors.add(TypeVariableExtractor.INSTANCE);
		typeInfoExtractors.add(RecursiveTypeInfoExtractor.INSTANCE);
		typeInfoExtractors.add(PojoTypeInfoExtractor.INSTANCE);
		typeInfoExtractors.add(GenericTypeInfoExtractor.INSTANCE);

		return typeInfoExtractors;
	}

	@VisibleForTesting
	static NavigableMap<AbstractTypeClass, TypeInformationExtractor> getEXTRACTORS() {
		if (EXTRACTORS.size() == 0) {
			loadExtractors();
		}
		return EXTRACTORS;
	}

	private static void loadExtractors() {
		if (EXTRACTORS.size() == 0) {
			ServiceLoader
				.load(TypeInformationExtractor.class, TypeExtractor.class.getClassLoader())
				.forEach(typeInformationExtractor -> typeInformationExtractor.getClassNames()
					.forEach(c -> EXTRACTORS.put(TypeClassFactoryFinder.forName(c), typeInformationExtractor)));
		}
	}
}
