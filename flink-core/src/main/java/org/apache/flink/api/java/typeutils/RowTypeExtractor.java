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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collections;

class RowTypeExtractor {

	private static final Logger LOG = LoggerFactory.getLogger(RowTypeExtractor.class);

	/**
	 * Extract the {@link TypeInformation} for the {@link org.apache.flink.types.Row} object.
	 * @param value the object needed to extract {@link TypeInformation}
	 * @return the {@link TypeInformation} of the type or {@code null} if the value is not the Tuple type.
	 */
	@Nullable
	static TypeInformation<?> extract(Object value) {
		if (!(value instanceof Row)){
			return null;
		}

		final Row row = (Row) value;
		final int arity = row.getArity();
		for (int i = 0; i < arity; i++) {
			if (row.getField(i) == null) {
				LOG.warn("Cannot extract type of Row field, because of Row field[" + i + "] is null. " +
					"Should define RowTypeInfo explicitly.");
				return TypeExtractor.extract(value.getClass(), Collections.emptyMap(), Collections.emptyList());
			}
		}
		final TypeInformation<?>[] typeArray = new TypeInformation<?>[arity];
		for (int i = 0; i < arity; i++) {
			typeArray[i] = TypeExtractor.getForObject(row.getField(i));
		}
		return new RowTypeInfo(typeArray);
	}
}
