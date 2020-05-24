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

import java.lang.reflect.Type;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.hasSuperclass;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.isClassType;
import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;

/**
 * See {@link HadoopWritableExtractor}.
 * TODO:: maybe we could deprecate the {@link AvroUtils}.
 */
class AvroTypeExtractor {

	private static final String AVRO_SPECIFIC_RECORD_BASE_CLASS = "org.apache.avro.specific.SpecificRecordBase";


	// ------------------------------------------------------------------------
	//  Extract TypeInformation for Avro
	// ------------------------------------------------------------------------

	static TypeInformation<?> extract(final Type type) {
		if (isClassType(type)) {
			final Class<?> clazz = typeToClass(type);
			if (hasSuperclass(clazz, AVRO_SPECIFIC_RECORD_BASE_CLASS)) {
				return AvroUtils.getAvroUtils().createAvroTypeInfo(clazz);
			}
		}
		return null;
	}
}
