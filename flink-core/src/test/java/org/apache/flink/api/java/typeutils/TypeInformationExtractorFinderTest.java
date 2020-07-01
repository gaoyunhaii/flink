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

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.java.typeutils.javaruntime.JavaTypeClass;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.Matchers.is;

/**
 * Test {@link TypeInformationExtractorFinder}.
 */
public class TypeInformationExtractorFinderTest {

	@Test(expected = RuntimeException.class)
	public void testThrowRuntimeExceptionForAvroClass() {
		TypeInformationExtractorFinder.findTypeInfoExtractor(new JavaTypeClass(TestAvroClass.class));
	}

	@Test(expected = RuntimeException.class)
	public void testThrowRuntimeExceptionForWritableClass() {
		TypeExtractor.createTypeInfo(TestWritable.class);
	}

	@Test
	public void testTheOrderOfRegisteringClasses() {
		Assert.assertThat(EXPECTED_CLASSES, is(new ArrayList<>(TypeInformationExtractorFinder.getEXTRACTORS().descendingKeySet())));
	}

	private static class TestWritable implements Writable {

		@Override
		public void write(DataOutput dataOutput) throws IOException {

		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {
			throw new IOException("test");
		}
	}

	private static class TestAvroClass extends SpecificRecordBase {

		@Override
		public Schema getSchema() {
			return null;
		}

		@Override
		public Object get(int i) {
			return null;
		}

		@Override
		public void put(int i, Object o) {

		}
	}

	private static final List<Class<?>> EXPECTED_CLASSES = Arrays.asList(
		void.class,
		short.class,
		org.apache.flink.types.Value.class,
		org.apache.flink.api.java.tuple.Tuple.class,
		long.class,
		java.sql.Timestamp.class,
		java.sql.Time.class,
		java.sql.Date.class,
		java.util.Date.class,
		java.time.Instant.class,
		java.math.BigInteger.class,
		java.math.BigDecimal.class,
		Void.class,
		String.class,
		Short.class,
		Long.class,
		Integer.class,
		Float.class,
		Enum.class,
		Double.class,
		Character.class,
		Byte.class,
		Boolean.class,
		int.class,
		float.class,
		double.class,
		char.class,
		byte.class,
		boolean.class);
}
