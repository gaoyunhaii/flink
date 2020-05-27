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

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.TypeVariable;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.typeToClass;
import static org.apache.flink.api.java.typeutils.TypeHierarchyBuilder.buildParameterizedTypeHierarchy;

/**
 * Test build type hierarchy.
 */
public class TypeHierarchyBuilderTest {

	@Test
	public void testBuildParameterizedTypeHierarchyFromSubClassToBaseClass() {
		final List<ParameterizedType> parameterizedTypeHierarchy =
			buildParameterizedTypeHierarchy(TypeResolverTest.MySimpleUDF.class, TypeResolverTest.BaseInterface.class);
		final ParameterizedType normalInterfaceType = parameterizedTypeHierarchy.get(parameterizedTypeHierarchy.size() - 1);

		Assert.assertEquals(4, parameterizedTypeHierarchy.size());
		Assert.assertEquals(TypeResolverTest.DefaultSimpleUDF.class, parameterizedTypeHierarchy.get(0).getRawType());
		Assert.assertEquals(TypeResolverTest.AbstractSimpleUDF.class, parameterizedTypeHierarchy.get(1).getRawType());
		Assert.assertEquals(TypeResolverTest.RichInterface.class, parameterizedTypeHierarchy.get(2).getRawType());

		Assert.assertEquals(TypeResolverTest.NormalInterface.class, normalInterfaceType.getRawType());
		Assert.assertTrue(normalInterfaceType.getActualTypeArguments()[0] instanceof TypeVariable);
		Assert.assertTrue(normalInterfaceType.getActualTypeArguments()[1] instanceof TypeVariable);
	}

	@Test
	public void testBuildParameterizedTypeHierarchyFromSubTypeToBaseClass() {
		final List<ParameterizedType> parameterizedTypeHierarchy =
			buildParameterizedTypeHierarchy(TypeResolverTest.MySimpleUDF.class.getGenericSuperclass(), TypeResolverTest.BaseInterface.class);
		final ParameterizedType abstractSimpleUDFType = parameterizedTypeHierarchy.get(parameterizedTypeHierarchy.size() - 1);

		Assert.assertEquals(2, parameterizedTypeHierarchy.size());
		Assert.assertEquals(TypeResolverTest.DefaultSimpleUDF.class, parameterizedTypeHierarchy.get(0).getRawType());
		Assert.assertEquals(TypeResolverTest.AbstractSimpleUDF.class, parameterizedTypeHierarchy.get(1).getRawType());

		Assert.assertTrue(abstractSimpleUDFType.getActualTypeArguments()[0] instanceof TypeVariable);
		Assert.assertTrue(abstractSimpleUDFType.getActualTypeArguments()[1] instanceof ParameterizedType);
		Assert.assertEquals(Tuple2.class, typeToClass(abstractSimpleUDFType.getActualTypeArguments()[1]));
	}

	@Test
	public void testBuildParameterizedTypeHierarchyByCustomizedPrediction() {
		final List<ParameterizedType> parameterizedTypeHierarchy =
			buildParameterizedTypeHierarchy(
				TypeResolverTest.MySimpleUDF.class.getGenericSuperclass(),
				clazz -> !clazz.equals(TypeResolverTest.MySimpleUDF.class),
				TypeResolverTest.BaseInterface.class::isAssignableFrom
			);

		final ParameterizedType parameterizedType = parameterizedTypeHierarchy.get(parameterizedTypeHierarchy.size() - 1);
		Assert.assertEquals(1, parameterizedTypeHierarchy.size());
		Assert.assertEquals(TypeResolverTest.DefaultSimpleUDF.class, typeToClass(parameterizedType));

	}

	@Test
	public void testBuildParameterizedTypeHierarchyWithoutInheritance() {
		final List<ParameterizedType> parameterizedTypeHierarchy =
			buildParameterizedTypeHierarchy(TypeResolverTest.MySimpleUDF.class, TypeResolverTest.class);
		Assert.assertEquals(Collections.emptyList(), parameterizedTypeHierarchy);
	}
}
