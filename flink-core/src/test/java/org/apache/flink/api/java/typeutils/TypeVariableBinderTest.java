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

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Either;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.TypeHierarchyBuilder.buildParameterizedTypeHierarchy;
import static org.apache.flink.api.java.typeutils.TypeResolver.resolveTypeFromTypeHierarchy;

/**
 * Test type variable binding.
 */
public class TypeVariableBinderTest {
	// --------------------------------------------------------------------------------------------
	// Test bind type variable
	// --------------------------------------------------------------------------------------------

	@Test
	public void testBindTypeVariablesFromSimpleTypeInformation() {
		final TypeResolverTest.DefaultSimpleUDF myUdf = new TypeResolverTest.DefaultSimpleUDF<String>();

		final TypeVariable<?> inputTypeVariable =  myUdf.getClass().getTypeParameters()[0];

		final TypeInformation<String> typeInformation = TypeInformation.of(new TypeHint<String>() {});

		final Map<TypeVariable<?>, TypeInformation<?>> expectedResult = new HashMap<>();
		expectedResult.put(inputTypeVariable, typeInformation);

		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(myUdf.getClass(), TypeResolverTest.BaseInterface.class);
		final Type baseType = typeHierarchy.get(typeHierarchy.size() - 1);
		final ParameterizedType resolvedType =
			(ParameterizedType) resolveTypeFromTypeHierarchy(baseType, typeHierarchy, true);

		final Map<TypeVariable<?>, TypeInformation<?>> result =
			TypeVariableBinder.bindTypeVariables(resolvedType.getActualTypeArguments()[0], typeInformation);

		Assert.assertEquals(expectedResult, result);
	}

	@Test
	public void testBindTypeVariableFromCompositeTypeInformation() {

		final TypeResolverTest.CompositeUDF myCompositeUDF = new TypeResolverTest.DefaultCompositeUDF<Long, Integer, Boolean>();

		final TypeInformation<Tuple2<Integer, Boolean>> in2TypeInformation = TypeInformation.of(new TypeHint<Tuple2<Integer, Boolean>>(){});

		final TypeVariable<?> second = TypeResolverTest.DefaultCompositeUDF.class.getTypeParameters()[1];
		final TypeVariable<?> third = TypeResolverTest.DefaultCompositeUDF.class.getTypeParameters()[2];
		final TypeInformation secondTypeInformation = TypeInformation.of(new TypeHint<Integer>() {});
		final TypeInformation thirdTypeInformation = TypeInformation.of(new TypeHint<Boolean>() {});

		final Map<TypeVariable<?>, TypeInformation<?>> expectedResult = new HashMap<>();
		expectedResult.put(second, secondTypeInformation);
		expectedResult.put(third, thirdTypeInformation);

		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(myCompositeUDF.getClass(), TypeResolverTest.BaseInterface.class);
		final Type baseType = typeHierarchy.get(typeHierarchy.size() - 1);
		final ParameterizedType resolvedType =
			(ParameterizedType) resolveTypeFromTypeHierarchy(baseType, typeHierarchy, true);

		final Map<TypeVariable<?>, TypeInformation<?>> result =
			TypeVariableBinder.bindTypeVariables(resolvedType.getActualTypeArguments()[1], in2TypeInformation);

		Assert.assertEquals(expectedResult, result);
	}

	@Test
	public void testBindTypeVariableFromGenericArrayTypeInformation() {
		final TypeResolverTest.GenericArrayUDF myGenericArrayUDF = new TypeResolverTest.DefaultGenericArrayUDF<Double, Boolean>();

		final TypeVariable<?> second = TypeResolverTest.GenericArrayUDF.class.getTypeParameters()[1];

		final TypeInformation<Boolean> secondTypeInformation = TypeInformation.of(new TypeHint<Boolean>() {});

		final Map<TypeVariable<?>, TypeInformation<?>> expectedResult = new HashMap<>();
		expectedResult.put(second, secondTypeInformation);

		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(myGenericArrayUDF.getClass(), TypeResolverTest.BaseInterface.class);
		final Type baseType = typeHierarchy.get(typeHierarchy.size() - 1);
		final ParameterizedType resolvedType =
			(ParameterizedType) resolveTypeFromTypeHierarchy(baseType, typeHierarchy, false);

		//Test ObjectArray
		TypeInformation<?> arrayTypeInformation =
			ObjectArrayTypeInfo.getInfoFor(Boolean[].class, TypeInformation.of(new TypeHint<Boolean>(){}));

		Map<TypeVariable<?>, TypeInformation<?>> result = TypeVariableBinder.bindTypeVariables(
			resolvedType.getActualTypeArguments()[1], arrayTypeInformation);

		Assert.assertEquals(expectedResult, result);

		//Test BasicArrayTypeInfo
		arrayTypeInformation = TypeInformation.of(new TypeHint<Boolean[]>(){});

		result = TypeVariableBinder.bindTypeVariables(
			resolvedType.getActualTypeArguments()[1],
			arrayTypeInformation);

		Assert.assertEquals(expectedResult, result);
	}

	@Test
	public void testBindTypeVariableFromPojoTypeInformation() {
		final TypeResolverTest.PojoUDF myPojoUDF = new TypeResolverTest.DefaultPojoUDF<Double, String, Integer>();

		final TypeVariable<?> second = TypeResolverTest.DefaultPojoUDF.class.getTypeParameters()[1];
		final TypeVariable<?> third = TypeResolverTest.DefaultPojoUDF.class.getTypeParameters()[2];

		final TypeInformation secondTypeInformation = TypeInformation.of(new TypeHint<String>() {});
		final TypeInformation thirdTypeInformation = TypeInformation.of(new TypeHint<Integer>(){});

		final Map<TypeVariable<?>, TypeInformation<?>> expectedResult = new HashMap<>();
		expectedResult.put(second, secondTypeInformation);
		expectedResult.put(third, thirdTypeInformation);

		final TypeInformation<TypeResolverTest.Pojo<String, Integer>> in2TypeInformation = TypeInformation.of(new TypeHint<TypeResolverTest.Pojo<String, Integer>>(){});

		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(myPojoUDF.getClass(), TypeResolverTest.BaseInterface.class);
		final Type baseType = typeHierarchy.get(typeHierarchy.size() - 1);
		final ParameterizedType resolvedType =
			(ParameterizedType) resolveTypeFromTypeHierarchy(baseType, typeHierarchy, false);

		final Map<TypeVariable<?>, TypeInformation<?>> result =
			TypeVariableBinder.bindTypeVariables(resolvedType.getActualTypeArguments()[1], in2TypeInformation);

		Assert.assertEquals(expectedResult, result);
	}

	@Test
	public void testBindTypeVariableFromTypeInfoFactory() {
		final TypeResolverTest.TypeInfoFactoryUDF typeInfoFactoryUDF = new TypeResolverTest.DefaultTypeInfoFactoryUDF<String[], Boolean, Integer[]>();

		final TypeInformation<Either<Boolean, Integer[]>> in2TypeInformation =
			TypeInformation.of(new TypeHint<Either<Boolean, Integer[]>>(){});

		final TypeVariable<?> second = TypeResolverTest.DefaultTypeInfoFactoryUDF.class.getTypeParameters()[1];
		final TypeVariable<?> third = TypeResolverTest.DefaultTypeInfoFactoryUDF.class.getTypeParameters()[2];

		final TypeInformation<Boolean> secondTypeInformation = TypeInformation.of(new TypeHint<Boolean>(){});
		final TypeInformation<Integer[]> thirdTypeInformation = TypeInformation.of(new TypeHint<Integer[]>(){});

		final Map<TypeVariable<?>, TypeInformation<?>> expectedResult = new HashMap<>();
		expectedResult.put(second, secondTypeInformation);
		expectedResult.put(third, thirdTypeInformation);

		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(typeInfoFactoryUDF.getClass(), TypeResolverTest.BaseInterface.class);
		final Type baseType = typeHierarchy.get(typeHierarchy.size() - 1);
		final ParameterizedType resolvedType =
			(ParameterizedType) resolveTypeFromTypeHierarchy(baseType, typeHierarchy, false);

		final Map<TypeVariable<?>, TypeInformation<?>> result =
			TypeVariableBinder.bindTypeVariables(resolvedType.getActualTypeArguments()[1], in2TypeInformation);

		Assert.assertEquals(expectedResult, result);
	}

	@Test
	public void testBindTypeVariableWhenAllTypeVariableHasConcreteClass() {

		final TypeInformation<Integer> in1TypeInformation = TypeInformation.of(new TypeHint<Integer>(){});
		final TypeInformation<Tuple2<String, Integer>> in2TypeInformation = TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){});

		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(TypeResolverTest.MyCompositeUDF.class, TypeResolverTest.BaseInterface.class);
		final Type baseType = typeHierarchy.get(typeHierarchy.size() - 1);
		final ParameterizedType resolvedType =
			(ParameterizedType) resolveTypeFromTypeHierarchy(baseType, typeHierarchy, false);

		Map<TypeVariable<?>, TypeInformation<?>> result =
			TypeVariableBinder.bindTypeVariables(resolvedType.getActualTypeArguments()[0], in1TypeInformation);

		Assert.assertEquals(Collections.emptyMap(), result);

		result = TypeVariableBinder.bindTypeVariables(resolvedType.getActualTypeArguments()[1], in2TypeInformation);

		Assert.assertEquals(Collections.emptyMap(), result);
	}
}
