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
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Either;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.GenericArrayType;
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
 * Test TypeExtractor contract.
 */
public class TypeExtractorContractTest {

	// --------------------------------------------------------------------------------------------
	// Test resolve type
	// --------------------------------------------------------------------------------------------

	@Test
	public void testGetTypeParameter() {
		final Type firstType = TypeExtractor.getParameterType(BaseInterface.class, MySimpleUDF.class, 0);
		final ParameterizedType secondType = (ParameterizedType) TypeExtractor.getParameterType(BaseInterface.class, MySimpleUDF.class, 1);

		Assert.assertEquals(Integer.class, firstType);
		Assert.assertEquals(Tuple2.class, secondType.getRawType());
		Assert.assertEquals(String.class, secondType.getActualTypeArguments()[0]);
		Assert.assertEquals(Integer.class, secondType.getActualTypeArguments()[1]);
	}

	@Test(expected = InvalidTypesException.class)
	public void getGetTypeParameterFail() {
		DefaultSimpleUDF defaultSimpleUDF = new DefaultSimpleUDF<>();
		TypeExtractor.getParameterType(DefaultSimpleUDF.class, defaultSimpleUDF.getClass(), 0);
	}

	@Test
	public void testResolveSimpleType() {
		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(MySimpleUDF.class, BaseInterface.class);
		final ParameterizedType normalInterfaceType = typeHierarchy.get(typeHierarchy.size() - 1);

		final ParameterizedType resolvedNormalInterfaceType =
			(ParameterizedType) TypeResolver.resolveTypeFromTypeHierarchy(normalInterfaceType, typeHierarchy, true);

		final ParameterizedType secondResolvedType = (ParameterizedType) resolvedNormalInterfaceType.getActualTypeArguments()[1];

		Assert.assertEquals(Integer.class, resolvedNormalInterfaceType.getActualTypeArguments()[0]);
		Assert.assertEquals(String.class, secondResolvedType.getActualTypeArguments()[0]);
		Assert.assertEquals(Integer.class, secondResolvedType.getActualTypeArguments()[1]);
	}

	@Test
	public void testResolveCompositeType() {
		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(MyCompositeUDF.class, BaseInterface.class);
		final ParameterizedType normalInterfaceType = typeHierarchy.get(typeHierarchy.size() - 1);
		final ParameterizedType resolvedNormalInterfaceType =
			(ParameterizedType) TypeResolver.resolveTypeFromTypeHierarchy(normalInterfaceType, typeHierarchy, true);

		Assert.assertEquals(Integer.class, resolvedNormalInterfaceType.getActualTypeArguments()[0]);

		final ParameterizedType resolvedTuple2Type = (ParameterizedType) resolvedNormalInterfaceType.getActualTypeArguments()[1];

		Assert.assertEquals(Tuple2.class, resolvedTuple2Type.getRawType());
		Assert.assertEquals(String.class, resolvedTuple2Type.getActualTypeArguments()[0]);
		Assert.assertEquals(Boolean.class, resolvedTuple2Type.getActualTypeArguments()[1]);
	}

	@Test
	public void testResolveGenericArrayType() {
		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(MyGenericArrayUDF.class, BaseInterface.class);

		final ParameterizedType normalInterfaceType = typeHierarchy.get(typeHierarchy.size() - 1);

		final ParameterizedType resolvedNormalInterfaceType =
			(ParameterizedType) TypeResolver.resolveTypeFromTypeHierarchy(normalInterfaceType, typeHierarchy, true);

		final GenericArrayType secondResolvedType = (GenericArrayType) resolvedNormalInterfaceType.getActualTypeArguments()[1];

		Assert.assertEquals(Integer.class, resolvedNormalInterfaceType.getActualTypeArguments()[0]);
		Assert.assertEquals(String.class, secondResolvedType.getGenericComponentType());
	}

	@Test
	public void testDoesNotResolveGenericArrayType() {
		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(MyGenericArrayUDF.class, BaseInterface.class);

		final ParameterizedType normalInterfaceType = typeHierarchy.get(typeHierarchy.size() - 1);

		final ParameterizedType resolvedNormalInterfaceType =
			(ParameterizedType) TypeResolver.resolveTypeFromTypeHierarchy(normalInterfaceType, typeHierarchy, false);

		final GenericArrayType genericArrayType = (GenericArrayType) resolvedNormalInterfaceType.getActualTypeArguments()[1];
		Assert.assertEquals(Integer.class, resolvedNormalInterfaceType.getActualTypeArguments()[0]);
		Assert.assertTrue(genericArrayType.getGenericComponentType() instanceof TypeVariable);
	}

	@Test
	public void testMaterializeTypeVariableToActualType() {

		final List<ParameterizedType> parameterizedTypes =
			buildParameterizedTypeHierarchy(MySimpleUDF.class, BaseInterface.class);

		final ParameterizedType normalInterfaceType = parameterizedTypes.get(parameterizedTypes.size() - 1);
		final TypeVariable firstTypeVariableOfNormalInterface = (TypeVariable) normalInterfaceType.getActualTypeArguments()[0];
		final TypeVariable secondTypeVariableOfNormalInterface = (TypeVariable) normalInterfaceType.getActualTypeArguments()[1];

		final Type materializedFirstTypeVariable = TypeResolver.materializeTypeVariable(parameterizedTypes, firstTypeVariableOfNormalInterface);
		final ParameterizedType materializedSecondTypeVariable = (ParameterizedType) TypeResolver.materializeTypeVariable(parameterizedTypes, secondTypeVariableOfNormalInterface);

		Assert.assertEquals(Integer.class, materializedFirstTypeVariable);
		Assert.assertEquals(String.class, materializedSecondTypeVariable.getActualTypeArguments()[0]);
		Assert.assertEquals(Integer.class, materializedSecondTypeVariable.getActualTypeArguments()[1]);
		Assert.assertEquals(Tuple2.class, materializedSecondTypeVariable.getRawType());
	}

	@Test
	public void testMaterializeTypeVariableToBottomTypeVariable() {
		final DefaultSimpleUDF myUdf = new DefaultSimpleUDF<String>();

		final List<ParameterizedType> parameterizedTypes =
			buildParameterizedTypeHierarchy(myUdf.getClass(), BaseInterface.class);

		final ParameterizedType normalInterfaceType = parameterizedTypes.get(parameterizedTypes.size() - 1);

		final TypeVariable firstTypeVariableOfNormalInterface = (TypeVariable) normalInterfaceType.getActualTypeArguments()[0];

		final ParameterizedType abstractSimpleUDFType = parameterizedTypes.get(0);

		final TypeVariable firstTypeVariableOfAbstractSimpleUDF = (TypeVariable) abstractSimpleUDFType.getActualTypeArguments()[0];
		final Type materializedFirstTypeVariable =
			TypeResolver.materializeTypeVariable(parameterizedTypes, firstTypeVariableOfNormalInterface);

		Assert.assertEquals(firstTypeVariableOfAbstractSimpleUDF, materializedFirstTypeVariable);
	}

	// --------------------------------------------------------------------------------------------
	// Test bind type variable
	// --------------------------------------------------------------------------------------------

	@Test
	public void testBindTypeVariablesFromSimpleTypeInformation() {
		final DefaultSimpleUDF myUdf = new DefaultSimpleUDF<String>();

		final TypeVariable<?> inputTypeVariable =  myUdf.getClass().getTypeParameters()[0];

		final TypeInformation<String> typeInformation = TypeInformation.of(new TypeHint<String>() {});

		final Map<TypeVariable<?>, TypeInformation<?>> expectedResult = new HashMap<>();
		expectedResult.put(inputTypeVariable, typeInformation);

		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(myUdf.getClass(), BaseInterface.class);
		final Type baseType = typeHierarchy.get(typeHierarchy.size() - 1);
		final ParameterizedType resolvedType =
			(ParameterizedType) resolveTypeFromTypeHierarchy(baseType, typeHierarchy, true);

		final Map<TypeVariable<?>, TypeInformation<?>> result =
			TypeVariableBinder.bindTypeVariables(resolvedType.getActualTypeArguments()[0], typeInformation);

		Assert.assertEquals(expectedResult, result);
	}

	@Test
	public void testBindTypeVariableFromCompositeTypeInformation() {

		final CompositeUDF myCompositeUDF = new DefaultCompositeUDF<Long, Integer, Boolean>();

		final TypeInformation<Tuple2<Integer, Boolean>> in2TypeInformation = TypeInformation.of(new TypeHint<Tuple2<Integer, Boolean>>(){});

		final TypeVariable<?> second = DefaultCompositeUDF.class.getTypeParameters()[1];
		final TypeVariable<?> third = DefaultCompositeUDF.class.getTypeParameters()[2];
		final TypeInformation secondTypeInformation = TypeInformation.of(new TypeHint<Integer>() {});
		final TypeInformation thirdTypeInformation = TypeInformation.of(new TypeHint<Boolean>() {});

		final Map<TypeVariable<?>, TypeInformation<?>> expectedResult = new HashMap<>();
		expectedResult.put(second, secondTypeInformation);
		expectedResult.put(third, thirdTypeInformation);

		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(myCompositeUDF.getClass(), BaseInterface.class);
		final Type baseType = typeHierarchy.get(typeHierarchy.size() - 1);
		final ParameterizedType resolvedType =
			(ParameterizedType) resolveTypeFromTypeHierarchy(baseType, typeHierarchy, true);

		final Map<TypeVariable<?>, TypeInformation<?>> result =
			TypeVariableBinder.bindTypeVariables(resolvedType.getActualTypeArguments()[1], in2TypeInformation);

		Assert.assertEquals(expectedResult, result);
	}

	@Test
	public void testBindTypeVariableFromGenericArrayTypeInformation() {
		final GenericArrayUDF myGenericArrayUDF = new DefaultGenericArrayUDF<Double, Boolean>();

		final TypeVariable<?> second = GenericArrayUDF.class.getTypeParameters()[1];

		final TypeInformation<Boolean> secondTypeInformation = TypeInformation.of(new TypeHint<Boolean>() {});

		final Map<TypeVariable<?>, TypeInformation<?>> expectedResult = new HashMap<>();
		expectedResult.put(second, secondTypeInformation);

		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(myGenericArrayUDF.getClass(), BaseInterface.class);
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
		final PojoUDF myPojoUDF = new DefaultPojoUDF<Double, String, Integer>();

		final TypeVariable<?> second = DefaultPojoUDF.class.getTypeParameters()[1];
		final TypeVariable<?> third = DefaultPojoUDF.class.getTypeParameters()[2];

		final TypeInformation secondTypeInformation = TypeInformation.of(new TypeHint<String>() {});
		final TypeInformation thirdTypeInformation = TypeInformation.of(new TypeHint<Integer>(){});

		final Map<TypeVariable<?>, TypeInformation<?>> expectedResult = new HashMap<>();
		expectedResult.put(second, secondTypeInformation);
		expectedResult.put(third, thirdTypeInformation);

		final TypeInformation<Pojo<String, Integer>> in2TypeInformation = TypeInformation.of(new TypeHint<Pojo<String, Integer>>(){});

		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(myPojoUDF.getClass(), BaseInterface.class);
		final Type baseType = typeHierarchy.get(typeHierarchy.size() - 1);
		final ParameterizedType resolvedType =
			(ParameterizedType) resolveTypeFromTypeHierarchy(baseType, typeHierarchy, false);

		final Map<TypeVariable<?>, TypeInformation<?>> result =
			TypeVariableBinder.bindTypeVariables(resolvedType.getActualTypeArguments()[1], in2TypeInformation);

		Assert.assertEquals(expectedResult, result);
	}

	@Test
	public void testBindTypeVariableFromTypeInfoFactory() {
		final TypeInfoFactoryUDF typeInfoFactoryUDF = new DefaultTypeInfoFactoryUDF<String[], Boolean, Integer[]>();

		final TypeInformation<Either<Boolean, Integer[]>> in2TypeInformation =
			TypeInformation.of(new TypeHint<Either<Boolean, Integer[]>>(){});

		final TypeVariable<?> second = DefaultTypeInfoFactoryUDF.class.getTypeParameters()[1];
		final TypeVariable<?> third = DefaultTypeInfoFactoryUDF.class.getTypeParameters()[2];

		final TypeInformation<Boolean> secondTypeInformation = TypeInformation.of(new TypeHint<Boolean>(){});
		final TypeInformation<Integer[]> thirdTypeInformation = TypeInformation.of(new TypeHint<Integer[]>(){});

		final Map<TypeVariable<?>, TypeInformation<?>> expectedResult = new HashMap<>();
		expectedResult.put(second, secondTypeInformation);
		expectedResult.put(third, thirdTypeInformation);

		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(typeInfoFactoryUDF.getClass(), BaseInterface.class);
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
			buildParameterizedTypeHierarchy(MyCompositeUDF.class, BaseInterface.class);
		final Type baseType = typeHierarchy.get(typeHierarchy.size() - 1);
		final ParameterizedType resolvedType =
			(ParameterizedType) resolveTypeFromTypeHierarchy(baseType, typeHierarchy, false);

		Map<TypeVariable<?>, TypeInformation<?>> result =
			TypeVariableBinder.bindTypeVariables(resolvedType.getActualTypeArguments()[0], in1TypeInformation);

		Assert.assertEquals(Collections.emptyMap(), result);

		result = TypeVariableBinder.bindTypeVariables(resolvedType.getActualTypeArguments()[1], in2TypeInformation);

		Assert.assertEquals(Collections.emptyMap(), result);
	}

	// --------------------------------------------------------------------------------------------
	// Test build parameterized type hierarchy.
	// --------------------------------------------------------------------------------------------

	@Test
	public void testBuildParameterizedTypeHierarchyForSimpleType() {
		final List<ParameterizedType> parameterizedTypeHierarchy =
			buildParameterizedTypeHierarchy(MySimpleUDF.class, BaseInterface.class);
		final ParameterizedType normalInterfaceType = parameterizedTypeHierarchy.get(parameterizedTypeHierarchy.size() - 1);

		Assert.assertEquals(4, parameterizedTypeHierarchy.size());
		Assert.assertEquals(DefaultSimpleUDF.class, parameterizedTypeHierarchy.get(0).getRawType());
		Assert.assertEquals(AbstractSimpleUDF.class, parameterizedTypeHierarchy.get(1).getRawType());
		Assert.assertEquals(RichInterface.class, parameterizedTypeHierarchy.get(2).getRawType());

		Assert.assertEquals(NormalInterface.class, normalInterfaceType.getRawType());
		Assert.assertTrue(normalInterfaceType.getActualTypeArguments()[0] instanceof TypeVariable);
		Assert.assertTrue(normalInterfaceType.getActualTypeArguments()[1] instanceof TypeVariable);
	}

	@Test
	public void testBuildParameterizedTypeHierarchyForCompositeType() {
		final List<ParameterizedType> typeHierarchy =
			buildParameterizedTypeHierarchy(MyCompositeUDF.class, BaseInterface.class);
		final ParameterizedType normalInterfaceType = typeHierarchy.get(typeHierarchy.size() - 1);

		Assert.assertTrue(normalInterfaceType.getActualTypeArguments()[0] instanceof TypeVariable);
		Assert.assertTrue(normalInterfaceType.getActualTypeArguments()[1] instanceof TypeVariable);
	}

	@Test
	public void testBuildParameterizedTypeHierarchyOnlyFromSuperClass() {
		//TODO:: does we have this scenarios?
		//false
//		final List<ParameterizedType> parameterizedTypeHierarchy =
//			buildParameterizedTypeHierarchy(MySimpleUDF.class, Object.class, false);
//
//		Assert.assertEquals(2, parameterizedTypeHierarchy.size());
//		Assert.assertEquals(DefaultSimpleUDF.class, parameterizedTypeHierarchy.get(0).getRawType());
//		Assert.assertEquals(AbstractSimpleUDF.class, parameterizedTypeHierarchy.get(1).getRawType());
	}

	@Test
	public void testBuildParameterizedTypeHierarchyWithoutInheritance() {
		final List<ParameterizedType> parameterizedTypeHierarchy =
			buildParameterizedTypeHierarchy(MySimpleUDF.class, TypeExtractorContractTest.class);
		Assert.assertEquals(Collections.emptyList(), parameterizedTypeHierarchy);
	}

	// --------------------------------------------------------------------------------------------
	// Basic interfaces.
	// --------------------------------------------------------------------------------------------

	interface BaseInterface {
	}

	interface NormalInterface<X, Y> extends BaseInterface{
		Y foo(X x);
	}

	interface RichInterface<X, Y> extends NormalInterface<X, Y> {
		void open(X x, Y y);
	}

	// --------------------------------------------------------------------------------------------
	// Generic parameter does not have composite type.
	// --------------------------------------------------------------------------------------------

	private abstract static class AbstractSimpleUDF<X, Y> implements RichInterface<X, Y> {

		@Override
		public void open(X x, Y y) {
		}

		@Override
		public Y foo(X x) {
			return null;
		}

		public abstract void bar();
	}

	private static class DefaultSimpleUDF<X> extends AbstractSimpleUDF<X, Tuple2<String, Integer>> {

		@Override
		public void bar() {
		}
	}

	private static class MySimpleUDF extends DefaultSimpleUDF<Integer> {

	}

	// --------------------------------------------------------------------------------------------
	// Generic parameter has composite type.
	// --------------------------------------------------------------------------------------------

	interface CompositeUDF<X, Y, Z> extends RichInterface<X, Tuple2<Y, Z>> {

	}

	private static class DefaultCompositeUDF<X, Y, Z> implements CompositeUDF<X, Y, Z> {

		@Override
		public Tuple2<Y, Z> foo(X x) {
			return null;
		}

		@Override
		public void open(X x, Tuple2<Y, Z> yzTuple2) {

		}
	}

	private static class MyCompositeUDF extends DefaultCompositeUDF<Integer, String, Boolean> {

	}

	// --------------------------------------------------------------------------------------------
	// Generic parameter has generic array type.
	// --------------------------------------------------------------------------------------------

	interface GenericArrayUDF<X, Y> extends RichInterface<X, Y[]> {

	}

	private static class DefaultGenericArrayUDF<X, Y> implements GenericArrayUDF<X, Y> {

		@Override
		public Y[] foo(X x) {
			return null;
		}

		@Override
		public void open(X x, Y[] ys) {

		}
	}

	private static class MyGenericArrayUDF extends DefaultGenericArrayUDF<Integer, String> {

	}

	// --------------------------------------------------------------------------------------------
	// Generic parameter has pojo type.
	// --------------------------------------------------------------------------------------------

	interface PojoUDF<X, Y, Z> extends RichInterface<X, Pojo<Y, Z>> {

	}

	private static class DefaultPojoUDF<X, Y, Z> implements PojoUDF<X, Y, Z> {

		@Override
		public Pojo<Y, Z> foo(X x) {
			return null;
		}

		@Override
		public void open(X x, Pojo<Y, Z> yzPojo) {

		}
	}

	/**
	 * Test Pojo class.
	 */
	public static class Pojo<X, Y> {
		private X x;
		private Y y;
		private Integer money;

		public Pojo() {
		}

		public X getX() {
			return x;
		}

		public void setX(X x) {
			this.x = x;
		}

		public Y getY() {
			return y;
		}

		public void setY(Y y) {
			this.y = y;
		}

		public Integer getMoney() {
			return money;
		}

		public void setMoney(Integer money) {
			this.money = money;
		}
	}

	// --------------------------------------------------------------------------------------------
	// Generic parameter has type info factory.
	// --------------------------------------------------------------------------------------------

	interface TypeInfoFactoryUDF<X, Y, Z> extends RichInterface<X, Either<Y, Z>>{

	}

	private static class DefaultTypeInfoFactoryUDF<X, Y, Z> implements TypeInfoFactoryUDF<X, Y, Z> {

		@Override
		public Either<Y, Z> foo(X x) {
			return null;
		}

		@Override
		public void open(X x, Either<Y, Z> yzEither) {

		}
	}
}
