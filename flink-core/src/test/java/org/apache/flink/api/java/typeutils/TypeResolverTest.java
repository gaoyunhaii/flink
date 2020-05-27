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
import org.apache.flink.types.Either;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;

import static org.apache.flink.api.java.typeutils.TypeHierarchyBuilder.buildParameterizedTypeHierarchy;

/**
 * Test type resolver.
 */
public class TypeResolverTest {

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

	abstract static class AbstractSimpleUDF<X, Y> implements RichInterface<X, Y> {

		@Override
		public void open(X x, Y y) {
		}

		@Override
		public Y foo(X x) {
			return null;
		}

		public abstract void bar();
	}

	static class DefaultSimpleUDF<X> extends AbstractSimpleUDF<X, Tuple2<String, Integer>> {

		@Override
		public void bar() {
		}
	}

	static class MySimpleUDF extends DefaultSimpleUDF<Integer> {

	}

	// --------------------------------------------------------------------------------------------
	// Generic parameter has composite type.
	// --------------------------------------------------------------------------------------------

	interface CompositeUDF<X, Y, Z> extends RichInterface<X, Tuple2<Y, Z>> {

	}

	static class DefaultCompositeUDF<X, Y, Z> implements CompositeUDF<X, Y, Z> {

		@Override
		public Tuple2<Y, Z> foo(X x) {
			return null;
		}

		@Override
		public void open(X x, Tuple2<Y, Z> yzTuple2) {

		}
	}

	static class MyCompositeUDF extends DefaultCompositeUDF<Integer, String, Boolean> {

	}

	// --------------------------------------------------------------------------------------------
	// Generic parameter has generic array type.
	// --------------------------------------------------------------------------------------------

	interface GenericArrayUDF<X, Y> extends RichInterface<X, Y[]> {

	}

	static class DefaultGenericArrayUDF<X, Y> implements GenericArrayUDF<X, Y> {

		@Override
		public Y[] foo(X x) {
			return null;
		}

		@Override
		public void open(X x, Y[] ys) {

		}
	}

	static class MyGenericArrayUDF extends DefaultGenericArrayUDF<Integer, String> {

	}

	// --------------------------------------------------------------------------------------------
	// Generic parameter has pojo type.
	// --------------------------------------------------------------------------------------------

	interface PojoUDF<X, Y, Z> extends RichInterface<X, Pojo<Y, Z>> {

	}

	static class DefaultPojoUDF<X, Y, Z> implements PojoUDF<X, Y, Z> {

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

	static class DefaultTypeInfoFactoryUDF<X, Y, Z> implements TypeInfoFactoryUDF<X, Y, Z> {

		@Override
		public Either<Y, Z> foo(X x) {
			return null;
		}

		@Override
		public void open(X x, Either<Y, Z> yzEither) {

		}
	}
}
