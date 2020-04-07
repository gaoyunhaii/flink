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

package org.apache.flink.api.scala.splittests;

import org.apache.flink.shaded.curator4.com.google.common.reflect.Parameter;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;

public class JavaDefiniation {

	public static class MyClass<T1, T2> {

	}

	public static class MyClass2<T1> extends MyClass<T1, Double> {

	}

	public static class MyClass3 extends MyClass2<scala.Double> {

	}

	public static void main(String[] args) {
		ParameterizedType pt = (ParameterizedType) MyClass3.class.getGenericSuperclass();
		System.out.println(Arrays.toString(pt.getActualTypeArguments()));
//
//		pt = (ParameterizedType) org.apache.flink.api.scala.splittests.test.Test.class.getGenericSuperclass();
//		System.out.println(Arrays.toString(pt.getActualTypeArguments()));
//
//		System.out.println(scala.Double.class);
	}
}
