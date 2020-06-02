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
package org.apache.flink.api.scala;

import org.apache.flink.api.scala.haha.MyObject;
import org.apache.flink.types.Nothing;
import scala.Array;
import scala.Enumeration;
import scala.Option;
import scala.Product;
import scala.Unit;
import scala.runtime.Nothing$;
import scala.util.Either;
import scala.util.Success;
import scala.util.Try;

import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.$colon$colon;

import java.util.Arrays;

public class JavaAccessScalaTest {

	public static <T> List<T> list(T ... ts) {
		List<T> result = List$.MODULE$.empty();
		for(int i = ts.length; i > 0; i--) {
			result = new $colon$colon(ts[i - 1], result);
		}
		return result;
	}

	public static void main(String[] args) {
		System.out.println(org.apache.flink.api.scala.haha.MyTrait.class);
		System.out.println(org.apache.flink.api.scala.haha.MyObject.class);
		System.out.println(org.apache.flink.api.scala.haha.MyObject.class.getGenericSuperclass());
		System.out.println(Arrays.toString(MyObject.class.getGenericInterfaces()));

		System.out.println("fields" + Arrays.toString(MyObject.class.getDeclaredFields()));
		MyObject myObject = new MyObject();

		System.out.println(Try.class.isAssignableFrom(Success.class));

		new Success<String>("haha");
		System.out.println(list(1, 2, 3));
		System.out.println(scala.collection.TraversableOnce.class.isAssignableFrom(list(1).getClass()));
		System.out.println(org.apache.flink.api.scala.splittests.test.MyScalaTupleCaseClass.class.getGenericSuperclass());
		System.out.println(Product.class.isAssignableFrom(org.apache.flink.api.scala.splittests.test.MyScalaTupleCaseClass.class));

		System.out.println(Unit.class);
		System.out.println(scala.runtime.Nothing$.class);
		System.out.println(scala.util.Either.class);
		System.out.println(scala.Option.class);
		System.out.println(scala.util.Try.class);
		System.out.println(scala.Product.class);

//		System.out.println(Nothing$.class);
//		System.out.println(Try.class);
//		System.out.println(Option.class);
//		System.out.println(Either.class);
//		System.out.println(Enumeration.Value.class);
	}

}
