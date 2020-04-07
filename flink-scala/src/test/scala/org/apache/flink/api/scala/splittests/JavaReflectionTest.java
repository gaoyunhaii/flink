///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.flink.api.scala.splittests;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.scala.haha.MyEnum;
//import org.apache.flink.api.scala.haha.MyEnum$;
//
//public class JavaReflectionTest {
//
//	public static void main(String[] args) {
//		MapFunction<String, MyEnum$> mapFunction = new MapFunction<String, MyEnum$>() {
//
//			@Override
//			public MyEnum$ map(String value) throws Exception {
//				return MyEnum$.MODULE$.Fri();
//			}
//		};
//
//		System.out.println(MyEnum$.MODULE$);
//	}
//
//}
