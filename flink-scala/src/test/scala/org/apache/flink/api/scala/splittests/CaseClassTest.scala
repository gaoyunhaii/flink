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

package org.apache.flink.api.scala.splittests

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment

object CaseClassTest {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val source = env.fromCollection(Array("test", "test2"))
    val ds = source.map(x => _classes.MyClass[String, Integer]("5a", new _classes.MyClassSub[Integer](5)))
    System.out.println(ds.getType())
  }
}

package _classes {

  case class MyClass[T1, T2](a: T1, b: MyClassSub[T2]){

  }

  class MyClassSub[T1](val a: T1) {
  }
}
