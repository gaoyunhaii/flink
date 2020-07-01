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

package org.apache.flink.api.scala

import org.apache.flink.api.java.tuple._
import org.apache.flink.api.scala.MacroTest.MyEnum.MyEnum
import org.apache.flink.api.scala.typeutils.types.scala.ScalaTypeBasedAbstractTypeClass

object MacroTest {
  object MyEnum extends Enumeration {
    type MyEnum = Value
    val Mon, Tue = Value
  }

  class Record extends org.apache.flink.api.java.tuple.Tuple2[String, MyEnum] {
    /**
     * Shallow tuple copy.
     *
     * @return A new Tuple with the same fields as this.
     */
    override def copy[T <: Tuple](): T = null.asInstanceOf[T]
  }

//  class MyTuple extends Tuple2[String, Int] {
//    /**
//     * Shallow tuple copy.
//     *
//     * @return A new Tuple with the same fields as this.
//     */
//    override def copy[T <: Tuple](): T = null.asInstanceOf[T]
//  }

  trait First {

  }

  trait Second {

  }

  abstract class BaseClass {

  }

  class MyClass extends Second with First {

  }

  class MyClass2 extends Second with First {

  }

  def main(args: Array[String]): Unit = {
    import scala.reflect.runtime.{universe => ru}

    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val tpe = ru.typeOf[MyClass]
    println(tpe)
    val abs = new ScalaTypeBasedAbstractTypeClass(tpe)
    abs.printMembers()

    val env = ExecutionEnvironment.getExecutionEnvironment
    val source = env.fromElements("a", "b", "f", "ab")
//    val ds = source.map((_: String) => 0)
    val ds = source.map((_: String) => new MyClass)
    println(ds.getType())
    println(ds.getType().getTypeClass)

//    import scala.reflect.runtime.{universe => ru}
//    val mirror = ru.runtimeMirror(getClass.getClassLoader)
//    val array = Array(Left(5), Right("test"))
//    val tpe = mirror.reflect(array).symbol.asType.toType
//    println(tpe.typeArgs)
  }
}
