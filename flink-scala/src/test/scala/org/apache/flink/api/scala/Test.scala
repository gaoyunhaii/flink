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

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.haha.MyEnum.MyEnum
import org.apache.flink.api.scala.haha.{MyClassSub, MyEnum}
import org.apache.flink.types.Nothing

import scala.util.Try

object Test {

  def main(args: Array[String]): Unit = {

    class MyObject(var a: Int, var b: String) {
      def this() = this(0, "")
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val source = env.fromCollection(Array("test", "test2"))
    // val ds = source.map(x => haha.MyClass[String, MyClassSub[List[Integer]]]("1", new MyClassSub[List[Integer]](List(5))))

//    val ds = source.map(x => haha.MyClass[String, Integer]("5a", new MyClassSub[Integer](5)))

    // val func: String => Either[Integer, String] = s => Either.cond(1 + 1 == 2, "5", 3)
//     val func: String => MyEnum.Value = s => MyEnum.Fri
//    val func : String => Int = s => 1

    val func : String => MyObject = s => new MyObject(5, "")

    val ds = source.map(func)

//    val func: String => Try[Integer] = s => Try.apply(5)
//    val ds = source.map(func)

//    val func: String => Option[Integer] = s => Option.empty
//    val ds = source.map(func)

    println(ds.getType(), ds.getType().getClass)
//    println(MyEnum.Fri.getClass)
    println("abcdedfghilmaaaaalaadb22aaaadaaaa2a22aa2aab2a2aa2a")

//    System.out.println(classOf[MyEnum.Value])

//    println(new haha.MyClassSub[String]("a").a)
//    println(MyEnum.showAll)
//
//    val enum: Enumeration  = haha.MyEnum

//    import scala.reflect.runtime.{universe => ru}
//    val mirror = ru.runtimeMirror(getClass.getClassLoader)
//
//
//    import scala.reflect.runtime.{universe => ru}
//    println("222")
//    val tpe = ru.typeOf[MyEnum.Value]
//    println(tpe.typeSymbol.owner)
//    val ru.TypeRef(_, sym, _) = tpe
//    println(sym.owner)
  }
}

package haha {
  case class MyClass[T1, T2](a: T1, b: MyClassSub[T2]){

  }

  case class Book(isbn: String)

  class MyClassSub[T1](val a: T1) {
  }

  class MyClass2[T <: MyClassSub[String]]() {
    def useEnum() : Unit = {

    }
  }

  object MyEnum extends Enumeration {
    type MyEnum = Value
    val Mon, Tue, Wen, Thu, Fri, SAT, Sun = Value

    def showAll(): Unit = this.values.foreach(println)
  }
}
