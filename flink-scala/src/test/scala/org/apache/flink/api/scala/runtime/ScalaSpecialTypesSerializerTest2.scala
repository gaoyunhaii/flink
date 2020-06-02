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
package org.apache.flink.api.scala.runtime

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.runtime.ScalaSpecialTypesSerializerTest2.{MyObject, MyObject2}
import org.junit.{Assert, Test}

object ScalaSpecialTypesSerializerTest2 {
  trait MyTrait[T1] {
    def a : T1
    def b : String = "haha"

    def c(i: Int): Int = i + 1
  }

  trait MyTrait2[T2] {
    def d : T2
    def e : String = "haha"

    def f(i: Int): Int = i + 1
  }

  abstract class BaseCase {

  }

  case class MyObject() extends BaseCase with MyTrait[String] with MyTrait2[Int] {
    def h : String = "haha"

    override def a: String = ""

    override def d: Int = 0
  }

  case class MyObject2() extends BaseCase with MyTrait[String] with MyTrait2[Int] {
    def h : String = "haha"

    override def a: String = ""

    override def d: Int = 0
  }
}

class ScalaSpecialTypesSerializerTest2 {

  @Test
  def testComplex() : Unit = {
    println("222222222")
    val testData = Array(MyObject(), MyObject2())
    runTests(testData)
  }

//  @Test
//  def testRight(): Unit = {
//    println("2222222")
//    val testData = Array(Left(10), Left("haha"), Right("CIao"))
//    runTests(testData)
//  }

  private final def runTests[T : TypeInformation](instances: Array[T]) {
    try {
      val typeInfo = implicitly[TypeInformation[T]]
      println("typeinfo22: " + typeInfo)
      val serializer = typeInfo.createSerializer(new ExecutionConfig)
      val typeClass = typeInfo.getTypeClass
      val test = new ScalaSpecialTypesSerializerTestInstance[T](
        serializer,
        typeClass,
        serializer.getLength,
        instances)
      test.testAll()
    } catch {
      case e: Exception => {
        System.err.println(e.getMessage)
        e.printStackTrace()
        Assert.fail(e.getMessage)
      }
    }
  }
}
