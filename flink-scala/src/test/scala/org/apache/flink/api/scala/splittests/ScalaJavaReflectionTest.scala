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

import org.apache.flink.api.scala.splittests.test.Test

import scala.reflect.runtime.{universe => ru}

/**
 *
 */
object ScalaJavaReflectionTest {

  def main(args : Array[String]): Unit = {
//    val clazz = classOf[test.Test]
//    val pt = clazz.getGenericSuperclass.asInstanceOf[ParameterizedType]
//    println(pt.getActualTypeArguments.mkString(", "))

    val sub = ru.typeOf[Test]
    val mirror = ru.runtimeMirror(getClass.getClassLoader)

    sub.baseClasses foreach {t =>
      println(t, t.asClass.typeParams)
      t.asClass.typeParams foreach {tp =>
        println("\t\t", tp, tp.isParameter, tp.asType.toType.asSeenFrom(sub, t))
      }
    }
  }
}

package test {

  import org.apache.flink.api.scala.splittests.JavaDefiniation._
  import scala.language.implicitConversions

  class Haha

  class Wrapper(val underlying: Int) extends AnyVal {
    def foo: Wrapper = new Wrapper(underlying * 19)
  }

  case class Test(additional: Boolean) extends MyClass2[Double]

  case class MyScalaTupleCaseClass(additional: Boolean) extends MyClass2[Double]
}
