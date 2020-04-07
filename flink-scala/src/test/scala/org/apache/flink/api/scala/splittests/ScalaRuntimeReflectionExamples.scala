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

import org.apache.flink.api.scala.splittests.scopeA.WhoAmI

import scala.reflect.runtime.{universe => ru}

object ScalaRuntimeReflectionExamples {

  def main(args: Array[String]): Unit = {
    // Now we try to give an example about scala's reflection
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    //    println(mirror)

    //    val who = new WhoAmI(1, "2");
    //    val symbol = mirror.classSymbol(who.getClass)
    //    println(symbol, symbol.getClass)
    //    println("symbol.toType & constr: " + symbol.toType.getClass, symbol.toTypeConstructor.getClass)
    //
    //    symbol.toType.decls.foreach(f => {
    //      println("f: " + f, f.getClass, f.isMethod)
    //
    ////      f match {
    ////        case m if m.isMethod => {
    ////          println("\t", m.asMethod.isPrimaryConstructor)
    ////        }
    ////      }
    //    })
    //
    //    println("members:")
    //    symbol.toType.members.foreach(m => {
    //      println(m.name, m.typeSignature)
    //      if (m.isMethod) {
    //        println("=>", m.asMethod.getter, m.asMethod.setter)
    //      }
    //    })
    //
    //    println("\nmethods")
    //    println("a: " + symbol.toType.decl(ru.TermName("a")))
    //
    //    println("\nfinal test")
    //    println(symbol.toType.decl(ru.termNames.CONSTRUCTOR).asMethod.paramLists)

    println("Another test for typeArgs & typeParams")
    val t = ru.typeOf[scopeA.Sub[_]]
    println(t.getClass)
    println("args", t.typeArgs)
    println("params", t.typeParams, t.paramLists)
    println(t.decl(ru.TermName("haha")).asMethod.typeParams, t.decl(ru.TermName("haha")).asMethod.paramLists, t.decl(ru.TermName("haha")))
//
//    val mm = mirror.reflect(new scopeA.Sub[String]("haha")).reflectMethod(t.decl(ru.TermName("haha")).asMethod)
//    println(mm.apply(5))

    println(scopeA.Nani.getType(List(1, 2, 3)).typeParams)
  }
}

package scopeA {
  import scala.reflect.runtime.universe._

  object Nani {
    def getType[T: WeakTypeTag](obj: T): Type = weakTypeOf[T]
  }


  case class WhoAmI(var a: Int, var b: String) {

  }

  class Generic[T1, T2](a: String, b: String, c: String) {

  }

  class Sub[T3](c: T3) extends Generic[String, T3]("", "", "haha") {
    def haha[T4, T5](v: T4, i: Int): T4 = v
  }

}
