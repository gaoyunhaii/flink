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

import scala.reflect.runtime.{universe => ru}
import java.lang.reflect.{ParameterizedType, Type}

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.java.typeutils.TypeExtractionUtils
import org.apache.flink.api.java.typeutils.TypeResolver.ResolvedParameterizedType

object ScalaRuntimeExtractionTest {

  class FirstClass[FF] extends org.apache.flink.api.java.tuple.Tuple2[FF, String] {
    /**
     * Shallow tuple copy.
     *
     * @return A new Tuple with the same fields as this.
     */
    override def copy[T <: Tuple](): T = null.asInstanceOf[T]
  }

  class SecondClass extends org.apache.flink.api.java.tuple.Tuple1[FirstClass[Double]] {
    /**
     * Shallow tuple copy.
     *
     * @return A new Tuple with the same fields as this.
     */
    override def copy[T <: Tuple](): T = null.asInstanceOf[T]
  }

  def copyParameterizedType(extracted: Type, seen: ru.Type): Type = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)

    extracted match {
      case clazz: Class[_] => {
        if (clazz.equals(classOf[Object])) {
          val seenClazz = mirror.runtimeClass(seen.typeSymbol.asClass)
          if (!clazz.equals(seenClazz)) {
            return seenClazz
          }
        }

        clazz
      }
      case pt: ParameterizedType => {
        // First let's make sure the base type is as ok...
        println("args", seen.typeArgs)
        println("ag", pt.getActualTypeArguments.mkString(", "))

        val newArgs = (pt.getActualTypeArguments zip seen.typeArgs).map(obj => {
          val (ag, seen) = obj
          copyParameterizedType(ag, seen)
        }).toArray
        println(newArgs.mkString(", "))

        new ResolvedParameterizedType(pt.getRawType, pt.getOwnerType, newArgs, pt.getTypeName)
      }
      case other: Type => other
    }
  }

  def main(args: Array[String]): Unit = {

    val sub = classOf[SecondClass]
    var cur: Class[_] = sub

    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val subClassSymbol = mirror.classSymbol(sub)

    while (cur != null) {
      val parent = cur.getGenericSuperclass

      if (parent.isInstanceOf[ParameterizedType]) {
        println(parent, parent.getClass)
        cur = TypeExtractionUtils.typeToClass(parent)

        // now try to deduct all the status of the type with scala runtime
        val curSymbol = mirror.classSymbol(cur)
        println(curSymbol.asClass.typeParams)

        val parameters = cur.getTypeParameters
        val actualArguments = parent.asInstanceOf[ParameterizedType].getActualTypeArguments

        val map = new java.util.HashMap[String, Type]()
        for ((a, b) <- parameters zip actualArguments) {
          map.put(a.getName, b)
        }
        println(map)

        curSymbol.asClass.typeParams foreach { tp =>
          val name = tp.asType.fullName.split("\\.").last
          println("[" + name + "]")

          // Now try to use as seen from for this
          val actual = tp.asType.toType.asSeenFrom(subClassSymbol.toType, curSymbol)
          println(actual, actual.getClass)
          println(actual.typeArgs)

          val actualType = copyParameterizedType(map.get(name), actual)
          println(actualType)
        }

      } else {
        cur = parent.asInstanceOf[Class[_]]
      }
    }
  }
}
