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

package org.apache.flink.api.scala.typeutils

import org.apache.flink.api.common.typeinfo.{NothingTypeInfo, TypeInformation}
import java.lang.reflect.ParameterizedType
import java.util

import org.apache.flink.annotation.Internal
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.types.Nothing

import scala.collection.generic.CanBuildFrom
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}
import scala.util.Try

/**
  * Extractors for scala types
  */
@Internal
class ScalaTypeInfoExtractor {

  def createTypeInfo(outputType: java.lang.reflect.Type): TypeInformation[_] = {
    val clazz = getTrueClass(outputType)

    if (isNothing(clazz)) {
      new NothingTypeInfo().asInstanceOf[TypeInformation[_]]
    } else if (isUnit(clazz)) {
      new UnitTypeInfo().asInstanceOf[TypeInformation[_]]
    } else if (isEither(clazz)) {
      val pt = outputType.asInstanceOf[ParameterizedType]
      val leftTypeInfo = createTypeInfo(pt.getActualTypeArguments()(0))
      val rightTypeInfo = createTypeInfo(pt.getActualTypeArguments()(1))

      new EitherTypeInfo(pt.getRawType.asInstanceOf[Class[_ <: Either[Any, Any]]], leftTypeInfo.asInstanceOf[TypeInformation[Any]], rightTypeInfo.asInstanceOf[TypeInformation[Any]])
    } else if (isEnum(clazz)) {
      val moduleObj = parseEnumModule(clazz)
      new EnumValueTypeInfo[Enumeration](moduleObj, clazz.asInstanceOf[Class[Enumeration#Value]])
    } else if (isTry(clazz)) {
      val pt = outputType.asInstanceOf[ParameterizedType]
      val subTypeInfo = createTypeInfo(pt.getActualTypeArguments()(0))
      new TryTypeInfo[Any, Try[Any]](subTypeInfo.asInstanceOf[TypeInformation[Any]])
    } else if (isOption(clazz)) {
      val pt = outputType.asInstanceOf[ParameterizedType]
      val subTypeInfo = createTypeInfo(pt.getActualTypeArguments()(0))
      new OptionTypeInfo[Any, Option[Any]](subTypeInfo.asInstanceOf[TypeInformation[Any]])
    } else if (isCaseClass(clazz)) {
      //      val pt = outputType.asInstanceOf[ParameterizedType]
      //      pt.getActualTypeArguments map { subType => createTypeInfo(subType) }
      // Now we need to try to get the actual class of the field
      val fields = parseCaseField(clazz)
      TypeExtractor.createTypeInfo(outputType)
    } else {
      TypeExtractor.createTypeInfo(outputType).asInstanceOf[TypeInformation[_]]
    }
  }

  def isNothing(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.toType =:= ru.typeOf[Nothing]
  }

  def isUnit(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.toType =:= ru.typeOf[Unit]
  }

  def isEither(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.toType <:< ru.typeOf[Either[_, _]]
  }

  def isEnum(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)

    println("name", clazz.getName)

    val owner = classSymbol.owner
    owner.isModule &&
      owner.typeSignature.baseClasses.contains(ru.typeOf[scala.Enumeration].typeSymbol)
  }

  def isTry(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.toType <:< ru.typeOf[Try[_]]
  }

  def isOption(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.toType <:< ru.typeOf[Option[_]]
  }

  def isCaseClass(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.asClass.isCaseClass
  }

  def isBitSet(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.toType <:< ru.typeOf[BitSet]
  }

  def isTraversalOnce(clazz: Class[_]): Boolean = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    classSymbol.toType <:< ru.typeOf[TraversableOnce[_]] &&
      !(classSymbol.toType <:< ru.typeOf[SortedMap[_, _]]) &&
      !(classSymbol.toType <:< ru.typeOf[SortedSet[_]])
  }

  def parseEnumModule(clazz: Class[_]): Enumeration = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    val owner = classSymbol.owner.asModule
    mirror.reflectModule(owner).instance.asInstanceOf[Enumeration]
  }

  def parseCaseField(clazz: Class[_]): Array[TypeInformation[_]] = {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.classSymbol(clazz)
    val caseClassType = classSymbol.toType
    val result = ArrayBuffer[TypeInformation[_]]()

    println("me", caseClassType, caseClassType.baseClasses)

    caseClassType.baseClasses exists {
      bc => {
        println("bc, ", bc, bc.getClass, caseClassType.getClass)
        !(bc.asClass.toType == caseClassType) && bc.asClass.isCaseClass
      }
    } match {

      case true =>
        throw new UnsupportedOperationException("Case-to-case inheritance is not supported.")

      case false =>
        val ctors = caseClassType.decls collect {
          case decl if decl.isConstructor && decl.asMethod.isPrimaryConstructor => decl.asMethod
        }

        ctors match {
          case c1 :: c2 :: _ =>
            throw new UnsupportedOperationException("Multiple constructors found, this is not supported.")
          case ctor :: Nil =>
            val caseFields = ctor.paramLists.flatten.map {
              sym => {
                val methodSym = caseClassType.member(sym.name).asMethod
                val getter = methodSym.getter
                val setter = methodSym.setter
                val returnType = methodSym.returnType.asSeenFrom(caseClassType, classSymbol)
                getter
              }
            }

            caseFields.foreach {
              g => {
                println("inside", g, g.getClass, caseClassType.decl(g.name), g.fullName.split("\\.").last)

                clazz.getFields foreach {
                  field => println("field: ", field)
                }

                clazz.getMethods foreach {
                  m => println("method: ", m)
                }

                println(clazz.getMethod("a").getGenericReturnType)
                println(clazz.getMethod("b").getGenericReturnType.getClass)

                // val javaField = clazz.getField(g.fullName.split("\\.").last)
                // println("javaField", javaField)
              }
            }

            println("case fields", caseFields)
        }
    }

    result.toArray

    //    def parseTraversalOnce(clazz: Class[_]): TypeInformation[_] = {
    //      val mirror = ru.runtimeMirror(getClass.getClassLoader)
    //      val classSymbol = mirror.classSymbol(clazz)
    //
    //      val traversable = classSymbol.toType.baseType(ru.typeOf[TraversableOnce[_]].typeSymbol)
    //      null
    //
    ////      traversable match {
    ////        case ru.TypeRef(_, _, elemTpe :: Nil) =>
    ////
    ////          import compat._ // this is needed in order to compile in Scala 2.11
    ////
    ////          // determine whether we can find an implicit for the CanBuildFrom because
    ////          // TypeInformationGen requires this. This catches the case where a user
    ////          // has a custom class that implements Iterable[], for example.
    ////          val cbfTpe = ru.TypeRef(
    ////            ru.typeOf[CanBuildFrom[_, _, _]],
    ////            ru.typeOf[CanBuildFrom[_, _, _]].typeSymbol,
    ////            classSymbol.toType :: elemTpe :: classSymbol.toType :: Nil)
    ////
    ////          val cbf = c.inferImplicitValue(cbfTpe, silent = true)
    ////
    ////          if (cbf == EmptyTree) {
    ////            None
    ////          } else {
    ////            Some(elemTpe.asSeenFrom(tpe, tpe.typeSymbol))
    ////          }
    ////        case _ => None
    ////      }
    //    }
  }

  def getTrueClass(outputType: java.lang.reflect.Type): Class[_] = {
    outputType match {
      case pt: ParameterizedType => pt.getRawType.asInstanceOf[Class[_]]
      case c: Class[_] => c
      case _ => throw new RuntimeException("Bad type")
    }
  }
}
