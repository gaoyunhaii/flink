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

package org.apache.flink.api.scala.typeutils.extractors

import java.lang.reflect.{GenericArrayType, ParameterizedType, Type}
import java.util
import java.util.Optional

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.{TypeExtractionUtils, TypeHierarchyBuilder, TypeInformationExtractor, TypeResolver}
import org.apache.flink.api.scala.typeutils.{TraversableSerializer, TraversableTypeInfo}

import scala.collection.mutable.ArrayBuffer
import scala.collection.TraversableOnce

object ScalaTraversableOnceTypeExtractor {

  private def BASIC_TYPE_MAPPING = new util.HashMap[String, String]() {{
    this.put("boolean", "Boolean")
    this.put("byte", "Byte")
    this.put("char", "Char")
    this.put("short", "Short")
    this.put("int", "Int")
    this.put("long", "Long")
    this.put("float", "Float")
    this.put("double", "Double")
    this.put("void", "Void")
  }}
}

class ScalaTraversableOnceTypeExtractor extends TypeInformationExtractor {
  /**
   *
   * @return the classes that the extractor could extract the{ @link TypeInformationExtractor} corresponding to.
   */
  override def getClasses: util.List[Class[_]] = util.Arrays.asList(classOf[TraversableOnce[_]])

  /**
   * Extract the {@link TypeInformation} of given type.
   *
   * @param javaType the type that is needed to extract { @link TypeInformation}
   * @param context  used to extract the { @link TypeInformation} for the generic parameters or components and contains some
   *                 information of extracting process.
   * @return { @link TypeInformation} of the given type or { @link Optional#empty()} if the extractor could not handle this type
   * @throws InvalidTypesException if error occurs during extracting the { @link TypeInformation}
   */
  override def extract(javaType: Type, context: TypeInformationExtractor.Context): Optional[TypeInformation[_]] = {
    val typeClass = TypeExtractionUtils.typeToClass(javaType)

    if (classOf[scala.collection.SortedSet[_]].isAssignableFrom(typeClass) ||
      classOf[scala.collection.SortedMap[_, _]].isAssignableFrom(typeClass)) {
      return Optional.empty()
    }

    val hierachy = TypeHierarchyBuilder.buildParameterizedTypeHierarchy(
      javaType,
      TypeHierarchyBuilder.isSameClass(classOf[TraversableOnce[_]]),
      TypeHierarchyBuilder.isSameClass(classOf[TraversableOnce[_]]).or(TypeHierarchyBuilder.assignTo(classOf[TraversableOnce[_]])))
    val argType = hierachy.get(hierachy.size() - 1).getActualTypeArguments()(0)
    val realType = TypeResolver.resolveTypeFromTypeHierarchy(argType, hierachy, true)
    val realTypeStr = encodeType(realType)
    val realTypeInfo = context.extract(realType)

    val cbfString = String.format("implicitly[scala.collection.generic.CanBuildFrom[%s[%s], %s, %s[%s]]]",
      typeClass.getName,
      realTypeStr,
      realTypeStr,
      typeClass.getName,
      realTypeStr)

    val cbfCode = try {
      TraversableSerializer.compileCbf(getClass.getClassLoader, cbfString)
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        null
      }
    }

    if (cbfCode == null) {
      return Optional.empty()
    }

    Optional.of(new TraversableTypeInfo[TraversableOnce[Any], Any](typeClass.asInstanceOf[Class[TraversableOnce[Any]]], realTypeInfo.asInstanceOf[TypeInformation[Any]]) {
      override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[TraversableOnce[Any]] = {
        new TraversableSerializer[TraversableOnce[Any], Any](
          realTypeInfo.createSerializer(executionConfig).asInstanceOf[TypeSerializer[Any]],
          cbfString)
      }
    })
  }

  private def encodeType(javaType: Type): String = {
    javaType match {
      case clazz: Class[_] =>
        var realName = clazz.getName
        println(ScalaTraversableOnceTypeExtractor.BASIC_TYPE_MAPPING)
        if (ScalaTraversableOnceTypeExtractor.BASIC_TYPE_MAPPING.containsKey(realName)) {
          realName = ScalaTraversableOnceTypeExtractor.BASIC_TYPE_MAPPING.get(realName)
        }

        realName
      case pt: ParameterizedType =>
        if (pt.getActualTypeArguments.length == 0) {
          encodeType(pt.getRawType)
        } else {
          val arrayBuffer = new ArrayBuffer[String]
          pt.getActualTypeArguments.foreach(arg => arrayBuffer.append(encodeType(arg)))
          val argNames = arrayBuffer.toArray.mkString(", ")

          encodeType(pt.getRawType) + "[" + argNames + "]"
        }
      case array: GenericArrayType =>
        "Array[" + encodeType(array.getGenericComponentType) + "]"
      case _ =>
        throw new UnsupportedOperationException()
    }
  }
}
