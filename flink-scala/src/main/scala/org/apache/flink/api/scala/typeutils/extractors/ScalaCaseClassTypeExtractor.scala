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

import java.lang.reflect.{ParameterizedType, Type}
import java.util
import java.util.Optional

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.{TypeExtractionUtils, TypeHierarchyBuilder, TypeInformationExtractor, TypeResolver}
import org.apache.flink.api.scala.typeutils.{CaseClassTypeInfo, ScalaCaseClassSerializer}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.{universe => ru}

class ScalaCaseClassTypeExtractor extends TypeInformationExtractor {
  /**
   *
   * @return the classes that the extractor could extract the{ @link TypeInformationExtractor} corresponding to.
   */
  override def getClasses: util.List[Class[_]] = util.Arrays.asList(classOf[Product])

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
    // Here we need to use scala runtime reflection to detect the fields to parse, then
    // resolve their actual types from the java hierarchy.
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val typeClass = TypeExtractionUtils.typeToClass(javaType)

    val scalaTypeSymbol = mirror.classSymbol(typeClass)
    if (!scalaTypeSymbol.isClass || !scalaTypeSymbol.asClass.isCaseClass) {
      return Optional.empty()
    }
    val scalaType = scalaTypeSymbol.asType.toType

    val result = ArrayBuffer[Tuple2[String, Type]]()
    if (scalaType.baseClasses exists {
      bc => {
        !(bc.asClass.toType == scalaType) && bc.asClass.isCaseClass
      }
    }) {
      throw new UnsupportedOperationException("Case-to-case inheritance is not supported.")
    } else {
      val ctors = scalaType.decls collect {
        case decl if decl.isConstructor && decl.asMethod.isPrimaryConstructor => decl.asMethod
      }

      ctors match {
        case c1 :: c2 :: _ =>
          throw new UnsupportedOperationException("Multiple constructors found, this is not supported.")
        case ctor :: Nil =>
          val caseFields = ctor.paramLists.flatten.map {
            sym => {
              val methodSym = scalaType.member(sym.name).asMethod
              val getter = methodSym.getter
              getter
            }
          }

          caseFields.foreach { getter =>
            val name = getter.name.toString

            println(typeClass.getMethod(name).getGenericReturnType)
            result.append((name, typeClass.getMethod(name).getGenericReturnType))
          }
      }
    }

    println("fields", result)

    val hierarchy = TypeHierarchyBuilder.buildParameterizedTypeHierarchy(
      javaType,
      TypeHierarchyBuilder.isSameClass(classOf[Object]),
      TypeHierarchyBuilder.isSameClass(classOf[Object]).or(TypeHierarchyBuilder.assignTo(classOf[Object])))

    val resolvedFields = result.toArray.map { t =>
      val realType = TypeResolver.resolveTypeFromTypeHierarchy(t._2, hierarchy, true)
      val realTypeInfo = context.extract(realType)
      (t._1, realType, realTypeInfo)
    }

    // We will also need to resolve the generic parameters
    val genericParameters = new ArrayBuffer[TypeInformation[_]]()
    if (javaType.isInstanceOf[ParameterizedType]) {
      javaType.asInstanceOf[ParameterizedType].getActualTypeArguments.foreach(arg => {
        val realArgType = TypeResolver.resolveTypeFromTypeHierarchy(arg, hierarchy, true)
        genericParameters.append(context.extract(realArgType))
      })
    }

    Optional.of(new CaseClassTypeInfo[Product](
      typeClass.asInstanceOf[Class[Product]],
      genericParameters.toArray,
      resolvedFields.map {t => t._3}.toSeq,
      resolvedFields.map {t => t._1}.toSeq) {

      /**
       * Creates a serializer for the type. The serializer may use the ExecutionConfig
       * for parameterization.
       *
       * @param config The config used to parameterize the serializer.
       * @return A serializer for this type.
       */
      override def createSerializer(config: ExecutionConfig): TypeSerializer[Product] = {
        val fieldSerializers: Array[TypeSerializer[_]] = new Array[TypeSerializer[_]](getArity)
        for (i <- 0 until getArity) {
          fieldSerializers(i) = types(i).createSerializer(config)
        }

        new ScalaCaseClassSerializer[Product](getTypeClass, fieldSerializers)
      }
    })
  }
}
