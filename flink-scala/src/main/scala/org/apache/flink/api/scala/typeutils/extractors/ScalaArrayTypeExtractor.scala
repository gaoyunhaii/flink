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

import java.lang.reflect.Type
import java.util
import java.util.Optional

import org.apache.flink.api.common.functions.InvalidTypesException
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, BasicTypeInfo, PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.TypeResolver.ResolvedParameterizedType
import org.apache.flink.api.java.typeutils.{ObjectArrayTypeInfo, TypeInformationExtractor}

class ScalaArrayTypeExtractor extends TypeInformationExtractor {
  /**
   *
   * @return the classes that the extractor could extract the{ @link TypeInformationExtractor} corresponding to.
   */
  override def getClasses: util.List[Class[_]] = util.Arrays.asList(classOf[Array[_]])

  /**
   * Extract the {@link TypeInformation} of given type.
   *
   * @param javaType the type that is needed to extract { @link TypeInformation}
   * @param context used to extract the { @link TypeInformation} for the generic parameters or components and contains some
   *                                            information of extracting process.
   * @return { @link TypeInformation} of the given type or { @link Optional#empty()} if the extractor could not handle this type
   * @throws InvalidTypesException if error occurs during extracting the { @link TypeInformation}
   */
  override def extract(javaType: Type, context: TypeInformationExtractor.Context): Optional[TypeInformation[_]] = {
    if (!javaType.isInstanceOf[ResolvedParameterizedType]) {
      return Optional.empty()
    }

    val arrayType = javaType.asInstanceOf[ResolvedParameterizedType]
    val elementType = arrayType.getActualTypeArguments()(0)

    // Since Array is final, we do not need to resolve it again.
    val elementTypeInfo = context.extract(elementType)
    elementTypeInfo match {
      case BasicTypeInfo.BOOLEAN_TYPE_INFO =>
        Optional.of(PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO)

      case BasicTypeInfo.BYTE_TYPE_INFO =>
        Optional.of(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)

      case BasicTypeInfo.CHAR_TYPE_INFO =>
        Optional.of(PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO)

      case BasicTypeInfo.DOUBLE_TYPE_INFO =>
        Optional.of(PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO)

      case BasicTypeInfo.FLOAT_TYPE_INFO =>
        Optional.of(PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO)

      case BasicTypeInfo.INT_TYPE_INFO =>
        Optional.of(PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO)

      case BasicTypeInfo.LONG_TYPE_INFO =>
        Optional.of(PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO)

      case BasicTypeInfo.SHORT_TYPE_INFO =>
        Optional.of(PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO)

      case BasicTypeInfo.STRING_TYPE_INFO =>
        Optional.of(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO)

      case _ =>
        Optional.of(ObjectArrayTypeInfo.getInfoFor(elementTypeInfo))
    }
  }
}
