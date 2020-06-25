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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.{TypeExtractionUtils, TypeHierarchyBuilder, TypeInformationExtractor, TypeResolver}
import org.apache.flink.api.scala.typeutils.TryTypeInfo

import scala.util.Try

class ScalaTryTypeExtractor extends TypeInformationExtractor {
  /**
   *
   * @return the classes that the extractor could extract the{ @link TypeInformationExtractor} corresponding to.
   */
  override def getClasses: util.List[Class[_]] = util.Arrays.asList(classOf[Try[_]])

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
    // Since javaType might be subclass of Either, we must try to find the parent class
    val hierachy = TypeHierarchyBuilder.buildParameterizedTypeHierarchy(
      javaType,
      TypeHierarchyBuilder.isSameClass(classOf[Try[_]]),
      TypeHierarchyBuilder.isSameClass(classOf[Try[_]]).or(TypeHierarchyBuilder.assignTo(classOf[Try[_]])))

    val parameterizedOptionType = hierachy.get(hierachy.size() - 1)
    // Try resolve the parameters
    val elementType = TypeResolver.resolveTypeFromTypeHierarchy(
      TypeExtractionUtils.extractTypeArgument(parameterizedOptionType, 0),
      hierachy,
      true)

    val elementTypeInfo = context.extract(elementType)

    Optional.of(new TryTypeInfo(elementTypeInfo))
  }
}
