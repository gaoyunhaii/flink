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

package org.apache.flink.api.scala.typeutils.scalaruntime

import org.apache.flink.api.java.typeutils.javaruntime.AbstractTypeClassFactory
import org.apache.flink.api.java.typeutils.types.AbstractTypeClass

class ScalaRuntimeTypeClassFactory extends AbstractTypeClassFactory {

  override def forName(name: String): AbstractTypeClass = {
    val clazz = Class.forName(name)
    val scalaClazz = new ScalaRuntimeTypeClass(clazz)

    if (scalaClazz.getType.typeSymbol.isJava) {
      null
    } else {
      scalaClazz
    }
  }
}
