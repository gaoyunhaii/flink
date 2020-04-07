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

object ScalaCollectionsTest {

  def main(args: Array[String]): Unit = {
    val list = List(1, 2, 3, 5, 7, 8, 9, 10)
    println((list.iterator grouped 3).next())


  }
}

package _collections_classes {
  class Mine extends Product2[Int, String] () {
    override def _1: Int = productElement(0).asInstanceOf[Int]

    override def _2: String = productElement(1).asInstanceOf[String]

    override def canEqual(that: Any): Boolean = true
  }
}
