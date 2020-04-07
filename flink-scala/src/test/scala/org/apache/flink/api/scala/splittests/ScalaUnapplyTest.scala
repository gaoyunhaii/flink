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

object ScalaUnapplyTest {

  def main(args: Array[String]): Unit = {
    val me = "second"
    val ret = me match {
      case _unapply_test.FirstBranch(result) => result
      case _unapply_test.SecondBranch(result) => result
      case _ => -1
    }

    println(ret)
  }
}

package _unapply_test {

  object FirstBranch {
    def unapply(o: String): Option[Int] = {
      if (o.equals("first")) {
        return Some(5)
      }

      None
    }
  }

  object SecondBranch {
    def unapply(o: String): Option[Int] = {
      return o match {
        case "second" => Some(6)
        case _ => None
      }
    }
  }
}
