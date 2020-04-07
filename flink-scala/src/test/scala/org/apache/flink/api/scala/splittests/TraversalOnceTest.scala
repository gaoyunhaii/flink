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

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.splittests._traversalonce_classes.WhatOO

import scala.collection

object TraversalOnceTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val source = env.fromCollection(Array("test", "test22222222222"))
    println(_traversalonce_classes.MyClass(5))
    // val ds = source.map(x => _traversalonce_classes.MyClass(x.length))
    val ds = source.map(x => List(x.length))
//    val ds = source.map(x => new WhatOO)
    System.out.println(ds.getType(), ds.getType().getClass, ds.getType().getClass.getSuperclass)
  }
}

package _traversalonce_classes {

  import scala.collection.generic.CanBuildFrom
  import scala.collection.mutable

  final class MyClass private(val length: Int) extends IndexedSeq[Int] {
    override def apply(idx: Int): Int = idx
  }

  object MyClass {
    def apply(length: Int): MyClass = {
      new _traversalonce_classes.MyClass(length)
    }

    implicit def canBuildFrom: CanBuildFrom[MyClass, Int, MyClass] =
      new CanBuildFrom[MyClass, Int, MyClass] {
        override def apply(from: MyClass): mutable.Builder[Int, MyClass] =
          new mutable.Builder[Int, MyClass] {
            var total: Int = from.length

            override def +=(elem: Int): this.type = {
              total += elem; this
            }

            override def clear(): Unit = {
              total = 0
            }

            override def result(): MyClass = {
              new MyClass(0)
            }
          }

        override def apply(): mutable.Builder[Int, MyClass] =
          new mutable.Builder[Int, MyClass] {
            var total: Int = 0

            override def +=(elem: Int): this.type = {
              total += elem; this
            }

            override def clear(): Unit = {
              total = 0
            }

            override def result(): MyClass = {
              new MyClass(0)
            }
          }
      }
  }


  final class WhatOO extends TraversableOnce[Int] {
    override def foreach[U](f: Int => U): Unit = {}

    override def isEmpty: Boolean = true

    override def hasDefiniteSize: Boolean = false

    override def seq: TraversableOnce[Int] = Nil

    override def forall(p: Int => Boolean): Boolean = true

    override def exists(p: Int => Boolean): Boolean = true

    override def find(p: Int => Boolean): Option[Int] = Option(0)

    override def copyToArray[B >: Int](xs: Array[B], start: Int, len: Int): Unit = {}

    override def toTraversable: Traversable[Int] = Nil

    override def isTraversableAgain: Boolean = true

    override def toStream: Stream[Int] = null

    override def toIterator: Iterator[Int] = null
  }
}
