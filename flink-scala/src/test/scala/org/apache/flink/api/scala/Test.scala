package org.apache.flink.api.scala

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.haha.MyClassSub

object Test {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val source = env.fromCollection(Array("test", "test2"))
    val ds = source.map(x => haha.MyClass[String, MyClassSub[List[Integer]]]("1", new MyClassSub[List[Integer]](List(5)))
    )
    println(ds.getType())
    println(new haha.MyClassSub[String]("a").a)
  }

}

package haha {

  case class MyClass[T1, T2](a: T1, b: T2) {
    println("abcde")
  }

  class MyClassSub[T1](val a: T1) {
  }

}
