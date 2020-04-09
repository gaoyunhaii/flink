package org.apache.flink.api.scala

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.haha.MyEnum.MyEnum
import org.apache.flink.api.scala.haha.{MyClassSub, MyEnum}
import org.apache.flink.types.Nothing

import scala.util.Try

object Test {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val source = env.fromCollection(Array("test", "test2"))
    // val ds = source.map(x => haha.MyClass[String, MyClassSub[List[Integer]]]("1", new MyClassSub[List[Integer]](List(5))))

    val ds = source.map(x => haha.MyClass[String, Integer]("5a", new MyClassSub[Integer](5)))

    // val func: String => Either[Integer, String] = s => Either.cond(1 + 1 == 2, "5", 3)
//    val func: String => MyEnum = s => MyEnum.Fri
//    val ds = source.map(func)

//    val func: String => Try[Integer] = s => Try.apply(5)
//    val ds = source.map(func)

//    val func: String => Option[Integer] = s => Option.empty
//    val ds = source.map(func)

    println(ds.getType(), ds.getType().getClass)
//    println(MyEnum.Fri.getClass)
    println("abcdedfghilmaaaaalaadbaaaadaaaaaaaaabaaaa")

//    println(new haha.MyClassSub[String]("a").a)
//    println(MyEnum.showAll)
//
//    val enum: Enumeration  = haha.MyEnum

  }
}

package haha {
  case class MyClass[T1, T2](a: T1, b: MyClassSub[T2]){

  }

  case class Book(isbn: String)

  class MyClassSub[T1](val a: T1) {
  }

  class MyClass2[T <: MyClassSub[String]]() {
    def useEnum() : Unit = {

    }
  }

  object MyEnum extends Enumeration {
    type MyEnum = Value
    val Mon, Tue, Wen, Thu, Fri, SAT, Sun = Value

    def showAll(): Unit = this.values.foreach(println)
  }
}
