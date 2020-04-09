package org.apache.flink.api.scala

object Test2 {
  def main(args: Array[String]) = {
    val a: Any = ""
    val b: Any = 5
    println(new Test(a, b, ("c", 6)))
  }

  class Test[A, B, T <: (A, B)](val a: A, val b: B, val t: T) {

  }

}
