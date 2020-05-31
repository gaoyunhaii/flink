package org.apache.flink.api.scala

import scala.language.experimental.macros
import scala.reflect.macros.Context

object ScalaMacro {

  trait Help {
    def a(i: Int): Int
  }

  def createFunction(c: Context): c.Tree = {
    import c.universe._

    val outer = new Help {
      override def a(i: Int): Int = 0
    }

    reify {
      val inner = new Help {
        override def a(i: Int): Int = 0
      }
      println(inner)

      new ScalaMacro.Help {
        override def a(i: Int): Int = 0
      }
    }.tree
  }

  def theA: Help = macro createFunction
}
