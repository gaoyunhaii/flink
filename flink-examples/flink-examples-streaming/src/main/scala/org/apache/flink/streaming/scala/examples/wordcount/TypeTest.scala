package org.apache.flink.streaming.scala.examples.wordcount

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.examples.wordcount.util.WordCountData
import org.apache.flink.streaming.api.scala._

import scala.beans.BeanProperty

object TypeTest {
  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val text = env.fromElements(WordCountData.WORDS.head)

    def a(value : String) = new TypeTest()
    text.map[TypeTest](v => a(v))

    // env.execute("Test")
  }

  class TypeTest() {
    @BeanProperty var name : String = _
  }
}
