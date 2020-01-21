package org.apache.flink.streaming.scala.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class JavaToScala {

	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<TypeTest> ds = env.fromElements("a")
				.flatMap(new FlatMapFunction<String, TypeTest>() {
					@Override
					public void flatMap(String value, Collector<TypeTest> out) throws Exception {

					}
				});

		System.out.println(ds.getTransformation().getOutputType());
	}
}
