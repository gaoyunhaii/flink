package org.apache.flink.streaming.scala.examples.wordcount;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

public class MyFunction {

	public static class MyFlatMapFunction extends RichFlatMapFunction<String, Haha> {

		@Override
		public void flatMap(String value, Collector<Haha> out) throws Exception {

		}
	}

	public static class Haha {
		private String you;

		public Haha() {
		}

		public Haha(String you) {
			this.you = you;
		}

		public String getYou() {
			return you;
		}

		public void setYou(String you) {
			this.you = you;
		}
	}
}
