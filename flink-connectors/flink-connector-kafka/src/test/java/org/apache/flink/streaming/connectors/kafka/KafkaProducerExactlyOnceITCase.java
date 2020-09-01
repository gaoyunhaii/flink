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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.poc1.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.testutils.FailingIdentityMapper;
import org.apache.flink.streaming.connectors.kafka.testutils.IntegerSource;
import org.apache.flink.test.util.TestUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Properties;

/**
 * IT cases for the {@link FlinkKafkaProducer}.
 */
@SuppressWarnings("serial")
public class KafkaProducerExactlyOnceITCase extends KafkaProducerTestBase {
	@BeforeClass
	public static void prepare() throws Exception {
		KafkaProducerTestBase.prepare();
		((KafkaTestEnvironmentImpl) kafkaServer).setProducerSemantic(FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
	}

	@Override
	public void testOneToOneAtLeastOnceRegularSink() throws Exception {
		// TODO: fix this test
		// currently very often (~50% cases) KafkaProducer live locks itself on commitTransaction call.
		// Somehow Kafka 10 doesn't play along with NetworkFailureProxy. This can either mean a bug in Kafka
		// that it doesn't work well with some weird network failures, or the NetworkFailureProxy is a broken design
		// and this test should be reimplemented in completely different way...
	}

	@Override
	public void testOneToOneAtLeastOnceCustomOperator() throws Exception {
		// TODO: fix this test
		// currently very often (~50% cases) KafkaProducer live locks itself on commitTransaction call.
		// Somehow Kafka 10 doesn't play along with NetworkFailureProxy. This can either mean a bug in Kafka
		// that it doesn't work well with some weird network failures, or the NetworkFailureProxy is a broken design
		// and this test should be reimplemented in completely different way...
	}

	@Test
	public void testNewApiExactlyOnce() throws Exception {
		//testExactlyOnce(false, 2);
		testExactlyOnceNewApi();
	}

	/**
	 * This test sets KafkaProducer so that it will  automatically flush the data and
	 * and fails the broker to check whether flushed records since last checkpoint were not duplicated.
	 */
	protected void testExactlyOnceNewApi() throws Exception {
		final String topic = "new-api-topic";
		final int partition = 0;
		final int numElements = 1000;
		final int failAfterElements = 333;
		final String sinkName = "new-sink-test";
		final long checkpointInterval = 500;

		createTestTopic(topic , 1, 1);

		IntegerKafkaSchema integerKafkaSchema = new IntegerKafkaSchema(topic);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(checkpointInterval);
		env.setParallelism(1);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

		Properties properties = new Properties();
		properties.putAll(standardProps);
		properties.putAll(secureProps);

		// process exactly failAfterElements number of elements and then shutdown Kafka broker and fail application
		List<Integer> expectedElements = getIntegersSequence(numElements);

		DataStream<Integer> inputStream = env
			.addSource(new IntegerSource(numElements))
			.map(new FailingIdentityMapper<Integer>(failAfterElements));

		KafkaSink kafkaSink = new KafkaSink(properties, topic, integerKafkaSchema, sinkName, checkpointInterval);
		inputStream.addUSink(kafkaSink);


		FailingIdentityMapper.failedBefore = false;
		TestUtils.tryExecute(env, "Exactly once test");

			// assert that before failure we successfully snapshot/flushed all expected elements
		assertExactlyOnceForTopic(
			properties,
			topic ,
			partition,
			expectedElements,
			KAFKA_READ_TIMEOUT);
		deleteTestTopic(topic );
	}

	public class IntegerKafkaSchema implements KafkaSerializationSchema<Integer> {

		private final TypeInformationSerializationSchema<Integer> schema = new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());
		private final String topic;

		public IntegerKafkaSchema(String topic) {
			this.topic = topic;
		}

		@Override
		public ProducerRecord<byte[], byte[]> serialize(Integer element, @Nullable Long timestamp) {
			byte[] key = schema.serialize(element);
			byte[] val = schema.serialize(element);
			return new ProducerRecord<>(topic, 0, key, val);
		}
	}
}
