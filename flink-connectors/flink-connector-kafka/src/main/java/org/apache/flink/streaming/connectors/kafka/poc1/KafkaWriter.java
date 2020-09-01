package org.apache.flink.streaming.connectors.kafka.poc1;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.connector.sink.USink;
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaInternalProducer;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

class KafkaWriter<T> implements Writer<T, FlinkKafkaProducer.KafkaTransactionState> {

	protected static final Logger LOG = LoggerFactory.getLogger(KafkaWriter.class);

	private static final ListStateDescriptor<TransactionId> TRANSACTION_ID_STATES_DESC =
		new ListStateDescriptor<>("transaction-ids", TransactionId.class);

	private static final ListStateDescriptor<Integer> MAX_PARALLELISM =
		new ListStateDescriptor<Integer>("max-parallelism", Integer.class);

	//TODO:: change the name and configure it
	private static final Integer MAX_SET_NUM = 128;

	private final Properties producerConfig;

	private final KafkaSerializationSchema<T> kafkaSchema;

	private final ListState<TransactionId> transactionIDStates;

	private final ListState<Integer> maxParallelismState;

	private final String topicForState;

	private final String consumerGroupId = "TODO_TO_NEED_TO_CHANGE_LATER";

	// These sets would not be added any committable data.
	private final List<TransactionId> pendingCommitableSets;

	private final Integer maxParallelism;

	private final boolean recordMaxParallelism;

	// ============================== Runtime =====================================

	private FlinkKafkaProducer.KafkaTransactionState kafkaTransactionState;

	private TransactionId activeSetCurrentTransactionId;

	public KafkaWriter(
		final String sessionPrefix,
		final Long checkpointInterval,
		final USink.InitialContext initialContext,
		final Properties producerConfig,
		final String topicForState,
		KafkaSerializationSchema<T> kafkaSchema) throws Exception {

		this.producerConfig = producerConfig;
		this.kafkaSchema = kafkaSchema;
		this.topicForState = topicForState;
		this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		this.producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		this.transactionIDStates = initialContext.getListState(TRANSACTION_ID_STATES_DESC);
		this.maxParallelismState = initialContext.getUnionListState(MAX_PARALLELISM);

		this.pendingCommitableSets = new ArrayList<>();

		Integer restoreMaxParallelism = 0;
		if (initialContext.isRestored()) {
			for (TransactionId transactionId : transactionIDStates.get()) {
				pendingCommitableSets.add(transactionId);
				// clean up the transaction id(unmanaged committable data) in the active sets(managed sets)
				abortTransactions(transactionId,  readCurrentTransactionId(transactionId.getSetId()));
			}
			Collections.sort(pendingCommitableSets);
			activeSetCurrentTransactionId = pendingCommitableSets.remove(0);
			restoreMaxParallelism = initialContext.getUnionListState(MAX_PARALLELISM).get().iterator().next();
			LOG.info("[USK] reuse ................ :" + activeSetCurrentTransactionId);
		} else {
			activeSetCurrentTransactionId = new TransactionId(new SetId("todo-change-to-operator-id", initialContext.getSubtaskIndex()), 0L);
			// clean up the transaction id(unmanaged committable data) in the active sets(managed sets)
			abortTransactions(activeSetCurrentTransactionId,  readCurrentTransactionId(activeSetCurrentTransactionId.getSetId()));
			writeCurrentTransactionId(activeSetCurrentTransactionId);
		}
		maxParallelism = restoreMaxParallelism > initialContext.getParallelism() ? restoreMaxParallelism : initialContext.getParallelism();

		// clean up the unmanaged set's unmanaged committable data
		for (int i = 1; i < MAX_SET_NUM / initialContext.getParallelism(); i++) {
			final Integer setIndex = i * initialContext.getParallelism() + initialContext.getSubtaskIndex();
			final SetId setId = new SetId("todo-change-to-operation-id", setIndex);
			final TransactionId transactionId = new TransactionId(setId, 0L);
			//TODO:: concurrent to do it.
			abortTransactions(transactionId, readCurrentTransactionId(setId));
		}

		if (initialContext.getSubtaskIndex() ==  0) {
			recordMaxParallelism = true;
		} else {
			recordMaxParallelism = false;
		}
		this.kafkaTransactionState = create(activeSetCurrentTransactionId);
	}

	@Override
	public void write(T t, Context context, Collector<FlinkKafkaProducer.KafkaTransactionState> collector) throws Exception {
		ProducerRecord<byte[], byte[]> record = kafkaSchema.serialize(t, System.currentTimeMillis());
		kafkaTransactionState.getProducer().send(record);

		kafkaTransactionState.setNum(kafkaTransactionState.getNum() + 1);
	}

	@Override
	public void persistent(Collector<FlinkKafkaProducer.KafkaTransactionState> output) throws Exception {

		if (kafkaTransactionState.getNum() == 0) {
			kafkaTransactionState.getProducer().abortTransaction();
		} else {
			kafkaTransactionState.getProducer().flush();
			kafkaTransactionState.getProducer().close(Duration.ofSeconds(0));
			output.collect(kafkaTransactionState);
		}
		transactionIDStates.clear();
		transactionIDStates.add(activeSetCurrentTransactionId);
		// we need to keep the pending sets because user could change parallism as 8,2,8;
		for (TransactionId transactionId : pendingCommitableSets) {
			transactionIDStates.add(transactionId);
		}
		maxParallelismState.clear();
		if (recordMaxParallelism) {
			maxParallelismState.add(maxParallelism);
		}

		activeSetCurrentTransactionId = new TransactionId(activeSetCurrentTransactionId);
		writeCurrentTransactionId(activeSetCurrentTransactionId);
		kafkaTransactionState = create(activeSetCurrentTransactionId);

	}

	@Override
	public void flush(Collector<FlinkKafkaProducer.KafkaTransactionState> output) throws IOException {
		kafkaTransactionState.getProducer().abortTransaction();

//		if (kafkaTransactionState.getNum() == 0) {
//			kafkaTransactionState.getProducer().abortTransaction();
//		} else {
//			kafkaTransactionState.getProducer().flush();
//			kafkaTransactionState.getProducer().close(Duration.ofSeconds(0));
//			output.collect(kafkaTransactionState);
//		}
	}

	@Override
	public void close() {
		//kafkaTransactionState.getProducer().close(Duration.ofSeconds(5));
	}

	private FlinkKafkaProducer.KafkaTransactionState create(TransactionId transactionId) {
		final Properties properties = new Properties();
		properties.putAll(this.producerConfig);
		properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionId.toString());

		final FlinkKafkaInternalProducer kafkaInternalProducer = new FlinkKafkaInternalProducer<>(properties);
		kafkaInternalProducer.initTransactions();
		kafkaInternalProducer.beginTransaction();
		return new FlinkKafkaProducer.KafkaTransactionState(transactionId.toString(), kafkaInternalProducer);
	}

	static class TransactionId implements Comparable<TransactionId> {

		private final SetId setId;

		private final Long counter;

		TransactionId(SetId setId, Long counter) {
			this.setId = setId;
			this.counter = counter;
		}

		TransactionId(TransactionId transactionId) {
			this.setId = transactionId.getSetId();
			this.counter = transactionId.getCounter() + 1;
		}

		public SetId getSetId() {
			return setId;
		}

		public long getCounter() {
			return counter;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) return false;
			TransactionId that = (TransactionId) o;
			return setId.equals(that.setId) &&
				counter.equals(that.counter);
		}

		@Override
		public int hashCode() {
			return Objects.hash(setId, counter);
		}

		@Override
		public String toString() {
			return String.format("%s-%s", setId, counter);
		}

		@Override
		public int compareTo(TransactionId o) {
			if (setId.equals(o.getSetId())) {
				//TODO:: fix
				return (int) (counter - o.getCounter());
			} else {
				return setId.compareTo(o.getSetId());
			}
		}
	}

	static class SetId implements Comparable<SetId> {

		private final String name;

		private final Integer index;

		public SetId(String name, int index) {
			this.name = name;
			this.index = index;
		}

		public int getIndex() {
			return index;
		}

		public String getName() {
			return name;
		}

		@Override
		public String toString() {
			return String.format("%s-%s", name, index);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			SetId setId = (SetId) o;
			return name.equals(setId.name) &&
				index.equals(setId.index);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, index);
		}

		@Override
		public int compareTo(SetId o) {
			if (name.equals(o.getName())) {
				return index - o.getIndex();
			} else {
				return name.compareTo(o.name);
			}
		}
	}

	//We assume that this would be success always!
	private void writeCurrentTransactionId(final TransactionId transactionId) {

		final SetId setId = transactionId.getSetId();

		final Properties properties = new Properties();
		properties.putAll(producerConfig);
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

		final Map<TopicPartition, OffsetAndMetadata> metas = new HashMap<>();

		properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, setId.toString());

		metas.put(
			new TopicPartition(topicForState, setId.getIndex()),
			new OffsetAndMetadata(transactionId.counter, transactionId.toString()));

		final KafkaProducer<String, Integer> producer = new KafkaProducer<>(properties);

		producer.initTransactions();
		producer.beginTransaction();

		producer.sendOffsetsToTransaction(metas, consumerGroupId);

		producer.flush();
		producer.commitTransaction();
		producer.close();
	}

	@Nullable
	private TransactionId readCurrentTransactionId(final SetId setId) {

		final Properties properties = new Properties();
		properties.putAll(producerConfig);

		properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"");

		properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		properties.put("group.id", consumerGroupId);

		final KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(Arrays.asList(topicForState));
		final TopicPartition topicPartition = new TopicPartition(topicForState, setId.getIndex());
		final OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
		consumer.close(Duration.ofSeconds(1));
		if (offsetAndMetadata != null) {
			return new TransactionId(setId, offsetAndMetadata.offset());
		} else {
			return null;
		}
	}

	private void abortTransactions(final TransactionId begin, @Nullable final TransactionId end) {
		TransactionId t = begin;
		do {
			final Properties properties = new Properties();
			properties.putAll(producerConfig);
			properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, t.toString());

			final FlinkKafkaInternalProducer cleanup =  new FlinkKafkaInternalProducer<>(properties);
			// it suffices to call initTransactions - this will abort any lingering transactions
			cleanup.initTransactions();
			cleanup.close(Duration.ofSeconds(0));
			LOG.info("[USK] abort ................ :" + t.toString());
			t = new TransactionId(t);
		} while(end != null && t.compareTo(end) <= 0);
	}
}
