package com.gsudan.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {
	private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoKeys.class);
	private static final String BOOTSTRAP_SERVER_LIST = "127.0.0.1:9092";
	private static final String TOPIC_NAME = "first_topic";

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		LOG.info("=== Create Producer Properties ===");
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		LOG.info("=== Create Producer ===");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		LOG.info("=== Send Data to Producer (Synchronous) ===");
		for (int i = 0; i < 10; i++) {
			String value = "Java 101 Kafka Producer with Keys - " + i;
			String key = "id_" + i;
			// id_0 partition 1
			// id_1 partition 0
			// id_2 partition 2
			// id_3 partition 0
			// id_4 partition 2
			// id_5 partition 2
			// id_6 partition 0
			// id_7 partition 2
			// id_8 partition 1
			// id_9 partition 2
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, key, value);
			LOG.info("Key: {}", key);
			producer.send(record, new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception exception) {
					LOG.info("*** Executes every time a record was sent successfully or an exception is thrown ***");
					if (exception == null) {
						LOG.info("||| Record was sent successfully |||");
						LOG.info("Received new metadata. \nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}",
								metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
					} else {
						LOG.error("Error while producing: {}", exception);
					}
				}
			}).get(); // blocking  the send() to make it synchronous - not production ideal
		}

		LOG.info("=== Flush & close producer ===");
		producer.flush();
		producer.close();
	}
}
