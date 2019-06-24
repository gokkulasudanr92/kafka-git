package com.gsudan.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
	private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
	private static final String BOOTSTRAP_SERVER_LIST = "127.0.0.1:9092";
	private static final String TOPIC_NAME = "first_topic";

	public static void main(String[] args) {
		LOG.info("=== Create Producer Properties ===");
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_LIST);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		LOG.info("=== Create Producer ===");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		LOG.info("=== Send Data to Producer (Asynchronous) ===");
		for (int i = 0; i < 10; i++) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME,
					"Java 101 Kafka Producer With Callbacks - " + i);
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
			});
		}

		LOG.info("=== Flush & close producer ===");
		producer.flush();
		producer.close();
	}
}
