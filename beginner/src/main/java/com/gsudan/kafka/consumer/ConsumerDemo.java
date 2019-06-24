package com.gsudan.kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
	private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemo.class);
	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String GROUP_ID = "my-fourth-application";
	private static final String TOPIC_NAME = "first_topic";

	public static void main(String[] args) {
		LOG.info("=== Create Consumer Properties ===");
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		LOG.info("=== Create Consumer ===");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		LOG.info("=== Subscribe the consumer to our topic(s) ===");
		consumer.subscribe(Collections.singleton(TOPIC_NAME));
		
		LOG.info("=== Poll for new data ===");
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record: records) {
				LOG.info("Key: {}, Value: {}", record.key(), record.value());
				LOG.info("Partition: {}, Offset: {}", record.partition(), record.offset());
			}
		}
//		consumer.close();
	}
}
