package com.gsudan.kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThreads {
	private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);
	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	private static final String GROUP_ID = "my-sixth-application";
	private static final String TOPIC_NAME = "first_topic";

	public static void main(String[] args) {
		new ConsumerDemoWithThreads().run();
	}

	public void run() {
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
		CountDownLatch latch = new CountDownLatch(1);
		ConsumerThread run = new ConsumerThread(latch, consumer);
		Thread my = new Thread(run);
		my.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			LOG.info("Graceful shutdown...");
			run.shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}));

		try {
			latch.wait();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public class ConsumerThread implements Runnable {

		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;

		public ConsumerThread(CountDownLatch latch, KafkaConsumer<String, String> consumer) {
			this.latch = latch;
			this.consumer = consumer;
		}

		public void run() {
			try {
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
					for (ConsumerRecord<String, String> record : records) {
						LOG.info("Key: {}, Value: {}", record.key(), record.value());
						LOG.info("Partition: {}, Offset: {}", record.partition(), record.offset());
					}
				}
			} catch (WakeupException wue) {
				LOG.info("Recieved the shutdown call...");
			} finally {
				consumer.close();
				latch.countDown();
			}
		}

		public void shutdown() {
			LOG.info(
					"The wake up method is special method which interrupts the consumer.poll() and throws an WakeUpException");
			consumer.wakeup();
		}
	}
}
