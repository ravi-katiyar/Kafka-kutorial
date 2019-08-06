/**
 * @author Ravi.Katiyar
 *
 * Creation Date : 24-Jul-2019
 
 * 
 * 
 */
package in.coding.ravi.kafka_practice.consumers;

import java.time.Duration;
import java.util.Arrays;
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

//The Consumer will run inside a thread
public class ConsumerDemoWithThreads {

	public static void main(String args[]) {
		new ConsumerDemoWithThreads().creteConsumerThread();
	}

	private ConsumerDemoWithThreads() {

	}

	private void creteConsumerThread() {

		final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class);

		String bootStrapServer = "127.0.0.1:9092";
		String groupId = "my-sixth-application";
		String topic = "first_topic";
		CountDownLatch latch = new CountDownLatch(1);

		logger.info("creating the consumer thread");
		Runnable myConsumerRunnable = new ConsumerRunnable(bootStrapServer, topic, groupId, latch);

		Thread myConsumerThread = new Thread(myConsumerRunnable);
		myConsumerThread.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Caught Shutdown hook");
			((ConsumerRunnable) myConsumerRunnable).shutdown();
			try {
				latch.await();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			logger.info("application exited");
		}));

		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error("application got interuptted");
		} finally {
			logger.info("application is closing");

		}

	}

	// This is a thread inside which a consumer consumes
	public class ConsumerRunnable implements Runnable {

		final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;

		public ConsumerRunnable(String bootStrapServer, String topic, String groupId, CountDownLatch latch) {

			Properties properties = new Properties();
			// 1. Create consumer CONFIG
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// earliest or latest

			this.latch = latch;
			this.consumer = new KafkaConsumer<String, String>(properties);
			consumer.subscribe(Arrays.asList(topic));

		}

		public void run() {
			try {
				while (true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

					for (ConsumerRecord<String, String> record : records) {
						logger.info("key = " + record.key() + " value = " + record.value());
						logger.info("Partition = " + record.partition() + " Offset = " + record.offset());
					}
				}
			} catch (WakeupException ex) {
				logger.info("received shutdown signal");
			} finally {
				consumer.close();
				// tell our main code that we are done with the consumer
				latch.countDown();
			}

		}

		// The wakeup method is used to stop the consumer poll , it will throw a
		// exception call wakeUpException
		public void shutdown() {
			consumer.wakeup();
		}

	}

}
