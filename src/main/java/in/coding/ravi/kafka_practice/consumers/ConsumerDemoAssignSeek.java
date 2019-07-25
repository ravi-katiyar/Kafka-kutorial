/**
 * @author Ravi.Katiyar
 *
 * Creation Date : 22-Jul-2019
 
 * 
 * 
 */
package in.coding.ravi.kafka_practice.consumers;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//Assign and seek are basically used to replay data or fetch a specific message
public class ConsumerDemoAssignSeek {

	public static void main(String[] args) {
		final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

		String bootStrapServer = "127.0.0.1:9092";
		String topic = "first_topic";

		Properties properties = new Properties();

		// 1. Create consumer CONFIG
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// earliest or latest

		TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
		// 2. Create consumer , for this consumer we don't have a group Id because we
		// assign and seek
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// assign
		consumer.assign(Arrays.asList(partitionToReadFrom));

		// seek
		long offsetToReadFrom = 15;
		consumer.seek(partitionToReadFrom, offsetToReadFrom);

		int numberOfMessagesToRead = 5;
		int totalMessagesRead = 0;
		boolean keepOnReading = true;

		while (keepOnReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {
				if (totalMessagesRead > numberOfMessagesToRead) {
					keepOnReading = false;
					break;
				}
				logger.info("key = " + record.key() + " value = " + record.value());
				logger.info("Partition = " + record.partition() + " Offset = " + record.offset());
				totalMessagesRead += 1;
			}
		}
		consumer.close();
	}

}
