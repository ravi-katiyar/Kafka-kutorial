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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	public static void main(String[] args) {
		final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

		String bootStrapServer = "127.0.0.1:9092";
		String groupId = "my-fifth-application";
		String topic = "first_topic";

		Properties properties = new Properties();

		// 1. Create consumer CONFIG
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		
		/** 1. Partitions from a topic will be balanced among the consumers of a consumer group
		 *  2. So If we have 3 partitions and 3 consumer in a consumer group each consumer will have 1 partition allocated to read from.
		 *  3. If we have only 1 Consumer in a consumer group then all partitions will be allocated to this consumer.
		 *  4. If the consumers in a group a more than partitions then some consumer will sit idle.   
		 */
		
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");// earliest or latest

		// 2. Create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// 3. Subscribe consumer to our topic(s)

		// we can also subsrcibe to multiple topics as well for eg :
		// //consumer.subscribe(Arrays.asList("a","b","c"));
		consumer.subscribe(Arrays.asList(topic));

		// 4. Poll for new data.

		// consumer doesn't get data until it asks for it
		// this while true is only for testing we don't write like this in production
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {
				logger.info("key = " + record.key() + " value = " + record.value());
				logger.info("Partition = " + record.partition() + " Offset = " + record.offset());
			}
		}

	}

}
