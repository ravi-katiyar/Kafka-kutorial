/**
 * @author Ravi.Katiyar
 */
package in.coding.ravi.kafka_practice.producers;

import java.text.SimpleDateFormat;
import java.util.Date;
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

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
		// Create Producer properties
		String bootStrapSever = "127.0.0.1:9092";
		Properties properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapSever);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss.SSS");
		Date date = new Date();

		for (int i = 0; i < 10; i++) {

			// create producer record
			final String message = "from java producer : message_" + i + "_" + sdf.format(date);
			final String key = "id_" + Integer.toString(i);
			String topic = "first_topic";
			
			// by providing a key with the message we gurantee that same keys will always be send to same parition  
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, message);

			logger.info("KEY" + key);
			// send data

			// KEY ID_0 is going to partition 1
			// KEY ID_1 is going to partition 0
			// KEY ID_2 is going to partition 2
			// KEY ID_3 is going to partition 0
			// KEY ID_4 is going to partition 2
			// KEY ID_5 is going to partition 2
			// KEY ID_6 is going to partition 0
			// KEY ID_7 is going to partition 2
			// KEY ID_8 is going to partition 1
			// KEY ID_9 is going to partition 2
			producer.send(record, new Callback() {

				// executes everyTime a record is succesfully sent or an exception is thrown
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// TODO Auto-generated method stub
					if (exception == null) {
						// message sent success
						logger.info("the message = " + message + " KEY = " + key + " was sent to offset = "
								+ metadata.offset());
						logger.info("the message = " + message + " was sent to partition = " + metadata.partition());
					} else {
						logger.error("something went wrong ", exception);
					}
				}
			}).get(); // block the send to make it synchronous NEVER TO BE DONE LIKE THIS IN
						// PRODUCTION;

		}
		producer.flush();
		producer.close();

	}

}
