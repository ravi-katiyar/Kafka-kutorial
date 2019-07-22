/**
 * @author Ravi.Katiyar
 */
package in.coding.ravi.kafka_practice.producers;

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

	public static void main(String[] args) {
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
		// Create Producer properties
		String bootStrapSever = "127.0.0.1:9092"; 
		Properties properties = new Properties();
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapSever);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		//create producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "from java producer : message 3");
		
		//send data
		producer.send(record, new Callback() {
			
			//executes everyTime a record is succesfully sent or an exception is thrown
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// TODO Auto-generated method stub
				if(exception == null) {
					//message sent success
					logger.info("the message was sent to offset " + metadata.offset());
					logger.info("the message was sent to partition " + metadata.partition());
				}else {
					logger.error("something went wrong ", exception);
				}
			}
		});
		producer.flush();
		producer.close();

	}

}
