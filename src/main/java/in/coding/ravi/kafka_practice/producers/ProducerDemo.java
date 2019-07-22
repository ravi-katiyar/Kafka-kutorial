/**
 * @author Ravi.Katiyar
 */
package in.coding.ravi.kafka_practice.producers;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {

	public static void main(String[] args) {
		// Create Producer properties
		String bootStrapSever = "127.0.0.1:9092"; 
		Properties properties = new Properties();
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapSever);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		//create producer record
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "from java producer : message 1");
		
		//send data
		producer.send(record);
		producer.flush();
		producer.close();

	}

}
