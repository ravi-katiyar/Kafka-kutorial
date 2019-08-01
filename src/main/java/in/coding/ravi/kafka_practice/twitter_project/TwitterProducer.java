/**
 * @author Ravi.Katiyar
 *
 * Creation Date : 29-Jul-2019
 * 
 */
package in.coding.ravi.kafka_practice.twitter_project;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterProducer {

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

	String consumerKey = "kQFassXWNxgOZ3tr3h5ghFAbx";
	String consumerSecretKey = "9z47adzFfuETYqbNWUqv5qYhl0kEMm6Xzz1hT4K56c0KgrO6Qx";
	String accessToken = "110235292-TMSSV1Ioues1YSyIEZFdmLhiMBgOcl6yPCGdUInt";
	String accessSecretToken = "mCcH9Cj3eUNI9AdV74knNPYjqrrXYX2kSI4LAlZLADC8K";
	String bootStrapServer = "127.0.0.1:9092";

	public static void main(String[] args) {

		new TwitterProducer().twitterClientWrapper();

		// loop to send tweets to kafka

	}

	public void twitterClientWrapper() {
		logger.info("starting twitter client setup");

		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		// create a twitter client
		Client client = createTwitterClient(msgQueue);

		client.connect();

		// create a kafka producer
		KafkaProducer<String, String> kafkaProducer = createKafkaProducer(bootStrapServer);

		// add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {

			logger.info("stopping application");
			logger.info("shutting down client from twitter");
			client.stop();
			logger.info("closing producer");
			kafkaProducer.close();
			logger.info("DONE");
		}));

		// loop to send tweets to kafka on a different thread,
		// or multiple different threads....
		while (!client.isDone()) {
			String msg = null;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				client.stop();
			}

			if (msg != null) {
				logger.info(msg);
				kafkaProducer.send(new ProducerRecord<String, String>("twitter_topic", null, msg), new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception exception) {

						if (exception != null) {
							logger.info("something bad happened", exception);
						}

					}
				});
			}

		}
		logger.info("end of application");

	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) {

		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		List<String> terms = Lists.newArrayList("kafka");

		hosebirdEndpoint.trackTerms(terms);

		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecretKey, accessToken, accessSecretToken);

		ClientBuilder builder = new ClientBuilder().name("hosebird-Client-01").hosts(hosebirdHosts)
				.authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		return hosebirdClient;

	}

	public KafkaProducer<String, String> createKafkaProducer(String bootStrapServer) {

		Properties properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		return producer;

	}

}