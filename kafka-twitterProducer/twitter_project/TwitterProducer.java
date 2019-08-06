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

	String consumerKey = "bMGyh10vgYPUeP2yX5HDSa9fX";
	String consumerSecretKey = "olP3VUZ58Kwop7F1izpHLKk2XBayvOyWVq1Jy0R3SE4eZqPmeU";
	String accessToken = "110235292-o6QkTMYbrf6RenxNBeqg3NzBYO8VOtDqvb65Hr2H";
	String accessSecretToken = "2psUhiiVeO6hURtU02SheJTeR9O1toWgZvR9ruaYMbmbg";
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

		List<String> terms = Lists.newArrayList("Kashmir");

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

		// define properties for a safe producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.
		
		//define properties for high throughput producer (at some cost of latency and CPU cycles)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));

		// create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		return producer;

	}

}
