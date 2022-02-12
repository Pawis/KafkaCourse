package com.pawis.kafka_producer_twitter;

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

	Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

	private String consumerKey ="YWUKjcjWikH6slrz8OaS9A3tN";
	private String consumerSecret ="2nRpVjqiQlYLU1M8hSN6I5DhdaaDRLc2UVgs2XTxXvjMt2U57z";
	private String token = "591689506-2CbPXXn1mBptM4Sb5NJLljQWCq62jxUm0doS54Zb";
	private String secret ="j69ssP7pC0tTIkgl2mOoPO1CPABQlE9AMqaak3kMdfUwF";

	private String topic = "Twets";
	private String bootstrapServer = "localhost:9092";
	private String keySerializer = StringSerializer.class.getName();
	private String valueSerializer = StringSerializer.class.getName();

	List<String> terms = Lists.newArrayList("bitcoin","usa","politics","sport","soccer"); // Search words

	/*
	 * Minimalne ustawienia tematu
	 * 
	 * kafka-topics.sh --bootstrap-server localhost:9092 --create --topic Twets --partitions 3 --replication-factor 2
	 */

	public TwitterProducer() {

	}
	public KafkaProducer<String,String> createKafkaProducer() {

		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
		
		/*
		* Jakiej odpowiedzi oczekuje producent 0-zadnej 1-tylko leader all-leader+repliki
		* properties.setProperty(ProducerConfig.ACKS_CONFIG, 0/1/all);
		*/
		
		// Wazne ustawienia pozwalajace osiagnac lepsze wyniki przesulu danych do Kafki
		
		/* IMPORTANT !!! */ properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); /* IMPORTANT !!! */
		/* IMPORTANT !!! */ properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20"); /* IMPORTANT !!! */
		/* IMPORTANT !!! */ properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32KB  /* IMPORTANT !!! */
		
		/*
		* Jesli bedziemy wysylali za duzo i za szybko do Kafki ktora ma za malo Brookerow/jest zle ustawiona moze dosc do przeladowania buffera co spowoduje ze 
		* nastepna proba wyslania bedzie blokujaca
		* Te ustawienia pozwalaja zwiekszyc byffer oraz czas oczekiwania na wyslanie przed wywaleniem wyjatku
		*/
		
		// properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000"); Czas ktory producer oczekuje po zapelnieniu buffera po czym wywala Exception.
		
		/*
		 * 
		 * Wielkosc buffera - domyslnie 32MB, po zapelmnieniu send() jest blokowany.
		 * Jesli mamy duzo partycji mzoemy go zwiekszyc
		 * properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "32"); 
		 */


		KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

		return producer;
	}


	public void run() {

		logger.info("Setup...");
		/*
		 * BlockingQueue to klolejka do ktorej mozemy wysylac oraz pobierac asynchronicznie
		 */
		BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
		
		final KafkaProducer<String,String> producer = createKafkaProducer();
		final Client client = createTwitterClients(msgQueue);
		client.connect();

		// on a different thread, or multiple different threads....
		while (!client.isDone()) {
			String msg =null;
			try {
				msg = msgQueue.poll(5,TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				client.stop();
			}
			if(msg != null) {
				
				logger.info(msg);
				/*
				 * Urzywamy KafkaProducer z typami klucza oraz wartosci do wyslania asynchronicznie w batchu danych z TwitterClient
				 * do argumentow mozemy dodac Callback przez ktory otrzymamy odpowiedz z Kafki z ktorej mozemy wyciagnac informacje 
				 * 
				*/
				producer.send(new ProducerRecord<String, String>(topic, null,msg),new Callback() {

					public void onCompletion(RecordMetadata metadata, Exception exception) {
						if(exception != null) {
							logger.error("Error: ", exception);
						}
						logger.info("Message send: ");
						logger.info("Topic: " + metadata.topic());
						logger.info("Partition: " + metadata.partition());

					}
				});
			}
		}
		// Wazne zeby zatrzymac klient i KafkaProducer !!!
		client.stop();
		producer.close();
		logger.info("End of app");
	}

	public Client createTwitterClients(BlockingQueue<String>  msgQueue) {

		logger.info("Creating TwitterClient...");
		/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		ClientBuilder builder = new ClientBuilder()
				.name("Hosebird-Client-01")                              // optional: mainly for the logs
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));

		Client hosebirdClient = builder.build();
		logger.info("TwitteClient created.");

		return  hosebirdClient;

	}

	public static void main(String[] args) {
		new TwitterProducer().run();
	}

}
