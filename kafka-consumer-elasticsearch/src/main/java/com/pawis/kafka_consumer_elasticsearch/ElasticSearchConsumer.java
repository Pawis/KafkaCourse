package com.pawis.kafka_consumer_elasticsearch;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;


public class ElasticSearchConsumer {

	// Klient ktory laczy sie z ElasticSearch
	public static RestHighLevelClient createClient(){

		//////////////////////////
		/////////// IF YOU USE LOCAL ELASTICSEARCH
		//////////////////////////

		//  String hostname = "localhost";
		//  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));


		//////////////////////////
		/////////// IF YOU USE BONSAI / HOSTED ELASTICSEARCH
		//////////////////////////

		// replace with your own credentials
		//  https://cv3811ies4:bzh43ai8d7@kafka-8801297307.eu-central-1.bonsaisearch.net:443
		String hostname = "kafka-8801297307.eu-central-1.bonsaisearch.net"; // localhost or bonsai url
		String username = "cv3811ies4"; // needed only for bonsai
		String password = "bzh43ai8d7"; // needed only for bonsai

		// credentials provider help supply username and password
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY,
				new UsernamePasswordCredentials(username, password));

		RestClientBuilder builder = RestClient.builder(
				new HttpHost(hostname, 443, "https"))
				.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
					public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
						return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
					}
				});


		RestHighLevelClient client = new RestHighLevelClient(builder);
		return client;
	}
	//KafkaConsumer ktory laczy sie z kafka
	public static KafkaConsumer<String,String> createConsumer(String topic) {

		String bootstrapServer = "localhost:9092";
		String keyDeserializer = StringDeserializer.class.getName();
		String valueDeserializer = StringDeserializer.class.getName();
		String groupId = "kafka-demo-elasticsearch";

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

		// Od ktorego offsetu zaczynac jesli group_id jest nowe
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// Jak commitowac aktualny offset do kafki, automatycznie - po okreslonym czasie oraz na kazdy poll() czy manulanie
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		// Minimalna wielkosc parti zanim zostanie wyslana do konsumera, pozwala to zmniejszyc ilosc requestow co zwieksza przepustowosc kosztem opoznienia 
		properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, Integer.toString(1024*16));  //16KB Default -- 1
		
		/*
		 * USTAWIENIA ZAAWANSOWANE
		 * 
		 * Maksymalna wielkosc parti, jesli zostanie przekroczona partia zostaje automatycznie wyslana --  Default 50MB
		 * properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG ""); 
		 *
		 * Maksymalna ilosc rekordow na jeden poll()
		 * properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG "") Default 500
		 * 
		 * Czas co jaki poll() musi zostac wyslane zeby konsumer nie zostal wyrzucony z grupy i nie nastapil rebalance
		 * properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG "") Default 5 min
		 * 
		 * MAX_POLL_RECORDS_CONFIG i MAX_POLL_INTERVAL_MS_CONFIG
		 * sa zalezne od siebie, najlepiej konfigurowac obydwa naraz. Mozemy zwiekszyc ilosc zwroconych rekordow jesli ilosc jest rowna ilosci ustawionej
		 * lecz musimy pamietac o tym zeby konsumer zdazyl przetworzyc te dane przed kolejnym pollem badz weikszyc poll interwal ms zeby nie spowodowac wyrzucenia konsumera.
		 * 
		 * Maksymalna wielkosc ktora moze wyslac pojedyncza partycja do konsumera podczas jedenej partii
		 * np. 20 partycji 5 konsumerow = 4MB na konsumera = 20MB ogolnej pamieci RAM jest potrzebne
		 * properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG ""); Default--  1MB
		 * 
		 */
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		// Tutaj zaczynaja dopiero plynac dane z kafki, bez tego ani rusz.
		/* IMPORTANT !!! */ consumer.subscribe(Arrays.asList(topic)); /* IMPORTANT !!! */

		/*
		 * Ile pamieci zajmuje konsumer kafki
		 * int memory = min(num brokers * max.fetch.bytes,  max.partition.fetch.bytes * num_partitions);
		 */

		return consumer;

	}

	private static String extractIdFromTweet(String tweetJson) {

		String id = JsonParser
				.parseString(tweetJson)
				.getAsJsonObject()
				.get("id_str")
				.getAsString();

		return id;
	}
	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

		RestHighLevelClient client = createClient();
		KafkaConsumer<String, String> consumer = createConsumer("Twets");

		/*
		 * Ogolnie potrzebujemy KafkaConsumer i celu danych ktore umieszczamy w tym bloku kodu.
		 * Tworzymy kontener danych typu ConsumerRecords z typem klucza oraz wartosci ktory wyciagamy z Kafki
		 * Loopujemy przez ConsumerRecords zapisujac dane z kafki do wybranego typu wartosci a nastepnie
		 * wysylamy je do okreslonego celu, wszytko dzieje sie w nieskonczonym loopie
		 * 
		 * Najlepiej jest pobierac dane w partiach(Batch) z kafki i dopiero wtedy je przetwazac lecz wymaga to odpowiednich ustawien
		 * 
		 * 
		 */
		
		while(true) { 
			logger.info("Polling start");
			ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100)); // Wyciaganie danych z kafki i zapisujemy w kontenerze
			int recivedCount = records.count();
			logger.info("Recived: " + recivedCount); // Ilosc rekordow ktora dostajemy z kazdego poll() -- Przydatne podczas ustawien konfiguracji konsumera

			BulkRequest bulkRequest = new BulkRequest();

			for(ConsumerRecord<String, String> record : records) { // Pojedynczo podajemy rekord z ConsumerRecords do elasticsearch

				/*
				 * Jesli nie mozemy znlezsc id to mozemy uzyc danych kafki jako id
				 * String tweetId = record.topic() + record.partition() + record.offset(); 
				 */

				// Wyciagamy id Tweetu co pozwoli stworzyc indepotentnego konsumera
				try {
				String tweetId = extractIdFromTweet(record.value());

				// Zapisywanie wyciagnietych danych z ConsumerRecords w Stringu
				String jsonString = record.value(); 
				
				IndexRequest indexRequest = new IndexRequest("twitter") //Tworze index do Elasticsearch z wartoscia z ConsumerRecords
						.source(jsonString,XContentType.JSON)
						/* 
						 * WAZNE !!! Bez tego jest mozliwosc wyslania duplikatow WAZNE !!!
						 * 
						 * Id pozwalajace uzyc At Least Once bez wysylania duplikatow do ElasticSearch.
						 * Elasticsearch nadaje to id do indexu i uzywa go do sprawdzenia czy juz istnieje
						 * 
						 */
						.id(tweetId);

				// Dodajemy kazdy IndexRequest do BulkRequest. --Lepiej wyslac cos spakowanego niz pojedynczo--
				logger.info("Addind indices to BulkRequest...");
				bulkRequest.add(indexRequest); 	
				} catch (Exception e) {
					logger.warn("Skiping bad data - no id" + record.value());
				}
				
				/*
				 * Powolne dodawanie do ElasticSearch kazdego indexu z osobna
				 * 
				  try {
					Thread.sleep(1000); //Opuznienie dla lepszej widocznosci
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				  try {
					 IndexResponse  indexResponse = client.index(indexRequest, RequestOptions.DEFAULT); // Tworze odpowiedz na zapytany index
					  String id = indexResponse.getId(); // Odpowidz z ElasticSearch
					  logger.info("Id: " + id);
				  } catch (IOException e) {
					  e.printStackTrace();
				  }
				 */

			}
			// Wysylam spakowane indeksy w BulkRequest
			if(recivedCount!=0) {
				try {
					logger.info("Sending BulkRequest to ElasticSearch... "  + records.count());
					BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
					logger.info("BulkRequest send done.");
					logger.info("Bulk send took: " + bulkResponse.getTook());
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}

			logger.info("Commiting offsets");
			consumer.commitSync(); // ENABLE_AUTO_COMMIT_CONFIG jest ustawione na false wiec musimy manulanie commitowac nasz offset po przerobieniu danych
			logger.info("Offset has ben commited");

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}
}
