package com.pawis.kafka_streams_twitter_filter;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import com.google.gson.JsonParser;

public class StreamsFilterTweets {
	
	public static int extractUserFollowersCount(String jsonTweets) {
		try {
		 return JsonParser.parseString(jsonTweets)
				.getAsJsonObject()
				.get("user")
				.getAsJsonObject()
				.get("followers_count")
				.getAsInt();
		} catch (NullPointerException e) {
			return 0;
		}
	}

	public static void main(String[] args) {
		
		
		/*
		 * KafkaStreams moze przetwarzac dane z Kafki do innego tematu Kafki z zachowaniem Exactly Once oraz z duza wydajnoscia
		 * 
		 */
		
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
		
		//Jak klucze i wartosci maja zostac zserializowane
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class.getName());
		
		// Budowanie strumienia
		StreamsBuilder streamBuilder = new StreamsBuilder();
		
		// z jakiego tematu maja byc pobierane dane
		KStream<String,String> inputTopic = streamBuilder.stream("Twets");
		
		// co sie z danymi ma dziac, w tym przypadku filtrowanie
		KStream<String,String> filteredStream = inputTopic.filter(
				
				(k,jsonTweets) -> extractUserFollowersCount(jsonTweets) > 10000);
		
		// do jakiego tematu dane maja zostac wyslane
		filteredStream.to("important_tweets");
		
		// Tworzenie strumienia z buildera i wlasciwosci 
		KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder.build(),properties);
		
		//streamowanie
		kafkaStreams.start();
				
	}
}
