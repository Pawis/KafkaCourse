package com.pawis.kafka_basics;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoGroups {
	
	public static void main(String[] args) {
		
		Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());
		
		String bootstrapServer = "localhost:9092";
		String keyDeserializer = StringDeserializer.class.getName();
		String valueDeserializer = StringDeserializer.class.getName();
		String groupId = "fourth-app";
		String topic = "second";
		
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		consumer.subscribe(Collections.singleton(topic));  // IMPORTANT !!!
		
		while(true) {
			
			ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100)); 
			
			for(ConsumerRecord<String, String> record : records) {
				logger.info("Key: " + record.key() + "Value: " + record.value());
				logger.info("Partition: " + record.partition());
			}
			
		}
	}

}
