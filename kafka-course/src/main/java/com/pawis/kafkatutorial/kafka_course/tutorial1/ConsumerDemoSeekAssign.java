package com.pawis.kafkatutorial.kafka_course.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoSeekAssign {

	public static void main(String[] args) {

		Logger logger = LoggerFactory.getLogger(ConsumerDemoSeekAssign.class.getName());

		String bootstrapServer = "localhost:9092";
		String keyDeserializer = StringDeserializer.class.getName();
		String valueDeserializer = StringDeserializer.class.getName();
		String topic = "second";

		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

		TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
		consumer.assign(Arrays.asList(partitionToReadFrom));

		consumer.seek(partitionToReadFrom, 20);

		int numberOfMessageToRead =5;
		int numberOfMessageReadSoFar =0;
		boolean keepOnReading = true;

		while(keepOnReading) {

			ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100)); 

			for(ConsumerRecord<String, String> record : records) {
				numberOfMessageReadSoFar += 1;
				logger.info("Key: " + record.key() + "Value: " + record.value());
				logger.info("Partition: " + record.partition() + " Offset: " + record.offset());
				if(numberOfMessageReadSoFar >= numberOfMessageToRead) {
					keepOnReading=false;
					break;
				}
			}


		}
		logger.info("Exiting the app");
		consumer.close();
	}

}
