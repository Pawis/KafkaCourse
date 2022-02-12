package com.pawis.kafkatutorial.kafka_course.tutorial1;

import java.util.Iterator;
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

public class ProducerDemoWithKeys {

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

		String bootstrapserver = "localhost:9092";

		Properties properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapserver);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		for(int i = 0;i<10;i++) {
			
			String topic = "second";
			String value = "hello world " + Integer.toString(i);			
			String id_= "id_" +Integer.toString(i);
			ProducerRecord<String, String> preducerRecord = new  ProducerRecord<String, String>(topic,id_,value);
			logger.info("Key: "  + id_);
			producer.send(preducerRecord ,new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception == null) {
						logger.info("Recived new metadata: \n"+
								"Topic: "+ metadata.topic() + "\n" + 
								"Partition: "+ metadata.partition() + "\n" + 
								"Offset: "+ metadata.offset() + "\n" + 
								"Timestamp: "+ metadata.timestamp());
					} else {
						logger.error("Error while producing " + exception);
					}

				}
			});
		}
		producer.close(); 
	}
}
