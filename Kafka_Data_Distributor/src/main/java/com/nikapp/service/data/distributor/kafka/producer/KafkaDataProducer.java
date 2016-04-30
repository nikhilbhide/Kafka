package com.nikapp.service.data.distributor.kafka.producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.nikapp.service.data.distributor.kafka.topic.TopicMapper;

/**
 * This class is responsible to publish data on Kafka topic. First of all, it creates kafka producer instance based on  
 *  
 * @author nikhil.bhide
 * @version 1.0
 * @Since 2016-04-22
 */
public class KafkaDataProducer {
	private Producer<String, String> kafkaProducer;

	/**
	 * Creates Kafka Producer based on the properties set.
	 * The properties should be configured in properties file; however, over here properties are hardcoded since it is a demonstration project. 
	 * 
	 */
	private void init (){
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		List<Future<RecordMetadata>> ackList = new ArrayList();
		kafkaProducer = new KafkaProducer<>(props);		
	}
	
	private void produceData() throws InterruptedException {
		ExecutorService executor = Executors.newFixedThreadPool(3);
		CountDownLatch latch = new CountDownLatch(1);
		KafkaDataProducerTask<String, String> kafkaDataProducerTask1 = 
				new KafkaDataProducerTask(latch, "src/main/resources/metric_data.txt", kafkaProducer);
		executor.submit(kafkaDataProducerTask1);
		latch.await();
		kafkaProducer.close();	
	}
	
	
	public static void main(String [] args) throws InterruptedException, IOException {
		TopicMapper.getInstace().loadPropertiesIntoMap("src/main/resources/topic_mapping.properties");
		KafkaDataProducer kafkaDataProducer = new KafkaDataProducer();		
		kafkaDataProducer.init();
		kafkaDataProducer.produceData();
	}
}