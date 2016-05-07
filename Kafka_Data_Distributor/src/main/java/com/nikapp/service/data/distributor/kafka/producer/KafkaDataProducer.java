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
 * @since 1.0
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

	/**
	 * Produces data on kafka topic. There are 2 modes of working
	 * 
	 * <h3> default mode - filepath is null </h3>
	 * Default mode demonstrates kafka producer functionality for different use case such as metric, log and user activity.
	 * 
	 * <h4>Metric</h4>
	 * This demonstrates how to use kafka for metric use case
	 * <p>
	 * It is assumed that metric data is from different sites such as facebook, google, gmail, amazon etc.. Metric data can be anything like cpu_performance, disk_performance, response_time etc..
	 * Data is not important here as its the demonstration of kafka producer.
	 * For distribution of data, we have a Topic named as "METRIC". Producer is constantly streaming data onto this topic and each vendor has its own partition.
	 * This is important as this will tremendously increase the scalability of the application.
	 * For example, metric data is read as a stream and using java's parallel stream data is put onto topic.
	 * Each vendor has mapping with partition which is defined in configuration file paritions_mapping.properties.
	 * </p>
	 * 
	 * <h4> Log </h4>
	 * This demonstrates how to use kafka for logging use case
	 * <p>
	 * It is assumed that log data is from different sites such as facebook, google, gmail, amazon etc.. Log data can be data containing exceptions ,errors or other logging information
	 * For distribution of data, we have a Topic named as "LOG". Producer is constantly streaming data onto this topic and each vendor has its own partition.
	 * This is important as this will tremendously increase the scalability of the application.
	 * </p>
	 * 
	 ** <h4> User Activity </h4>
	 * This demonstrates how to use kafka for User Activity
	 * <p>
	 * It is assumed that user activity is from different sites such as facebook, google, gmail, amazon etc.. 
	 * Site activities are published onto topic ACTIVITY with each vendor having its dedicated partition.
	 * </p>
	 * 
	 * <h3> user mode - filepath is provided </h3>
	 * User can provide a file containing data which gets published onto topic.
	 * Optionally, user can also provider callback handler
	 * <p>
	 * 
	 * @param filePath The path of the file from which data is to be read; this is an optional parameter.
	 * @param callback - The kafka producer call back handler; this is an optional parameter.
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void produceData(String filePath, ProducerCallBack callback) throws InterruptedException, IOException {
		TopicMapper.getInstace().loadPropertiesIntoMap("src/main/resources/topic_mapping.properties");
		init();
		ExecutorService executor = Executors.newFixedThreadPool(3);
		CountDownLatch latch = null;
		if(filePath!=null) {
			latch = new CountDownLatch(1);
			if(callback==null)
				callback = new ProducerCallBack();
			KafkaDataProducerTask<String, String> kafkaDataProducerTask = 
					new KafkaDataProducerTask(latch, filePath, kafkaProducer, callback);
			executor.submit(kafkaDataProducerTask);
		} else {
		}
		latch.await();
		kafkaProducer.close();	
	}

	public static void main(String [] args) throws InterruptedException, IOException {
		KafkaDataProducer kafkaDataProducer = new KafkaDataProducer();		
		kafkaDataProducer.produceData(null,null);
	}
}