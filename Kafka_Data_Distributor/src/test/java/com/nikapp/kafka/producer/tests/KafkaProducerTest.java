package com.nikapp.kafka.producer.tests;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.FetchRequest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import com.nikapp.service.data.distributor.kafka.producer.KafkaDataProducer;
import com.nikapp.service.data.distributor.kafka.producer.ProducerCallBack;
import com.nikapp.service.data.distributor.kafka.topic.TopicMapper;

public class KafkaProducerTest {
	private static Properties props = new Properties();
	private static KafkaConsumer<String, String> consumer;
	private static final String testFilePath = "src/test/resources/metric_data.txt";
	@BeforeClass
	public static void setUp() throws IOException {		
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "earliest");
		consumer = new KafkaConsumer<>(props);
		String messageToWrite = "Google:date-6/05/2016;,users-3m";
		Files.createFile(Paths.get(testFilePath));
		Files.write(Paths.get(testFilePath), messageToWrite.getBytes());
	}

	@Test
	public void testKafkaProducer() throws IOException, InterruptedException{
		TopicMapper.getInstace().loadPropertiesIntoMap("src/main/resources/topic_mapping.properties");
		KafkaDataProducer kafkaDataProducer = new KafkaDataProducer();
		//consumer.subscribe(Arrays.asList("METRIC"));

		//ConsumerRecords<String, String> records = consumer.poll(1000);
		ProducerCallBack callbackHandler = new ProducerCallBack();
		kafkaDataProducer.produceData(testFilePath,callbackHandler);
		assertEquals("METRIC",callbackHandler.getTopic());
		TopicPartition partition0 = new TopicPartition(callbackHandler.getTopic(), callbackHandler.getPartition());
		consumer.assign(Arrays.asList(partition0));
		consumer.seek(partition0,callbackHandler.getOffset());
		ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
		records.count();
		assertTrue(records.count()==1);
		String messageKey = records.iterator().next().key();
		String messageValue = records.iterator().next().value();
		assertEquals("Google", messageKey);
		assertEquals("date-6/05/2016;,users-3m", messageValue);
	}

	@AfterClass
	public static void tearDown() throws IOException {
		Files.delete(Paths.get(testFilePath));
	}
}