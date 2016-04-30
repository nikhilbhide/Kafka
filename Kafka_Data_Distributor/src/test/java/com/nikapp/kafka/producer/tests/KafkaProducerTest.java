package com.nikapp.kafka.producer.tests;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import com.nikapp.service.data.distributor.kafka.producer.KafkaDataProducer;
import com.nikapp.service.data.distributor.kafka.topic.TopicMapper;

public class KafkaProducerTest {
	private Properties props = new Properties();
	private KafkaConsumer<String, String> consumer;
	@Before
	public void setUp() {		
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("autooffset.reset", "smallest");
		consumer = new KafkaConsumer<>(props);
	}
	@Test
	public void testKafkaProducer() throws IOException, InterruptedException{
		TopicMapper.getInstace().loadPropertiesIntoMap("src/main/resources/topic_mapping.properties");
		KafkaDataProducer kafkaDataProducer = new KafkaDataProducer();
		kafkaDataProducer.main(null);
		consumer.subscribe(Arrays.asList("METRIC"));
		TopicPartition partition0 = new TopicPartition("METRIC", 0);
		consumer.seekToBeginning(partition0);
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records)
				System.out.printf("\noffset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
		}
	}
}