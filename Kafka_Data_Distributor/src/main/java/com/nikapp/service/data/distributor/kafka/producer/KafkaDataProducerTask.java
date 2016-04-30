package com.nikapp.service.data.distributor.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.nikapp.service.data.distributor.kafka.topic.TopicMapper;
import com.nikapp.service.files.FileHandler;

public class KafkaDataProducerTask<K,V> implements Callable {
	private CountDownLatch latch;
	private String filePath;
	private Producer<K, V> producer;
	ProducerCallBack callbackHandler = new ProducerCallBack();

	public KafkaDataProducerTask(CountDownLatch latch, String filePath, Producer<K, V> producer) {
		super();
		this.latch = latch;
		this.filePath = filePath;
		this.producer = producer;
	}
	@Override
	public Object call() throws Exception {
		try {
		FileHandler<String> fileHander = new FileHandler();
		List<String> messages = fileHander.readFile(filePath);
		List<Future<RecordMetadata>> ackList = new ArrayList();
		String topicName = TopicMapper.getInstace().getTopicName(fileHander.getFileName());
		messages.parallelStream()
		.forEach(message -> {
			Object messagePart[] = message.split(":");	
			K key = (K) messagePart[0];
			V value = (V) messagePart[1];
			Future<RecordMetadata> ack = producer.send(new ProducerRecord<K, V>(topicName, key, value),callbackHandler);
			ackList.add(ack);});
		latch.countDown();
		return ackList;
		}
		catch (Exception e) {
			System.out.println(e);
			throw new RuntimeException(e);
		}
	}	
}