package com.nikapp.service.data.distributor.kafka.partitioner;

import java.io.IOException;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class TextFilePartitioner implements Partitioner {
	private TextFilePartitioner textFileParitioner = new TextFilePartitioner(); 
	private FlatFileParitioner<String> partioner = new FlatFileParitioner<>();
	@Override
	public void configure(Map<String, ?> configs) {
		try {
			partioner.loadPropertiesIntoMap("src/main/resources/configTest.properties");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		return partioner.getParition((String) key);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}
}
