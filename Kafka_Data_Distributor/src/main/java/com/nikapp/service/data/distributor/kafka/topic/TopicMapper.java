package com.nikapp.service.data.distributor.kafka.topic;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class TopicMapper {
	private static TopicMapper topicMapper = new TopicMapper();
	private HashMap<String, String> topicMapping = new java.util.HashMap();
	
	private TopicMapper(){
	
	}
	
	public Map<String, String> loadPropertiesIntoMap(String filePath) throws IOException {
		Properties properties = new Properties();
		File file = new File(filePath);
		InputStream input = new FileInputStream(file);
		properties.load(input);
		properties.forEach((Object key, Object value) -> {
			String topicName = (String) value;
			topicMapping.put(key.toString(), topicName);
		});
		return Collections.unmodifiableMap(topicMapping);
	}

	public String getTopicName(String fileName) {
		if(topicMapping.get(fileName)==null) {
			throw new RuntimeException("Topic not found for input file "+fileName);
		}
		return topicMapping.get(fileName);
	}

	public static TopicMapper getInstace() {
		return topicMapper;
	}
}