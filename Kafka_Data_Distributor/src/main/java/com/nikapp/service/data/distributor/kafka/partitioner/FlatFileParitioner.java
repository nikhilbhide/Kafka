package com.nikapp.service.data.distributor.kafka.partitioner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This class partitions message based on the key. Topic is decided by the type of data and partition is decided by the key of the data.
 * @author nik
 * @version 1.0
 * @Since 2016-04-22
 * @param <U>
 */
public class FlatFileParitioner<U> {
	Map<Object, Object> paritionerMapping = new HashMap();
	
	/** This method loads properties into map. Properties are in the form of key and values where key is the paritioning_scheme and value is parition_id. 
	 * Based on the key, data is partitioned when produced by Kafka producer. 
	 */
	public Map<Object, Object> loadPropertiesIntoMap(String filePath) throws IOException {
		Properties properties = new Properties();
		File file = new File(filePath);
		InputStream input = new FileInputStream(file);
		properties.load(input);
		properties.forEach((Object key, Object value) -> {
			Integer paritionId = (Integer) value;
			paritionerMapping.put(key, paritionId);
		});
		return properties;
	}
	
	/**
	 * This method returns key based on which data is partitioned.
	 * 
	 * @param U key - Based on the key, message would be put on the corresponding partition.
	 * @return int parition id
	 */
	public int getParition(U key) {
		return (int) paritionerMapping.get(key);
	}
}