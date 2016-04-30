package com.nikapp.file.tests;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import com.nikapp.service.data.distributor.kafka.partitioner.FlatFileParitioner;

public class FileParitionerTest {
	private final String configPropertiesTestFilePath = "src/configTest.properties";
	private Map<String,String> configPropertiesMapOrdered = new HashMap<String,String>(){
		{ 
			put("https://www.amazon.com","1");
			put("https://www.google.com","2");
			put("https://www.gmail.com","3");
		}
	};
	private Map<String,String> configPropertiesMapInOrdered = new HashMap<String,String>(){
		{ 
			put("https://www.amazon.com","3");
			put("https://www.google.com","1");
			put("https://www.google.com","12");
		}
	};

	@Before
	public void setup() throws IOException {
		try {
			Properties properties = new Properties();
			File propertiesFile = new File(configPropertiesTestFilePath);
			propertiesFile.createNewFile();
			OutputStream output = new FileOutputStream(propertiesFile);
			properties.put("https://www.amazon.com","1");
			properties.put("https://www.google.com", "2");
			properties.put("https://www.gmail.com", "3");
			properties.store(output, null);
			output.close();
		}
		catch(Exception e) {
			System.out.println(e);
		}
	} 

	@Test 
	public void testFileParitionerMapper() throws IOException {
		FlatFileParitioner<String> flatFileParitionerInstance = new FlatFileParitioner();
		File file = new File(configPropertiesTestFilePath);
		System.out.println(file.getAbsolutePath());
		Map<Object, Object> configPropertiesMapper = flatFileParitionerInstance.loadPropertiesIntoMap(configPropertiesTestFilePath);
		assertEquals(configPropertiesMapper.size(), configPropertiesMapOrdered.size());
		assertEquals(configPropertiesMapper.entrySet(), configPropertiesMapOrdered.entrySet());
		assertEquals(configPropertiesMapper.keySet(), configPropertiesMapOrdered.keySet());
		configPropertiesMapOrdered.forEach((key,value) -> 	assertEquals(configPropertiesMapper.get(key), configPropertiesMapOrdered.get(key)));
		configPropertiesMapInOrdered.forEach((key,value) -> 	assertNotEquals(configPropertiesMapper.get(key), configPropertiesMapInOrdered.get(key)));	
	}
}