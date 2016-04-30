package com.nikapp.kafka.topic.tests;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import static org.junit.Assert.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.nikapp.service.data.distributor.kafka.topic.TopicMapper;

public class TopicMapperTest {
private List<String> fileContentList = Arrays.asList("LOGGER:LOG","METRICS:METRIC","Exception:ERROR","ERROR:ERROR"); 

	@Before
	public void setup() throws IOException {
		String fileContent = this.fileContentList.stream()
								.collect(Collectors.joining("\n"));
		Files.write(Paths.get("src/testTopicMapper.txt"), fileContent.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING );
	}
	
	@Test
	public void testToppicMapping() throws URISyntaxException, IOException{
		Map<String, String> topicMapping = TopicMapper.getInstace().loadPropertiesIntoMap(("src/testTopicMapper.txt"));
		assertEquals("LOG",topicMapping.get("LOGGER"));
		assertEquals("METRIC",topicMapping.get("METRICS"));
		assertEquals("ERROR",topicMapping.get("Exception"));
		assertEquals("ERROR",topicMapping.get("ERROR"));
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void testTopicMapper() throws IOException{
		Map<String, String> topicMapping = TopicMapper.getInstace().loadPropertiesIntoMap(("src/testTopicMapper.txt"));
		topicMapping.put("new_key", "new_value");
	}
	
	@After
	public void tearDown() throws IOException {
		Files.delete(Paths.get("src/testTopicMapper.txt"));
	}
}