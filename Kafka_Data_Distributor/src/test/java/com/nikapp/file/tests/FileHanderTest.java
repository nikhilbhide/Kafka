package com.nikapp.file.tests;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.nikapp.service.files.FileHandler;

public class FileHanderTest {
private List<String> fileContentList = Arrays.asList("Exception:This is test exception","This is log line","This is line 3"); 

	@Before
	public void setup() throws IOException {
		String fileContent = this.fileContentList.stream()
								.collect(Collectors.joining("\n"));
		Files.write(Paths.get("src/testFile.txt"), fileContent.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING );
	}
	
	@Test
	public void testFileReader() throws URISyntaxException{
		FileHandler<String> handler = new FileHandler();
		File file = new File("src/testFile.txt");
		List<String> actualFileContentList = handler.readFile("src/testFile.txt");
		assertEquals(fileContentList, actualFileContentList);
	}
	
	@After
	public void tearDown() throws IOException {
		Files.delete(Paths.get("src/testFile.txt"));
	}
}
