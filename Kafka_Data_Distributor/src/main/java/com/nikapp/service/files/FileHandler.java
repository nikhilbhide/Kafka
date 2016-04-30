package com.nikapp.service.files;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class FileHandler<U>{
	private String fileName;

	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}	

	public List<U> readFile(String filePath) throws URISyntaxException {
		List<U> content = new ArrayList();
		Path path = Paths.get(filePath);
		setFileName(path.getFileName().toString());
		try(Stream<U> stream =  (Stream<U>) Files.lines(path)) {
			stream.forEach( s-> {
				content.add(s);
			});
		}catch(IOException exception){
			exception.printStackTrace();
		}
		return content;
	}	
}