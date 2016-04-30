package com.nikapp.service.data.distributor.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerCallBack implements Callback{

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		System.out.println("This is the metadata - offset : " + metadata.offset() + ", partition : "+metadata.partition() + " and topic :" + metadata.topic());
	}
}
