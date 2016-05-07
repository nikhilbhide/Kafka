package com.nikapp.service.data.distributor.kafka.producer;

/**
 * Callback handler for producer. This code gets executed when kakfa producer sends message onto topic.
 * It implements interface {@Callback}
 * 
 * @author nikhil.bhide
 * 
 * 
 * 
 */
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerCallBack implements Callback {	
	private long offset;
	private int partition;
	String topic;
	
	/**
	 * @return the offset
	 */
	public long getOffset() {
		return offset;
	}
	/**
	 * @return the partition
	 */
	public int getPartition() {
		return partition;
	}
	/**
	 * @return the topic
	 */
	public String getTopic() {
		return topic;
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		offset = metadata.offset();
		partition = metadata.partition();
		topic = metadata.topic();
	}
}
