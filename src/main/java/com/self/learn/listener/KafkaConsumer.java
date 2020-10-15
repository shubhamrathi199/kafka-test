package com.self.learn.listener;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.self.learn.web.model.User;

@Service
public class KafkaConsumer {

//	private static final String MY_TOPIC = "my_kafka_topic_1";
//
//	@KafkaListener(topics = MY_TOPIC, groupId = "my_group_id")
//	public void consumer(String receivedUser) {
//		System.out.println("__________________________________");
//		System.out.println(receivedUser);
//		System.out.println("__________________________________");
//	}
//
//	@KafkaListener(topics = MY_TOPIC, groupId = "my_group_json", containerFactory = "userKafkaListenerFactory")
//	public void userConsumer(User receivedUser) {
//		System.out.println("!!__________________________________!!");
//		System.out.println(receivedUser);
//		System.out.println("!!__________________________________!!");
//	}

	@KafkaListener(topics = "abc_topic_1")
	public void customTopicConsumer(String receivedUser) {
		System.out.println(">>>>>>>>>>>>");
		System.out.println(receivedUser);
		System.out.println("<<<<<<<<<<<<");
	}

	@KafkaListener(groupId = "my_group_2", topicPartitions = @TopicPartition(topic = "abc_topic_2", partitionOffsets = {
			@PartitionOffset(partition = "0", initialOffset = "0") }))
	public void customTopicConsumer2(
			@Payload String message,
		    @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
		    @Header(KafkaHeaders.OFFSET) int offset) {
		System.out.println("----------");
		System.out.println(message + "  " + partition + "  " + offset);
		System.out.println("----------");
	}
	
	@KafkaListener(groupId = "my_group_2", topicPartitions = @TopicPartition(topic = "abc_topic_2", partitionOffsets = {
			@PartitionOffset(partition = "1", initialOffset = "0") }))
	public void customTopicConsumer3(
			@Payload String message,
		    @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
		    @Header(KafkaHeaders.OFFSET) int offset) {
		System.out.println("----------");
		System.out.println(message + "  " + partition + "  " + offset);
		System.out.println("----------");
	}

}
