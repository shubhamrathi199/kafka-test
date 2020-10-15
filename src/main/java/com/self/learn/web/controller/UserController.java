package com.self.learn.web.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.self.learn.web.model.User;

@RestController
@RequestMapping("/kafka")
public class UserController {

	public static final String MY_TOPIC = "my_kafka_topic_1";

	@Autowired
	KafkaTemplate<String, User> kafkaTemplate;

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate2;

	@GetMapping("/publish/{message}")
	public String postMsg(@PathVariable("message") final String name) {
		kafkaTemplate.send(MY_TOPIC, new User(name, 11));

		return "published successfully";
	}

	@GetMapping("/publish2/{message}")
	public String postMsg2(@PathVariable("message") final String message) {
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate2.send("abc_topic_2", message);

		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
			@Override
			public void onSuccess(SendResult<String, String> result) {
				System.out.println("Message [{}] delivered with offset {}" + message + " " + result.getRecordMetadata().offset());
			}

			@Override
			public void onFailure(Throwable ex) {
				System.out.println("Unable to deliver message [{}]. {}" + message + " " + ex.getMessage());
			}
		});

		return "published successfully on 2";
	}
	
	@GetMapping("/publish3/{message}")
	public String postMsg3(@PathVariable("message") final String message) {
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate2.send("abc_topic_2", 0, "test_key", message);

		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
			@Override
			public void onSuccess(SendResult<String, String> result) {
				System.out.println("Message [{}] delivered with offset {}" + message + " " + result.getRecordMetadata().offset());
			}

			@Override
			public void onFailure(Throwable ex) {
				System.out.println("Unable to deliver message [{}]. {}" + message + " " + ex.getMessage());
			}
		});

		return "published successfully on 3";
	}
	
	@GetMapping("/publish4/{message}")
	public String postMsg4(@PathVariable("message") final String message) {
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate2.send("abc_topic_2", 1, "test_key", message);

		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
			@Override
			public void onSuccess(SendResult<String, String> result) {
				System.out.println("Message [{}] delivered with offset {}" + message + " " + result.getRecordMetadata().offset());
			}

			@Override
			public void onFailure(Throwable ex) {
				System.out.println("Unable to deliver message [{}]. {}" + message + " " + ex.getMessage());
			}
		});

		return "published successfully on 4";
	}

}
