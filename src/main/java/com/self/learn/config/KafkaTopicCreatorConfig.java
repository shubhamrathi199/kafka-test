package com.self.learn.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicCreatorConfig {
	
	@Value("${kafka.server}")
	public String kafkaServer;
	
	@Bean
	public KafkaAdmin admin() {
	    Map<String, Object> configs = new HashMap<>();
	    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
	    return new KafkaAdmin(configs);
	}
	
	@Bean
	public NewTopic topic1() {
	    return TopicBuilder.name("abc_topic_1")
	            .partitions(1)
	            .replicas(1)
	            .build();
	}
	
	@Bean
	public NewTopic topic2() {
	    return TopicBuilder.name("abc_topic_2")
	            .partitions(2)
	            .replicas(1)
	            .build();
	}
	
}
