package com.g;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Configuration
public class KafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumer.class);

    public static final String EXAMPLE_TOPIC = "example-topic";
    public static final String EXAMPLE_GROUP_ID = "example-group-id";

    @KafkaListener(
        topics = EXAMPLE_TOPIC,
        concurrency = "1",
        groupId = EXAMPLE_GROUP_ID,
        containerFactory = "kafkaPlainStringListenerContainerFactory"
    )
    public void receive(
        String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition, @Header(KafkaHeaders.OFFSET) long offset,
        @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp, Acknowledgment acknowledgment
    ) {
        LOGGER.info("Received message: {} on topic: {} partition: {} offset: {} at time: {}", message, topic, partition,
            offset, timestamp);
        acknowledgment.acknowledge();
    }
}