package com.g;

import static com.g.KafkaConsumer.EXAMPLE_TOPIC;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://localhost:9092", "port=9092" })
class EmbeddedKafkaIntegrationTest {

    @Autowired
    public KafkaTemplate<String, String> template;

    @Test
    public void sendMessageToKafka() {
        template.send(EXAMPLE_TOPIC, "Sending a random message");
        waitForSeconds(100);
    }

    @SneakyThrows
    private void waitForSeconds(int n) {
        for (int i = 0; i < n; i++) {
            int j = 0;
            Thread.sleep(1000);
        }
    }
}

