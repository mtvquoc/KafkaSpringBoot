package com.example.KafkaSpringBoot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "abc", groupId = "g1")
    public void consume(String message) throws InterruptedException {
        logger.info("Waiting....");
        Thread.sleep(30000);
        logger.info(String.format("%s", message));
    }
}
