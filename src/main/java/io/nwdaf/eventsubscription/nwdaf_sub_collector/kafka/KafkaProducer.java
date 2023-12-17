package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka;

import java.io.IOException;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.Setter;


@Component
@Getter
@Setter
public class KafkaProducer {

    final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendMessage(String msg, String topicName) throws IOException {
        kafkaTemplate.send(topicName, msg);
    }
}
