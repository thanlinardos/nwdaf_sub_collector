package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus;

import org.springframework.context.ApplicationEvent;

import lombok.Getter;

@Getter
public class KafkaDataCollectionEvent extends ApplicationEvent{
    private String message;

    public KafkaDataCollectionEvent(Object source, String msg){
        super(source);
        this.message = msg;
    }
}