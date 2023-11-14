package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus;

import io.nwdaf.eventsubscription.model.NwdafEvent;
import org.springframework.context.ApplicationEvent;

import lombok.Getter;

import java.util.List;

@Getter
public class KafkaDataCollectionEvent extends ApplicationEvent{
    private final List<NwdafEvent.NwdafEventEnum> message;

    public KafkaDataCollectionEvent(Object source, List<NwdafEvent.NwdafEventEnum> msg){
        super(source);
        this.message = msg;
    }
}