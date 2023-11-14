package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy;

import io.nwdaf.eventsubscription.model.NwdafEvent;
import org.springframework.context.ApplicationEvent;

import lombok.Getter;

import java.util.List;

@Getter
public class KafkaDummyDataEvent extends ApplicationEvent {
    private final List<NwdafEvent.NwdafEventEnum> message;

    public KafkaDummyDataEvent(Object source, List<NwdafEvent.NwdafEventEnum> msg){
        super(source);
        this.message = msg;
    }
}
