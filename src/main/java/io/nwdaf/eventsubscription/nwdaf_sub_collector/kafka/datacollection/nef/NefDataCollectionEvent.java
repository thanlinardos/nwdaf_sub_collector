package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.nef;

import io.nwdaf.eventsubscription.model.NwdafEvent;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

import java.util.List;

@Getter
public class NefDataCollectionEvent extends ApplicationEvent {
    private final List<NwdafEvent.NwdafEventEnum> message;

    public NefDataCollectionEvent(Object source, List<NwdafEvent.NwdafEventEnum> msg) {
        super(source);
        this.message = msg;
    }
}