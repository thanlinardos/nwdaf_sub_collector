package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy;

import io.nwdaf.eventsubscription.model.NwdafEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaDummyDataPublisher {
    private final ApplicationEventPublisher eventPublisher;

	KafkaDummyDataPublisher(ApplicationEventPublisher publisher) {
        this.eventPublisher = publisher;
    }

    public void publishDataCollection(final List<NwdafEvent.NwdafEventEnum> message) {
        final KafkaDummyDataEvent e = new KafkaDummyDataEvent(this, message);
        eventPublisher.publishEvent(e);
    }
}
