package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus;

import io.nwdaf.eventsubscription.model.NwdafEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
public class KafkaDataCollectionPublisher {
	private final ApplicationEventPublisher eventPublisher;

	KafkaDataCollectionPublisher(ApplicationEventPublisher publisher) {
        this.eventPublisher = publisher;
    }

    public void publishDataCollection(final List<NwdafEvent.NwdafEventEnum> message) {
        final KafkaDataCollectionEvent e = new KafkaDataCollectionEvent(this, message); 
        eventPublisher.publishEvent(e);
    }
}
