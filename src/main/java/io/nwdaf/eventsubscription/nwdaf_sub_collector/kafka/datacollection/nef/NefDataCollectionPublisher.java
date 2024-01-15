package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.nef;

import io.nwdaf.eventsubscription.model.NwdafEvent;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus.KafkaDataCollectionEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.List;


@Component
public class NefDataCollectionPublisher {
	private final ApplicationEventPublisher eventPublisher;

	NefDataCollectionPublisher(ApplicationEventPublisher publisher) {
        this.eventPublisher = publisher;
    }

    public void publishDataCollection(final List<NwdafEvent.NwdafEventEnum> message) {
        final NefDataCollectionEvent e = new NefDataCollectionEvent(this, message);
        eventPublisher.publishEvent(e);
    }
}
