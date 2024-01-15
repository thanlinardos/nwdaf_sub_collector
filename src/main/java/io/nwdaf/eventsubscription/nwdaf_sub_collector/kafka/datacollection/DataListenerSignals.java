package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection;

import io.nwdaf.eventsubscription.model.NwdafEvent;
import lombok.Getter;
import lombok.Setter;

import org.slf4j.Logger;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Getter @Setter
public class DataListenerSignals {
    private AtomicInteger noDataListener = new AtomicInteger(0);
    private List<NwdafEvent.NwdafEventEnum> supportedEvents = new ArrayList<>();
    private ConcurrentHashMap<NwdafEvent.NwdafEventEnum, Boolean> eventProducerIsActive = new ConcurrentHashMap<>();
    private ConcurrentHashMap<NwdafEvent.NwdafEventEnum, Boolean> eventProducerStartedSending = new ConcurrentHashMap<>();
    private ConcurrentHashMap<NwdafEvent.NwdafEventEnum, OffsetDateTime> eventStartedCollectingTimes = new ConcurrentHashMap<>();
    private ConcurrentHashMap<NwdafEvent.NwdafEventEnum, Long> eventProducerCounters = new ConcurrentHashMap<>();
    private OffsetDateTime startTime;
    private OffsetDateTime endTime;
    private final Logger logger;

    public DataListenerSignals(List<NwdafEvent.NwdafEventEnum> supportedEvents, Logger logger) {
        this.supportedEvents.addAll(supportedEvents);
        this.logger = logger;
    }

    public boolean start() {
        if (noDataListener.get() < 1) {
            noDataListener.incrementAndGet();
            this.startTime = OffsetDateTime.now();
            logger.info("producing dummy data to send to kafka...");
            return true;
        }
        return false;
    }

    public void stop() {
        if (noDataListener.get() > 0) {
            noDataListener.decrementAndGet();
            this.endTime = OffsetDateTime.now();
        }
        for (NwdafEvent.NwdafEventEnum eType : getSupportedEvents()) {
            deactivate(eType);
        }
    }

    public void startSending(NwdafEvent.NwdafEventEnum eventTopic) {
        if (!eventProducerStartedSending.get(eventTopic)) {
           eventProducerStartedSending.compute(eventTopic, (k, v) -> true);
            eventStartedCollectingTimes.put(eventTopic, OffsetDateTime.now());
            eventProducerCounters.compute(eventTopic, (k, v) -> 0L);
        }
    }

    public void stopSending(NwdafEvent.NwdafEventEnum eventTopic) {
        if (eventProducerStartedSending.get(eventTopic)) {
           eventProducerStartedSending.compute(eventTopic, (k, v) -> false);
            eventStartedCollectingTimes.put(eventTopic, OffsetDateTime.MIN);
        }
    }

    public void activate(NwdafEvent.NwdafEventEnum eventTopic) {
        if (!eventProducerIsActive.get(eventTopic)) {
           eventProducerIsActive.compute(eventTopic, (k, v) -> true);
        }
    }

    public void deactivate(NwdafEvent.NwdafEventEnum eventTopic) {
        if (eventProducerIsActive.get(eventTopic)) {
           eventProducerIsActive.compute(eventTopic, (k, v) -> false);
            stopSending(eventTopic);
        }
    }
}
