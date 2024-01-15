package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.nef;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nwdaf.eventsubscription.model.NwdafEvent;
import io.nwdaf.eventsubscription.model.UeMobility;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.KafkaProducer;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.DataListenerSignals;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus.KafkaDataCollectionListener;
import io.nwdaf.eventsubscription.requestbuilders.NefRequestBuilder;
import io.nwdaf.eventsubscription.utilities.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
public class NefDataCollectionListener {
    public static DataListenerSignals nefDataListenerSignals = new DataListenerSignals(
            Arrays.asList(NwdafEvent.NwdafEventEnum.UE_MOBILITY),
            LoggerFactory.getLogger(KafkaDataCollectionListener.class));

    @Value(value = "${nnwdaf-eventsubscription.nef_url}")
    String nefUrl;

    @Value(value = "${nnwdaf-eventsubscription.nef_state_ues}")
    String nefStateUes;

    @Value(value = "${nnwdaf-eventsubscription.nef_token}")
    String token;

    @Value("${trust.store}")
    private Resource trustStore;

    final KafkaProducer producer;
    final ObjectMapper objectMapper;
    private static final Logger logger = LoggerFactory.getLogger(KafkaDataCollectionListener.class);

    public NefDataCollectionListener(KafkaProducer producer, ObjectMapper objectMapper) {
        this.producer = producer;
        this.objectMapper = objectMapper;
        for (NwdafEvent.NwdafEventEnum eType : nefDataListenerSignals.getSupportedEvents()) {
            nefDataListenerSignals.getEventProducerStartedSending().put(eType, false);
            nefDataListenerSignals.getEventProducerIsActive().put(eType, false);
            nefDataListenerSignals.getEventStartedCollectingTimes().put(eType, OffsetDateTime.MIN);
            nefDataListenerSignals.getEventProducerCounters().put(eType, 0L);
        }
    }

    @Async
    @EventListener(id = "nef")
    public void onApplicationEvent(final NefDataCollectionEvent event) {

        if (!nefDataListenerSignals.getSupportedEvents().containsAll(event.getMessage())) {

            System.out.println("Data Producer doesn't support one of the following events: " + event.getMessage() +
                    " , Supported Events: " + nefDataListenerSignals.getSupportedEvents() +
                    " , Active events:" + nefDataListenerSignals.getEventProducerStartedSending().entrySet().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey).toList());
            return;
        }
        for (NwdafEvent.NwdafEventEnum e : event.getMessage()) {
            nefDataListenerSignals.activate(e);
        }
        if (!nefDataListenerSignals.start()) {
            return;
        }
        System.out.println("Started sending data for events: " + event.getMessage()
                + " , Supported Events: " + nefDataListenerSignals.getSupportedEvents()
                + " , Active events:" + nefDataListenerSignals.getEventProducerStartedSending().entrySet().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey).toList());

        NefRequestBuilder nefRequestBuilder = new NefRequestBuilder(token, trustStore);

        if (nefRequestBuilder.getTemplate() == null) {
            nefDataListenerSignals.stop();
        }

        while (nefDataListenerSignals.getNoDataListener().get() > 0) {

            long start, nef_delay, diff, wait_time;
            start = System.nanoTime();
            nef_delay = 0L;
            for (NwdafEvent.NwdafEventEnum eType : nefDataListenerSignals.getSupportedEvents()) {
                switch (eType) {
                    case UE_MOBILITY:
                        if (!nefDataListenerSignals.getEventProducerIsActive().get(eType)) {
                            break;
                        }
                        List<UeMobility> ueMobilities;
                        try {
                            long t = System.nanoTime();
                            ueMobilities = nefRequestBuilder.execute(eType, nefUrl + nefStateUes, objectMapper);
                            nef_delay += (System.nanoTime() - t) / 1000000L;
                        } catch (JsonProcessingException e) {
                            logger.error("Failed to collect nef data for event: " + eType, e);
                            nefDataListenerSignals.stop();
                            continue;
                        } catch (RestClientException e) {
                            logger.error("Failed to connect to nef for event: " + eType, e);
                            nefDataListenerSignals.stop();
                            continue;
                        }
                        if (ueMobilities == null || ueMobilities.isEmpty()) {
                            logger.error("Failed to collect nef data for event: " + eType);
                            nefDataListenerSignals.stop();
                            continue;
                        }
                        for (int j = 0; j < ueMobilities.size(); j++) {
                            try {
                                // System.out.println("uemobility info "+j+": "+ueMobilities.get(j));
                                producer.sendMessage(objectMapper.writeValueAsString(ueMobilities.get(j)), eType.toString());
                                if (j == 0) {
                                    logger.info("collector sent uemobility with time:" + ueMobilities.get(j).getTs());
                                }
                                nefDataListenerSignals.startSending(eType);
                            } catch (Exception e) {
                                logger.error("Failed to send uemobility info to kafka", e);
                                nefDataListenerSignals.stop();
                                continue;
                            }
                        }
                        break;
                    default:
                        break;
                }
            }
            diff = (System.nanoTime() - start) / 1000000L;
            wait_time = (long) Constants.MIN_PERIOD_SECONDS * 1000L;
            if (diff < wait_time) {
                try {
                    Thread.sleep(wait_time - diff);
                } catch (InterruptedException e) {
                    logger.error("Failed to wait for timeout", e);
                    nefDataListenerSignals.stop();
                    continue;
                }
            }
            logger.info("NEF request delay = " + nef_delay + "ms");
            logger.info("data coll total delay = " + diff + "ms");
        }
        logger.info("NEF Data Collection stopped!");
    }
}
