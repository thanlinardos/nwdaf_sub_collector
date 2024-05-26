package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.nwdaf.eventsubscription.model.NwdafEvent;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.DataListenerSignals;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy.KafkaDummyDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.nwdaf.eventsubscription.utilities.Constants;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.KafkaProducer;
import io.nwdaf.eventsubscription.model.NfLoadLevelInformation;
import io.nwdaf.eventsubscription.model.NwdafEvent.NwdafEventEnum;
import io.nwdaf.eventsubscription.requestbuilders.PrometheusRequestBuilder;

@Component
public class KafkaDataCollectionListener {
    public static DataListenerSignals dataListenerSignals = new DataListenerSignals(
            List.of(NwdafEventEnum.NF_LOAD),
            LoggerFactory.getLogger(KafkaDataCollectionListener.class));

    @Value(value = "${nnwdaf-eventsubscription.prometheus_url}")
    String prometheusUrl;

    final KafkaProducer producer;

    final ObjectMapper objectMapper;
    private static final Logger logger = LoggerFactory.getLogger(KafkaDataCollectionListener.class);

    public KafkaDataCollectionListener(KafkaProducer producer, ObjectMapper objectMapper) {
        this.producer = producer;
        this.objectMapper = objectMapper;
        for (NwdafEventEnum eType : dataListenerSignals.getSupportedEvents()) {
            dataListenerSignals.getEventProducerStartedSending().put(eType, false);
            dataListenerSignals.getEventProducerIsActive().put(eType, false);
            dataListenerSignals.getEventStartedCollectingTimes().put(eType, OffsetDateTime.MIN);
            dataListenerSignals.getEventProducerCounters().put(eType, 0L);
        }
    }

    @Async
    @EventListener(id = "data")
    public void onApplicationEvent(final KafkaDataCollectionEvent event) {

        if (!dataListenerSignals.getSupportedEvents().containsAll(event.getMessage())) {

            System.out.println("Data Producer doesn't support one of the following events: " + event.getMessage() +
                    " , Supported Events: " + dataListenerSignals.getSupportedEvents() +
                    " , Active events:" + dataListenerSignals.getEventProducerStartedSending().entrySet().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey).toList());
            return;
        }
        for (NwdafEventEnum e : event.getMessage()) {
            dataListenerSignals.activate(e);
        }
        if (!dataListenerSignals.start()) {
            return;
        }
        System.out.println("Started sending data for events: " + event.getMessage()
                + " , Supported Events: " + dataListenerSignals.getSupportedEvents()
                + " , Active events:" + dataListenerSignals.getEventProducerStartedSending().entrySet().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey).toList());

        PrometheusRequestBuilder prometheusRequestBuilder = new PrometheusRequestBuilder();

        while (dataListenerSignals.getNoDataListener().get() > 0) {

            long start, prom_delay, diff, wait_time;
            start = System.nanoTime();
            prom_delay = 0L;
            for (NwdafEventEnum eType : dataListenerSignals.getSupportedEvents()) {
                switch (eType) {
                    case NF_LOAD:
                        if (!dataListenerSignals.getEventProducerIsActive().get(eType)) {
                            break;
                        }
                        List<NfLoadLevelInformation> nfloadinfos;
                        try {
                            long t = System.nanoTime();
                            nfloadinfos = prometheusRequestBuilder.execute(eType, prometheusUrl);
                            prom_delay += (System.nanoTime() - t) / 1000000L;
                        } catch (JsonProcessingException e) {
                            logger.error("Failed to collect data for event: {}", eType, e);
                            dataListenerSignals.stop();
                            continue;
                        }
                        if (nfloadinfos == null || nfloadinfos.isEmpty()) {
                            logger.error("Failed to collect data for event: {}", eType);
                            dataListenerSignals.stop();
                            continue;
                        } else {
                            for (int j = 0; j < nfloadinfos.size(); j++) {
                                try {
                                    producer.sendMessage(objectMapper.writeValueAsString(nfloadinfos.get(j)), eType.toString());
                                    if (j == 0) {
                                        logger.info("collector sent nfload with time:{}", nfloadinfos.get(j).getTimeStamp());
                                    }
                                    dataListenerSignals.startSending(eType);
                                } catch (Exception e) {
                                    logger.error("Failed to send nfloadlevelinfo to kafka", e);
                                    dataListenerSignals.stop();
                                    continue;
                                }
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
                    dataListenerSignals.stop();
                    continue;
                }
            }
            logger.info("prom request delay = " + prom_delay + "ms");
            logger.info("data coll total delay = " + diff + "ms");
        }
        logger.info("Prometheus Data Collection stopped!");
    }
}
