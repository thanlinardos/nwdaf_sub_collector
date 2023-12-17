package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
    public static Integer no_dataCollectionEventListeners = 0;
    public static final Object dataCollectionLock = new Object();
    public static Boolean startedSendingData = false;
    public static final Object startedSendingDataLock = new Object();
    private static final Logger logger = LoggerFactory.getLogger(KafkaDataCollectionListener.class);
    public static final Object availableOffsetLock = new Object();
    public static List<NwdafEventEnum> supportedEvents = new ArrayList<>(List.of(NwdafEventEnum.NF_LOAD));
    public static List<NwdafEventEnum> activeEvents = new ArrayList<>();
    public static final Object activeEventsLock = new Object();
    public static List<OffsetDateTime> startedCollectingTimes = new ArrayList<>();

    @Value(value = "${nnwdaf-eventsubscription.prometheus_url}")
    String prometheusUrl;

    final KafkaProducer producer;

    final ObjectMapper objectMapper;

    public KafkaDataCollectionListener(KafkaProducer producer, ObjectMapper objectMapper) {
        this.producer = producer;
        this.objectMapper = objectMapper;
        while (startedCollectingTimes.size() < supportedEvents.size()) {
            startedCollectingTimes.add(null);
            activeEvents.add(null);
        }
    }

    @Async
    @EventListener(id = "data")
    public void onApplicationEvent(final KafkaDataCollectionEvent event) {
        if (!start()) {
            return;
        }
        System.out.println("Started sending data for events: " + event.getMessage()
                + " , Supported Events: " + supportedEvents + " , Active events:" + activeEvents);

        while (no_dataCollectionEventListeners > 0) {

            long start, prom_delay, diff, wait_time;
            start = System.nanoTime();
            prom_delay = 0L;
            for (NwdafEventEnum eType : supportedEvents) {
                switch (eType) {
                    case NF_LOAD:
                        if (!synchronizedIsEventInsideActiveList(NwdafEventEnum.NF_LOAD)) {
                            break;
                        }
                        List<NfLoadLevelInformation> nfloadinfos;
                        try {
                            long t = System.nanoTime();
                            nfloadinfos = new PrometheusRequestBuilder().execute(eType, prometheusUrl);
                            prom_delay += (System.nanoTime() - t) / 1000000L;
                        } catch (JsonProcessingException e) {
                            logger.error("Failed to collect data for event: " + eType, e);
                            stop();
                            continue;
                        }
                        if (nfloadinfos == null || nfloadinfos.isEmpty()) {
                            logger.error("Failed to collect data for event: " + eType);
                            stop();
                            continue;
                        } else {
                            for (int j = 0; j < nfloadinfos.size(); j++) {
                                try {
                                    // System.out.println("nfloadinfo"+j+": "+nfloadinfos.get(j));
                                    producer.sendMessage(objectMapper.writeValueAsString(nfloadinfos.get(j)), eType.toString());
                                    if (j == 0) {
                                        logger.info("collector sent nfload with time:" + nfloadinfos.get(j).getTimeStamp());
                                    }
                                    if (!startedSendingData) {
                                        startSending(activeEvents.indexOf(eType));
                                    }
                                } catch (Exception e) {
                                    logger.error("Failed to send nfloadlevelinfo to kafka", e);
                                    stop();
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
                    stop();
                    continue;
                }
            }
            logger.info("prom request delay = " + prom_delay + "ms");
            logger.info("data coll total delay = " + diff + "ms");
        }
        logger.info("Prometheus Data Collection stopped!");
    }

    public static boolean start() {
        synchronized (dataCollectionLock) {
            if (no_dataCollectionEventListeners < 1) {
                no_dataCollectionEventListeners++;
                logger.info("producing dummy data to send to kafka...");
                return true;
            }
        }
        return false;
    }

    private static void stop() {
        synchronized (dataCollectionLock) {
            if (no_dataCollectionEventListeners > 0) {
                no_dataCollectionEventListeners--;
            }
        }
        synchronized (startedSendingDataLock) {
            startedSendingData = false;
        }
        synchronized (availableOffsetLock) {
            for (int i = 0; i < supportedEvents.size(); i++) {
                startedCollectingTimes.set(i, null);
            }
        }
    }

    public static void startSending(int i) {
        synchronized (startedSendingDataLock) {
            startedSendingData = true;
        }
        synchronized (availableOffsetLock) {
            startedCollectingTimes.set(i, OffsetDateTime.now());
        }
    }

    public static void stopSending(int i) {
        synchronized (startedSendingDataLock) {
            startedSendingData = false;
        }
        synchronized (availableOffsetLock) {
            startedCollectingTimes.set(i, null);
            activeEvents.set(i, null);
        }
    }

    public static boolean synchronizedIsEventInsideActiveList(NwdafEventEnum event) {
        synchronized (activeEventsLock) {
            if (activeEvents.contains(event)) {
                return true;
            }
        }
        return false;
    }
}
