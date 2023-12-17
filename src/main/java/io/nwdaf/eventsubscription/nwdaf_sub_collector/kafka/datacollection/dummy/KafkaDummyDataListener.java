package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.nwdaf.eventsubscription.model.NwdafEvent;
import io.nwdaf.eventsubscription.model.UeCommunication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.nwdaf.eventsubscription.utilities.DummyDataGenerator;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.KafkaProducer;
import io.nwdaf.eventsubscription.model.NfLoadLevelInformation;
import io.nwdaf.eventsubscription.model.NwdafEvent.NwdafEventEnum;
import io.nwdaf.eventsubscription.model.UeMobility;
import io.nwdaf.eventsubscription.utilities.Constants;

import static io.nwdaf.eventsubscription.utilities.ParserUtil.safeParseInteger;

@Component
public class KafkaDummyDataListener {
    public static AtomicInteger no_kafkaDummyDataListeners = new AtomicInteger(0);
    public static List<NwdafEventEnum> supportedEvents = new ArrayList<>(Arrays.asList(NwdafEventEnum.NF_LOAD, NwdafEventEnum.UE_MOBILITY, NwdafEventEnum.UE_COMM));
    public static ConcurrentHashMap<NwdafEventEnum, Boolean> eventProducerIsActive = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<NwdafEventEnum, Boolean> eventProducerStartedSending = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<NwdafEventEnum, OffsetDateTime> eventStartedCollectingTimes = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<NwdafEvent.NwdafEventEnum, Long> eventProducerCounters = new ConcurrentHashMap<>();
    public static OffsetDateTime startTime;
    public static OffsetDateTime endTime;

    public final String no_dummy_nfload;
    public final String no_dummy_uemob;
    public final String no_dummy_uecomm;

    private static final Logger logger = LoggerFactory.getLogger(KafkaDummyDataListener.class);
    private List<NfLoadLevelInformation> nfloadinfos;
    private List<UeMobility> ueMobilities;
    private List<UeCommunication> ueCommunications;
    final Environment env;
    final KafkaProducer producer;
    final ObjectMapper objectMapper;

    public KafkaDummyDataListener(Environment env, KafkaProducer producer, ObjectMapper objectMapper) {

        this.env = env;
        this.producer = producer;
        this.objectMapper = objectMapper;

        for(NwdafEventEnum eType : supportedEvents) {
            eventProducerStartedSending.put(eType, false);
            eventProducerIsActive.put(eType, false);
            eventStartedCollectingTimes.put(eType, OffsetDateTime.MIN);
            eventProducerCounters.put(eType, 0L);
        }

        no_dummy_nfload = env.getProperty("nnwdaf-eventsubscription.no_dummy_nfload");
        no_dummy_uemob = env.getProperty("nnwdaf-eventsubscription.no_dummy_uemob");
        no_dummy_uecomm = env.getProperty("nnwdaf-eventsubscription.no_dummy_uecomm");
    }

    @Async
    @EventListener(id = "dummy")
    public void onApplicationEvent(final KafkaDummyDataEvent event) {

        if (!supportedEvents.containsAll(event.getMessage())) {

            System.out.println("Dummy Data Producer doesn't support one of the following events: " + event.getMessage() +
                    " , Supported Events: " + supportedEvents +
                    " , Active events:" + eventProducerStartedSending.entrySet().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey).toList());
            return;
        }
            for (NwdafEventEnum e : event.getMessage()) {
                activate(e);
            }
        if (!start()) {
            return;
        }

        if (no_kafkaDummyDataListeners.get() > 0) {
            nfloadinfos = DummyDataGenerator.generateDummyNfLoadLevelInfo(safeParseInteger(no_dummy_nfload));
            ueMobilities = DummyDataGenerator.generateDummyUeMobilities(safeParseInteger(no_dummy_uemob));
            ueCommunications = DummyDataGenerator.generateDummyUeCommunications(safeParseInteger(no_dummy_uecomm));
        }
        long start;

        System.out.println("Started sending dummy data for events: " + event.getMessage() +
                " , Supported Events: " + supportedEvents +
                " , Active events:" + eventProducerStartedSending.entrySet().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey).toList());

        while (no_kafkaDummyDataListeners.get() > 0) {
            start = System.nanoTime();
            for (NwdafEventEnum eType : supportedEvents) {
                switch (eType) {
                    case NF_LOAD:
                        if (!eventProducerIsActive.get(NwdafEventEnum.NF_LOAD)) {
                            break;
                        }

                        DummyDataGenerator.changeNfLoadTimeDependentProperties(nfloadinfos);
                        for (int k = 0; k < nfloadinfos.size(); k++) {
                            try {
                                producer.sendMessage(objectMapper.writeValueAsString(nfloadinfos.get(k)), eType.toString());
                                if (k == 0) {
                                    logger.info("collector sent nfload with time:" + nfloadinfos.get(k).getTimeStamp());
                                }
                                startSending(eType);

                            } catch (Exception e) {
                                logger.error("Failed to send dummy nfloadlevelinfo to broker", e);
                                stop();
                                continue;
                            }
                        }
                        break;
                    case UE_MOBILITY:
                        if (!eventProducerIsActive.get(NwdafEventEnum.UE_MOBILITY)) {
                            break;
                        }

                        DummyDataGenerator.changeUeMobilitiesTimeDependentProperties(ueMobilities);
                        for (int k = 0; k < ueMobilities.size(); k++) {
                            try {
                                if (k == 0) {
                                    logger.info("collector sent ue_mobility with time:" + ueMobilities.get(k).getTs());
                                }
                                producer.sendMessage(objectMapper.writeValueAsString(ueMobilities.get(k)), eType.toString());
                                startSending(eType);

                            } catch (Exception e) {
                                logger.error("Failed to send dummy ueMobilities to broker", e);
                                stop();
                                continue;
                            }
                        }
                        break;
                    case UE_COMM:
                        if (!eventProducerIsActive.get(NwdafEventEnum.UE_COMM)) {
                            break;
                        }

                        DummyDataGenerator.changeUeCommunicationsTimeDependentProperties(ueCommunications);
                        for (int k = 0; k < ueCommunications.size(); k++) {
                            try {
                                if (k == 0) {
                                    logger.info("collector sent ue_communication with time:" + ueCommunications.get(k).getTs());
                                }
                                producer.sendMessage(objectMapper.writeValueAsString(ueCommunications.get(k)), eType.toString());
                                startSending(eType);

                            } catch (Exception e) {
                                logger.error("Failed to send dummy ueCommunications to broker", e);
                                stop();
                                continue;
                            }
                        }
                        break;
                    default:
                        break;
                }
            }

            long diff = (System.nanoTime() - start) / 1000000L;
            long wait_time = (long) Constants.MIN_PERIOD_SECONDS * 1000L;
            if (diff < wait_time) {
                try {
                    Thread.sleep(wait_time - diff);
                } catch (InterruptedException e) {
                    logger.error("Failed to wait for timeout", e);
                    stop();
                    continue;
                }
            }
        }
        logger.info("Dummy Data Production stopped!");
    }

    public static boolean start() {
        if (no_kafkaDummyDataListeners.get() < 1) {
            no_kafkaDummyDataListeners.incrementAndGet();
            startTime = OffsetDateTime.now();
            logger.info("producing dummy data to send to kafka...");
            return true;
        }
        return false;
    }

    private static void stop() {
        if (no_kafkaDummyDataListeners.get() > 0) {
            no_kafkaDummyDataListeners.decrementAndGet();
            endTime = OffsetDateTime.now();
        }
        for(NwdafEventEnum eType : supportedEvents) {
            deactivate(eType);
        }
    }

    public static void startSending(NwdafEventEnum eventTopic) {
        if (!eventProducerStartedSending.get(eventTopic)) {
            eventProducerStartedSending.compute(eventTopic, (k, v) -> true);
            eventStartedCollectingTimes.put(eventTopic, OffsetDateTime.now());
            eventProducerCounters.compute(eventTopic, (k, v) -> 0L);
        }
    }

    public static void stopSending(NwdafEventEnum eventTopic) {
        if (eventProducerStartedSending.get(eventTopic)) {
            eventProducerStartedSending.compute(eventTopic, (k, v) -> false);
            eventStartedCollectingTimes.put(eventTopic, OffsetDateTime.MIN);
        }
    }

    public static void activate(NwdafEventEnum eventTopic) {
        if (!eventProducerIsActive.get(eventTopic)) {
            eventProducerIsActive.compute(eventTopic, (k, v) -> true);
        }
    }

    public static void deactivate(NwdafEventEnum eventTopic) {
        if (eventProducerIsActive.get(eventTopic)) {
            eventProducerIsActive.compute(eventTopic, (k, v) -> false);
            stopSending(eventTopic);
        }
    }
}
