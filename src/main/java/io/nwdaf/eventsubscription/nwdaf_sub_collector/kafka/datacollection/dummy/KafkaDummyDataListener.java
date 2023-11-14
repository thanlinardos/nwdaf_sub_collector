package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy;

import java.time.OffsetDateTime;
import java.util.*;

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

@Component
public class KafkaDummyDataListener {
    public static Integer no_kafkaDummyDataListeners = 0;
    public static final Object kafkaDummyDataLock = new Object();
    public static Boolean startedSendingData = false;
    public static final Object startedSendingDataLock = new Object();
    private static final Logger logger = LoggerFactory.getLogger(KafkaDummyDataListener.class);
    private List<NfLoadLevelInformation> nfloadinfos;
    private List<UeMobility> ueMobilities;
    private List<UeCommunication> ueCommunications;
    public static List<NwdafEventEnum> supportedEvents = new ArrayList<>(Arrays.asList(NwdafEventEnum.NF_LOAD, NwdafEventEnum.UE_MOBILITY));
    public static List<NwdafEventEnum> activeEvents = new ArrayList<>();
    public static final Object activeEventsLock = new Object();
    public static List<OffsetDateTime> startedCollectingTimes = new ArrayList<>();
    public static final Object availableOffsetLock = new Object();

    final Environment env;
    final KafkaProducer producer;
    final ObjectMapper objectMapper;

    public KafkaDummyDataListener(Environment env, KafkaProducer producer, ObjectMapper objectMapper) {
        this.env = env;
        this.producer = producer;
        this.objectMapper = objectMapper;
        if (startedCollectingTimes.size() < supportedEvents.size()) {
            for (int i = 0; i < supportedEvents.size(); i++) {
                startedCollectingTimes.add(null);
                activeEvents.add(null);
            }
        }
    }

    @Async
    @EventListener(id = "dummy")
    public void onApplicationEvent(final KafkaDummyDataEvent event) {
        if (!supportedEvents.containsAll(event.getMessage())) {
            System.out.println("Dummy Data Producer doesn't support one of the following events: " + event.getMessage()
                    + " , Supported Events: " + supportedEvents + " , Active events:" + activeEvents);
            return;
        }
        synchronized (activeEventsLock) {
            for (NwdafEventEnum e : event.getMessage()) {
                if (!activeEvents.contains(e)) {
                    activeEvents.set(supportedEvents.indexOf(e), e);
                }
            }
        }
        if (!start()) {
            return;
        }
        if (no_kafkaDummyDataListeners > 0) {
            nfloadinfos = DummyDataGenerator.generateDummyNfLoadLevelInfo(10);
            ueMobilities = DummyDataGenerator.generateDummyUeMobilities(10);
            ueCommunications = DummyDataGenerator.generateDummyUeCommunications(10);
        }
        long start;
        System.out.println("Started sending dummy data for events: " + event.getMessage()
                + " , Supported Events: " + supportedEvents + " , Active events:" + activeEvents);
        while (no_kafkaDummyDataListeners > 0) {
            start = System.nanoTime();
            for (NwdafEventEnum eType : supportedEvents) {
                switch (eType) {
                    case NF_LOAD:
                        if (!synchronizedIsEventInsideActiveList(NwdafEventEnum.NF_LOAD)) {
                            break;
                        }
                        nfloadinfos = DummyDataGenerator.changeNfLoadTimeDependentProperties(nfloadinfos);
                        for (int k = 0; k < nfloadinfos.size(); k++) {
                            try {
                                producer.sendMessage(objectMapper.writeValueAsString(nfloadinfos.get(k)), eType.toString());
                                if (k == 0) {
                                    logger.info("collector sent nfload with time:" + nfloadinfos.get(k).getTimeStamp());
                                }
                                if (!startedSendingData) {
                                    startSending(activeEvents.indexOf(eType));
                                }
                            } catch (Exception e) {
                                logger.error("Failed to send dummy nfloadlevelinfo to broker", e);
                                stop();
                                continue;
                            }
                        }
                        break;
                    case UE_MOBILITY:
                        if (!synchronizedIsEventInsideActiveList(NwdafEventEnum.UE_MOBILITY)) {
                            break;
                        }
                        ueMobilities = DummyDataGenerator.changeUeMobilitiesTimeDependentProperties(ueMobilities);
                        for (int k = 0; k < ueMobilities.size(); k++) {
                            try {
                                if (k == 0) {
                                    logger.info("collector sent ue_mobility with time:" + ueMobilities.get(k).getTs());
                                }
                                producer.sendMessage(objectMapper.writeValueAsString(ueMobilities.get(k)), eType.toString());
                                if (!startedSendingData) {
                                    startSending(activeEvents.indexOf(eType));
                                }
                            } catch (Exception e) {
                                logger.error("Failed to send dummy ueMobilities to broker", e);
                                stop();
                                continue;
                            }
                        }
                        break;
                    case UE_COMM:
                        if (!synchronizedIsEventInsideActiveList(NwdafEventEnum.UE_COMM)) {
                            break;
                        }
                        ueCommunications = DummyDataGenerator.changeUeCommunicationsTimeDependentProperties(ueCommunications);
                        for (int k = 0; k < ueCommunications.size(); k++) {
                            try {
                                if (k == 0) {
                                    logger.info("collector sent ue_communication with time:" + ueCommunications.get(k).getTs());
                                }
                                producer.sendMessage(objectMapper.writeValueAsString(ueCommunications.get(k)), eType.toString());
                                if (!startedSendingData) {
                                    startSending(activeEvents.indexOf(eType));
                                }
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
            long diff = (System.nanoTime() - start) / 1000000l;
            long wait_time = (long) Constants.MIN_PERIOD_SECONDS * 1000l;
            if (diff < wait_time) {
                try {
                    Thread.sleep(wait_time - diff);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    stop();
                    continue;
                }
            }
        }
        logger.info("Dummy Data Production stopped!");
        return;
    }

    public static boolean start() {
        synchronized (kafkaDummyDataLock) {
            if (no_kafkaDummyDataListeners < 1) {
                no_kafkaDummyDataListeners++;
                logger.info("producing dummy data to send to kafka...");
                return true;
            }
        }
        return false;
    }

    private static void stop() {
        synchronized (kafkaDummyDataLock) {
            if (no_kafkaDummyDataListeners > 0) {
                no_kafkaDummyDataListeners--;
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
