package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy;

import java.io.IOException;
import java.io.InputStream;
import java.lang.Exception;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.type.TypeReference;
import io.nwdaf.eventsubscription.Main;
import io.nwdaf.eventsubscription.model.*;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.DataListenerSignals;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.nwdaf.eventsubscription.utilities.DummyDataGenerator;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.KafkaProducer;
import io.nwdaf.eventsubscription.model.NwdafEvent.NwdafEventEnum;
import io.nwdaf.eventsubscription.utilities.Constants;


@Component
public class KafkaDummyDataListener {
    public static DataListenerSignals dummyDataListenerSignals = new DataListenerSignals(
            Arrays.asList(NwdafEventEnum.NF_LOAD, NwdafEventEnum.UE_MOBILITY, NwdafEventEnum.UE_COMM),
            LoggerFactory.getLogger(KafkaDummyDataListener.class));

    @Value("${nnwdaf-eventsubscription.no_dummy_nfload}")
    public Integer no_dummy_nfload;
    @Value("${nnwdaf-eventsubscription.no_dummy_uemob}")
    public Integer no_dummy_uemob;
    @Value("${nnwdaf-eventsubscription.no_dummy_uecomm}")
    public Integer no_dummy_uecomm;

    private static final Logger logger = LoggerFactory.getLogger(KafkaDummyDataListener.class);
    private List<NfLoadLevelInformation> nfloadinfos;
    private List<UeMobility> ueMobilities;
    private List<UeCommunication> ueCommunications;
    final Environment env;
    final KafkaProducer producer;
    final ObjectMapper objectMapper;

    private UeMobility locationInfoExamples;

    public KafkaDummyDataListener(Environment env, KafkaProducer producer, ObjectMapper objectMapper) {

        this.env = env;
        this.producer = producer;
        this.objectMapper = objectMapper;

        for (NwdafEventEnum eType : dummyDataListenerSignals.getSupportedEvents()) {
            dummyDataListenerSignals.getEventProducerStartedSending().put(eType, false);
            dummyDataListenerSignals.getEventProducerIsActive().put(eType, false);
            dummyDataListenerSignals.getEventStartedCollectingTimes().put(eType, OffsetDateTime.MIN);
            dummyDataListenerSignals.getEventProducerCounters().put(eType, 0L);
        }

        InputStream locInfosStream = Main.class.getResourceAsStream("/visitedAreasExample.json");
        try {
            locationInfoExamples = objectMapper.readValue(locInfosStream, new TypeReference<>() {
            });
        } catch (IOException e) {
            logger.error("Failed to read location info examples", e);
        }
    }

    @Async
    @EventListener(id = "dummy")
    public void onApplicationEvent(final KafkaDummyDataEvent event) {

        if (!dummyDataListenerSignals.getSupportedEvents().containsAll(event.getMessage())) {

            System.out.println("Dummy Data Producer doesn't support one of the following events: " + event.getMessage() +
                    " , Supported Events: " + dummyDataListenerSignals.getSupportedEvents() +
                    " , Active events:" + dummyDataListenerSignals.getEventProducerStartedSending().entrySet().stream().filter(Entry::getValue).map(Entry::getKey).toList());
            return;
        }
        for (NwdafEventEnum e : event.getMessage()) {
            dummyDataListenerSignals.activate(e);
        }
        if (!dummyDataListenerSignals.start()) {
            return;
        }

        if (dummyDataListenerSignals.getNoDataListener().get() > 0) {
            nfloadinfos = DummyDataGenerator.generateDummyNfLoadLevelInfo(no_dummy_nfload);
            ueMobilities = DummyDataGenerator.generateDummyUeMobilities(no_dummy_uemob, locationInfoExamples);
            ueCommunications = DummyDataGenerator.generateDummyUeCommunications(no_dummy_uecomm);
        }
        long start;

        System.out.println("Started sending dummy data for events: " + event.getMessage() +
                " , Supported Events: " + dummyDataListenerSignals.getSupportedEvents() +
                " , Active events:" + dummyDataListenerSignals.getEventProducerStartedSending().entrySet().stream().filter(Entry::getValue).map(Entry::getKey).toList());

        while (dummyDataListenerSignals.getNoDataListener().get() > 0) {
            start = System.nanoTime();
            for (NwdafEventEnum eType : dummyDataListenerSignals.getSupportedEvents()) {
                switch (eType) {
                    case NF_LOAD:
                        if (!dummyDataListenerSignals.getEventProducerIsActive().get(NwdafEventEnum.NF_LOAD)) {
                            break;
                        }

                        DummyDataGenerator.changeNfLoadTimeDependentProperties(nfloadinfos);
                        for (int k = 0; k < nfloadinfos.size(); k++) {
                            try {
                                producer.sendMessage(objectMapper.writeValueAsString(nfloadinfos.get(k)), eType.toString());
                                if (k == 0) {
                                    logger.info("collector sent nfload with time:" + nfloadinfos.get(k).getTimeStamp());
                                }
                                dummyDataListenerSignals.startSending(eType);

                            } catch (Exception e) {
                                logger.error("Failed to send dummy nfloadlevelinfo to broker", e);
                                dummyDataListenerSignals.stop();
                                continue;
                            }
                        }
                        break;
                    case UE_MOBILITY:
                        if (!dummyDataListenerSignals.getEventProducerIsActive().get(NwdafEventEnum.UE_MOBILITY)) {
                            break;
                        }

                        DummyDataGenerator.changeUeMobilitiesTimeDependentProperties(ueMobilities, 1);
                        for (int k = 0; k < ueMobilities.size(); k++) {
                            try {
                                if (k == 0) {
                                    logger.info("collector sent ue_mobility with time:{}", ueMobilities.get(k).getTs());
                                }
                                producer.sendMessage(objectMapper.writeValueAsString(ueMobilities.get(k)), eType.toString());
                                dummyDataListenerSignals.startSending(eType);

                            } catch (Exception e) {
                                logger.error("Failed to send dummy ueMobilities to broker", e);
                                dummyDataListenerSignals.stop();
                                continue;
                            }
                        }
                        break;
                    case UE_COMM:
                        if (!dummyDataListenerSignals.getEventProducerIsActive().get(NwdafEventEnum.UE_COMM)) {
                            break;
                        }

                        DummyDataGenerator.changeUeCommunicationsTimeDependentProperties(ueCommunications);
                        for (int k = 0; k < ueCommunications.size(); k++) {
                            try {
                                if (k == 0) {
                                    logger.info("collector sent ue_communication with time:" + ueCommunications.get(k).getTs());
                                }
                                producer.sendMessage(objectMapper.writeValueAsString(ueCommunications.get(k)), eType.toString());
                                dummyDataListenerSignals.startSending(eType);

                            } catch (Exception e) {
                                logger.error("Failed to send dummy ueCommunications to broker", e);
                                dummyDataListenerSignals.stop();
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
                    dummyDataListenerSignals.stop();
                    continue;
                }
            }
        }
        logger.info("Dummy Data Production stopped!");
    }


}
