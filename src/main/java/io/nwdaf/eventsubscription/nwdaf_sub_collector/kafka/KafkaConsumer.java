package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import io.nwdaf.eventsubscription.model.NwdafEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.nwdaf.eventsubscription.customModel.DiscoverMessage;
import io.nwdaf.eventsubscription.customModel.WakeUpMessage;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy.KafkaDummyDataListener;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy.KafkaDummyDataPublisher;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus.KafkaDataCollectionListener;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus.KafkaDataCollectionPublisher;
import io.nwdaf.eventsubscription.utilities.Constants;

import org.apache.kafka.common.TopicPartition;

import static io.nwdaf.eventsubscription.nwdaf_sub_collector.NwdafSubCollectorApplication.CollectorAreaOfInterest;
import static io.nwdaf.eventsubscription.nwdaf_sub_collector.NwdafSubCollectorApplication.NWDAF_COLLECTOR_INSTANCE_ID;
import static io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy.KafkaDummyDataListener.no_kafkaDummyDataListeners;
import static io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus.KafkaDataCollectionListener.no_dataCollectionEventListeners;
import static io.nwdaf.eventsubscription.utilities.Constants.supportedEvents;

@Component
public class KafkaConsumer {
    public static ConcurrentHashMap<NwdafEvent.NwdafEventEnum, Boolean> discoverProducerStartedProducing = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<NwdafEvent.NwdafEventEnum, Boolean> wakeUpConsumerStartedReceiving = new ConcurrentHashMap<>();
    public static AtomicBoolean isListening = new AtomicBoolean(true);
    public static BlockingQueue<String> wakeUpMessageQueue = new LinkedBlockingQueue<>();

    @Value("${nnwdaf-eventsubscription.allow_dummy_data}")
    private boolean allowDummyData;
    @Value("${nnwdaf-eventsubscription.allow_prom_data}")
    private boolean allowPrometheusData;
    private final Consumer<String, String> kafkaConsumer;
    private final KafkaProducer kafkaProducer;
    private final ObjectMapper objectMapper;
    private final KafkaDataCollectionPublisher kafkaDataCollectionPublisher;
    private final KafkaDummyDataPublisher kafkaDummyDataPublisher;

    public KafkaConsumer(@Qualifier("consumer") Consumer<String, String> kafkaConsumer, KafkaProducer kafkaProducer, ObjectMapper objectMapper, KafkaDataCollectionPublisher kafkaDataCollectionPublisher, KafkaDummyDataPublisher kafkaDummyDataPublisher) {
        this.kafkaConsumer = kafkaConsumer;
        this.kafkaProducer = kafkaProducer;
        this.objectMapper = objectMapper;
        this.kafkaDataCollectionPublisher = kafkaDataCollectionPublisher;
        this.kafkaDummyDataPublisher = kafkaDummyDataPublisher;

        for (NwdafEvent.NwdafEventEnum e : supportedEvents) {
            discoverProducerStartedProducing.put(e, false);
            wakeUpConsumerStartedReceiving.put(e, false);
        }
    }

    @Scheduled(fixedDelay = 1)
    private void wakeUpListener() {
        List<PartitionInfo> partitions = kafkaConsumer.partitionsFor("WAKE_UP");

        // Get the beginning offset for each partition and convert it to a timestamp
        long earliestTimestamp = Long.MAX_VALUE;
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (PartitionInfo partition : partitions) {

            TopicPartition topicPartition = new TopicPartition("WAKE_UP", partition.partition());
            topicPartitions.add(topicPartition);
            kafkaConsumer.assign(Collections.singletonList(topicPartition));
            kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));

            long beginningOffset = kafkaConsumer.position(topicPartition);
            OffsetAndTimestamp offsetAndTimestamp = kafkaConsumer.offsetsForTimes(Collections.singletonMap(topicPartition, beginningOffset)).get(topicPartition);
            if (offsetAndTimestamp != null) {
                long partitionTimestamp = offsetAndTimestamp.timestamp();
                if (partitionTimestamp < earliestTimestamp) {
                    earliestTimestamp = partitionTimestamp;
                }
            }
        }
        if (earliestTimestamp == Long.MAX_VALUE) {
            return;
        }

        // Set the desired timestamps for the beginning and end of the range
        long endTimestamp = Instant.parse(OffsetDateTime.now().toString()).toEpochMilli();
        long startTimestamp = Instant.parse(OffsetDateTime.now().minusSeconds(2).toString()).toEpochMilli();

        // Seek to the beginning timestamp
        for (org.apache.kafka.common.TopicPartition partition : topicPartitions) {
            OffsetAndTimestamp offsetAndTimestamp = kafkaConsumer.offsetsForTimes(Collections.singletonMap(partition, startTimestamp)).get(partition);
            if (offsetAndTimestamp != null) {
                kafkaConsumer.seek(partition, offsetAndTimestamp.offset());
            }
        }

        // consume messages inside the desired range
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1));

        // Process the received messages here
        records.forEach(record -> {
            // Check if the message timestamp is within the desired range
            if (record.timestamp() <= endTimestamp && record.timestamp() >= startTimestamp) {

                System.out.println("Received message: " + record.value());
                if (!wakeUpMessageQueue.offer(record.value())) {
                    System.out.println("InterruptedException while writing to wakeup message queue.");
                }
            }
        });
    }

    @Scheduled(fixedDelay = 1)
    private void handleWakeUp() {
        WakeUpMessage msg;
        long availableOffset, listenerAvailableOffset;
        double maxWait, waitTime;
        boolean hasData;
        int expectedWaitTime;
        int max_bean_wait_time = 100;

        while (!wakeUpMessageQueue.isEmpty()) {

            msg = null;
            availableOffset = 0;
            hasData = false;
            expectedWaitTime = 0;
            listenerAvailableOffset = 0;

            try {
                msg = objectMapper.reader().readValue(wakeUpMessageQueue.poll(), WakeUpMessage.class);
            } catch (IOException e) {
                System.out.println("IOException while converting WAKE_UP message String to WakeUpMessage object");
            }
            if (msg == null || msg.getRequestedEvent() == null) {
                continue;
            }

            NwdafEvent.NwdafEventEnum event = msg.getRequestedEvent();
            startedReceiving(event);

            // dummy data collector
            if (allowDummyData && KafkaDummyDataListener.supportedEvents.contains(event)) {
                maxWait = 1000.0;
                waitTime = 0.0;

                kafkaDummyDataPublisher.publishDataCollection(List.of(event));
                if (!KafkaDummyDataListener.eventStartedCollectingTimes.get(event).equals(OffsetDateTime.MIN)) {
                    listenerAvailableOffset = (Instant.now().toEpochMilli() - KafkaDummyDataListener.eventStartedCollectingTimes.get(event).toInstant().toEpochMilli()) / 1000;
                }

                try {
                    double bean_wait_time = 0.0;
                    while (no_kafkaDummyDataListeners.get() == 0 && bean_wait_time <= max_bean_wait_time) {
                        Thread.sleep(0, 1_000);
                        bean_wait_time += 0.001;
                    }
                    while (waitTime < maxWait && no_kafkaDummyDataListeners.get() > 0 && !KafkaDummyDataListener.eventProducerStartedSending.get(event)) {
                        Thread.sleep(0, 1_000);
                        waitTime += 0.001;
                    }
                } catch (InterruptedException e) {
                    System.out.println("Failed to wait for dummy datacollector to start sending dummy data to kafka");
                }

                System.out.println("(DUMMY-" + NWDAF_COLLECTOR_INSTANCE_ID + "-" + CollectorAreaOfInterest.getId() + ") event=" + event + " waitTime=" + waitTime + " startedsending=" +
                        KafkaDummyDataListener.eventProducerStartedSending.get(event) + " collectTime=" + KafkaDummyDataListener.eventStartedCollectingTimes.get(event) +
                        " no_collectors=" + no_kafkaDummyDataListeners);

                if (KafkaDummyDataListener.eventProducerStartedSending.get(event)) {
                    if (KafkaDummyDataListener.eventStartedCollectingTimes.get(event).equals(OffsetDateTime.MIN)) {

                        KafkaDummyDataListener.startSending(event);
                        if (msg.getRequestedOffset() == null || msg.getRequestedOffset() <= Constants.MIN_PERIOD_SECONDS) {
                            hasData = true;
                        } else {
                            expectedWaitTime = msg.getRequestedOffset();
                        }

                        System.out.println("(DUMMY-" + NWDAF_COLLECTOR_INSTANCE_ID + "-" + CollectorAreaOfInterest.getId() + ") started sending with parameters: hasData= " + hasData +
                                " collectTime= " + KafkaDummyDataListener.eventStartedCollectingTimes.get(event) + " expectedWaitTime= " + expectedWaitTime);

                    } else if (msg.getRequestedOffset() == null || listenerAvailableOffset > msg.getRequestedOffset()) {
                        availableOffset = listenerAvailableOffset;
                        hasData = true;
                    } else {
                        expectedWaitTime = (int) (msg.getRequestedOffset() - listenerAvailableOffset);
                    }
                }
            }

            // prometheus collector
            int activeEventIndex;
            if (allowPrometheusData && (activeEventIndex = KafkaDataCollectionListener.supportedEvents.indexOf(event)) != -1) {

                maxWait = 1000.0;
                waitTime = 0.0;
                kafkaDataCollectionPublisher.publishDataCollection(List.of(event));

                try {

                    double bean_wait_time = 0.0;

                    while (no_dataCollectionEventListeners == 0 && bean_wait_time <= max_bean_wait_time) {
                        Thread.sleep(0, 1_000);
                        bean_wait_time += 0.001;
                    }

                    while (waitTime < maxWait && no_dataCollectionEventListeners > 0 && !KafkaDataCollectionListener.startedSendingData) {
                        Thread.sleep(0, 1_000);
                        waitTime += 0.001;
                    }
                } catch (InterruptedException e) {
                    System.out.println("Failed to wait for datacollector to start sending data to kafka");
                }

                System.out.println("(PROM-" + NWDAF_COLLECTOR_INSTANCE_ID + "-" + CollectorAreaOfInterest.getId() + ") wait_time=" + waitTime + " startedsending=" + KafkaDataCollectionListener.startedSendingData +
                        " collectTime=" + KafkaDataCollectionListener.startedCollectingTimes.get(activeEventIndex) + " no_collectors=" + no_dataCollectionEventListeners);

                if (KafkaDataCollectionListener.startedCollectingTimes.get(activeEventIndex) != null) {
                    listenerAvailableOffset = (Instant.now().toEpochMilli() - KafkaDataCollectionListener.startedCollectingTimes.get(activeEventIndex).toInstant().toEpochMilli()) / 1000;
                } else {
                    listenerAvailableOffset = 0;
                }

                if (KafkaDataCollectionListener.startedSendingData) {
                    if (KafkaDataCollectionListener.startedCollectingTimes.get(activeEventIndex) == null) {

                        KafkaDataCollectionListener.startSending(activeEventIndex);
                        if (msg.getRequestedOffset() == null || msg.getRequestedOffset() <= Constants.MIN_PERIOD_SECONDS) {
                            hasData = true;
                        } else {
                            expectedWaitTime = msg.getRequestedOffset();
                        }

                        System.out.println("(PROM-" + NWDAF_COLLECTOR_INSTANCE_ID + "-" + CollectorAreaOfInterest.getId() + ") started sending with parameters: hasData= " + hasData +
                                " collectTime= " + KafkaDataCollectionListener.startedCollectingTimes.get(activeEventIndex) + " expectedWaitTime= " + expectedWaitTime);
                    } else if (msg.getRequestedOffset() == null || listenerAvailableOffset > msg.getRequestedOffset()) {
                        availableOffset = listenerAvailableOffset;
                        hasData = true;
                    } else {
                        expectedWaitTime = (int) (msg.getRequestedOffset() - listenerAvailableOffset);
                    }
                }
            }

            DiscoverMessage response = DiscoverMessage.builder()
                    .collectorInstanceId(NWDAF_COLLECTOR_INSTANCE_ID)
                    .requestedEvent(event)
                    .requestedOffset(msg.getRequestedOffset())
                    .hasData(hasData).availableOffset((int) availableOffset)
                    .expectedWaitTime(expectedWaitTime)
                    .build();

            try {
                kafkaProducer.sendMessage(response.toString(), "DISCOVER");
                startedProducing(event);
            } catch (IOException e) {
                System.out.println("IOException while sending DISCOVER message");
            }
        }
    }

    public static void startedReceiving(NwdafEvent.NwdafEventEnum e) {
        if (!wakeUpConsumerStartedReceiving.get(e)) {
            wakeUpConsumerStartedReceiving.compute(e, (k, v) -> true);
        }
    }

    public static void stoppedReceiving(NwdafEvent.NwdafEventEnum e) {
        if (wakeUpConsumerStartedReceiving.get(e)) {
            wakeUpConsumerStartedReceiving.compute(e, (k, v) -> false);
        }
    }

    public static void startedProducing(NwdafEvent.NwdafEventEnum e) {
        if (!discoverProducerStartedProducing.get(e)) {
            discoverProducerStartedProducing.compute(e, (k, v) -> true);
        }
    }

    public static void stoppedProducing(NwdafEvent.NwdafEventEnum e) {
        if (discoverProducerStartedProducing.get(e)) {
            discoverProducerStartedProducing.compute(e, (k, v) -> false);
        }
    }

    public static void startListening() {
        isListening.set(true);
    }

    public static void stopListening() {
        isListening.set(false);
        for (NwdafEvent.NwdafEventEnum e : supportedEvents) {
            stoppedProducing(e);
            stoppedReceiving(e);
        }
    }
}
