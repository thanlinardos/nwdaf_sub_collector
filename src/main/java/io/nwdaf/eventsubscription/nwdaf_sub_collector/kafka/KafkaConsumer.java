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

import io.nwdaf.eventsubscription.customModel.KafkaTopic;
import io.nwdaf.eventsubscription.model.NwdafEvent;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.DataListenerSignals;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.nef.NefDataCollectionPublisher;
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
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy.KafkaDummyDataPublisher;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus.KafkaDataCollectionPublisher;
import io.nwdaf.eventsubscription.utilities.Constants;

import org.apache.kafka.common.TopicPartition;

import static io.nwdaf.eventsubscription.nwdaf_sub_collector.NwdafSubCollectorApplication.CollectorAreaOfInterest;
import static io.nwdaf.eventsubscription.nwdaf_sub_collector.NwdafSubCollectorApplication.NWDAF_COLLECTOR_INSTANCE_ID;
import static io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy.KafkaDummyDataListener.dummyDataListenerSignals;
import static io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.nef.NefDataCollectionListener.nefDataListenerSignals;
import static io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus.KafkaDataCollectionListener.dataListenerSignals;
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
    @Value("${nnwdaf-eventsubscription.allow_nef_data}")
    private boolean allowNefData;
    private final Consumer<String, String> kafkaConsumer;
    private final KafkaProducer kafkaProducer;
    private final ObjectMapper objectMapper;
    private final KafkaDataCollectionPublisher kafkaDataCollectionPublisher;
    private final NefDataCollectionPublisher nefDataCollectionPublisher;
    private final KafkaDummyDataPublisher kafkaDummyDataPublisher;

    public KafkaConsumer(@Qualifier("consumer") Consumer<String, String> kafkaConsumer, KafkaProducer kafkaProducer, ObjectMapper objectMapper, KafkaDataCollectionPublisher kafkaDataCollectionPublisher, NefDataCollectionPublisher nefDataCollectionPublisher, KafkaDummyDataPublisher kafkaDummyDataPublisher) {
        this.kafkaConsumer = kafkaConsumer;
        this.kafkaProducer = kafkaProducer;
        this.objectMapper = objectMapper;
        this.kafkaDataCollectionPublisher = kafkaDataCollectionPublisher;
        this.nefDataCollectionPublisher = nefDataCollectionPublisher;
        this.kafkaDummyDataPublisher = kafkaDummyDataPublisher;

        for (NwdafEvent.NwdafEventEnum e : supportedEvents) {
            discoverProducerStartedProducing.put(e, false);
            wakeUpConsumerStartedReceiving.put(e, false);
        }
    }

    @Scheduled(fixedDelay = 1)
    private void wakeUpListener() {
        String topic = KafkaTopic.WAKE_UP.name();
        List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(topic);

        // Get the beginning offset for each partition and convert it to a timestamp
        long earliestTimestamp = Long.MAX_VALUE;
        List<TopicPartition> topicPartitions = new ArrayList<>();
        for (PartitionInfo partition : partitions) {

            TopicPartition topicPartition = new TopicPartition(topic, partition.partition());
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
        long startTimestamp = Instant.parse(OffsetDateTime.now().minusNanos(500_000_000).toString()).toEpochMilli();

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
        if(wakeUpMessageQueue.isEmpty()) {
            return;
        }
        List<WakeUpMessage> wakeUpMessages = new ArrayList<>();

        while (!wakeUpMessageQueue.isEmpty()) {

            try {
                WakeUpMessage wakeUpMessage = objectMapper.reader().readValue(wakeUpMessageQueue.poll(), WakeUpMessage.class);
                if (wakeUpMessage != null && wakeUpMessage.getRequestedEvent() != null && !wakeUpMessages.contains(wakeUpMessage)) {
                    wakeUpMessages.add(wakeUpMessage);
                }
            } catch (IOException e) {
                System.out.println("IOException while converting WAKE_UP message String to WakeUpMessage object");
            }
        }

        for (WakeUpMessage msg : wakeUpMessages) {
            NwdafEvent.NwdafEventEnum event = msg.getRequestedEvent();

            DiscoverMessage response = DiscoverMessage.builder()
                    .collectorInstanceId(NWDAF_COLLECTOR_INSTANCE_ID)
                    .requestedEvent(event)
                    .requestedOffset(msg.getRequestedOffset())
                    .build();

            startedReceiving(event);
            double maxWait = 2000.0;

            if (allowDummyData && dummyDataListenerSignals.getSupportedEvents().contains(event)) {
                kafkaDummyDataPublisher.publishDataCollection(List.of(event));
                activateDataListener(msg, event, maxWait, response, dummyDataListenerSignals, "DUMMY");
            }

            if (allowPrometheusData && dataListenerSignals.getSupportedEvents().contains(event)) {
                kafkaDataCollectionPublisher.publishDataCollection(List.of(event));
                activateDataListener(msg, event, maxWait, response, dataListenerSignals, "PROMETHEUS");
            }

            if (allowNefData && nefDataListenerSignals.getSupportedEvents().contains(event)) {
                nefDataCollectionPublisher.publishDataCollection(List.of(event));
                activateDataListener(msg, event, maxWait, response, nefDataListenerSignals, "NEF");
            }

            try {
                kafkaProducer.sendMessage(response.toString(), KafkaTopic.DISCOVER.name());
                startedProducing(event);
            } catch (IOException e) {
                System.out.println("IOException while sending DISCOVER message");
            }
        }
    }

    private static void activateDataListener(WakeUpMessage msg, NwdafEvent.NwdafEventEnum event, double maxWait, DiscoverMessage response, DataListenerSignals dataListenerSignals, String collectorType) {
        int max_bean_wait_time = 100;
        double waitTime = 0.0;
        long listenerAvailableOffset = 0;
        long availableOffset = 0;
        boolean hasData = false;
        long expectedWaitTime = 0;
        if (!dataListenerSignals.getEventStartedCollectingTimes().get(event).equals(OffsetDateTime.MIN)) {
            listenerAvailableOffset = (Instant.now().toEpochMilli() - dataListenerSignals.getEventStartedCollectingTimes().get(event).toInstant().toEpochMilli()) / 1000;
        }

        try {
            double bean_wait_time = 0.0;
            while (dataListenerSignals.getNoDataListener().get() == 0 && bean_wait_time <= max_bean_wait_time) {
                Thread.sleep(0, 1_000);
                bean_wait_time += 0.001;
            }
            while (waitTime < maxWait && dataListenerSignals.getNoDataListener().get() > 0 && !dataListenerSignals.getEventProducerStartedSending().get(event)) {
                Thread.sleep(0, 1_000);
                waitTime += 0.001;
            }
        } catch (InterruptedException e) {
            System.out.println("Failed to wait for" + collectorType + " datacollector to start sending dummy data to kafka");
        }

        System.out.println("(" + collectorType + "-" + NWDAF_COLLECTOR_INSTANCE_ID + "-" + CollectorAreaOfInterest.getId() + ") event=" + event + " waitTime=" + waitTime + " startedsending=" +
                dataListenerSignals.getEventProducerStartedSending().get(event) + " collectTime=" + dataListenerSignals.getEventStartedCollectingTimes().get(event) +
                " no_collectors=" + dataListenerSignals.getNoDataListener());

        if (dataListenerSignals.getEventProducerStartedSending().get(event)) {
            if (dataListenerSignals.getEventStartedCollectingTimes().get(event).equals(OffsetDateTime.MIN)) {

                dataListenerSignals.startSending(event);
                if (msg.getRequestedOffset() == null || msg.getRequestedOffset() <= Constants.MIN_PERIOD_SECONDS) {
                    hasData = true;
                } else {
                    expectedWaitTime = msg.getRequestedOffset();
                }

                System.out.println("(" + collectorType + "-" + NWDAF_COLLECTOR_INSTANCE_ID + "-" + CollectorAreaOfInterest.getId() + ") started sending with parameters: hasData= " + hasData +
                        " collectTime= " + dataListenerSignals.getEventStartedCollectingTimes().get(event) + " expectedWaitTime= " + expectedWaitTime);

            } else if (msg.getRequestedOffset() == null || listenerAvailableOffset > msg.getRequestedOffset()) {
                availableOffset = listenerAvailableOffset;
                hasData = true;
            } else {
                expectedWaitTime = msg.getRequestedOffset() - listenerAvailableOffset;
            }
        }

        response.setHasData(hasData);
        response.setAvailableOffset(availableOffset);
        response.setExpectedWaitTime(expectedWaitTime);
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
