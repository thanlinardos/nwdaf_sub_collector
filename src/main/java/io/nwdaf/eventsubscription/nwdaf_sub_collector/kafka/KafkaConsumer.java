package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import io.nwdaf.eventsubscription.nwdaf_sub_collector.NwdafSubCollectorApplication;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
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

import static io.nwdaf.eventsubscription.nwdaf_sub_collector.NwdafSubCollectorApplication.NWDAF_COLLECTOR_INSTANCE_ID;
import static io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy.KafkaDummyDataListener.no_kafkaDummyDataListeners;
import static io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus.KafkaDataCollectionListener.no_dataCollectionEventListeners;

@Component
public class KafkaConsumer {
    public static Boolean startedReceivingData = false;
    public static final Object startedReceivingDataLock = new Object();

    public static Boolean startedSavingData = false;
    public static final Object startedSavingDataLock = new Object();

    public static Boolean isListening = true;
    public static final Object isListeningLock = new Object();

    public static BlockingQueue<String> wakeUpMessageQueue = new LinkedBlockingQueue<>();

    @Value("${nnwdaf-eventsubscription.allow_dummy_data}")
    private boolean allow_dummy_data;

    @Value("${nnwdaf-eventsubscription.allow_prom_data}")
    private boolean allow_prom_data;

    @Autowired
    @Qualifier("consumer")
    private Consumer<String, String> kafkaConsumer;

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaDataCollectionPublisher kafkaDataCollectionPublisher;

    @Autowired
    private KafkaDummyDataPublisher kafkaDummyDataPublisher;

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
            // dummy data collector
            int activeEventIndex;
            if (allow_dummy_data && (activeEventIndex = KafkaDummyDataListener.supportedEvents.indexOf(msg.getRequestedEvent())) != -1) {
                maxWait = 1000.0;
                waitTime = 0.0;
                kafkaDummyDataPublisher.publishDataCollection(List.of(msg.getRequestedEvent()));
                if (KafkaDummyDataListener.startedCollectingTimes.get(activeEventIndex) != null) {
                    listenerAvailableOffset = (Instant.now().toEpochMilli() - KafkaDummyDataListener.startedCollectingTimes.get(activeEventIndex).toInstant().toEpochMilli()) / 1000;
                }
                try {
                    double bean_wait_time = 0.0;
                    while (no_kafkaDummyDataListeners == 0 && bean_wait_time <= max_bean_wait_time) {
                        Thread.sleep(0,1_000);
                        bean_wait_time += 0.001;
                    }
                    while (waitTime < maxWait && no_kafkaDummyDataListeners > 0 && !KafkaDummyDataListener.startedSendingData) {
                        Thread.sleep(0,1_000);
                        waitTime += 0.001;
                    }
                } catch (InterruptedException e) {
                    System.out.println("Failed to wait for dummy datacollector to start sending dummy data to kafka");
                }
                System.out.println("(DUMMY) waitTime=" + waitTime + " startedsending=" + KafkaDummyDataListener.startedSendingData + " collectTime=" + KafkaDummyDataListener.startedCollectingTimes.get(activeEventIndex) + " no_collectors=" + no_kafkaDummyDataListeners);
                if (KafkaDummyDataListener.startedSendingData && KafkaDummyDataListener.activeEvents.contains(msg.getRequestedEvent())) {
                    if (KafkaDummyDataListener.startedCollectingTimes.get(activeEventIndex) == null) {
                        KafkaDummyDataListener.startSending(activeEventIndex);
                        if (msg.getRequestedOffset() == null || msg.getRequestedOffset() <= Constants.MIN_PERIOD_SECONDS) {
                            hasData = true;
                        } else {
                            expectedWaitTime = msg.getRequestedOffset();
                        }
                        System.out.println("(DUMMY) started sending with parameters: hasData= " + hasData + " collectTime= " + KafkaDummyDataListener.startedCollectingTimes.get(activeEventIndex) + " expectedWaitTime= " + expectedWaitTime);
                    } else if (msg.getRequestedOffset() == null || listenerAvailableOffset > msg.getRequestedOffset()) {
                        availableOffset = listenerAvailableOffset;
                        hasData = true;
                    } else {
                        expectedWaitTime = (int) (msg.getRequestedOffset() - listenerAvailableOffset);
                    }
                }
            }
            // prometheus collector
            if (allow_prom_data && (activeEventIndex = KafkaDataCollectionListener.supportedEvents.indexOf(msg.getRequestedEvent())) != -1) {
                maxWait = 1000.0;
                waitTime = 0.0;
                kafkaDataCollectionPublisher.publishDataCollection(List.of(msg.getRequestedEvent()));
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
                System.out.println("(PROM) wait_time=" + waitTime + " startedsending=" + KafkaDataCollectionListener.startedSendingData + " collectTime=" + KafkaDataCollectionListener.startedCollectingTimes.get(activeEventIndex) + " no_collectors=" + no_dataCollectionEventListeners);
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
                        System.out.println("(DUMMY) started sending with parameters: hasData= " + hasData + " collectTime= " + KafkaDummyDataListener.startedCollectingTimes.get(activeEventIndex) + " expectedWaitTime= " + expectedWaitTime);
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
                    .requestedEvent(msg.getRequestedEvent())
                    .requestedOffset(msg.getRequestedOffset())
                    .hasData(hasData).availableOffset((int) availableOffset)
                    .expectedWaitTime(expectedWaitTime)
                    .build();
            try {
                kafkaProducer.sendMessage(response.toString(), "DISCOVER");
            } catch (IOException e) {
                System.out.println("IOException while sending DISCOVER message");
            }
        }
    }

    public static void startedSaving() {
        synchronized (startedSavingDataLock) {
            startedSavingData = true;
        }
    }

    public static void stoppedSaving() {
        synchronized (startedSavingDataLock) {
            startedSavingData = false;
        }
    }

    public static void startListening() {
        synchronized (isListeningLock) {
            isListening = true;
        }
    }

    public static void stopListening() {
        synchronized (isListeningLock) {
            isListening = false;
        }
        synchronized (startedSavingDataLock) {
            startedSavingData = false;
        }
        synchronized (startedReceivingDataLock) {
            startedReceivingData = false;
        }
    }
}
