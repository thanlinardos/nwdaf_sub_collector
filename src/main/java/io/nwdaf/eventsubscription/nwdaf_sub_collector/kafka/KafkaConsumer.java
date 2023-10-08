package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.SynchronousQueue;

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

@Component
public class KafkaConsumer {
    public static Boolean startedReceivingData = false;
	public static final Object startedReceivingDataLock = new Object();

	public static Boolean startedSavingData = false;
	public static final Object startedSavingDataLock = new Object();

	public static Boolean isListening = true;
	public static final Object isListeningLock = new Object();

	public static SynchronousQueue<String> wakeUpMessageQueue = new SynchronousQueue<>();

    @Value("${nnwdaf-eventsubscription.allow_dummy_data}")
    private boolean allow_dummy_data;

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

    @Scheduled(fixedDelay = 1000)
    private void wakeUpListener(){
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
            if(offsetAndTimestamp!=null){
                long partitionTimestamp = offsetAndTimestamp.timestamp();
                if (partitionTimestamp < earliestTimestamp) {
                    earliestTimestamp = partitionTimestamp;
                }
            }
        }
        if(earliestTimestamp == Long.MAX_VALUE){
			return;
		}
        // Convert the earliest timestamp to a human-readable format
        String formattedTimestamp = Instant.ofEpochMilli(earliestTimestamp).toString();
        System.out.println("Earliest Timestamp in the topic: " + formattedTimestamp);

        // Set the desired timestamps for the beginning and end of the range
        long endTimestamp = Instant.parse(OffsetDateTime.now().toString()).toEpochMilli();
        long startTimestamp = Instant.parse(OffsetDateTime.now().minusSeconds(60).toString()).toEpochMilli();

        // Seek to the beginning timestamp
        for (org.apache.kafka.common.TopicPartition partition : topicPartitions) {
            kafkaConsumer.seek(partition, kafkaConsumer.offsetsForTimes(Collections.singletonMap(partition, startTimestamp)).get(partition).offset());
        }

        // consume messages inside the desired range
         while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            // Process the received messages here
            records.forEach(record -> {
                // Check if the message timestamp is within the desired range
                if (record.timestamp() <= endTimestamp) {
                    System.out.println("Received message: " + record.value());
                    try {
                        wakeUpMessageQueue.put(record.value());
                    } catch (InterruptedException e) {
                        System.out.println("InterruptedException while writing to wakeup message queue.");
                    }
                }
            });
        }

    }

    @Scheduled(fixedDelay = 1000)
    private void handleWakeUp(){
        WakeUpMessage msg;
        long availableOffset,maxWait,wait_time;
        boolean hasData;
        int expectedWaitTime;
        while(wakeUpMessageQueue.size()>0){
            msg=null;
            availableOffset=0;
            hasData=false;
            expectedWaitTime = 0;
            try {
                msg = objectMapper.reader().readValue(wakeUpMessageQueue.take(),WakeUpMessage.class);
            } catch (IOException e) {
                System.out.println("IOException while converting WAKE_UP message String to WakeUpMessage object");
            } catch (InterruptedException e) {
                System.out.println("InterruptedException while reading wakeup message queue.");
            }
            if(msg==null || msg.getRequestedEvent()==null){return;}
            // prometheus collector
            if(KafkaDataCollectionListener.supportedEvents.contains(msg.getRequestedEvent())){
                maxWait = 200;
		        wait_time = 0;
                kafkaDataCollectionPublisher.publishDataCollection(msg.getRequestedEvent().toString());
                try {
                    while(wait_time<maxWait&&KafkaDataCollectionListener.no_dataCollectionEventListeners>0&&!KafkaDataCollectionListener.startedSendingData){
                        Thread.sleep(50);
                        wait_time+=50;
                    }
                } catch (InterruptedException e) {System.out.println("Failed to wait for datacollector to start sending data to kafka");}

                if(KafkaDataCollectionListener.startedSendingData){
                    if(KafkaDataCollectionListener.availableOffset==null){
                        if(msg.getRequestedOffset()==null || msg.getRequestedOffset()<=Constants.MIN_PERIOD_SECONDS){
                            hasData=true;
                        }
                        else{
                            expectedWaitTime = msg.getRequestedOffset();
                        }
                    }
                    else if(msg.getRequestedOffset()==null || KafkaDataCollectionListener.availableOffset.toInstant().toEpochMilli() / 1000 > msg.getRequestedOffset()){
                        availableOffset = KafkaDataCollectionListener.availableOffset.toInstant().toEpochMilli() / 1000;
                        hasData = true;
                    }
                    else{
                        expectedWaitTime =(int) (msg.getRequestedOffset() - KafkaDataCollectionListener.availableOffset.toInstant().toEpochMilli() / 1000);
                    }
                }
            }
            // dummy data collector
            if(allow_dummy_data && KafkaDummyDataListener.supportedEvents.contains(msg.getRequestedEvent())){
                maxWait = 200;
		        wait_time = 0;
                kafkaDummyDataPublisher.publishDataCollection(msg.getRequestedEvent().toString());
                try {
                    while(wait_time<maxWait&&KafkaDummyDataListener.no_kafkaDummyDataListeners>0&&!KafkaDummyDataListener.startedSendingData){
                        Thread.sleep(50);
                        wait_time+=50;
                    }
                } catch (InterruptedException e) {System.out.println("Failed to wait for dummy datacollector to start sending dummy data to kafka");}

                if(KafkaDummyDataListener.startedSendingData){
                    if(KafkaDummyDataListener.availableOffset==null){
                        if(msg.getRequestedOffset()==null || msg.getRequestedOffset()<=Constants.MIN_PERIOD_SECONDS){
                            hasData=true;
                        }
                        else{
                            expectedWaitTime = msg.getRequestedOffset();
                        }
                    }
                    else if(msg.getRequestedOffset()==null || KafkaDummyDataListener.availableOffset.toInstant().toEpochMilli() / 1000 > msg.getRequestedOffset()){
                        availableOffset = KafkaDummyDataListener.availableOffset.toInstant().toEpochMilli() / 1000;
                        hasData = true;
                    }
                    else{
                        expectedWaitTime =(int) (msg.getRequestedOffset() - KafkaDummyDataListener.availableOffset.toInstant().toEpochMilli() / 1000);
                    }
                }
            }
            DiscoverMessage response = DiscoverMessage.builder()
                    .requestedEvent(msg.getRequestedEvent())
                    .requestedOffset(msg.getRequestedOffset())
                    .hasData(hasData).availableOffset((int)availableOffset)
                    .expectedWaitTime(expectedWaitTime)
                    .build();
            try{
                kafkaProducer.sendMessage(response.toString(), "DISCOVER");
            }catch(IOException e){
                System.out.println("IOException while sending DISCOVER message");
            }
        }
    }

	public static void startedSaving(){
		synchronized(startedSavingDataLock){
			startedSavingData = true;
		}
	}
    public static void stoppedSaving(){
		synchronized(startedSavingDataLock){
			startedSavingData = false;
		}
	}
	public static void startListening(){
		synchronized(isListeningLock){
			isListening = true;
		}
	}
    public static void stopListening(){
		synchronized(isListeningLock){
			isListening = false;
		}
		synchronized(startedSavingDataLock){
			startedSavingData = false;
		}
		synchronized(startedReceivingDataLock){
			startedReceivingData = false;
		}
	}
}
