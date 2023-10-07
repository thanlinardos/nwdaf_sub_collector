package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.nwdaf.eventsubscription.customModel.DiscoverMessage;
import io.nwdaf.eventsubscription.customModel.WakeUpMessage;
import io.nwdaf.eventsubscription.model.NwdafEvent.NwdafEventEnum;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy.KafkaDummyDataListener;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy.KafkaDummyDataPublisher;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus.KafkaDataCollectionListener;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus.KafkaDataCollectionPublisher;

@Component
public class KafkaConsumer {
    public static Boolean isListening = true;
	public static final Object isListeningLock = new Object();

    @Autowired
	private KafkaDummyDataPublisher kafkaDummyDataPublisher;

	@Autowired
	private KafkaDataCollectionPublisher kafkaDataCollectionPublisher;

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private ObjectMapper objectMapper;

    private Logger logger = LoggerFactory.getLogger(KafkaConsumer.class); 

    @KafkaListener(topics="WAKE_UP", groupId = "nwdaf_sub_collector", containerFactory = "kafkaListenerContainerFactory")
    public String dataListener(ConsumerRecord<String,String> record) throws InterruptedException{
        String topic = record.topic();
		String in = record.value();
        WakeUpMessage inMessage = new WakeUpMessage();
        try {
            inMessage = objectMapper.reader().readValue(in,WakeUpMessage.class);
        } catch (IOException e) {}
		if(!isListening || !topic.equals("WAKE_UP") || inMessage==null || inMessage.getRequestedEvent()==null){
			return "";
		}
        System.out.println("wake_up msg: "+inMessage.toString());
        kafkaDataCollectionPublisher.publishDataCollection("kafka prom data production");
		kafkaDummyDataPublisher.publishDataCollection("kafka dummy data production");
        long maxWait = 200;
		long wait_time = 0;
        // dummy collector
        List<NwdafEventEnum> supportedEvents = new ArrayList<>();
        Integer expectedWaitTime = 1;
        Integer availableOffset = 10000;
        while(wait_time<maxWait&&KafkaDummyDataListener.getNo_kafkaDummyDataListeners()>0&&!KafkaDummyDataListener.getStartedSendingData()){
            Thread.sleep(50);
            wait_time+=50;
        }
        if(KafkaDummyDataListener.getStartedSendingData()){
            for(NwdafEventEnum e:KafkaDummyDataListener.supportedEvents){
                supportedEvents.add(e);
            }
        }
        System.out.println("dummy supportedEvents:"+supportedEvents.toString());
        DiscoverMessage discoverMessage;
        if(supportedEvents.contains(inMessage.getRequestedEvent())){
            discoverMessage = DiscoverMessage.builder()
                .hasData(true)
                .expectedWaitTime(0)
                .availableOffset(availableOffset)
                .requestedEvent(inMessage.getRequestedEvent())
                .requestedOffset(inMessage.getRequestedOffset())
                .build();
        }
        else{
            discoverMessage = DiscoverMessage.builder()
                .hasData(false)
                .expectedWaitTime(expectedWaitTime)
                .availableOffset(availableOffset)
                .requestedEvent(inMessage.getRequestedEvent())
                .requestedOffset(inMessage.getRequestedOffset())
                .build();
        }
        try {
            kafkaProducer.sendMessage(discoverMessage.toString(), topic);
        } catch (IOException e) {
            logger.error("Couldn't send message to WAKE_UP topic", e);
            return "";
        }
        // prometheus collector
        wait_time=0;
        supportedEvents.clear();
        expectedWaitTime = 1;
        availableOffset = 10000;
        while(wait_time<maxWait&&KafkaDataCollectionListener.getNo_dataCollectionEventListeners()>0&&!KafkaDataCollectionListener.getStartedSendingData()){
            Thread.sleep(50);
            wait_time+=50;
        }
        if(KafkaDataCollectionListener.getStartedSendingData()){
            for(NwdafEventEnum e:KafkaDataCollectionListener.supportedEvents){
                supportedEvents.add(e);
            }
        }
        System.out.println("supportedEvents:"+supportedEvents.toString());
        if(supportedEvents.contains(inMessage.getRequestedEvent())){
            discoverMessage = DiscoverMessage.builder()
                .hasData(true)
                .expectedWaitTime(0)
                .availableOffset(availableOffset)
                .requestedEvent(inMessage.getRequestedEvent())
                .requestedOffset(inMessage.getRequestedOffset())
                .build();
        }
        else{
            discoverMessage = DiscoverMessage.builder()
                .hasData(false)
                .expectedWaitTime(expectedWaitTime)
                .availableOffset(availableOffset)
                .requestedEvent(inMessage.getRequestedEvent())
                .requestedOffset(inMessage.getRequestedOffset())
                .build();
        }
        try {
            kafkaProducer.sendMessage(discoverMessage.toString(), "DISCOVER");
        } catch (IOException e) {
            logger.error("Couldn't send message to DISCOVER topic", e);
            return "";
        }
        return in;
    }
}
