package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
	private static Logger logger = LoggerFactory.getLogger(KafkaDummyDataListener.class);
    private List<NfLoadLevelInformation> nfloadinfos;
    private List<UeMobility> ueMobilities;
    public static List<NwdafEventEnum> supportedEvents = new ArrayList<>(Arrays.asList(NwdafEventEnum.NF_LOAD,NwdafEventEnum.UE_MOBILITY));
    public static OffsetDateTime startedCollectingTime = null;
    public static final Object availableOffsetLock = new Object();

	@Autowired
	Environment env;
    
    @Autowired
    KafkaProducer producer;
	
    @Autowired
    ObjectMapper objectMapper;

    @Async
    @EventListener(id = "dummy")
    public void onApplicationEvent(final KafkaDummyDataEvent event){
        if(!start()) {
			return;
		}
        if(no_kafkaDummyDataListeners>0) {
            nfloadinfos = DummyDataGenerator.generateDummyNfloadLevelInfo(10);
            ueMobilities = DummyDataGenerator.generateDummyUeMobilities(0);
        }
        long start;
        System.out.println("Started sending dummy data");
        while(no_kafkaDummyDataListeners>0) {
            start = System.nanoTime();
            for(NwdafEventEnum eType : supportedEvents) {
                switch(eType){
                    case NF_LOAD:
                        nfloadinfos = DummyDataGenerator.changeNfLoadTimeDependentProperties(nfloadinfos);
                        for(int k=0;k<nfloadinfos.size();k++) {
                            try {
                                logger.info("collector sent nfload with time:"+nfloadinfos.get(k).getTimeStamp());
                                producer.sendMessage(objectMapper.writeValueAsString(nfloadinfos.get(k)), eType.toString());
                                if(!startedSendingData){
                                    startSending();
                                }
                            }
                            catch(Exception e) {
                                logger.error("Failed to send dummy nfloadlevelinfo to broker",e);
                                stop();
                                continue;
                            }
                        }
                        break;
                    case UE_MOBILITY:
                        ueMobilities = DummyDataGenerator.changeUeMobilitiesTimeDependentProperties(ueMobilities);
                        for(int k=0;k<ueMobilities.size();k++) {
                            try {
                                producer.sendMessage(objectMapper.writeValueAsString(ueMobilities.get(k)), eType.toString());
                                if(!startedSendingData){
                                    startSending();
                                }
                            }
                            catch(Exception e) {
                                logger.error("Failed to send dummy ueMobilities to broker",e);
                                stop();
                                continue;
                            }
                        }
                        break;
                    default:
                        break;
                }
            }
            long diff = (System.nanoTime()-start) / 1000000l;
    		long wait_time = (long)Constants.MIN_PERIOD_SECONDS*1000l;
            if(diff<wait_time) {
	    		try {
					Thread.sleep(wait_time-diff);
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
			if(no_kafkaDummyDataListeners<1) {
				no_kafkaDummyDataListeners++;
                logger.info("producing dummy data to send to kafka...");
                return true;
			}
		}
        return false;
    }
    private static void stop() {
        synchronized (kafkaDummyDataLock) {
            if(no_kafkaDummyDataListeners>0) {
                no_kafkaDummyDataListeners--;
            }
		}
		synchronized(startedSendingDataLock) {
			startedSendingData = false;
		}
        synchronized(availableOffsetLock) {
			startedCollectingTime = null;
		}
    }
    public static void startSending() {
        synchronized(startedSendingDataLock) {
            startedSendingData = true;
        }
        synchronized(availableOffsetLock) {
            startedCollectingTime = OffsetDateTime.now();
        }
    }
    public static void stopSending() {
        synchronized(startedSendingDataLock) {
            startedSendingData = false;
        }
        synchronized(availableOffsetLock) {
            startedCollectingTime = null;
        }
    }
}
