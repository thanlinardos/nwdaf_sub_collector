package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
	private static Logger logger = LoggerFactory.getLogger(KafkaDataCollectionListener.class);
	public static final Object availableOffsetLock = new Object();
	public static OffsetDateTime startedCollectingTime = null;
	public static List<NwdafEventEnum> supportedEvents = new ArrayList<>(Arrays.asList(NwdafEventEnum.NF_LOAD));
	private List<NfLoadLevelInformation> nfloadinfos;

	@Value(value = "${nnwdaf-eventsubscription.prometheus_url}")
	String prometheusUrl;

	@Autowired
    KafkaProducer producer;

	@Autowired
    ObjectMapper objectMapper;
	
    @Async
    @EventListener(id="data")
    public void onApplicationEvent(final KafkaDataCollectionEvent event) {
    	start();
    	while(no_dataCollectionEventListeners>0) {
			long start,prom_delay,diff,wait_time;
    		start = System.nanoTime();
			prom_delay = 0l;
    		for(NwdafEventEnum eType : supportedEvents) {
				switch(eType){
					case NF_LOAD:
						nfloadinfos=new ArrayList<>();
						try {
							long t = System.nanoTime();
							nfloadinfos = new PrometheusRequestBuilder().execute(eType, prometheusUrl);
							prom_delay += (System.nanoTime() - t) / 1000000l;
						} catch (JsonProcessingException e) {
							logger.error("Failed to collect data for event: "+eType,e);
							stop();
							continue;
						}
						if(nfloadinfos==null || nfloadinfos.size()==0) {
							logger.error("Failed to collect data for event: "+eType);
							stop();
							continue;
						}
						else {
							for(int j=0;j<nfloadinfos.size();j++) {
								try {
									// System.out.println("nfloadinfo"+j+": "+nfloadinfos.get(j));
									producer.sendMessage(objectMapper.writeValueAsString(nfloadinfos.get(j)),eType.toString());
									if(!startedSendingData){
										startSending();
									}
								}
								catch(Exception e) {
									logger.error("Failed to send nfloadlevelinfo to kafka",e);
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
    		diff = (System.nanoTime()-start) / 1000000l;
    		wait_time = (long)Constants.MIN_PERIOD_SECONDS*1000l;
    		if(diff<wait_time) {
	    		try {
					Thread.sleep(wait_time-diff);
				} catch (InterruptedException e) {
					e.printStackTrace();
					stop();
					continue;
				}
    		}
    		logger.info("prom request delay = "+prom_delay+"ms");
    		logger.info("data coll total delay = "+diff+"ms");
    	}
    	logger.info("Prometheus Data Collection stopped!");
        return;
    }
	public static void start(){
		synchronized (dataCollectionLock) {
		if(no_dataCollectionEventListeners<1) {
			no_dataCollectionEventListeners++;
			logger.info("collecting data...");
		}
		}
	}
	public static void stop(){
		synchronized (dataCollectionLock) {
    		no_dataCollectionEventListeners--;
    	}
		synchronized(startedSendingDataLock){
			startedSendingData = false;
		}
		synchronized(availableOffsetLock){
			startedCollectingTime = null;
		}
	}
	public static void startSending(){
        synchronized(startedSendingDataLock){
            startedSendingData = true;
        }
        synchronized(availableOffsetLock){
            startedCollectingTime = OffsetDateTime.now();
        }
    }
    public static void stopSending(){
        synchronized(startedSendingDataLock){
            startedSendingData = false;
        }
        synchronized(availableOffsetLock){
            startedCollectingTime = null;
        }
    }
}
