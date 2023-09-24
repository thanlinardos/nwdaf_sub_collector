package io.nwdaf.eventsubscription.nwdaf_sub_collector;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableAsync;

import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.dummy.KafkaDummyDataPublisher;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus.KafkaDataCollectionPublisher;

@SpringBootApplication
@EnableAsync
public class NwdafSubCollectorApplication {

	@Autowired
	private KafkaDummyDataPublisher kafkaDummyDataPublisher;

	@Autowired
	private KafkaDataCollectionPublisher kafkaDataCollectionPublisher;

	public static void main(String[] args) {
		SpringApplication.run(NwdafSubCollectorApplication.class, args);
	}

	@Bean
	public ApplicationRunner run(){

		return args ->{
			kafkaDataCollectionPublisher.publishDataCollection("kafka prom data production");
			kafkaDummyDataPublisher.publishDataCollection("kafka dummy data production");
		};
	}
}
