package io.nwdaf.eventsubscription.nwdaf_sub_collector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class NwdafSubCollectorApplication {

	public static void main(String[] args) {
		SpringApplication.run(NwdafSubCollectorApplication.class, args);
	}
}
