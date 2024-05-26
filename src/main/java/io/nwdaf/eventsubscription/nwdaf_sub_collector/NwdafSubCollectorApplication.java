package io.nwdaf.eventsubscription.nwdaf_sub_collector;


import io.nwdaf.eventsubscription.model.NetworkAreaInfo;
import io.nwdaf.eventsubscription.utilities.Constants;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.UUID;

@SpringBootApplication
@EnableAsync
@EnableScheduling
public class NwdafSubCollectorApplication {

    public static final UUID NWDAF_COLLECTOR_INSTANCE_ID = UUID.randomUUID();
    public static NetworkAreaInfo CollectorAreaOfInterest = Constants.AreaOfInterestExample1;

    final Environment env;

    @Value("${nnwdaf-eventsubscription.collector_area_of_interest}")
    private UUID collectorAreaOfInterestId;

    public NwdafSubCollectorApplication(Environment env) {
        this.env = env;
        if (collectorAreaOfInterestId != null) {
            CollectorAreaOfInterest = Constants.ExampleAOIsMap.get(collectorAreaOfInterestId);
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(NwdafSubCollectorApplication.class, args);
    }
}
