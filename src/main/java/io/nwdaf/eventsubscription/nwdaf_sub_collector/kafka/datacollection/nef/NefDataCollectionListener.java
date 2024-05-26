package io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.nef;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nwdaf.eventsubscription.Main;
import io.nwdaf.eventsubscription.customModel.*;
import io.nwdaf.eventsubscription.model.NwdafEvent;
import io.nwdaf.eventsubscription.model.UeMobility;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.NwdafSubCollectorApplication;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.KafkaProducer;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.DataListenerSignals;
import io.nwdaf.eventsubscription.nwdaf_sub_collector.kafka.datacollection.prometheus.KafkaDataCollectionListener;
import io.nwdaf.eventsubscription.requestbuilders.NefRequestBuilder;
import io.nwdaf.eventsubscription.utilities.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestClientException;

import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static io.nwdaf.eventsubscription.utilities.ConvertUtil.toOptional;

@Component
public class NefDataCollectionListener {
    public static DataListenerSignals nefDataListenerSignals = new DataListenerSignals(
            Arrays.asList(NwdafEvent.NwdafEventEnum.UE_MOBILITY),
            LoggerFactory.getLogger(KafkaDataCollectionListener.class));
    private final KafkaProducer kafkaProducer;

    @Value(value = "${nnwdaf-eventsubscription.nef_url}")
    private String nefUrl;

    @Value(value = "${nnwdaf-eventsubscription.nef_state_ues}")
    private String nefStateUes;

    private String token;

    @Value("${nnwdaf-eventsubscription.nef_username}")
    private String username;

    @Value("${nnwdaf-eventsubscription.nef_password}")
    private String password;

    @Value("${nnwdaf-eventsubscription.allow_nef_data}")
    private boolean allowNefData;

    @Value("${trust.store}")
    private Resource trustStore;

    @Value("${nnwdaf-eventsubscription.nef_group_id}")
    private String groupId;

    final KafkaProducer producer;
    final ObjectMapper objectMapper;
    private static final Logger logger = LoggerFactory.getLogger(KafkaDataCollectionListener.class);
    private static final AtomicBoolean isLoopStarted = new AtomicBoolean(false);

    public static Integer selectedScenarioIndex;

    @Value("${nnwdaf-eventsubscription.selected_scenario:0}")
    private Integer defaultScenarioIndex;

    public static NefScenario selectedScenario;

    public static NefScenario defaultExportedScenario;

    private static final Logger log = LoggerFactory.getLogger(NefDataCollectionListener.class);

    public NefDataCollectionListener(KafkaProducer producer, ObjectMapper objectMapper, KafkaProducer kafkaProducer) {
        this.producer = producer;
        this.objectMapper = objectMapper;
        for (NwdafEvent.NwdafEventEnum eType : nefDataListenerSignals.getSupportedEvents()) {
            nefDataListenerSignals.getEventProducerStartedSending().put(eType, false);
            nefDataListenerSignals.getEventProducerIsActive().put(eType, false);
            nefDataListenerSignals.getEventStartedCollectingTimes().put(eType, OffsetDateTime.MIN);
            nefDataListenerSignals.getEventProducerCounters().put(eType, 0L);
        }
        this.kafkaProducer = kafkaProducer;
    }

    @Async
    @EventListener(id = "nef")
    public void onApplicationEvent(final NefDataCollectionEvent event) {

        if (!nefDataListenerSignals.getSupportedEvents().containsAll(event.getMessage())) {

            System.out.println("Data Producer doesn't support one of the following events: " + event.getMessage() +
                    " , Supported Events: " + nefDataListenerSignals.getSupportedEvents() +
                    " , Active events:" + nefDataListenerSignals.getEventProducerStartedSending().entrySet().stream().filter(Map.Entry::getValue).map(Map.Entry::getKey).toList());
            return;
        }
        for (NwdafEvent.NwdafEventEnum e : event.getMessage()) {
            nefDataListenerSignals.activate(e);
        }
        if (!nefDataListenerSignals.start()) {
            return;
        }
        log.info("Started sending data for events: {} , Supported Events: {} , Active events:{}", event.getMessage(),
                nefDataListenerSignals.getSupportedEvents(),
                nefDataListenerSignals.getEventProducerStartedSending()
                        .entrySet()
                        .stream()
                        .filter(Map.Entry::getValue)
                        .map(Map.Entry::getKey)
                        .toList());

        token = getNefAccessToken(new NefRequestBuilder(null, trustStore), username, password);

        if (token == null) {
            log.error("Failed to get NEF access token");
            nefDataListenerSignals.stop();
            return;
        }

        NefRequestBuilder nefRequestBuilder = new NefRequestBuilder(token, trustStore);

        if (nefRequestBuilder.getTemplate() == null) {
            nefDataListenerSignals.stop();
        }

        boolean stoppedActiveUEs = false;

        defaultExportedScenario = getDefaultScenario(nefRequestBuilder, nefUrl, token);
        if (defaultExportedScenario == null) {
            log.error("Failed to get default scenario");
        }

        if (defaultScenarioIndex >= 0) {
            stopUEsAndReplaceDefaultScenario(nefRequestBuilder, stoppedActiveUEs);
        } else if (defaultExportedScenario != null) {
            log.info("Default scenario unchanged.");
        } else {
            log.error("Failed to run any scenario for data collection from NEF with uri {}", nefUrl);
            nefDataListenerSignals.stop();
        }
        if (selectedScenario == null) {
            if (defaultExportedScenario == null) {
                log.error("Failed to run any scenario for data collection from NEF with uri {}", nefUrl);
                nefDataListenerSignals.stop();
            }
            selectedScenario = defaultExportedScenario;
        }

        while (nefDataListenerSignals.getNoDataListener().get() > 0) {

            long start, nef_delay, diff, wait_time;
            start = System.nanoTime();
            nef_delay = 0L;
            for (NwdafEvent.NwdafEventEnum eType : nefDataListenerSignals.getSupportedEvents()) {
                switch (eType) {
                    case UE_MOBILITY:
                        if (!nefDataListenerSignals.getEventProducerIsActive().get(eType)) {
                            break;
                        }
                        List<UeMobility> ueMobilities;
                        try {
                            long t = System.nanoTime();
                            if (!isLoopStarted.get()) {
                                startMovementLoop(nefRequestBuilder);
                            }
                            ueMobilities = nefRequestBuilder.getUEMetrics(eType, nefUrl + nefStateUes,
                                    groupId, objectMapper, selectedScenario.getCells());
                            nef_delay += (System.nanoTime() - t) / 1000000L;
                        } catch (JsonProcessingException e) {
                            logger.error("Failed to collect nef data for event: {}", eType, e);
                            nefDataListenerSignals.stop();
                            continue;
                        } catch (RestClientException e) {
                            logger.error("Failed to connect to nef for event: {}", eType, e);
                            nefDataListenerSignals.stop();
                            continue;
                        }
                        if (ueMobilities == null || ueMobilities.isEmpty()) {
                            logger.error("Failed to collect nef data for event: {}", eType);
                            nefDataListenerSignals.stop();
                            continue;
                        }
                        for (int j = 0; j < ueMobilities.size(); j++) {
                            try {
                                // System.out.println("uemobility info "+j+": "+ueMobilities.get(j));
                                producer.sendMessage(objectMapper.writeValueAsString(ueMobilities.get(j)), eType.toString());
                                if (j == 0) {
                                    logger.info("collector sent uemobility with time:{}", ueMobilities.get(j).getTs());
                                }
                                nefDataListenerSignals.startSending(eType);
                            } catch (Exception e) {
                                logger.error("Failed to send uemobility info to kafka", e);
                                nefDataListenerSignals.stop();
                                continue;
                            }
                        }
                        break;
                    default:
                        break;
                }
            }
            diff = (System.nanoTime() - start) / 1000000L;
            wait_time = (long) Constants.MIN_PERIOD_SECONDS * 1000L;
            if (diff < wait_time) {
                try {
                    Thread.sleep(wait_time - diff);
                } catch (InterruptedException e) {
                    logger.error("Failed to wait for timeout", e);
                    nefDataListenerSignals.stop();
                    continue;
                }
            }
            logger.info("NEF request delay = {}ms", nef_delay);
            logger.info("data coll total delay = {}ms", diff);
        }
        logger.info("NEF Data Collection stopped!");
    }

    private void stopUEsAndReplaceDefaultScenario(NefRequestBuilder nefRequestBuilder, boolean stoppedActiveUEs) {
        try {
            List<UeMobility> activeUEs = nefRequestBuilder.getUEMetrics(NwdafEvent.NwdafEventEnum.UE_MOBILITY,
                    nefUrl + nefStateUes, groupId, objectMapper,
                    defaultExportedScenario != null ? defaultExportedScenario.getCells() : null);
            if (activeUEs != null && !activeUEs.isEmpty()) {
                List<String> activeSUPIs = activeUEs.stream().map(UeMobility::getSupi).toList();
                stoppedActiveUEs = stopMovementLoop(nefRequestBuilder, activeSUPIs);
            } else {
                stoppedActiveUEs = true;
            }
        } catch (Exception e) {
            logger.error("Failed to stop movement loop", e);
        }


        boolean success = stoppedActiveUEs && switchScenario(defaultScenarioIndex, objectMapper, nefRequestBuilder, nefUrl, token);
        if (!success) {
            log.error("Failed to switch scenario to: {}", selectedScenarioIndex);
        } else {
            log.info("Scenario switched to: {}", selectedScenarioIndex);
        }
    }

    private void startMovementLoop(NefRequestBuilder nefRequestBuilder) {
        List<String> ueSupiList = getUeSupiList(nefRequestBuilder);
        if (ueSupiList.isEmpty()) {
            return;
        }
        for (String supi : ueSupiList) {
            if (supi == null || supi.isEmpty()) {
                continue;
            }
            try {
                String body = "{\"supi\":\"" + supi + "\"}";
                ResponseEntity<String> response = nefRequestBuilder.getTemplate().exchange(
                        nefUrl + "ue_movement/start-loop", HttpMethod.POST,
                        nefRequestBuilder.setupPostRequest(token, body), String.class);
                if (response.getBody() != null && response.getBody().contains("Loop started")) {
                    logger.info("Started movement loop for supi: {}", supi);
                }
            } catch (RestClientException e) {
                logger.error("Failed to connect to nef 'startMovementLoop'", e);
            } catch (Exception e) {
                logger.error("Failed to startMovementLoop", e);
            }
        }
        isLoopStarted.set(true);
    }

    private boolean stopMovementLoop(NefRequestBuilder nefRequestBuilder, List<String> ueSupiList) throws InterruptedException {
        if (ueSupiList.isEmpty()) {
            return false;
        }

        for (String supi : ueSupiList) {
            if (supi == null || supi.isEmpty()) {
                continue;
            }
            try {
                String body = "{\"supi\":\"" + supi + "\"}";
                ResponseEntity<String> response = nefRequestBuilder.getTemplate().exchange(
                        nefUrl + "ue_movement/stop-loop", HttpMethod.POST,
                        nefRequestBuilder.setupPostRequest(token, body), String.class);
                if (response.getStatusCode().is2xxSuccessful() &&
                        response.getBody() != null &&
                        response.getBody().contains("Loop ended")) {
                    logger.info("Stopped movement loop for supi: {}", supi);
                } else {
                    return false;
                }
            } catch (RestClientException e) {
                logger.error("Failed to connect to nef 'stopMovementLoop'", e);
                return false;
            } catch (Exception e) {
                logger.error("Failed to stopMovementLoop", e);
                return false;
            }
            Thread.sleep(50L);
        }
        isLoopStarted.set(false);
        return true;
    }

    private List<String> getUeSupiList(NefRequestBuilder nefRequestBuilder) {
        List<String> ueSupiList = new ArrayList<>();
        try {
            String result = nefRequestBuilder.getTemplate().exchange(
                    nefUrl + "UEs", HttpMethod.GET,
                    nefRequestBuilder.setupGetRequest(token), String.class).getBody();

            List<?> ueList = objectMapper.reader().readValue(result, List.class);
            ueSupiList.addAll(
                    ueList.stream()
                            .map(ue -> objectMapper.convertValue(ue, NefUe.class))
                            .filter(nefUe -> nefUe != null && nefUe.getSupi() != null)
                            .map(NefUe::getSupi)
                            .toList()
            );
        } catch (IOException e) {
            logger.error("Failed to parse nef 'getUeSupiList' response", e);
        } catch (RestClientException e) {
            logger.error("Failed to connect to nef 'getUeSupiList'", e);
        } catch (Exception e) {
            logger.error("Failed to get ueSupiList", e);
        }
        return ueSupiList;
    }

    private String getNefAccessToken(NefRequestBuilder nefRequestBuilder, String username, String password) {
        String token = null;
        try {
            MultiValueMap<String, String> requestBody = new LinkedMultiValueMap<>();
            requestBody.add("username", username);
            requestBody.add("password", password);
            String response = nefRequestBuilder.getTemplate().exchange(
                    nefUrl + "login/access-token", HttpMethod.POST,
                    nefRequestBuilder.setupPostRequest(null, requestBody), String.class).getBody();
            NefAccessToken nefAccessToken = objectMapper.readValue(response, NefAccessToken.class);
            token = toOptional(nefAccessToken).map(NefAccessToken::getAccess_token).orElse(null);
        } catch (RestClientException e) {
            logger.error("Failed to connect to nef 'login/acess-token'", e);
        } catch (Exception e) {
            logger.error("Failed to get nef access token", e);
        }
        return token;
    }

    public static boolean switchScenario(int scenarioIndex,
                                         ObjectMapper objectMapper,
                                         NefRequestBuilder nefRequestBuilder,
                                         String nefUrl,
                                         String token) {

        InputStream nefScenariosStream = Main.class.getResourceAsStream("/nefScenarios.json");
        List<NefScenario> nefScenarios;
        try {
            nefScenarios = objectMapper.readValue(nefScenariosStream, new TypeReference<>() {
            });
        } catch (IOException e) {
            return false;
        }

        if (nefScenarios.isEmpty() || scenarioIndex < 0 || scenarioIndex >= nefScenarios.size()) {
            return false;
        }

        selectedScenarioIndex = scenarioIndex;
        selectedScenario = nefScenarios.get(selectedScenarioIndex);

        try {
            ResponseEntity<String> response = nefRequestBuilder.getTemplate().exchange(
                    nefUrl + "utils/import/scenario", HttpMethod.POST,
                    nefRequestBuilder.setupPostRequest(token, selectedScenario), String.class);
            if (!response.getStatusCode().is2xxSuccessful()) {
                throw new RestClientException("Failed to import NEF scenario with status code: " +
                        response.getStatusCode());
            }
        } catch (Exception e) {
            log.error("Failed to import NEF scenario {} with error: {}", selectedScenarioIndex, e.getMessage());
            return false;
        }
        return true;
    }

    public static NefScenario getDefaultScenario(NefRequestBuilder nefRequestBuilder,
                                                 String nefUrl,
                                                 String token) {
        try {
            return nefRequestBuilder.getTemplate().exchange(
                    nefUrl + "utils/export/scenario", HttpMethod.GET,
                    nefRequestBuilder.setupGetRequest(token), NefScenario.class).getBody();
        } catch (Exception e) {
            log.error("Failed to get default NEF scenario with error: {}", e.getMessage());
            return null;
        }
    }

    @Scheduled(fixedDelay = 10000, initialDelay = 250)
    public void updateNefScenario() throws IOException {
        if (!allowNefData) {
            return;
        }
        if (token == null) {
            token = getNefAccessToken(new NefRequestBuilder(null, trustStore), username, password);
            if (token == null) {
                log.error("Failed to refresh NEF access token");
                return;
            }
        }
        NefScenario result = getDefaultScenario(new NefRequestBuilder(token, trustStore), nefUrl, token);

        if (result == null) {
            log.error("Failed to get default scenario");
            return;
        }

        selectedScenario = result;

        kafkaProducer.sendMessage(objectMapper.writeValueAsString(result), KafkaTopic.NEF_SCENARIO.name());
    }
}
