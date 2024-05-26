FROM eclipse-temurin:21-jre-alpine
VOLUME /tmp
ARG JAR_FILE=target/nwdaf_sub_collector-0.0.1-SNAPSHOT.jar
ARG SSL_FILE=src/main/resources/certificates/local-nef-ssl.p12
ARG CRT_FILE=src/main/resources/certificates/combined_nef.crt
ARG NEF_SCENARIOS_FILE=src/main/resources/nefScenarios.json
ADD ${JAR_FILE} app.jar
ADD ${SSL_FILE} src/main/resources/certificates/local-nef-ssl.p12
ADD ${CRT_FILE} /usr/local/share/ca-certificates/combined_nef.crt
ADD ${NEF_SCENARIOS_FILE} src/main/resources/nefScenarios.json
RUN chmod 644 /usr/local/share/ca-certificates/combined_nef.crt && update-ca-certificates