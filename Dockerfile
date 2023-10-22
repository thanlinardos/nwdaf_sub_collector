FROM eclipse-temurin:17-jre-alpine
VOLUME /tmp
EXPOSE 8091
ARG JAR_FILE=target/nwdaf_sub_collector-0.0.1-SNAPSHOT.jar
ADD ${JAR_FILE} app.jar
ENTRYPOINT ["java","-jar","/app.jar"]