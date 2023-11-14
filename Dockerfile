FROM eclipse-temurin:21-jre-alpine
VOLUME /tmp
EXPOSE 8091 5006
ARG JAR_FILE=target/nwdaf_sub_collector-0.0.1-SNAPSHOT.jar
ADD ${JAR_FILE} app.jar
ENTRYPOINT ["java","-Xdebug","-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5006","-jar","/app.jar"]