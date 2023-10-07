FROM eclipse-temurin:17-jre-alpine
VOLUME /tmp
EXPOSE 8091
ADD target/nwdaf_sub_collector bin
ENTRYPOINT ["bin" ]