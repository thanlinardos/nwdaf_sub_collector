docker compose -f ../NWDAF_SUB/src/main/resources/compose_files/docker-compose.yml down nwdafSubCollector &&
docker compose -f ../NWDAF_SUB/src/main/resources/compose_files/docker-compose.yml down nwdafSubCollector2
docker compose -f ../NWDAF_SUB/src/main/resources/compose_files/docker-compose.yml down nwdafSubCollector3
cd ../nwdaf_library &&
mvn -DskipTests clean install &&
cd ../nwdaf_sub_collector &&
mvn -DskipTests clean install &&
docker build . --tag thanlinardos/nwdaf_sub_collector:0.0.1-SNAPSHOT &&
docker compose -f ../NWDAF_SUB/src/main/resources/compose_files/docker-compose.yml up nwdafSubCollector -d &&
docker compose -f ../NWDAF_SUB/src/main/resources/compose_files/docker-compose.yml up nwdafSubCollector2 -d
docker compose -f ../NWDAF_SUB/src/main/resources/compose_files/docker-compose.yml up nwdafSubCollector3 -d
docker logs nwdafSubCollector --follow