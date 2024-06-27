FROM flink-node-poc:latest

MAINTAINER PSNC
COPY ./docker/flink-node-with-application/flink-connector-cassandra_2.12-3.1.0-1.17.jar /opt/flink/lib/
COPY ./docker/flink-node-with-application/metrics-core-3.0.2.jar /opt/flink/lib/
#COPY ./target/flink-poc.jar /opt/flink/lib/
COPY ./flinkPoCPG/target/flink-poc-postgres.jar /opt/flink/lib/
