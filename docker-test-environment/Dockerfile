FROM maven:3-ibm-semeru-17-focal AS build
WORKDIR /app
COPY pom.xml pom.xml
COPY src ./src
COPY LICENSE LICENSE
RUN mvn clean package

FROM apache/kafka:4.0.0
WORKDIR /opt/kafka
RUN mkdir -p /opt/kafka/plugins/kafka-connect-elastic-sink
COPY --from=build /app/target/*-jar-with-dependencies.jar /opt/kafka/plugins/kafka-connect-elastic-sink/
RUN mkdir -p /tmp/config
COPY docker-test-environment/config/connect-distributed.properties /tmp/config/