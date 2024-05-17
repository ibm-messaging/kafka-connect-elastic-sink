FROM quay.io/strimzi/kafka:latest-kafka-3.6.1 as kafka

FROM ibm-semeru-runtimes:open-17-jre

RUN addgroup --gid 5000 --system esgroup && \
    adduser --uid 5000 --ingroup esgroup --system esuser

# Copy over the files we need to run a Kafka connector
COPY --chown=esuser:esgroup --from=kafka /opt/kafka/bin/ /opt/kafka/bin/
COPY --chown=esuser:esgroup --from=kafka /opt/kafka/libs/ /opt/kafka/libs/
COPY --chown=esuser:esgroup --from=kafka /opt/kafka/config/connect-log4j.properties /opt/kafka/config/
RUN mkdir /opt/kafka/logs && chown esuser:esgroup /opt/kafka/logs

# These are the key files needed from this repository to get something running. The hostnames in the
# properties files reflect the docker-compose names
COPY --chown=esuser:esgroup target/kafka-connect-elastic-sink-*-jar-with-dependencies.jar /opt/kafka/libs/
COPY --chown=esuser:esgroup quickstart/elastic-sink-qs.properties /opt/kafka/config
COPY --chown=esuser:esgroup quickstart/connect-standalone-qs.properties /opt/kafka/config/

WORKDIR /opt/kafka

USER esuser

ENTRYPOINT ["./bin/connect-standalone.sh", "config/connect-standalone-qs.properties", "config/elastic-sink-qs.properties"]
