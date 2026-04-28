# YugabyteDB Debezium Connect Base Image
#
# Provides a Kafka Connect runtime with Confluent serialization libraries.
# Based on Rapid Fort hardened Azul Zulu JDK 21 on Ubuntu 22.04 (Jammy).
#
# Build (from repo root):
#   docker build -f base_source_image/Dockerfile -t yugabyte/debezium-connect-base:latest .

FROM eclipse-temurin:21-jre-jammy

LABEL maintainer="yugabyte" \
      debezium.version="1.9.5.Final"

#
# Set the Debezium version, Kafka version, and Scala version (needed for download URL).
#
ARG DEBEZIUM_VERSION=1.9
ARG KAFKA_VERSION=3.9.2
ARG SCALA_VERSION=2.13

#
# Kafka home and plugin directories
#
ENV KAFKA_HOME=/opt/kafka \
    KAFKA_DATA=/opt/kafka/data
ENV KAFKA_CONNECT_PLUGINS_DIR=$KAFKA_HOME/connect \
    KAFKA_CONNECT_PLUGINS=$KAFKA_HOME/new-connector-plugins \
    CONNECT_PLUGIN_PATH=$KAFKA_HOME/connect \
    KAFKA_VERSION=${KAFKA_VERSION} \
    SCALA_VERSION=${SCALA_VERSION}

#
# Install minimal packages (curl needed for healthchecks and JAR downloads)
#
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        iproute2 \
    && rm -rf /var/lib/apt/lists/*

#
# Create a user and home directory for Kafka
#
USER root

RUN groupadd -r kafka -g 1001 && \
    useradd -u 1001 -r -g kafka -m -d $KAFKA_HOME -s /sbin/nologin -c "Kafka user" kafka && \
    chmod -R 755 $KAFKA_HOME

#
# Download and install Apache Kafka
#
RUN mkdir -p /tmp/kafka-download && \
    curl -fsSL \
      "https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
      -o /tmp/kafka-download/kafka.tgz || \
    curl -fsSL \
      "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
      -o /tmp/kafka-download/kafka.tgz && \
    tar -xzf /tmp/kafka-download/kafka.tgz -C /tmp/kafka-download && \
    cp -r /tmp/kafka-download/kafka_${SCALA_VERSION}-${KAFKA_VERSION}/* $KAFKA_HOME/ && \
    rm -rf /tmp/kafka-download

#
# Create connector plugin directories:
#   - KAFKA_CONNECT_PLUGINS_DIR: where Kafka Connect discovers plugins at runtime
#   - KAFKA_CONNECT_PLUGINS: staging volume; entrypoint copies from here into connector-plugins
#   - connector-plugins: actual plugin path set in connect-distributed.properties
#
RUN mkdir -p "$KAFKA_CONNECT_PLUGINS_DIR" "$KAFKA_CONNECT_PLUGINS" "$KAFKA_HOME/connector-plugins" "$KAFKA_DATA"

#
# Confluent serialization libraries (for Avro / Schema Registry support)
# Confluent 7.9.x aligns with Kafka 3.9.x
#
ARG CONFLUENT_VERSION=7.9.0
ARG AVRO_VERSION=1.12.0

RUN cd $KAFKA_HOME/libs && \
    curl -sfLO "https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/${CONFLUENT_VERSION}/kafka-connect-avro-converter-${CONFLUENT_VERSION}.jar" && \
    curl -sfLO "https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/${CONFLUENT_VERSION}/kafka-connect-avro-data-${CONFLUENT_VERSION}.jar" && \
    curl -sfLO "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/${CONFLUENT_VERSION}/kafka-avro-serializer-${CONFLUENT_VERSION}.jar" && \
    curl -sfLO "https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/${CONFLUENT_VERSION}/kafka-schema-serializer-${CONFLUENT_VERSION}.jar" && \
    curl -sfLO "https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/${CONFLUENT_VERSION}/kafka-schema-registry-client-${CONFLUENT_VERSION}.jar" && \
    curl -sfLO "https://packages.confluent.io/maven/io/confluent/common-config/${CONFLUENT_VERSION}/common-config-${CONFLUENT_VERSION}.jar" && \
    curl -sfLO "https://packages.confluent.io/maven/io/confluent/common-utils/${CONFLUENT_VERSION}/common-utils-${CONFLUENT_VERSION}.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/org/apache/avro/avro/${AVRO_VERSION}/avro-${AVRO_VERSION}.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/com/google/guava/guava/33.4.0-jre/guava-33.4.0-jre.jar"

#
# Download the kafka-connect start script from upstream (handles CONNECT_* env var
# mapping, JMX setup, and launches connect-distributed.sh)
#
RUN mkdir /scripts
RUN curl -fsSL https://raw.githubusercontent.com/debezium/container-images/main/connect-base/${DEBEZIUM_VERSION}/docker-entrypoint.sh -o /scripts/kafka-connect-start.sh

#
# Preserve original config (entrypoint restores from here on startup)
#
RUN mkdir $KAFKA_HOME/config.orig && \
    mv $KAFKA_HOME/config/* $KAFKA_HOME/config.orig && \
    chown -R 1001:kafka $KAFKA_HOME/config.orig

# Remove unnecessary files (sources, javadoc, scaladoc JARs)
RUN rm -f $KAFKA_HOME/libs/*-{sources,javadoc,scaladoc}.jar*

#
# Allow random UID to use Kafka
#
RUN chmod -R g+w,o+w $KAFKA_HOME && \
    chown -R 1001:kafka $KAFKA_HOME

WORKDIR $KAFKA_HOME

EXPOSE 8083
VOLUME ["${KAFKA_HOME}/config", "${KAFKA_CONNECT_PLUGINS_DIR}"]

#
# Copy entrypoint script and switch user
#
COPY base_source_image/docker-entrypoint.sh /
RUN chmod 755 /docker-entrypoint.sh && chmod -R 755 /scripts
USER kafka
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["kafka-connect"]
