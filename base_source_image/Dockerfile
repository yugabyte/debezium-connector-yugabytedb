# =============================================================================
# YugabyteDB Debezium Connect Base Image
# =============================================================================
#
# Purpose
# -------
# Provides a Kafka Connect runtime with Confluent serialization libraries,
# replacing the upstream `debezium/connect:1.9.5.Final` image. Rolling our
# own base lets us pin JDK 21, Kafka 3.9.2, Ubuntu 22.04 (Jammy), and a
# non-root `kafka` user (UID 1001), and patch CVEs on our own cadence.
#
# Image contents
# --------------
#   - Eclipse Temurin JDK 21 (JRE) on Ubuntu 22.04
#   - Apache Kafka ${KAFKA_VERSION} (Scala ${SCALA_VERSION})
#   - Confluent serialization libs ${CONFLUENT_VERSION}
#     (avro-converter, avro-data, avro-serializer, schema-serializer,
#      schema-converter, schema-registry-client, common-config, common-utils)
#   - Avro, Guava, logredactor, and the transitive deps Confluent requires
#     (commons-codec, commons-compress, re2j, minimal-json, snakeyaml, etc.)
#   - Debezium 1.9 kafka-connect-start.sh fetched at build time from the
#     upstream debezium/container-images repo.
#
# Where this image fits
# ---------------------
#   eclipse-temurin:21-jre-jammy            (public base)
#         |
#         v
#   quay.io/yugabyte/debezium-connect-base  (this Dockerfile)
#         |
#         v
#   quay.io/yugabyte/debezium-connector     (top-level Dockerfile, layers
#                                            the YugabyteDB connector jar)
#
# When to rebuild
# ---------------
# Rebuild and push this image only when its contents change, e.g.:
#   - Kafka, Confluent, JDK, or Ubuntu version bump
#   - New jar added to $KAFKA_HOME/libs
#   - System package change (apt-get line)
#   - Security patch
# The connector jar itself does NOT require rebuilding the base — the
# top-level Dockerfile only COPYs the connector jar on top.
#
# Build and push (from repo root)
# -------------------------------
#   docker build -f base_source_image/Dockerfile \
#       -t quay.io/yugabyte/debezium-connect-base:latest .
#   docker push quay.io/yugabyte/debezium-connect-base:latest
#
# Local smoke test
# --------------------------------------------------------
#   docker run --rm --entrypoint sh \
#       quay.io/yugabyte/debezium-connect-base:latest \
#       -c 'ls $KAFKA_HOME/libs | grep -E "kafka-connect-avro|avro-|guava-"'
#
# =============================================================================

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
#   - KAFKA_CONNECT_PLUGINS: staging volume; entrypoint copies from here into KAFKA_CONNECT_PLUGINS_DIR
#
RUN mkdir -p "$KAFKA_CONNECT_PLUGINS_DIR" "$KAFKA_CONNECT_PLUGINS" "$KAFKA_DATA"

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
    curl -sfLO "https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/${CONFLUENT_VERSION}/kafka-schema-converter-${CONFLUENT_VERSION}.jar" && \
    curl -sfLO "https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/${CONFLUENT_VERSION}/kafka-schema-registry-client-${CONFLUENT_VERSION}.jar" && \
    curl -sfLO "https://packages.confluent.io/maven/io/confluent/common-config/${CONFLUENT_VERSION}/common-config-${CONFLUENT_VERSION}.jar" && \
    curl -sfLO "https://packages.confluent.io/maven/io/confluent/common-utils/${CONFLUENT_VERSION}/common-utils-${CONFLUENT_VERSION}.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/org/apache/avro/avro/${AVRO_VERSION}/avro-${AVRO_VERSION}.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/com/google/guava/guava/33.4.0-jre/guava-33.4.0-jre.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.2/failureaccess-1.0.2.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/com/google/guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/org/checkerframework/checker-qual/3.43.0/checker-qual-3.43.0.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/com/google/j2objc/j2objc-annotations/3.0.0/j2objc-annotations-3.0.0.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/com/eclipsesource/minimal-json/minimal-json/0.9.5/minimal-json-0.9.5.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/com/google/re2j/re2j/1.6/re2j-1.6.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/org/yaml/snakeyaml/2.0/snakeyaml-2.0.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/io/confluent/logredactor/1.0.12/logredactor-1.0.12.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/io/confluent/logredactor-metrics/1.0.12/logredactor-metrics-1.0.12.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/commons-codec/commons-codec/1.16.1/commons-codec-1.16.1.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.26.1/commons-compress-1.26.1.jar"

#
# Debezium 3.x SMTs placed on the system classpath so they're discoverable by Kafka Connect
# across all plugins.
#
ARG DEBEZIUM_SMT_VERSION=3.5.1.Final
RUN cd $KAFKA_HOME/libs && \
    curl -sfLO "https://repo1.maven.org/maven2/io/debezium/debezium-connect-plugins/${DEBEZIUM_SMT_VERSION}/debezium-connect-plugins-${DEBEZIUM_SMT_VERSION}.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/io/debezium/debezium-connector-common/${DEBEZIUM_SMT_VERSION}/debezium-connector-common-${DEBEZIUM_SMT_VERSION}.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/io/debezium/debezium-config/${DEBEZIUM_SMT_VERSION}/debezium-config-${DEBEZIUM_SMT_VERSION}.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/io/debezium/debezium-util/${DEBEZIUM_SMT_VERSION}/debezium-util-${DEBEZIUM_SMT_VERSION}.jar" && \
    curl -sfLO "https://repo1.maven.org/maven2/io/debezium/debezium-api/${DEBEZIUM_SMT_VERSION}/debezium-api-${DEBEZIUM_SMT_VERSION}.jar"

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

# Pin stdout appender threshold to INFO (matches upstream debezium/connect).
# Stops PUT /admin/loggers->DEBUG from flooding stdout and stalling the connector,
# while still letting WARN/ERROR through.
RUN for f in $KAFKA_HOME/config.orig/log4j.properties $KAFKA_HOME/config.orig/connect-log4j.properties; do \
        if [ -f "$f" ] && ! grep -q '^log4j.appender.stdout.threshold' "$f"; then \
            sed -i '/^log4j.appender.stdout=org.apache.log4j.ConsoleAppender/a log4j.appender.stdout.threshold=INFO' "$f"; \
        fi; \
    done

# Remove unnecessary files (sources, javadoc, scaladoc JARs)
RUN rm -f $KAFKA_HOME/libs/*-{sources,javadoc,scaladoc}.jar*

#
# Allow random UID to use Kafka
#
RUN chmod -R g+w,o+w $KAFKA_HOME && \
    chown -R 1001:kafka $KAFKA_HOME

WORKDIR $KAFKA_HOME

EXPOSE 8083
# NOTE: $KAFKA_CONNECT_PLUGINS_DIR is intentionally NOT declared as a VOLUME here.
# The classic Docker builder discards modifications to VOLUME paths made by RUN
# commands in child images, which broke fabric8 docker-maven-plugin builds that
# curl additional connector jars into the plugin dir.
VOLUME ["${KAFKA_HOME}/config"]

#
# Copy entrypoint script and switch user
#
COPY base_source_image/docker-entrypoint.sh /
RUN chmod 755 /docker-entrypoint.sh && chmod -R 755 /scripts
USER kafka
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["kafka-connect"]
