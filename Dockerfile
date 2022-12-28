# On your terminal, run the following to build the image:
#  - mvn clean package -Dquick

# The base image is derived from debezium/connect:1.7 and contains the following drivers
# and connectors prepopulated:
#  - JDBC Sink Connector v10.6.0
#  - YugabyteDB JDBC Driver v42.3.5-yb-1
#  - MySql JDBC Driver v8.0.31
#  - PostgreSQL JDBC Driver v42.5.1
FROM quay.io/yugabyte/connect-base-yb:0.4

# Create the directories for the connectors to be placed into
ENV KAFKA_CONNECT_YB_DIR=$KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-yugabytedb
RUN mkdir $KAFKA_CONNECT_YB_DIR && cd $KAFKA_CONNECT_YB_DIR

# Copy the Debezium Connector for YugabyteDB
COPY target/debezium-connector-yugabytedb-*.jar $KAFKA_CONNECT_YB_DIR/

# Set the TLS version to be used by Kafka processes
ENV KAFKA_OPTS="-Djdk.tls.client.protocols=TLSv1.2"

# Add Jmx agent and metrics pattern file to expose the metrics info
RUN mkdir /kafka/etc && cd /kafka/etc && curl -so jmx_prometheus_javaagent-0.17.2.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.17.2/jmx_prometheus_javaagent-0.17.2.jar

ADD metrics.yml /etc/jmx-exporter/
