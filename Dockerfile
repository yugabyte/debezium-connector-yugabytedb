# The base image contains the following drivers and connectors prepopulated:
#  - JDBC Sink Connector v10.2.5
#  - YugabyteDB JDBC Driver v42.3.5-yb-1
#  - MySql JDBC Driver v8.0.21
FROM quay.io/yugabyte/connect-base-yb:0.1

# Create the directories for the connectors to be placed into
ENV KAFKA_CONNECT_YB_DIR=$KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-yugabytedb
RUN mkdir $KAFKA_CONNECT_YB_DIR && cd $KAFKA_CONNECT_YB_DIR

# Copy the Debezium Connector for YugabyteDB
COPY target/debezium-connector-yugabytedb-*.jar $KAFKA_CONNECT_YB_DIR/

# Set the TLS version to be used by Kafka processes
ENV KAFKA_OPTS="-Djdk.tls.client.protocols=TLSv1.2"