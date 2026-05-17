# On your terminal, run the following to build the image:
# mvn clean package -Dquick

FROM yugabyte/debezium-connect-base:latest

# Create the directories for the connectors to be placed into
ENV KAFKA_CONNECT_YB_DIR=$KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-yugabytedb
RUN mkdir $KAFKA_CONNECT_YB_DIR && cd $KAFKA_CONNECT_YB_DIR

# Copy the Debezium Connector for YugabyteDB
COPY --chown=kafka:kafka target/debezium-connector-yugabytedb-*.jar $KAFKA_CONNECT_YB_DIR/

# Set the TLS version to be used by Kafka processes
ENV KAFKA_OPTS="-Djdk.tls.client.protocols=TLSv1.2"

# Add the required jar files to be packaged with the base connector.
RUN cd $KAFKA_CONNECT_YB_DIR && curl -sfLo kafka-connect-jdbc-10.6.5.jar https://github.com/yugabyte/kafka-connect-jdbc/releases/download/10.6.5-CUSTOM/kafka-connect-jdbc-10.6.5.jar
RUN cd $KAFKA_CONNECT_YB_DIR && curl -sfLo jdbc-yugabytedb-42.3.5-yb-1.jar https://repo1.maven.org/maven2/com/yugabyte/jdbc-yugabytedb/42.3.5-yb-1/jdbc-yugabytedb-42.3.5-yb-1.jar
RUN cd $KAFKA_CONNECT_YB_DIR && curl -sfLo mysql-connector-j-9.2.0.jar https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.2.0/mysql-connector-j-9.2.0.jar
RUN cd $KAFKA_CONNECT_YB_DIR && curl -sfLo postgresql-42.7.7.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar

# Add JMX Prometheus agent and metrics pattern file to expose metrics
RUN mkdir -p $KAFKA_HOME/etc && cd $KAFKA_HOME/etc && curl -sfo jmx_prometheus_javaagent-1.0.1.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/1.0.1/jmx_prometheus_javaagent-1.0.1.jar

ADD metrics.yml /etc/jmx-exporter/

ENV CLASSPATH=$KAFKA_HOME
