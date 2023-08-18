# On your terminal, run the following to build the image:
# mvn clean package -Dquick

FROM debezium/connect:1.9.5.Final

# Create the directories for the connectors to be placed into
ENV KAFKA_CONNECT_YB_DIR=$KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-yugabytedb
RUN mkdir $KAFKA_CONNECT_YB_DIR && cd $KAFKA_CONNECT_YB_DIR

# Copy the Debezium Connector for YugabyteDB
COPY target/debezium-connector-yugabytedb-*.jar $KAFKA_CONNECT_YB_DIR/

# Set the TLS version to be used by Kafka processes
ENV KAFKA_OPTS="-Djdk.tls.client.protocols=TLSv1.2"

# Add the required jar files to be packaged with the base connector
RUN cd $KAFKA_CONNECT_YB_DIR && curl -sLo kafka-connect-jdbc-10.6.5.jar https://github.com/yugabyte/kafka-connect-jdbc/releases/download/10.6.5-CUSTOM/kafka-connect-jdbc-10.6.5.jar
RUN cd $KAFKA_CONNECT_YB_DIR && curl -sLo jdbc-yugabytedb-42.3.5-yb-1.jar https://repo1.maven.org/maven2/com/yugabyte/jdbc-yugabytedb/42.3.5-yb-1/jdbc-yugabytedb-42.3.5-yb-1.jar
RUN cd $KAFKA_CONNECT_YB_DIR && curl -sLo mysql-connector-java-8.0.30.jar https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.30/mysql-connector-java-8.0.30.jar
RUN cd $KAFKA_CONNECT_YB_DIR && curl -sLo postgresql-42.5.1.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.1/postgresql-42.5.1.jar

# Add Jmx agent and metrics pattern file to expose the metrics info
RUN mkdir /kafka/etc && cd /kafka/etc && curl -so jmx_prometheus_javaagent-0.17.2.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.17.2/jmx_prometheus_javaagent-0.17.2.jar

ADD metrics.yml /etc/jmx-exporter/

ENV CLASSPATH=$KAFKA_HOME

