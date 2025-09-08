# On your terminal, run the following to build the image:
# mvn clean package -Dquick

FROM quay.io/debezium/connect:3.1.3.Final

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

RUN mkdir -p $KAFKA_CONNECT_YB_DIR/avro-supporting-jars/
WORKDIR $KAFKA_CONNECT_YB_DIR/avro-supporting-jars
RUN curl -sLo kafka-connect-avro-converter-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.5.3/kafka-connect-avro-converter-7.5.3.jar 
RUN curl -sLo kafka-connect-avro-data-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/7.5.3/kafka-connect-avro-data-7.5.3.jar
RUN curl -sLo kafka-avro-serializer-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.5.3/kafka-avro-serializer-7.5.3.jar
RUN curl -sLo kafka-schema-serializer-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/7.5.3/kafka-schema-serializer-7.5.3.jar 
RUN curl -sLo kafka-schema-converter-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/7.5.3/kafka-schema-converter-7.5.3.jar
RUN curl -sLo kafka-schema-registry-client-7.5.3.jar https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.5.3/kafka-schema-registry-client-7.5.3.jar 
RUN curl -sLo common-config-7.5.3.jar https://packages.confluent.io/maven/io/confluent/common-config/7.5.3/common-config-7.5.3.jar
RUN curl -sLo common-utils-7.5.3.jar https://packages.confluent.io/maven/io/confluent/common-utils/7.5.3/common-utils-7.5.3.jar 

RUN curl -sLo avro-1.11.3.jar https://repo1.maven.org/maven2/org/apache/avro/avro/1.11.3/avro-1.11.3.jar
RUN curl -sLo commons-compress-1.21.jar https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.21/commons-compress-1.21.jar
RUN curl -sLo failureaccess-1.0.jar https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0/failureaccess-1.0.jar
RUN curl -sLo guava-33.0.0-jre.jar https://repo1.maven.org/maven2/com/google/guava/guava/33.0.0-jre/guava-33.0.0-jre.jar 
RUN curl -sLo minimal-json-0.9.5.jar https://repo1.maven.org/maven2/com/eclipsesource/minimal-json/minimal-json/0.9.5/minimal-json-0.9.5.jar
RUN curl -sLo re2j-1.6.jar https://repo1.maven.org/maven2/com/google/re2j/re2j/1.6/re2j-1.6.jar 
RUN curl -sLo slf4j-api-1.7.36.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.36/slf4j-api-1.7.36.jar
RUN curl -sLo snakeyaml-2.0.jar https://repo1.maven.org/maven2/org/yaml/snakeyaml/2.0/snakeyaml-2.0.jar 
RUN curl -sLo swagger-annotations-2.1.10.jar https://repo1.maven.org/maven2/io/swagger/core/v3/swagger-annotations/2.1.10/swagger-annotations-2.1.10.jar 
RUN curl -sLo jackson-databind-2.14.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-databind/2.14.2/jackson-databind-2.14.2.jar 
RUN curl -sLo jackson-core-2.14.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-core/2.14.2/jackson-core-2.14.2.jar 
RUN curl -sLo jackson-annotations-2.14.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/core/jackson-annotations/2.14.2/jackson-annotations-2.14.2.jar 
RUN curl -sLo jackson-dataformat-csv-2.14.2.jar https://repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-csv/2.14.2/jackson-dataformat-csv-2.14.2.jar 
RUN curl -sLo logredactor-1.0.12.jar https://repo1.maven.org/maven2/io/confluent/logredactor/1.0.12/logredactor-1.0.12.jar
RUN curl -sLo logredactor-metrics-1.0.12.jar https://repo1.maven.org/maven2/io/confluent/logredactor-metrics/1.0.12/logredactor-metrics-1.0.12.jar
WORKDIR /

# Add Jmx agent and metrics pattern file to expose the metrics info
RUN mkdir /kafka/etc && cd /kafka/etc && curl -so jmx_prometheus_javaagent-0.17.2.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.17.2/jmx_prometheus_javaagent-0.17.2.jar

ADD metrics.yml /etc/jmx-exporter/

ENV CLASSPATH=$KAFKA_HOME

