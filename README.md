# Ingesting YugabyteDB change events

This module defines the connector that ingests change events from YugabyteDB databases.

## Using the YugabyteDB connector with Kafka Connect

The YugabyteDB connector is designed to work with [Kafka Connect](http://kafka.apache.org/documentation.html#connect) and to be deployed to a Kafka Connect runtime service. The deployed connector will monitor one or more schemas within a database server and write all change events to Kafka topics, which can be independently consumed by one or more clients. Kafka Connect can be distributed to provide fault tolerance to ensure the connectors are running and continually keeping up with changes in the database.

Kafka Connect can also be run standalone as a single process, although doing so is not tolerant of failures.

## Embedding the YugabyteDB connector

The YugabyteDB connector can also be used as a library without Kafka or Kafka Connect, enabling applications and services to directly connect to a YugabyteDB database and obtain the ordered change events. This approach requires the application to record the progress of the connector so that upon restart the connect can continue where it left off. Therefore, this may be a useful approach for less critical use cases. For production use cases, we highly recommend using this connector with Kafka and Kafka Connect.

## Building the connector jar

  1. Navigate to the debezium repository. This will assume that you are inside the parent repository of this connector.
  2. Build the jar files using maven
      
      ```sh
      mvn clean package -Dquick
      ```

  3. Create a custom folder for the custom-connector
      
      ```sh
      mkdir ~/custom-connector
      ```

  4. Copy the debezium-connector-yugabytedb2 jar to the custom-connector directory
      
      ```sh
      cp target/debezium-connector-yugabytedb-*.jar ~/custom-connector/
      ```

  5. Navigate to the directory you created
      
      ```sh
      cd ~/custom-connector
      ```

  6. Download the Kafka-Connect-JDBC jar file
      
      ```sh
      wget https://packages.confluent.io/maven/io/confluent/kafka-connect-jdbc/10.2.5/kafka-connect-jdbc-10.2.5.jar
      ```

  7. Create a Dockerfile 
      
      ```sh
      vi Dockerfile
      ```
      
      Now copy the following contents to the Dockerfile:
      
      ```Dockerfile
      FROM debezium/connect:1.6
      ENV KAFKA_CONNECT_YB_DIR=$KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-yugabytedb

      # Deploy Kafka Connect yugabytedb
      RUN mkdir $KAFKA_CONNECT_YB_DIR && cd $KAFKA_CONNECT_YB_DIR

      COPY debezium-connector-yugabytedb-*.jar \
      $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-yugabytedb/

      COPY kafka-connect-jdbc-10.2.5.jar $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-yugabytedb
      
      ENV KAFKA_OPTS="-Djdk.tls.client.protocols=TLSv1.2"
      ```

  8. Build the image
      ```sh
      docker build . -t yb-test-connector
      ```
