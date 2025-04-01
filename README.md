# Ingesting YugabyteDB change events

This module defines the connector that ingests change events from YugabyteDB databases.

## Using the YugabyteDB connector with Kafka Connect

The YugabyteDB connector is designed to work with [Kafka Connect](http://kafka.apache.org/documentation.html#connect) and to be deployed to a Kafka Connect runtime service. The deployed connector will monitor one or more schemas within a database server and write all change events to Kafka topics, which can be independently consumed by one or more clients. Kafka Connect can be distributed to provide fault tolerance to ensure the connectors are running and continually keeping up with changes in the database.

Kafka Connect can also be run standalone as a single process, although doing so is not tolerant of failures.

## Embedding the YugabyteDB connector

The YugabyteDB connector can also be used as a library without Kafka or Kafka Connect, enabling applications and services to directly connect to a YugabyteDB database and obtain the ordered change events. This approach requires the application to record the progress of the connector so that upon restart the connect can continue where it left off. Therefore, this may be a useful approach for less critical use cases. For production use cases, we highly recommend using this connector with Kafka and Kafka Connect.

## Building the connector jar

1. Navigate inside the repository.
2. Build the jar files using maven, note that this step will also generate a docker image with the connector
      
    ```sh
    mvn clean package -Dquick
    ```

    The docker image will be tagged as:
    ```
    quay.io/yugabyte/debezium-connector:latest
    ```
    The above mentioned image is nothing but a Kafka Connect image bundled with the Debezium Connector for YugabyteDB.

## Quick start

1. Start Zookeeper:
  ```sh
  docker run -it --rm --name zookeeper -p 2181:2181 -p 2888:2888 -p 3888:3888 debezium/zookeeper:1.9.5.Final
  ```
2. Start Kafka:
  ```sh
  docker run -it --rm --name kafka -p 9092:9092 --link zookeeper:zookeeper debezium/kafka:1.9.5.Final
  ```
3. Assign your machine's IP to an environment variable:
  ```sh
  # macOS:
  export IP=$(ipconfig getifaddr en0)

  # Linux:
  export IP=$(hostname -i)
  ```
4. Start a cluster using yugabyted. Note that you need to run yugabyted with the IP of your machine; otherwise, it would consider localhost (which would be mapped to the docker host instead of your machine). The yugabyted binary along with other required binaries can be downloaded from [download.yugabyte.com](https://download.yugabyte.com/).
  ```sh
  ./yugabyted start --advertise_address $IP
  ```
5. Connect using ysqlsh and create a table:
  ```
  ./bin/ysqlsh -h $IP
  
  create table test (id int primary key, name text, days_worked bigint);
  ```
6. Create a DB stream ID:
  ```
  ./yb-admin --master_addresses ${IP}:7100 create_change_data_stream ysql.yugabyte
  ```
7. Start Kafka Connect:
  ```sh
  docker run -it --rm \
    --name connect -p 8083:8083 -e GROUP_ID=1 \
    -e CONFIG_STORAGE_TOPIC=my_connect_configs \
    -e OFFSET_STORAGE_TOPIC=my_connect_offsets \
    -e STATUS_STORAGE_TOPIC=my_connect_statuses \
    --link zookeeper:zookeeper --link kafka:kafka \
        quay.io/yugabyte/debezium-connector:latest
  ```
8. Deploy the configuration for the connector:
  **NOTE: Do not forget to change the `database.streamid` with the value you obtained in step 6**
  ```sh
  curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
    localhost:8083/connectors/ \
    -d '{
    "name": "ybconnector",
    "config": {
        "connector.class": "io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector",
        "database.hostname":"'$IP'",
        "database.port":"5433",
        "database.master.addresses": "'$IP':7100",
        "database.user": "yugabyte",
        "database.password": "yugabyte",
        "database.dbname" : "yugabyte",
        "database.server.name": "dbserver1",
        "table.include.list":"public.test",
        "database.streamid":"d540f5e4890c4d3b812933cbfd703ed3",
        "snapshot.mode":"never"
    }
    }'
  ```
9. Start a Kafka console consumer:
  ```sh
  docker run -it --rm --name consumer --link zookeeper:zookeeper --link kafka:kafka debezium/kafka:1.9.5.Final \
  watch-topic -a dbserver1.public.test
  ```
