# Ingesting YugabyteDB change events

This module defines the connector that ingests change events from YugabyteDB databases.

## Using the YugabyteDB connector with Kafka Connect

The YugabyteDB connector is designed to work with [Kafka Connect](http://kafka.apache.org/documentation.html#connect) and to be deployed to a Kafka Connect runtime service. The deployed connector will monitor one or more schemas within a database server and write all change events to Kafka topics, which can be independently consumed by one or more clients. Kafka Connect can be distributed to provide fault tolerance to ensure the connectors are running and continually keeping up with changes in the database.

Kafka Connect can also be run standalone as a single process, although doing so is not tolerant of failures.

## Embedding the YugabyteDB connector

The YugabyteDB connector can also be used as a library without Kafka or Kafka Connect, enabling applications and services to directly connect to a YugabyteDB database and obtain the ordered change events. This approach requires the application to record the progress of the connector so that upon restart the connect can continue where it left off. Therefore, this may be a useful approach for less critical use cases. For production use cases, we highly recommend using this connector with Kafka and Kafka Connect.

## Prerequisites

- **JDK 21** is required to build the project. The compiled bytecode targets Java 17.

## Building the connector jar

1. Navigate inside the repository.
2. Build the jar files using maven, note that this step will also generate a docker image with the connector
      
    ```sh
    mvn clean package -Dquick
    ```

    The docker image will be tagged as:
    ```
    quay.io/yugabyte/debezium-connector:dz.1.9.5.yb.grpc.2025.2-SNAPSHOT
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
        quay.io/yugabyte/debezium-connector:dz.1.9.5.yb.grpc.2025.2-SNAPSHOT
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

## Polling configuration

Each task polls its tablets with `GetChanges`, then pauses before the next pass over them.

| Property | Default | Description |
|---|---|---|
| `cdc.poll.interval.active.ms` | `10` | Pause before the next pass when any tablet in the last pass returned changes. |
| `cdc.poll.interval.idle.ms` | `500` | Pause before the next pass when no tablet returned changes. |
| `cdc.poll.max.concurrent.tablets` | `1` | How many of the task's tablets can have a `GetChanges` request in flight at once (1–32). At `1` a pass costs one round trip per tablet; above `1` the next tablets' requests are sent while the current tablet's response is awaited and processed, so a pass costs about one round trip. Responses are still processed one tablet at a time, in order. Values around 8–16 capture most of the gain when tablets per task are high or the connector is far from the tservers. Ignored with `transaction.ordering` enabled. |
| `cdc.poll.max.inflight.bytes` | `0` | With concurrency above `1`, sets each `GetChanges` request's response size limit to this budget divided by the concurrency (at least 64 KiB), bounding the memory a task's buffered responses use. The value replaces the tserver limit (`cdc_stream_records_threshold_size_bytes`, 4 MiB by default) rather than lowering it, so a budget above concurrency × 4 MiB allows larger responses. `0` keeps the tserver limit. |

### Scaling tasks automatically

With `tasks.autoscale.enabled=true` the connector chooses how many of up to `tasks.max` tasks to run, and which tablets each one polls. Every `tasks.autoscale.interval.ms` it reads, for each tablet of the stream, the latest WAL index from the tablet's leader and the stored CDC checkpoint from the master, then:

- runs `max(write rate ÷ tasks.autoscale.target.ops.per.task, tablets ÷ max tablets per task, tasks.autoscale.min)` tasks, where max tablets per task is `tasks.autoscale.max.tablets.per.task`, lowered to `cdc.poll.max.concurrent.tablets` when that is above 1 so no task gets more tablets than it keeps requests in flight for (at 1 the configured cap applies and a warning is logged), plus one while the backlog is larger than `tasks.autoscale.lag.seconds` of the highest write rate seen in that time (or of the target per task, whichever is higher) and still growing;
- places tablets on tasks by write rate, so busy tablets are spread;
- reconfigures again when tablets split.

Scaling up waits for `tasks.autoscale.scale.up.evaluations` evaluations (default 20, about 20 minutes of sustained demand) and `tasks.autoscale.scale.up.cooldown.ms` (30 min); scaling down waits for `tasks.autoscale.scale.down.evaluations` (120, about 2 hours) and `tasks.autoscale.scale.down.cooldown.ms` (60 min), and only to a count that carries the peak of the whole low stretch (leaving out its first sample, which can straddle the drop) at `tasks.autoscale.scale.down.headroom.percent` (60%) of the target. A backlog that is large and still growing scales up without waiting for the evaluations. Every reconfiguration restarts all of the connector's tasks, which replay events since the last offset commit (at-least-once); under load that costs p99 of tens of seconds for a minute or two, which is why the defaults only react to sustained changes. Replayed against several days of bursty per-minute write rates, they reconfigure about once a day per connector. Rates are in WAL operations, not events: a single-row update is about two operations. When the connector itself restarts or moves to another worker, the first plan is built from two samples 10 s apart without the scale-up evaluations, so task configs usually change and the tasks restart once more. Until that first plan exists, the connector runs as few tasks as the tablet cap and `tasks.autoscale.min` allow. If sampling produces no plan within six attempts, or the autoscaler thread stops before its first plan, it runs `tasks.max` tasks as without autoscaling until a plan arrives. A split parent's tablet is left out of the samples from the split until its children are polled, so its writes and backlog are briefly not counted.

The pipelining metrics of a task are exported as `debezium.yugabytedb:type=connector-metrics,server=<server>,task=<task>,context=getchanges` (in-flight requests, requests issued, reused and abandoned, last round trip, last pass duration).

Compatibility of `cdc.poll.max.concurrent.tablets` above `1`:

- Server: any version that serves the gRPC CDC `GetChanges` API; tested against 2025.2.8.0-b60. `cdc.poll.max.inflight.bytes` sets the request's `getchanges_resp_max_size_bytes`, which the tserver honours while its `enable_cdcsdk_setting_get_changes_response_byte_limit` flag is on (the default).
- yb-client 0.8.122 and older (and the client shipped with 2025.2.8) close a tserver connection only when nothing has been read from it for `socket.read.timeout.ms`. With several requests on one connection, the other tablets' responses keep it open, so a single lost response is never timed out by the client. The connector therefore waits at most `socket.read.timeout.ms` (which must then be positive) at a time on each response when pipelining, moving on to the task's other tablets in between, and sends a new request for the tablet only once the old one is older than `operation.timeout.ms`, when yb-client has stopped retrying it. yb-client 0.8.123 and newer time out each request on its own.
- A tablet never has two requests outstanding: the tserver stores the checkpoint of each request it serves without comparing it to the stored one, so a late older request could move the tablet's checkpoint backwards.
