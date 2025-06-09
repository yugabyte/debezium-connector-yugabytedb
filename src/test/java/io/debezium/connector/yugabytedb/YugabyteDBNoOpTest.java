package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.yb.CommonTypes;
import org.yb.client.*;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests to validate that streaming is not affected nor is there any data loss when there are
 * operations to be streamed in combination with different failure/restart scenarios when there
 * are NO_OP messages as well.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class YugabyteDBNoOpTest extends YugabytedTestBase {
  @BeforeAll
  public static void beforeClass() throws SQLException {
    setTserverFlags("cdc_max_stream_intent_records=10");
    initializeYBContainer();
  }

  @BeforeEach
  public void before() {
    initializeConnectorTestFramework();
  }

  @AfterEach
  public void after() throws Exception {
    stopConnector();
    TestHelper.executeDDL("drop_tables_and_databases.ddl");
  }

  @AfterAll
  public static void afterClass() {
    shutdownYBContainer();
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void shouldNotBeAffectedByMultipleNoOps(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    // Start connector.
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // Insert records.
    TestHelper.execute("INSERT INTO t1 VALUES (generate_series(0, 99), 'Vaibhav', 'Kushwaha');");

    // Wait for some records to get checkpointed.
    TestHelper.waitFor(Duration.ofSeconds(10));

    LOGGER.info("Creating snapshot schedule on database: {}", DEFAULT_DB_NAME);
    YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress());
    CreateSnapshotScheduleResponse resp =
      ybClient.createSnapshotSchedule(CommonTypes.YQLDatabase.YQL_DATABASE_PGSQL, DEFAULT_DB_NAME, 15 * 60, 2 /* interval */);

    // Stop updating explicit checkpoint for some time.
    LOGGER.info("Stopping update of explicit checkpoint for 16 seconds");
    YugabyteDBStreamingChangeEventSource.TEST_UPDATE_EXPLICIT_CHECKPOINT = false;
    TestHelper.waitFor(Duration.ofSeconds(16));
    YugabyteDBStreamingChangeEventSource.TEST_UPDATE_EXPLICIT_CHECKPOINT = true;

    // Restart the connector.
    stopConnector();
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // Insert a few more records.
    TestHelper.execute("INSERT INTO t1 VALUES (generate_series(100, 199), 'Vaibhav', 'Kushwaha');");

    // Consume all the records.
    List<SourceRecord> records = new ArrayList<>();
    waitAndConsumeUnique(records, 200, 5 * 60 * 1000);

    Set<Integer> pk = new HashSet<>();
    for (SourceRecord record : records) {
      Struct value = (Struct) record.value();
      pk.add(value.getStruct("after").getStruct("id").getInt32("value"));
    }

    assertEquals(200, pk.size());

    // Delete the snapshot schedule at the end of the test.
    ybClient.deleteSnapshotSchedule(resp.getSnapshotScheduleUUID());
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void multiShardTransactionFollowedWithNoOps(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    // Start connector.
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // Execute a transaction.
    TestHelper.execute("INSERT INTO t1 VALUES (generate_series(1,500), 'Vaibhav', 'Kushwaha');");

    LOGGER.info("Creating snapshot schedule on database: {} with a retention of 15 minutes", DEFAULT_DB_NAME);
    YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress());
    CreateSnapshotScheduleResponse snapshotScheduleResponse =
      ybClient.createSnapshotSchedule(CommonTypes.YQLDatabase.YQL_DATABASE_PGSQL, DEFAULT_DB_NAME, 15 * 60, 2 /* interval */);

    // Wait for a minute and then restart the connector.
    TestHelper.waitFor(Duration.ofMinutes(1));
    stopConnector();
    TestHelper.waitFor(Duration.ofSeconds(5));
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // Everything should be normal, keep asserting that the connector is still running.
    for (int i = 0; i < 10; ++i) {
      assertConnectorIsRunning();
    }

    waitAndFailIfCannotConsume(new ArrayList<>(), 500);

    // Delete the snapshot schedule at the end of the test.
    ybClient.deleteSnapshotSchedule(snapshotScheduleResponse.getSnapshotScheduleUUID());
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void singleShardTransactionFollowedWithNoOps(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    // Start connector.
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // Execute multiple single shard transactions.
    for (int i = 0; i < 100; ++i) {
      TestHelper.execute(String.format("INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha');", i));
    }


    LOGGER.info("Creating snapshot schedule on database: {} with a retention of 15 minutes", DEFAULT_DB_NAME);
    YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress());
    CreateSnapshotScheduleResponse snapshotScheduleResponse =
      ybClient.createSnapshotSchedule(CommonTypes.YQLDatabase.YQL_DATABASE_PGSQL, DEFAULT_DB_NAME, 15 * 60, 2 /* interval */);

    // Wait for a minute and then restart the connector.
    TestHelper.waitFor(Duration.ofMinutes(1));
    stopConnector();
    TestHelper.waitFor(Duration.ofSeconds(5));
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // Everything should be normal, keep asserting that the connector is still running.
    for (int i = 0; i < 10; ++i) {
      assertConnectorIsRunning();
    }

    waitAndFailIfCannotConsume(new ArrayList<>(), 100);

    // Delete the snapshot schedule at the end of the test.
    ybClient.deleteSnapshotSchedule(snapshotScheduleResponse.getSnapshotScheduleUUID());
  }
}
