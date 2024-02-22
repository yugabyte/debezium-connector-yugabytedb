package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests to validate that streaming is not affected nor is there any data loss when there are
 * transactions to be streamed.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBTransactionalTest extends YugabytedTestBase {
  @BeforeAll
  public static void beforeClass() throws SQLException {
    initializeYBContainer(null, "cdc_max_stream_intent_records=10");
    TestHelper.dropAllSchemas();
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

  @Test
  public void reproduceAdvancingOfExplicitCheckpointing() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    // Start the connector.
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    final int recordsToBeInserted = 100;
    // Insert records which will be streamed in 2 GetChanges calls.
    TestHelper.execute("INSERT INTO t1 VALUES (generate_series(0,49), 'Vaibhav', 'Kushwaha');");
    TestHelper.execute("INSERT INTO t1 VALUES (generate_series(50,99), 'Vaibhav', 'Kushwaha');");

    LOGGER.info("Observe the logs for 40 seconds now");
    waitAndConsume(new ArrayList<>(), recordsToBeInserted, 3 * 60 * 1000);
    TestHelper.waitFor(Duration.ofSeconds(40));

  }

  @Test
  public void shouldBeNoDataLossWithExplicitCheckpointAdvancing() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    YugabyteDBStreamingChangeEventSource.TRACK_EXPLICIT_CHECKPOINTS = true;

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    // Start connector.
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // Insert a record.
    TestHelper.execute("INSERT INTO t1 VALUES (generate_series(0,99), 'Vaibhav', 'Kushwaha');");

    // Wait for a few seconds so that the checkpoint can be advanced.
    TestHelper.waitFor(Duration.ofSeconds(3));

    // Stop updating checkpoints in callbacks.
    YugabyteDBStreamingChangeEventSource.UPDATE_EXPLICIT_CHECKPOINT = false;

    TestHelper.waitFor(Duration.ofSeconds(10));

    // Stop the connector.
    stopConnector();

    // Consume all available records.
    List<SourceRecord> recordsBeforeRestart = new ArrayList<>();
    int consumedRecords = consumeAvailableRecords(recordsBeforeRestart::add);
    assertNoRecordsToConsume();

    LOGGER.info("Total consumed records before restart: {}", recordsBeforeRestart.size());

    // We will update the checkpoints now.
    YugabyteDBStreamingChangeEventSource.UPDATE_EXPLICIT_CHECKPOINT = true;

    // Restart the connector.
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // We should be consuming only 1 record.
    List<SourceRecord> recordsAfterRestart = new ArrayList<>();
    waitAndConsume(recordsAfterRestart, 100, 3 * 60 * 1000);

    Set<Integer> primaryKeysAfterRestart = new HashSet<>();
    for (SourceRecord record : recordsAfterRestart) {
      Struct value = (Struct) record.value();
      primaryKeysAfterRestart.add(value.getStruct("after").getStruct("id").getInt32("value"));
    }

    assertEquals(100, primaryKeysAfterRestart.size());

    assertNoRecordsToConsume();

    YugabyteDBStreamingChangeEventSource.TRACK_EXPLICIT_CHECKPOINTS = false;
  }

  @Test
  public void shouldHaveNoDataLossWithSplitAfterSplit() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    // Start connector.
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress());
    YBTable ybTable = TestHelper.getYbTable(ybClient, "t1");
    assertNotNull(ybTable);

    Set<String> tablets = ybClient.getTabletUUIDs(ybTable);

    String initialTabletId = tablets.iterator().next();
    assertNotNull(initialTabletId);
    assertEquals(1, tablets.size());

    // Insert records.
    TestHelper.execute("INSERT INTO t1 VALUES (generate_series(0, 4999), 'Vaibhav', 'Kushwaha');");

    // Flush table and wait.
    TestHelper.waitFor(Duration.ofSeconds(8));
    LOGGER.info("Flushing table");
    ybClient.flushTable(ybTable.getTableId());
    TestHelper.waitFor(Duration.ofSeconds(20));

    // Split tablet and then split one again while GetChanges is paused.
    LOGGER.info("Splitting first tablet {}", initialTabletId);
    ybClient.splitTablet(initialTabletId);
    TestHelper.waitForTablets(ybClient, ybTable, 2);

    LOGGER.info("Flushing table");
    ybClient.flushTable(ybTable.getTableId());
    TestHelper.waitFor(Duration.ofSeconds(20));

    Set<String> tabletsAfterFirstSplit = ybClient.getTabletUUIDs(ybTable);
    assertEquals(2, tabletsAfterFirstSplit.size());

    YugabyteDBStreamingChangeEventSource.TEST_PAUSE_GET_CHANGES_CALLS = false;
    String childGen1 = tabletsAfterFirstSplit.iterator().next();
    ybClient.splitTablet(childGen1);
    TestHelper.waitForTablets(ybClient, ybTable, 3);

    LOGGER.info("Resuming GetChanges calls");
    YugabyteDBStreamingChangeEventSource.TEST_PAUSE_GET_CHANGES_CALLS = false;

    // Insert a few more records.
    TestHelper.execute("INSERT INTO t1 VALUES (generate_series(5000, 5499), 'Vaibhav', 'Kushwaha');");

    // Consume all the records.
    List<SourceRecord> records = new ArrayList<>();
    waitAndConsume(records, 5500, 5 * 60 * 1000);

    int totalConsumed = consumeAvailableRecords(records::add);
    Set<Integer> pk = new HashSet<>();
    for (SourceRecord record : records) {
      Struct value = (Struct) record.value();
      pk.add(value.getStruct("after").getStruct("id").getInt32("value"));
    }

    LOGGER.info("PK size is {}", pk.size());
  }
}
