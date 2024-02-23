package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
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
 * operations to be streamed in combination with different failure/restart scenarios.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBTransactionalTest extends YugabytedTestBase {
  @BeforeAll
  public static void beforeClass() throws SQLException {
    initializeYBContainer("enable_tablet_split_of_cdcsdk_streamed_tables=true", "cdc_max_stream_intent_records=10");
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

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void shouldResumeLargeTransactions(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    /*
     * The goal behind this test is to make sure that when explicit checkpoints are not being
     * updated, the safe time should also not be moving forward since it will cause data loss
     * by filtering the records.
     *
     * 1. Start connector
     * 2. Stop updating the explicit checkpoint
     * 3. Execute transactions - since the checkpoints are not being updated, nothing will go
     *    to the cdc_state table
     * 4. Restart the connector - once the connector restarts, it should consume all the events
     *    from the starting without any data loss
     */
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    YugabyteDBStreamingChangeEventSource.TEST_TRACK_EXPLICIT_CHECKPOINTS = true;

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress());
    YBTable ybTable = TestHelper.getYbTable(ybClient, "t1");
    assertNotNull(ybTable);

    Set<String> tablets = ybClient.getTabletUUIDs(ybTable);

    String tabletId = tablets.iterator().next();
    assertNotNull(tabletId);
    assertEquals(1, tablets.size());

    // This check will verify that we are getting some explicit checkpoint values, once that is confirmed, stop updating the explicit checkpoints.
    Awaitility.await()
      .atMost(Duration.ofMinutes(1))
      .until(() -> YugabyteDBStreamingChangeEventSource.TEST_explicitCheckpoints.get(tabletId) != null);
    YugabyteDBStreamingChangeEventSource.TEST_UPDATE_EXPLICIT_CHECKPOINT = false;

    final int recordsToBeInserted = 200;
    TestHelper.execute(
      String.format("INSERT INTO t1 VALUES (generate_series(0, %d), 'Vaibhav', 'Kushwaha');",
        99));
    TestHelper.execute(
      String.format("INSERT INTO t1 VALUES (generate_series(%d, %d), 'Vaibhav', 'Kushwaha');",
        100, recordsToBeInserted - 1));

    // Consume all the records.
    List<SourceRecord> recordsBeforeRestart = new ArrayList<>();
    Set<Integer> primaryKeysBeforeRestart = new HashSet<>();
    waitAndConsume(recordsBeforeRestart, recordsToBeInserted, 2 * 60 * 1000);

    // Now that we have some value for explicit checkpoint, we can stop connector.
    stopConnector();

    for (SourceRecord record : recordsBeforeRestart) {
      Struct value = (Struct) record.value();
      primaryKeysBeforeRestart.add(value.getStruct("after").getStruct("id").getInt32("value"));
    }

    // Assert that we have consumed all the records.
    assertEquals(recordsToBeInserted, primaryKeysBeforeRestart.size());

    // Since we have stopped updating the checkpoints, enable it again and consume the records,
    // We should get all the records again. Note that enabling the checkpointing will have no
    // impact on the test result here, it could have been kept disabled as well.
    YugabyteDBStreamingChangeEventSource.TEST_UPDATE_EXPLICIT_CHECKPOINT = true;

    // Consume records.
    List<SourceRecord> recordsAfterRestart = new ArrayList<>();
    Set<Integer> primaryKeysAfterRestart = new HashSet<>();

    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    waitAndConsume(recordsAfterRestart, recordsToBeInserted, 2 * 60 * 1000);

    for (SourceRecord record : recordsAfterRestart) {
      Struct value = (Struct) record.value();
      primaryKeysAfterRestart.add(value.getStruct("after").getStruct("id").getInt32("value"));
    }

    assertEquals(recordsToBeInserted, primaryKeysAfterRestart.size());

    YugabyteDBStreamingChangeEventSource.TEST_TRACK_EXPLICIT_CHECKPOINTS = false;
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void shouldBeNoDataLossWithExplicitCheckpointAdvancing(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    YugabyteDBStreamingChangeEventSource.TEST_TRACK_EXPLICIT_CHECKPOINTS = true;

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    // Start connector.
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // Insert a record.
    TestHelper.execute("INSERT INTO t1 VALUES (generate_series(0,99), 'Vaibhav', 'Kushwaha');");

    // Wait for a few seconds so that the checkpoint can be advanced.
    TestHelper.waitFor(Duration.ofSeconds(3));

    // Stop updating checkpoints in callbacks.
    YugabyteDBStreamingChangeEventSource.TEST_UPDATE_EXPLICIT_CHECKPOINT = false;

    TestHelper.waitFor(Duration.ofSeconds(10));

    // Stop the connector.
    stopConnector();

    // Consume all available records.
    List<SourceRecord> recordsBeforeRestart = new ArrayList<>();
    int consumedRecords = consumeAvailableRecords(recordsBeforeRestart::add);
    assertNoRecordsToConsume();

    LOGGER.info("Total consumed records before restart: {}", recordsBeforeRestart.size());

    // We will update the checkpoints now.
    YugabyteDBStreamingChangeEventSource.TEST_UPDATE_EXPLICIT_CHECKPOINT = true;

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

    YugabyteDBStreamingChangeEventSource.TEST_TRACK_EXPLICIT_CHECKPOINTS = false;
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
    waitAndConsume(records, 200, 5 * 60 * 1000);

    Set<Integer> pk = new HashSet<>();
    for (SourceRecord record : records) {
      Struct value = (Struct) record.value();
      pk.add(value.getStruct("after").getStruct("id").getInt32("value"));
    }

    assertEquals(200, pk.size());
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void verifyNoHoldingOfResourcesOnServiceAfterSplit(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    /*
     * The goal of this test is to verify that if a tablet split is there then we are not holding
     * up any resources on the children tablets if there are no operations on the children or there
     * are no records for children.
     *
     * Basically, we are verifying that if there is no callback on the children tablets (which
     * would be the case if there are no records) then we do not hold up any resources. This will be
     * verified by reading the GetCheckpointResponse on the children and verifying that the OpId
     * and snapshot_time is equal to the explicit checkpoint for the parent.
     */

    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    YugabyteDBStreamingChangeEventSource.TEST_TRACK_EXPLICIT_CHECKPOINTS = true;

    // Start connector.
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // Insert records.
    TestHelper.execute("INSERT INTO t1 VALUES (generate_series(0, 49), 'Vaibhav', 'Kushwaha');");

    YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress());
    YBTable ybTable = TestHelper.getYbTable(ybClient, "t1");
    assertNotNull(ybTable);
    Set<String> tablets = ybClient.getTabletUUIDs(ybTable);
    assertEquals(1, tablets.size());
    String parentTablet = tablets.iterator().next();

    List<SourceRecord> records = new ArrayList<>();
    waitAndFailIfCannotConsume(records, 50);

    // Flush the table and split its tablet.
    ybClient.flushTable(ybTable.getTableId());
    TestHelper.waitFor(Duration.ofSeconds(20));
    ybClient.splitTablet(parentTablet);
    TestHelper.waitForTablets(ybClient, ybTable, 2);

    // Once the split happens, children will be initialised with an explicit checkpoint value
    // returned by GetTabletListToPoll call, and since there will be no further records for the
    // children tablets, the explicit checkpoint will never change from this assigned value, we
    // can now use GetCheckpoint to compare the values.
    List<String> tabletsAfterSplit = new ArrayList<>(ybClient.getTabletUUIDs(ybTable));
    assertEquals(2, tabletsAfterSplit.size());

    CdcSdkCheckpoint parentExplicitCheckpoint = YugabyteDBStreamingChangeEventSource.TEST_explicitCheckpoints.get(parentTablet);
    assertNotNull(parentExplicitCheckpoint);

    GetCheckpointResponse childCheckpoint1 = ybClient.getCheckpoint(ybTable, dbStreamId, tabletsAfterSplit.get(0));
    assertEquals(parentExplicitCheckpoint.getTerm(), childCheckpoint1.getTerm());
    assertEquals(parentExplicitCheckpoint.getIndex(), childCheckpoint1.getIndex());
    assertEquals(parentExplicitCheckpoint.getTime(), childCheckpoint1.getSnapshotTime());

    GetCheckpointResponse childCheckpoint2 = ybClient.getCheckpoint(ybTable, dbStreamId, tabletsAfterSplit.get(1));
    assertEquals(parentExplicitCheckpoint.getTerm(), childCheckpoint2.getTerm());
    assertEquals(parentExplicitCheckpoint.getIndex(), childCheckpoint2.getIndex());
    assertEquals(parentExplicitCheckpoint.getTime(), childCheckpoint2.getSnapshotTime());
  }
}
