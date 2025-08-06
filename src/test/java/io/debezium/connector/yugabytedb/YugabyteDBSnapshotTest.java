package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic unit tests to increase the test coverage for snapshot of tables in YugabyteDB. This class
 * contains parameterized tests as well which will run the tests once on colocated tables and then
 * on non-colocated tables.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBSnapshotTest extends YugabytedTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        initializeYBContainer();
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
    }

    @BeforeEach
    public void before() throws Exception {
        initializeConnectorTestFramework();
        TestHelper.dropAllSchemas();
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_SETTING_INITIAL_CHECKPOINT = false;
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_BOOTSTRAP_GET_CHANGES = false;
        YugabyteDBSnapshotChangeEventSource.FAIL_WHEN_MARKING_SNAPSHOT_DONE = false;
    }

    @AfterEach
    public void after() throws Exception {
        stopConnector();
        dropAllTablesInColocatedDB();
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
        TestHelper.dropAllSchemas();
        resetCommitCallbackDelay();
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_SETTING_INITIAL_CHECKPOINT = false;
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_BOOTSTRAP_GET_CHANGES = false;
        YugabyteDBSnapshotChangeEventSource.FAIL_WHEN_MARKING_SNAPSHOT_DONE = false;
    }

    @AfterAll
    public static void afterClass() {
        shutdownYBContainer();
    }

    @ParameterizedTest
    @MethodSource("streamTypeProviderForSnapshotWithColocation")
    public void testSnapshotRecordConsumption(boolean consistentSnapshot, boolean useSnapshot, boolean colocation) throws Exception {
        setCommitCallbackDelay(10000);
        createTablesInColocatedDB(colocation);
        final int recordsCount = 5000;
        insertBulkRecordsInColocatedDB(recordsCount, "public.test_1");

        LOGGER.info("Creating DB stream ID");
        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder =
          TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());
        startEngine(configBuilder);

        // awaitUntilConnectorIsReady();

        // Only verifying the record count since the snapshot records are not ordered, so it may be
        // a little complex to verify them in the sorted order at the moment
        verifyRecordCount(recordsCount);
    }

    @ParameterizedTest
    @MethodSource("streamTypeProviderForSnapshotWithColocation")
    public void testSnapshotRecordCountInInitialOnlyMode(boolean consistentSnapshot, boolean useSnapshot, boolean colocation) throws Exception {
        setCommitCallbackDelay(10000);
        createTablesInColocatedDB(colocation);
        final int recordsCount = 4000;
        insertBulkRecordsInColocatedDB(recordsCount, "public.test_1");

        LOGGER.info("Creating DB stream ID");
        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder =
          TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.INITIAL_ONLY.getValue());
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        // Only verifying the record count since the snapshot records are not ordered, so it may be
        // a little complex to verify them in the sorted order at the moment
        verifyRecordCount(recordsCount);
    }

    @ParameterizedTest
    @MethodSource("streamTypeProviderForSnapshotWithColocation")
    public void shouldOnlySnapshotTablesInList(boolean consistentSnapshot, boolean useSnapshot, boolean colocation) throws Exception {
        createTablesInColocatedDB(colocation);
        int recordCountT1 = 5000;

        // Insert records in the table test_1
        insertBulkRecordsInColocatedDB(recordCountT1, "public.test_1");

        // Create table and insert records in all_types
        TestHelper.executeInDatabase(HelperStrings.CREATE_ALL_TYPES, DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase(HelperStrings.INSERT_ALL_TYPES, DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase(HelperStrings.INSERT_ALL_TYPES, DEFAULT_COLOCATED_DB_NAME);

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1,public.all_types", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial");
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE_TABLES, "public.test_1");

        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        // Dummy wait condition to wait for another 15 seconds
        TestHelper.waitFor(Duration.ofSeconds(15));

        SourceRecords records = consumeByTopic(recordCountT1);

        assertNotNull(records);

        // Assert that there are the expected number of records in the snapshot table
        assertEquals(recordCountT1, records.recordsForTopic("test_server.public.test_1").size());

        // Since there are no records for this topic, the topic itself won't be created
        // so if the topic simply doesn't exist then the test should pass
        assertFalse(records.topics().contains("test_server.public.all_types"));
    }

    @ParameterizedTest
    @MethodSource("streamTypeProviderForSnapshotWithColocation")
    public void snapshotTableThenStreamData(boolean consistentSnapshot, boolean useSnapshot, boolean colocation) throws Exception {
        createTablesInColocatedDB(colocation);

        int recordCountT1 = 5000;

        // Insert records in the table test_1
        insertBulkRecordsInColocatedDB(recordCountT1, "public.test_1");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial");

        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        // Dummy wait for some time so that the connector has some time to transition to streaming.
        TestHelper.waitFor(Duration.ofSeconds(30));
        String insertStringFormat = "INSERT INTO test_1 VALUES (%s);";
        TestHelper.executeInDatabase(
          String.format(insertStringFormat,
            String.format("generate_series(%d, %d)",
              recordCountT1, recordCountT1 + 1000)), DEFAULT_COLOCATED_DB_NAME);

        // Total records inserted at this stage would be recordCountT1 + 1001
        int totalRecords = recordCountT1 + 1001;

        // Consume and assert that we have received all the records now.
        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, totalRecords);
    }

    // GitHub issue: https://github.com/yugabyte/debezium-connector-yugabytedb/issues/143
    @ParameterizedTest
    @MethodSource("streamTypeProviderForSnapshotWithColocation")
    public void snapshotTableWithCompaction(boolean consistentSnapshot, boolean useSnapshot, boolean colocation) throws Exception {
        createTablesInColocatedDB(colocation);

        int recordCount = 5000;

        // Insert records in the table test_1
        insertBulkRecordsInColocatedDB(recordCount, "public.test_1");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial");

        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        // Assuming that at this point snapshot would still be running, update a few records and
        // compact the table.
        TestHelper.executeInDatabase("UPDATE test_1 SET name='fname' WHERE id < 10;", DEFAULT_COLOCATED_DB_NAME);
        YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress());
        YBTable ybTable = TestHelper.getYbTable(ybClient, "test_1");
        ybClient.flushTable(ybTable.getTableId());

        // Consume and assert that we have received all the records now.
        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordCount + 10 /* updates */);
    }

    @ParameterizedTest
    @MethodSource("streamTypeProviderForSnapshotWithColocation")
    public void snapshotForMultipleTables(boolean consistentSnapshot, boolean useSnapshot, boolean colocation) throws Exception {
        // Create colocated tables
        createTablesInColocatedDB(colocation);

        final int recordsTest1 = 10;
        final int recordsTest2 = 20;
        final int recordsTest3 = 30;
        insertBulkRecordsInColocatedDB(recordsTest1, "public.test_1");
        insertBulkRecordsInColocatedDB(recordsTest2, "public.test_2");
        insertBulkRecordsInColocatedDB(recordsTest3, "public.test_3");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder =
          TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1,public.test_2,public.test_3", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        List<SourceRecord> recordsForTest1 = new ArrayList<>();
        List<SourceRecord> recordsForTest2 = new ArrayList<>();
        List<SourceRecord> recordsForTest3 = new ArrayList<>();

        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsTest1 + recordsTest2 + recordsTest3);

        // Iterate over the records and add them to their respective topic
        for (SourceRecord record : records) {
            if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_1")) {
                recordsForTest1.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_2")) {
                recordsForTest2.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_3")) {
                recordsForTest3.add(record);
            }
        }

        assertEquals(recordsTest1, recordsForTest1.size());
        assertEquals(recordsTest2, recordsForTest2.size());
        assertEquals(recordsTest3, recordsForTest3.size());
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForSnapshot")
    public void snapshotMixOfColocatedNonColocatedTables(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        // Create tables.
        createTablesInColocatedDB(true /* enforce creation of the colocated tables only */);

        final int recordCountForTest1 = 1000;
        final int recordCountForTest2 = 2000;
        final int recordCountForTest3 = 3000;
        final int recordCountInNonColocated = 4000;
        insertBulkRecordsInColocatedDB(recordCountForTest1, "public.test_1");
        insertBulkRecordsInColocatedDB(recordCountForTest2, "public.test_2");
        insertBulkRecordsInColocatedDB(recordCountForTest3, "public.test_3");
        insertBulkRecordsInColocatedDB(recordCountInNonColocated, "public.test_no_colocated");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder =
          TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1,public.test_2,public.test_3,public.test_no_colocated", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        List<SourceRecord> recordsForTest1 = new ArrayList<>();
        List<SourceRecord> recordsForTest2 = new ArrayList<>();
        List<SourceRecord> recordsForTest3 = new ArrayList<>();
        List<SourceRecord> recordsForNonColocated = new ArrayList<>();

        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordCountForTest1 + recordCountForTest2 + recordCountForTest3 + recordCountInNonColocated);

        // Iterate over the records and add them to their respective topic
        for (SourceRecord record : records) {
            if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_1")) {
                recordsForTest1.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_2")) {
                recordsForTest2.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_3")) {
                recordsForTest3.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_no_colocated")) {
                recordsForNonColocated.add(record);
            }
        }

        assertEquals(recordCountForTest1, recordsForTest1.size());
        assertEquals(recordCountForTest2, recordsForTest2.size());
        assertEquals(recordCountForTest3, recordsForTest3.size());
        assertEquals(recordCountInNonColocated, recordsForNonColocated.size());
    }

    @ParameterizedTest
    @MethodSource("streamTypeProviderForSnapshotWithColocation")
    public void snapshotColocatedNonColocatedThenStream(boolean consistentSnapshot, boolean useSnapshot, boolean initialOnly) throws Exception {
        // Create tables.
        createTablesInColocatedDB(true /* enforce creation of the colocated tables only */);

        final int recordCountForTest1 = 1000;
        final int recordCountForTest2 = 2000;
        final int recordCountForTest3 = 3000;
        final int recordCountInNonColocated = 4000;
        insertBulkRecordsInColocatedDB(recordCountForTest1, "public.test_1");
        insertBulkRecordsInColocatedDB(recordCountForTest2, "public.test_2");
        insertBulkRecordsInColocatedDB(recordCountForTest3, "public.test_3");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder =
          TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1,public.test_2,public.test_3,public.test_no_colocated", dbStreamId);
        if (initialOnly) {
            configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE,
                    YugabyteDBConnectorConfig.SnapshotMode.INITIAL_ONLY.getValue());
        } else {
            configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE,
                    YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());
        }

        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        List<SourceRecord> recordsForTest1 = new ArrayList<>();
        List<SourceRecord> recordsForTest2 = new ArrayList<>();
        List<SourceRecord> recordsForTest3 = new ArrayList<>();
        List<SourceRecord> recordsForNonColocated = new ArrayList<>();

        // Wait for some time so that the connector can transition to the streaming mode.
        TestHelper.waitFor(Duration.ofSeconds(60));

        insertBulkRecordsInColocatedDB(recordCountInNonColocated, "public.test_no_colocated");

        // Inserting 1001 records to test_1
        TestHelper.executeInDatabase("INSERT INTO test_1 VALUES (generate_series(1000, 2000));", DEFAULT_COLOCATED_DB_NAME);

        // Inserting 3001 records to test_3
        TestHelper.executeInDatabase("INSERT INTO test_3 VALUES (generate_series(3000, 6000));", DEFAULT_COLOCATED_DB_NAME);

        List<SourceRecord> records = new ArrayList<>();
        if (initialOnly) {
            waitAndFailIfCannotConsume(records, recordCountForTest1 + recordCountForTest2 + recordCountForTest3 );
        } else {
            waitAndFailIfCannotConsume(records, recordCountForTest1 + recordCountForTest2 + recordCountForTest3 + recordCountInNonColocated + 1001 + 3001);
        }

        // Iterate over the records and add them to their respective topic
        for (SourceRecord record : records) {
            if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_1")) {
                recordsForTest1.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_2")) {
                recordsForTest2.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_3")) {
                recordsForTest3.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_no_colocated")) {
                recordsForNonColocated.add(record);
            }
        }
        if (initialOnly) {
            assertEquals(recordCountForTest1 , recordsForTest1.size());
            assertEquals(recordCountForTest2, recordsForTest2.size());
            assertEquals(recordCountForTest3 , recordsForTest3.size());
        } else {
            assertEquals(recordCountForTest1 + 1001, recordsForTest1.size());
            assertEquals(recordCountForTest2, recordsForTest2.size());
            assertEquals(recordCountForTest3 + 3001, recordsForTest3.size());
            assertEquals(recordCountInNonColocated, recordsForNonColocated.size());
        }
    }

    // This test should not be run with consistent snapshot stream since it verifies
    // the behaviour on failure after snapshot bootstrap call. For consistent
    // snapshot streams, the very first getChanges call starts the snapshot consumption
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldSnapshotWithFailureAfterBootstrapSnapshotCall(boolean colocation)
        throws Exception {
        // This test verifies that if there is a failure after snapshot is bootstrapped,
        // then snapshot is taken normally once the connector restarts.
        createTablesInColocatedDB(colocation);

        // Insert records to be snapshotted.
        final int recordsCount = 10;
        insertBulkRecordsInColocatedDB(recordsCount, "public.test_1");
        insertBulkRecordsInColocatedDB(recordsCount, "public.test_2");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(
            DEFAULT_COLOCATED_DB_NAME, "public.test_1,public.test_2", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial");

        // Enable the failure flag to introduce an explicit failure.
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_BOOTSTRAP_GET_CHANGES = true;
        startEngine(configBuilder);

        // Since we have specified the failure flag, we should not get any snapshot and
        // connector would fail after the first GetChanges call to all the tablets. Verify that
        // we haven't received any record even after waiting for a minute.
        TestHelper.waitFor(Duration.ofMinutes(1));
        assertNoRecordsToConsume();

        // Stop the connector.
        stopConnector();

        // Disable the failure flag so that execution can happen normally.
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_BOOTSTRAP_GET_CHANGES = false;
        startEngine(configBuilder);

        // Wait until connector is started.
        awaitUntilConnectorIsReady();

        // This time we will get the records inserted earlier, this will be the result of snapshot.
        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, 2 * recordsCount);

        // Iterate over the records and add them to their respective topic
        List<SourceRecord> recordsForTest1 = new ArrayList<>();
        List<SourceRecord> recordsForTest2 = new ArrayList<>();
        for (SourceRecord record : records) {
            if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_1")) {
                recordsForTest1.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_2")) {
                recordsForTest2.add(record);
            }
        }

        assertEquals(recordsCount, recordsForTest1.size());
        assertEquals(recordsCount, recordsForTest2.size());
    }

    @ParameterizedTest
    @MethodSource("streamTypeProviderForSnapshotWithColocation")
    public void shouldSnapshotWithFailureAfterSettingInitialCheckpoint(boolean consistentSnapshot, boolean useSnapshot, boolean colocation)
        throws Exception {
        // This test verifies that if there is a failure after the call to set the checkpoint,
        // then snapshot is taken normally once the connector restarts.
        createTablesInColocatedDB(colocation);

        // Insert records to be snapshotted.
        final int recordsCount = 10;
        insertBulkRecordsInColocatedDB(recordsCount, "public.test_1");
        insertBulkRecordsInColocatedDB(recordsCount, "public.test_2");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(
            DEFAULT_COLOCATED_DB_NAME, "public.test_1,public.test_2", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial");

        // Enable the failure flag to introduce an explicit failure.
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_SETTING_INITIAL_CHECKPOINT = true;
        startEngine(configBuilder);

        // Since we have specified the failure flag, we should not get any snapshot and
        // connector would fail after setting the checkpoint on all the tablets. Verify that
        // we haven't received any record even after waiting for a minute.
        TestHelper.waitFor(Duration.ofMinutes(1));
        assertNoRecordsToConsume();

        // Stop the connector.
        stopConnector();

        // Disable the failure flag so that execution can happen normally.
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_SETTING_INITIAL_CHECKPOINT = false;
        startEngine(configBuilder);

        // Wait until connector is started.
        awaitUntilConnectorIsReady();

        // This time we will get the records inserted earlier, this will be the result of snapshot.
        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, 2 * recordsCount);

        // Iterate over the records and add them to their respective topic
        List<SourceRecord> recordsForTest1 = new ArrayList<>();
        List<SourceRecord> recordsForTest2 = new ArrayList<>();
        for (SourceRecord record : records) {
            if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_1")) {
                recordsForTest1.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_2")) {
                recordsForTest2.add(record);
            }
        }

        assertEquals(recordsCount, recordsForTest1.size());
        assertEquals(recordsCount, recordsForTest2.size());
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForSnapshot")
    public void shouldNotSnapshotAgainIfSnapshotCompletedOnce(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        /* This test aims to verify that if snapshot is taken on certain streamID + tabletId
           combination once then we should not be taking it again. To verify the same, we will
           perform the following steps:
           1. Start connector in initial_only mode - this will ensure that we only take
              the snapshot
           2. Verify that we have received all the records in snapshot
           3. Stop connector
           4. Start connector in snapshot.mode = initial
           5. This time we are expecting that the connector will not take a snapshot and proceed
              to streaming mode directly
           6. To verify that the connector is publishing nothing, we can assert that there are
              no records to consume
         */ 

        createTablesInColocatedDB(false);

        final int recordsCount = 50;
        insertBulkRecordsInColocatedDB(recordsCount, "public.test_1");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder =
          TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial_only");

        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsCount);
        assertEquals(recordsCount, records.size());

        // Verify that there are no records to consume anymore.
        assertNoRecordsToConsume();

        // Stop connector and wait till the engine stops.
        stopConnector();

        Awaitility.await()
            .pollDelay(Duration.ofSeconds(1))
            .atMost(Duration.ofSeconds(60))
            .until(() -> {
                return engine == null;
            });

        assertConnectorNotRunning();

        // Change snapshot.mode to initial and start connector.
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial");
        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        // Verify that there is nothing to consume.
        for (int i = 0; i < 5; ++i) {
            assertNoRecordsToConsume();
            TestHelper.waitFor(Duration.ofSeconds(2));
        }
    }

    @ParameterizedTest
    @MethodSource("streamTypeProviderForSnapshotWithColocation")
    public void shouldContinueStreamingInNeverAfterSnapshotCompleteInInitialOnly(boolean consistentSnapshot, boolean useSnapshot, boolean colocation)
        throws Exception {
        /* This test aims to verify that if snapshot is taken on certain streamID + tabletId
           combination once with initial_only and then the connector is deployed with 
           snapshot.mode = never it should only stream the changes and no snapshot record
           should be streamed. Steps are as follows:
           1. Start connector in initial_only mode - this will ensure that we only take
              the snapshot
           2. Verify that we have received all the records in snapshot
           3. Stop connector
           4. Insert a few records here to make sure they are not streamed as part of snapshot
           5. Start connector in snapshot.mode = never
           6. This time we are expecting that the connector will not take a snapshot and proceed
              to streaming mode directly
           7. Verify that we get the change records here.
         */ 

        createTablesInColocatedDB(colocation);

        final int recordsCount = 50;
        insertBulkRecordsInColocatedDB(recordsCount, "public.test_1");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder =
          TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial_only");

        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsCount);
        assertEquals(recordsCount, records.size());

        // Insert records in other tables, this shouldn't cause an issue.
        insertBulkRecordsInColocatedDB(1000, "public.test_2");
        insertBulkRecordsInColocatedDB(500, "public.test_3");

        // Verify that there are no records to consume anymore.
        assertNoRecordsToConsume();

        // Stop connector and wait till the engine stops.
        stopConnector();

        Awaitility.await()
            .pollDelay(Duration.ofSeconds(1))
            .atMost(Duration.ofSeconds(60))
            .until(() -> {
                return engine == null;
            });

        assertConnectorNotRunning();

        // Insert a few records --> total records inserted here would be 10, [50, 60)
        final int insertRecords = 10;
        insertBulkRecordsInRangeInColocatedDB(recordsCount, recordsCount + insertRecords, "public.test_1");

        // Change snapshot.mode to initial and start connector.
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "never");
        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        // Verify that we consume new records.
        List<SourceRecord> recordsAfterRestart = new ArrayList<>();
        waitAndFailIfCannotConsume(recordsAfterRestart, insertRecords);

        assertEquals(insertRecords, recordsAfterRestart.size());

        int startIdx = recordsCount;
        for (int i = 0; i < recordsAfterRestart.size(); ++i) {
            Struct s = (Struct) recordsAfterRestart.get(i).value();
            assertEquals("c", s.getString("op"));
            assertValueField(recordsAfterRestart.get(i), "after/id/value", startIdx);

            // Increment startIdx for next record verification.
            ++startIdx;
        }
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForSnapshot")
    public void snapshotShouldNotBeAffectedByDroppingUnrelatedTables(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        /* The objective of the test is to verify that when an unrelated table is dropped, it
           should not cause any harm to the existing flow. At the same time, if tablet split
           occurs, we should ensure that the parent doesn't get deleted before we start
           consuming from the children, and when it does, if the connector restarts it 
           should not take the snapshot on the children in any situation. The test performs
           the following steps:
           1. Creates 3 tables: t1, t2 and t3
           2. Starts streaming on table t1 only
           3. We set a static flag to wait before getting children - this will ensure that we
              wait 60s after receiving tablet split and before asking for the children
           4. Insert some records in t1 and start the connector in snapshot mode
           5. Drop table t2
           6. Verify that we have consumed snapshot records
           7. Restart the connector
           8. Ensure that we are not receiving any record
         */
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 1 TABLETS;");
        TestHelper.execute("CREATE TABLE t2 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 1 TABLETS;");
        TestHelper.execute("CREATE TABLE t3 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 1 TABLETS;");

        YugabyteDBStreamingChangeEventSource.TEST_WAIT_BEFORE_GETTING_CHILDREN = true;

        int recordsCount = 50;
        String insertFormat = "INSERT INTO t1 VALUES (%d, 'value for split table');";
        for (int i = 0; i < recordsCount; ++i) {
            TestHelper.execute(String.format(insertFormat, i));
        }

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false, false, consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial");

        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        // Drop another unrelated table.
        LOGGER.info("Dropping table t2");
        TestHelper.execute("DROP TABLE t2;");
        LOGGER.info("Waiting 10s after dropping table t2");
        TestHelper.waitFor(Duration.ofSeconds(10));

        YBClient ybClient = TestHelper.getYbClient(getMasterAddress());
        YBTable table = TestHelper.getYbTable(ybClient, "t1");
        
        // Verify that there is just a single tablet.
        Set<String> tablets = ybClient.getTabletUUIDs(table);
        int tabletCountBeforeSplit = tablets.size();
        assertEquals(1, tabletCountBeforeSplit);

        // Compact the table to ready it for splitting.
        ybClient.flushTable(table.getTableId());

        // Wait for 20s for the table to be flushed.
        TestHelper.waitFor(Duration.ofSeconds(20));

        // Split the tablet. There is just one tablet so it is safe to assume that the iterator will
        // return just the desired tablet.
        ybClient.splitTablet(tablets.iterator().next());

        // Wait till there are 2 tablets for the table.
        TestHelper.waitForTablets(ybClient, table, 2);

        LOGGER.info("Dummy wait to see the logs in connector");
        TestHelper.waitFor(Duration.ofSeconds(120));

        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsCount);

        assertEquals(recordsCount, records.size());

        // Also verify all of them are snapshot records.
        for (SourceRecord record : records) {
            assertEquals("r", TestHelper.getOpValue(record));
        }

        // Stop the connector, wait till it stops and then restart.
        stopConnector();

        Awaitility.await()
            .pollDelay(Duration.ofSeconds(1))
            .atMost(Duration.ofSeconds(60))
            .until(() -> {
                return engine == null;
            });

        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        // No record should be consumed now.
        for (int i = 0; i < 10; ++i) {
            assertNoRecordsToConsume();
            TestHelper.waitFor(Duration.ofSeconds(2));
        }

        YugabyteDBStreamingChangeEventSource.TEST_WAIT_BEFORE_GETTING_CHILDREN = false;
    }

    // This test should not be run with consistent snapshot stream since it verifies
    // the behaviour on failure after snapshot bootstrap call. For consistent
    // snapshot streams, the very first getChanges call starts the snapshot consumption
    @ParameterizedTest
    @MethodSource("argumentProviderForEmptyNonEmptyNonColocatedTables")
    public void snapshotTwoColocatedAndEmptyNonEmptyNonColocatedThenStream(boolean emptyNonColocated, boolean colocation) throws Exception {
        /*
         * The objective of this test is to verify that we are able to consume all
         * snapshot records on a combination of empty & non-empty tables and 
         * successfully switch to streaming phase and consume the streaming records for all the tables.
         */

        // Create tables.
        createTablesInColocatedDB(colocation);

        // 2 colocated non-empty tables + 1 colocated empty table + 1 non-colocated empty table
        final int recordCountForTest1 = 1000;
        final int recordCountForTest2 = 2000;
        final int recordCountInNonColocated = 2000;
        insertBulkRecordsInColocatedDB(recordCountForTest1, "public.test_1");
        insertBulkRecordsInColocatedDB(recordCountForTest2, "public.test_2");

        if (!emptyNonColocated) {
            insertBulkRecordsInColocatedDB(recordCountInNonColocated, "public.test_no_colocated");
        }

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");
        Configuration.Builder configBuilder =
                TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1,public.test_2,public.test_3,public.test_no_colocated", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE,
                YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());

        // Enable the failure flag to introduce an explicit failure.
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_BOOTSTRAP_GET_CHANGES = true;
        startEngine(configBuilder);

        // Since we have specified the failure flag, we should not get any snapshot and
        // connector would fail after the first GetChanges call to all the tablets. Verify that
        // we haven't received any record even after waiting for a minute.
        TestHelper.waitFor(Duration.ofMinutes(1));
        assertNoRecordsToConsume();

        // Stop the connector.
        stopConnector();

        // Disable the failure flag so that execution can happen normally.
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_BOOTSTRAP_GET_CHANGES = false;
        startEngine(configBuilder);

        // Wait until connector is started.
        awaitUntilConnectorIsReady();

        List<SourceRecord> recordsForTest1 = new ArrayList<>();
        List<SourceRecord> recordsForTest2 = new ArrayList<>();
        List<SourceRecord> recordsForTest3 = new ArrayList<>();
        List<SourceRecord> recordsForNonColocated = new ArrayList<>();

        final int totalStreamingRecords = insertStreamingRecordsInAllTables();

        List<SourceRecord> records = new ArrayList<>();

        if (emptyNonColocated) {
            waitAndFailIfCannotConsume(records, recordCountForTest1 + recordCountForTest2 + totalStreamingRecords);
        } else {
            waitAndFailIfCannotConsume(records, recordCountForTest1 + recordCountForTest2 + recordCountInNonColocated + totalStreamingRecords);
        }

        // Iterate over the records and add them to their respective topic
        for (SourceRecord record : records) {
            if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_1")) {
                recordsForTest1.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_2")) {
                recordsForTest2.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_3")) {
                recordsForTest3.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_no_colocated")) {
                recordsForNonColocated.add(record);
            }
        }

        assertEquals(recordCountForTest1 + 101, recordsForTest1.size());
        assertEquals(recordCountForTest2 + 201, recordsForTest2.size());
        assertEquals(301, recordsForTest3.size());
        if (emptyNonColocated) {
            assertEquals(401, recordsForNonColocated.size());
        } else {
            assertEquals(recordCountInNonColocated + 401, recordsForNonColocated.size());
        }
    }

    @ParameterizedTest
    @MethodSource("argumentProviderForEmptyNonEmptyNonColocatedTables")
    public void snapshotTwoColocatedAndEmptyNonEmptyNonColocatedThenStreamWithConsistentSnapshot(boolean emptyNonColocated, boolean colocation) throws Exception {
        /*
         * The objective of this test is to verify that we are able to consume all
         * snapshot records on a combination of empty & non-empty tables and successfully
         * switch to streaming phase and consume the streaming records for all the tables.
         */

        // Create tables.
        createTablesInColocatedDB(colocation);

        // 2 colocated non-empty tables + 1 colocated empty table + 1 non-colocated empty table
        final int recordCountForTest1 = 1000;
        final int recordCountForTest2 = 2000;
        final int recordCountInNonColocated = 2000;
        insertBulkRecordsInColocatedDB(recordCountForTest1, "public.test_1");
        insertBulkRecordsInColocatedDB(recordCountForTest2, "public.test_2");

        if (!emptyNonColocated) {
            insertBulkRecordsInColocatedDB(recordCountInNonColocated, "public.test_no_colocated");
        }

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", true, true);
        Configuration.Builder configBuilder =
                TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1,public.test_2,public.test_3,public.test_no_colocated", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE,
                YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());

        startEngine(configBuilder);

        // Wait until connector is started.
        awaitUntilConnectorIsReady();

        List<SourceRecord> recordsForTest1 = new ArrayList<>();
        List<SourceRecord> recordsForTest2 = new ArrayList<>();
        List<SourceRecord> recordsForTest3 = new ArrayList<>();
        List<SourceRecord> recordsForNonColocated = new ArrayList<>();

        final int totalStreamingRecords = insertStreamingRecordsInAllTables();

        List<SourceRecord> records = new ArrayList<>();

        if (emptyNonColocated) {
            waitAndFailIfCannotConsume(records, recordCountForTest1 + recordCountForTest2 + totalStreamingRecords);
        } else {
            waitAndFailIfCannotConsume(records, recordCountForTest1 + recordCountForTest2 + recordCountInNonColocated + totalStreamingRecords);
        }
        
        // Iterate over the records and add them to their respective topic
        for (SourceRecord record : records) {
            if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_1")) {
                recordsForTest1.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_2")) {
                recordsForTest2.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_3")) {
                recordsForTest3.add(record);
            } else if (record.topic().equals(TestHelper.TEST_SERVER + ".public.test_no_colocated")) {
                recordsForNonColocated.add(record);
            }
        }

        assertEquals(recordCountForTest1 + 101, recordsForTest1.size());
        assertEquals(recordCountForTest2 + 201, recordsForTest2.size());
        assertEquals(301, recordsForTest3.size());
        if (emptyNonColocated) {
            assertEquals(401, recordsForNonColocated.size());
        } else {
            assertEquals(recordCountInNonColocated + 401, recordsForNonColocated.size());
        }
    }

    @ParameterizedTest
    @MethodSource("streamTypeProviderForSnapshotWithColocation")
    public void verifyConnectorFailsIfMarkSnapshotDoneFails(boolean consistentSnapshot, boolean useSnapshot, boolean colocation) throws Exception {
        createTablesInColocatedDB(colocation);

        int recordCountT1 = 5000;

        // Insert records in the table test_1
        insertBulkRecordsInColocatedDB(recordCountT1, "public.test_1");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial")
            .with(YugabyteDBConnectorConfig.MAX_CONNECTOR_RETRIES, "1");
        YugabyteDBSnapshotChangeEventSource.FAIL_WHEN_MARKING_SNAPSHOT_DONE = true;
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        // Dummy wait for some time so that the connector has some time to transition to streaming.
        TestHelper.waitFor(Duration.ofSeconds(30));
        String insertStringFormat = "INSERT INTO test_1 VALUES (%s);";
        TestHelper.executeInDatabase(
          String.format(insertStringFormat,
            String.format("generate_series(%d, %d)",
              recordCountT1, recordCountT1 + 10000)), DEFAULT_COLOCATED_DB_NAME);

        // Total records inserted at this stage would be recordCountT1 + 1001
        int totalRecords = recordCountT1 + 1001;

        List<SourceRecord> records = new ArrayList<>();
        // We are intentionally passing expected records as totalRecords but the expected behavior 
        // with the test flag enabled is that we will only receive snapshot records and no streaming
        // records.
        waitAndConsume(records, totalRecords, 300*1000);
        assertNoRecordsToConsume();
        // Should have recevied only snapshot records.
        assertNotEquals(totalRecords, records.size());
        assertEquals(recordCountT1, records.size());
        YugabyteDBSnapshotChangeEventSource.FAIL_WHEN_MARKING_SNAPSHOT_DONE = false;
    }

    @Test
    public void snapshotShouldBeCompletedOnParentIfSplitHappenedAfterStreamCreation() throws Exception {
        /*
        * The objective of the test is to verify that snapshot takes place on the parent tablet if a split
        * happens after a consistent stream is created and before connector has been deployed. Additionally,
        * any DMLs performed after the split are not part of snapshot records.
        */
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        int recordsCount = 10;
        String insertFormat = "INSERT INTO t1 VALUES (%d, 'value for split table');";
        for (int i = 0; i < recordsCount; ++i) {
            TestHelper.execute(String.format(insertFormat, i));
        }

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false, true, true, true);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial");

        YBClient ybClient = TestHelper.getYbClient(getMasterAddress());
        YBTable table = TestHelper.getYbTable(ybClient, "t1");

        // Verify that there is just a single tablet.
        Set<String> tablets = ybClient.getTabletUUIDs(table);
        int tabletCountBeforeSplit = tablets.size();
        assertEquals(1, tabletCountBeforeSplit);

        // Compact the table to ready it for splitting.
        ybClient.flushTable(table.getTableId());

        // Wait for 20s for the table to be flushed.
        TestHelper.waitFor(Duration.ofSeconds(20));

        // Split the tablet. There is just one tablet so it is safe to assume that the iterator will
        // return just the desired tablet.
        ybClient.splitTablet(tablets.iterator().next());

        // Wait till there are 2 tablets for the table.
        TestHelper.waitForTablets(ybClient, table, 2);

        // Insert 5 more rows. Since these records are inserted after the stream is created & split is completed,
        // they should not be consumed at the time of consumption of snapshot from parent tablet.
        for (int i = recordsCount; i < recordsCount + 5; ++i) {
            TestHelper.execute(String.format(insertFormat, i));
        }

        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsCount + 5);

        assertEquals(recordsCount + 5, records.size());

        int snapshotRecords = 0;
        int streamingRecords = 0;
        for (SourceRecord record : records) {
            if (TestHelper.getOpValue(record).equals("r")) {
                snapshotRecords++;
            } else if (TestHelper.getOpValue(record).equals("c")) {
                streamingRecords++;
            }
        }

        assertEquals(recordsCount, snapshotRecords);
        assertEquals(5, streamingRecords);
    }

    private void verifyRecordCount(long recordsCount) {
        waitAndFailIfCannotConsume(new ArrayList<>(), recordsCount);
    }

    static Stream<Arguments> streamTypeProviderForSnapshotWithColocation() {
        return Stream.of(
                // Arguments.of(false, false, true), // Older stream with colocation
                // Arguments.of(false, false, false), // Older stream without colocation
                // Arguments.of(true, true, true)), // USE_SNAPSHOT stream with colocation
                Arguments.of(true, true, false));  // USE_SNAPSHOT stream without colocation
    }

    static Stream<Arguments> argumentProviderForEmptyNonEmptyNonColocatedTables() {
        return Stream.of(
            Arguments.of(true, true), // Empty Non Colocated table with colocation
            Arguments.of(true, false), // Empty Non Colocated table without colocation
            Arguments.of(false, true), // Non-empty Non Colocated table with colocation
            Arguments.of(false, false)); // Non-empty Non Colocated table without colocation
    }

    private int insertStreamingRecordsInAllTables() {
        // Inserting 1001 records to test_1
        TestHelper.executeInDatabase("INSERT INTO test_1 VALUES (generate_series(1000, 1100));", DEFAULT_COLOCATED_DB_NAME);

        // Inserting 2001 records to test_1
        TestHelper.executeInDatabase("INSERT INTO test_2 VALUES (generate_series(2000, 2200));", DEFAULT_COLOCATED_DB_NAME);

        // Inserting 3001 records to test_3
        TestHelper.executeInDatabase("INSERT INTO test_3 VALUES (generate_series(3000, 3300));", DEFAULT_COLOCATED_DB_NAME);

        // Inserting 4001 records to test_no_colocated
        TestHelper.executeInDatabase("INSERT INTO test_no_colocated VALUES (generate_series(4000, 4400));", DEFAULT_COLOCATED_DB_NAME);
        final int totalRecordsInserted = 101 + 201 + 301 + 401;
        return totalRecordsInserted;
    }
}
