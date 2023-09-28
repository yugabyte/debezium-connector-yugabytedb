package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

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
    }

    @AfterEach
    public void after() throws Exception {
        stopConnector();
        dropAllTables();
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
        TestHelper.dropAllSchemas();
        resetCommitCallbackDelay();
    }

    @AfterAll
    public static void afterClass() {
        shutdownYBContainer();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSnapshotRecordConsumption(boolean colocation) throws Exception {
        setCommitCallbackDelay(10000);
        createTables(colocation);
        final int recordsCount = 5000;
        insertBulkRecords(recordsCount, "public.test_1");

        LOGGER.info("Creating DB stream ID");
        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");
        Configuration.Builder configBuilder =
          TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        // Only verifying the record count since the snapshot records are not ordered, so it may be
        // a little complex to verify them in the sorted order at the moment
        CompletableFuture.runAsync(() -> verifyRecordCount(recordsCount))
          .exceptionally(throwable -> {
              throw new RuntimeException(throwable);
          }).get();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSnapshotRecordCountInInitialOnlyMode(boolean colocation) throws Exception {
        setCommitCallbackDelay(10000);
        createTables(colocation);
        final int recordsCount = 4000;
        insertBulkRecords(recordsCount, "public.test_1");

        LOGGER.info("Creating DB stream ID");
        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");
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
    @ValueSource(booleans = {true, false})
    public void shouldOnlySnapshotTablesInList(boolean colocation) throws Exception {
        createTables(colocation);

        int recordCountT1 = 5000;

        // Insert records in the table test_1
        insertBulkRecords(recordCountT1, "public.test_1");

        // Create table and insert records in all_types
        TestHelper.executeInDatabase(HelperStrings.CREATE_ALL_TYPES, DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase(HelperStrings.INSERT_ALL_TYPES, DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase(HelperStrings.INSERT_ALL_TYPES, DEFAULT_COLOCATED_DB_NAME);

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");
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
    @ValueSource(booleans = {true, false})
    public void snapshotTableThenStreamData(boolean colocation) throws Exception {
        createTables(colocation);

        int recordCountT1 = 5000;

        // Insert records in the table test_1
        insertBulkRecords(recordCountT1, "public.test_1");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");
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
    @ValueSource(booleans = {true, false})
    public void snapshotTableWithCompaction(boolean colocation) throws Exception {
        createTables(colocation);

        int recordCount = 5000;

        // Insert records in the table test_1
        insertBulkRecords(recordCount, "public.test_1");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");
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
    @ValueSource(booleans = {true, false})
    public void snapshotForMultipleTables(boolean colocation) throws Exception {
        // Create colocated tables
        createTables(colocation);

        final int recordsTest1 = 10;
        final int recordsTest2 = 20;
        final int recordsTest3 = 30;
        insertBulkRecords(recordsTest1, "public.test_1");
        insertBulkRecords(recordsTest2, "public.test_2");
        insertBulkRecords(recordsTest3, "public.test_3");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");
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

    @Test
    public void snapshotMixOfColocatedNonColocatedTables() throws Exception {
        // Create tables.
        createTables(true /* enforce creation of the colocated tables only */);

        final int recordCountForTest1 = 1000;
        final int recordCountForTest2 = 2000;
        final int recordCountForTest3 = 3000;
        final int recordCountInNonColocated = 4000;
        insertBulkRecords(recordCountForTest1, "public.test_1");
        insertBulkRecords(recordCountForTest2, "public.test_2");
        insertBulkRecords(recordCountForTest3, "public.test_3");
        insertBulkRecords(recordCountInNonColocated, "public.test_no_colocated");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");
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
    @ValueSource(booleans = {true, false})
    public void snapshotColocatedNonColocatedThenStream(boolean initialOnly) throws Exception {
        // Create tables.
        createTables(true /* enforce creation of the colocated tables only */);

        final int recordCountForTest1 = 1000;
        final int recordCountForTest2 = 2000;
        final int recordCountForTest3 = 3000;
        final int recordCountInNonColocated = 4000;
        insertBulkRecords(recordCountForTest1, "public.test_1");
        insertBulkRecords(recordCountForTest2, "public.test_2");
        insertBulkRecords(recordCountForTest3, "public.test_3");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");
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

        insertBulkRecords(recordCountInNonColocated, "public.test_no_colocated");

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldSnapshotWithFailureAfterBootstrapSnapshotCall(boolean colocation)
        throws Exception {
        // This test verifies that if there is a failure after snapshot is bootstrapped,
        // then snapshot is taken normally once the connector restarts.
        createTables(colocation);

        // Insert records to be snapshotted.
        final int recordsCount = 10;
        insertBulkRecords(recordsCount, "public.test_1");
        insertBulkRecords(recordsCount, "public.test_2");

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
    @ValueSource(booleans = {true, false})
    public void shouldSnapshotWithFailureAfterSettingInitialCheckpoint(boolean colocation)
        throws Exception {
        // This test verifies that if there is a failure after the call to set the checkpoint,
        // then snapshot is taken normally once the connector restarts.
        createTables(colocation);

        // Insert records to be snapshotted.
        final int recordsCount = 10;
        insertBulkRecords(recordsCount, "public.test_1");
        insertBulkRecords(recordsCount, "public.test_2");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");
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

    @Test
    public void shouldNotSnapshotAgainIfSnapshotCompletedOnce() throws Exception {
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

        createTables(false);

        final int recordsCount = 50;
        insertBulkRecords(recordsCount, "public.test_1");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");
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
    @ValueSource(booleans = {true, false})
    public void shouldContinueStreamingInNeverAfterSnapshotCompleteInInitialOnly(boolean colocation)
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

        createTables(colocation);

        final int recordsCount = 50;
        insertBulkRecords(recordsCount, "public.test_1");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");
        Configuration.Builder configBuilder =
          TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial_only");

        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsCount);
        assertEquals(recordsCount, records.size());

        // Insert records in other tables, this shouldn't cause an issue.
        insertBulkRecords(1000, "public.test_2");
        insertBulkRecords(500, "public.test_3");

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
        insertBulkRecordsInRange(recordsCount, recordsCount + insertRecords, "public.test_1");

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

    @Test
    public void shouldNotBeAffectedByDroppingUnrelatedTables() throws Exception {
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

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false, false);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial");

        YugabyteDBStreamingChangeEventSource.TEST_WAIT_BEFORE_GETTING_CHILDREN = true;

        int recordsCount = 50;
        String insertFormat = "INSERT INTO t1 VALUES (%d, 'value for split table');";
        for (int i = 0; i < recordsCount; ++i) {
            TestHelper.execute(String.format(insertFormat, i));
        }

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
    }

    /**
     * Helper function to create the required tables in the database DEFAULT_COLOCATED_DB_NAME
     */
    private void createTables(boolean colocation) {
        LOGGER.info("Creating tables with colocation: {}", colocation);
        final String createTest1 = String.format("CREATE TABLE test_1 (id INT PRIMARY KEY," +
                                                 "name TEXT DEFAULT 'Vaibhav Kushwaha') " +
                                                  "WITH (COLOCATION = %b);", colocation);
        final String createTest2 = String.format("CREATE TABLE test_2 (text_key TEXT PRIMARY " +
                                                 "KEY) WITH (COLOCATION = %b);", colocation);
        final String createTest3 =
          String.format("CREATE TABLE test_3 (hours FLOAT PRIMARY KEY, " +
                        "hours_in_text VARCHAR(40) DEFAULT 'some_default_hour_value') " +
                        "WITH (COLOCATION = %b);", colocation);
        final String createTestNoColocated = "CREATE TABLE test_no_colocated (id INT PRIMARY KEY," +
                                             "name TEXT DEFAULT 'name_for_non_colocated') " +
                                             "WITH (COLOCATION = false) SPLIT INTO 3 TABLETS;";

        TestHelper.executeInDatabase(createTest1, DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase(createTest2, DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase(createTest3, DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase(createTestNoColocated, DEFAULT_COLOCATED_DB_NAME);
    }

    /**
     * Helper function to drop all the tables being created as a part of this test.
     */
    private void dropAllTables() {
        TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_1;", DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_2;", DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_3;", DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_no_colocated;", DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase("DROP TABLE IF EXISTS all_types;", DEFAULT_COLOCATED_DB_NAME);
    }

    private void insertBulkRecords(int numRecords, String fullTableName) {
        String formatInsertString = "INSERT INTO " + fullTableName + " VALUES (%d);";
        TestHelper.executeBulk(formatInsertString, numRecords, DEFAULT_COLOCATED_DB_NAME);
    }

    private void insertBulkRecordsInRange(int beginKey, int endKey, String fullTableName) {
        String formatInsertString = "INSERT INTO " + fullTableName + " VALUES (%d);";
        TestHelper.executeBulkWithRange(formatInsertString, beginKey, endKey, DEFAULT_COLOCATED_DB_NAME);
    }

    private void verifyRecordCount(long recordsCount) {
        waitAndFailIfCannotConsume(new ArrayList<>(), recordsCount);
    }
}
