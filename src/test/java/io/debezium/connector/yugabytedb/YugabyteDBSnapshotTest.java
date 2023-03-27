package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.*;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class YugabyteDBSnapshotTest extends YugabyteDBContainerTestBase {

    @BeforeAll
    public static void beforeClass() throws SQLException {
        initializeYBContainer();
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
    public void testSnapshotRecordConsumption() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
        final int recordsCount = 5000;
        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        insertBulkRecords(recordsCount);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());
        start(YugabyteDBConnector.class, configBuilder.build());

        awaitUntilConnectorIsReady();

        // Only verifying the record count since the snapshot records are not ordered, so it may be
        // a little complex to verify them in the sorted order at the moment
        CompletableFuture.runAsync(() -> verifyRecordCount(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }

    @Test
    public void shouldOnlySnapshotTablesInList() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        int recordCountT1 = 5000;

        // Insert records in the table t1
        insertBulkRecords(recordCountT1);

        // Insert records in the table all_types
        TestHelper.execute(HelperStrings.INSERT_ALL_TYPES);
        TestHelper.execute(HelperStrings.INSERT_ALL_TYPES);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1,public.all_types", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial");
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE_TABLES, "public.t1");

        start(YugabyteDBConnector.class, configBuilder.build());

        awaitUntilConnectorIsReady();

        // Dummy wait condition to wait for another 15 seconds
        TestHelper.waitFor(Duration.ofSeconds(15));

        SourceRecords records = consumeRecordsByTopic(recordCountT1);

        assertNotNull(records);

        // Assert that there are the expected number of records in the snapshot table
        assertEquals(recordCountT1, records.recordsForTopic("test_server.public.t1").size());

        // Since there are no records for this topic, the topic itself won't be created
        // so if the topic simply doesn't exist then the test should pass
        assertFalse(records.topics().contains("test_server.public.all_types"));
    }

    @Test
    public void snapshotTableThenStreamData() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        int recordCountT1 = 5000;

        // Insert records in the table t1
        insertBulkRecords(recordCountT1);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial");

        start(YugabyteDBConnector.class, configBuilder.build());

        awaitUntilConnectorIsReady();

        // Dummy wait for some time so that the connector has some time to transition to streaming.
        TestHelper.waitFor(Duration.ofSeconds(30));
        String insertStringFormat = "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 30);";
        TestHelper.executeBulkWithRange(insertStringFormat, recordCountT1, recordCountT1 + 1000);

        // Total records inserted at this stage would be recordCountT1 + 1000
        int totalRecords = recordCountT1 + 1000;

        // Consume and assert that we have received all the records now.
        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, totalRecords);
    }

    // GitHub issue: https://github.com/yugabyte/debezium-connector-yugabytedb/issues/143
    @Test
    public void snapshotTableWithCompaction() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        int recordCount = 5000;

        // Insert records in the table t1
        insertBulkRecords(recordCount);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial");

        start(YugabyteDBConnector.class, configBuilder.build());

        awaitUntilConnectorIsReady();

        // Assuming that at this point snapshot would still be running, update a few records and
        // compact the table.
        TestHelper.execute("UPDATE t1 SET first_name='fname' WHERE id < 10;");
        YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress());
        YBTable ybTable = TestHelper.getYbTable(ybClient, "t1");
        ybClient.flushTable(ybTable.getTableId());

        // Consume and assert that we have received all the records now.
        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordCount + 10 /* updates */);
    }

    private void insertBulkRecords(int numRecords) throws Exception {
        String formatInsertString = "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 30);";
        CompletableFuture.runAsync(() -> TestHelper.executeBulk(formatInsertString, numRecords))
                .exceptionally(throwable -> {
            throw new RuntimeException(throwable);
        }).get();
    }

    private void verifyRecordCount(long recordsCount) {
        waitAndFailIfCannotConsume(new ArrayList<>(), recordsCount);
    }

    private void waitAndFailIfCannotConsume(List<SourceRecord> records, long recordsCount) {
        waitAndFailIfCannotConsume(records, recordsCount, 300 * 1000 /* 5 minutes */);
    }

    /**
     * Consume the records available and add them to a list for further assertion purposes.
     * @param records list to which we need to add the records we consume, pass a
     * {@code new ArrayList<>()} if you do not need assertions on the consumed values
     * @param recordsCount total number of records which should be consumed
     * @param milliSecondsToWait duration in milliseconds to wait for while consuming
     */
    private void waitAndFailIfCannotConsume(List<SourceRecord> records, long recordsCount,
                                            long milliSecondsToWait) {
        AtomicLong totalConsumedRecords = new AtomicLong();
        long seconds = milliSecondsToWait / 1000;
        try {
            Awaitility.await()
                    .atMost(Duration.ofSeconds(seconds))
                    .until(() -> {
                        int consumed = super.consumeAvailableRecords(record -> {
                            LOGGER.debug("The record being consumed is " + record);
                            records.add(record);
                        });
                        if (consumed > 0) {
                            totalConsumedRecords.addAndGet(consumed);
                            LOGGER.debug("Consumed " + totalConsumedRecords + " records");
                        }

                        return totalConsumedRecords.get() == recordsCount;
                    });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + recordsCount + " in " + seconds + " seconds", exception);
        }

        assertEquals(recordsCount, totalConsumedRecords.get());
    }
}
