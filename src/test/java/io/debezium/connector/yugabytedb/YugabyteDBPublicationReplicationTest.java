package io.debezium.connector.yugabytedb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.awaitility.core.ConditionTimeoutException;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.yb.client.GetDBStreamInfoResponse;
import org.yb.client.YBClient;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests to verify that connector works well with Publication and Replication slots.
 * The minimum service version required for these tests to work is 2.20.2
 * 
 * @author Sumukh Phalgaonkar (sumukh.phalgaonkar@yugabyte.com)
 */
public class YugabyteDBPublicationReplicationTest extends YugabyteDBContainerTestBase {

    public static String insertStatementFormatfort2 = "INSERT INTO t2 values (%d);";
    public static String insertStatementFormatfort3 = "INSERT INTO t3 values (%d);";
    private final String formatInsertString =
        "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 12.345);";

    private static final String PUB_NAME = "pub";
    private static final String SLOT_NAME = "test_replication_slot";

    @BeforeAll
    public static void beforeClass() throws SQLException {
        setMasterFlags("ysql_yb_enable_replication_commands=true,ysql_cdc_active_replication_slot_window_ms=0");
        setTserverFlags("ysql_yb_enable_replication_commands=true,ysql_cdc_active_replication_slot_window_ms=0");
        initializeYBContainer( );
        TestHelper.dropAllSchemas();
    }

    @BeforeEach
    public void before() throws Exception {
        initializeConnectorTestFramework();
        dropReplicationSlot();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
    }

    @AfterEach
    public void after() throws Exception {
        stopConnector();
        Thread.sleep(10000);
        Awaitility.await()
            .atMost(Duration.ofSeconds(65))
            .until(() -> {
                try {
                    return dropReplicationSlot();
                } catch (Exception e) {
                    return false;
                }
            });
        TestHelper.execute(TestHelper.dropPublicationStatement);
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
        TestHelper.execute("DROP TABLE IF EXISTS ri_full, ri_default, ri_nothing, ri_change;");
    }

    @AfterAll
    public static void afterClass() {
        shutdownYBContainer();
    }

    @Test
    public void testPublicationReplicationStreamingConsumption() throws Exception {
        TestHelper.execute(String.format(TestHelper.createPublicationForTableStatement, "pub", "t1"));
        TestHelper.execute(TestHelper.createReplicationSlotStatement);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderWithPublication("yugabyte", "pub", "test_replication_slot"); 
        startEngine(configBuilder);
        final long recordsCount = 1;

        awaitUntilConnectorIsReady();

        //Introducing sleep to ensure that we insert the records after bootstrap is completed.
        Thread.sleep(30000);

        insertRecords(recordsCount);

        verifyPrimaryKeyOnly(recordsCount);
    }

    @Test
    public void testPublicationReplicationSnapshotConsumption() throws Exception {
        TestHelper.execute("CREATE TABLE IF NOT EXISTS t2_snapshot (id int primary key);");
        TestHelper.execute(String.format(TestHelper.createPublicationForTableStatement, "pub", "t2_snapshot"));
        TestHelper.execute(TestHelper.createReplicationSlotStatement);

        String insertStatement = "INSERT INTO t2_snapshot values (%d);";
        final int recordsCount = 1000;
        TestHelper.executeBulk(insertStatement, recordsCount);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderWithPublication("yugabyte", "pub", "test_replication_slot"); 
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        verifyRecordCount(recordsCount);
        // TestHelper.execute("DROP TABLE IF EXISTS t2_snapshot;");
    }

    @Test
    public void testAllTablesPublicationAutoCreateMode() throws Exception {
        TestHelper.execute(TestHelper.createReplicationSlotStatement);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderWithPublication("yugabyte", "pub", "test_replication_slot");
        configBuilder.with(YugabyteDBConnectorConfig.PUBLICATION_AUTOCREATE_MODE, "all_tables");
        startEngine(configBuilder);
        final int recordsCount = 10;

        awaitUntilConnectorIsReady();

        // Introducing sleep to ensure that we insert the records after bootstrap is completed.
        Thread.sleep(30000);

        TestHelper.executeBulk(insertStatementFormatfort2, recordsCount);
        TestHelper.executeBulk(insertStatementFormatfort3, recordsCount);

        verifyRecordCount(recordsCount * 2);
    }

    @Test
    public void testFilteredPublicationAutoCreateMode() throws Exception {
        TestHelper.execute(TestHelper.createReplicationSlotStatement);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderWithPublication("yugabyte", "pub", "test_replication_slot");
        configBuilder.with(YugabyteDBConnectorConfig.PUBLICATION_AUTOCREATE_MODE, "filtered");
        configBuilder.with(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST, "public.t2");

        startEngine(configBuilder);
        final int recordsCount = 10;

        awaitUntilConnectorIsReady();

        // Introducing sleep to ensure that we insert the records after bootstrap is completed.
        Thread.sleep(30000);

        TestHelper.executeBulk(insertStatementFormatfort2, recordsCount);
        TestHelper.executeBulk(insertStatementFormatfort3, recordsCount);

        verifyRecordCount(recordsCount);

    }

    @Test
    public void oldConfigStreamIDShouldNotBePartOfReplicationSlot() throws Exception {
        TestHelper.execute(TestHelper.createReplicationSlotStatement);
        TestHelper.execute(String.format(TestHelper.createPublicationForTableStatement, "pub", "t1"));
        String streamId = TestHelper.getStreamIdFromSlot("test_replication_slot");

        LOGGER.info("Using stream ID =  " + streamId);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("yugabyte", "public.t1", streamId);
        Configuration config = configBuilder.build();

        DebeziumException exception = assertThrows(DebeziumException.class, ()->YugabyteDBConnectorConfig.shouldUsePublication(config));

        String errorMessage = String.format(
         "Stream ID %s is associated with replication slot %s. Please use slot name in the config instead of Stream ID.",
                streamId, "test_replication_slot");
        assertTrue(exception.getMessage().contains(errorMessage));

    }

    @Test
    public void testReplicationSlotAutoCreation() throws Exception {
        TestHelper.execute(String.format(TestHelper.createPublicationForTableStatement, "pub", "t1"));

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderWithPublication("yugabyte", "pub", "test_replication_slot");
        startEngine(configBuilder);
        final long recordsCount = 10;

        awaitUntilConnectorIsReady();

        // Introducing sleep to ensure that we insert the records after bootstrap is completed.
        Thread.sleep(30000);

        insertRecords(recordsCount);

        verifyPrimaryKeyOnly(recordsCount);

    }

    @Test
    public void testAlterPublicationInsideFilteredAutocreateMode() throws Exception {
        TestHelper.execute(String.format(TestHelper.createPublicationForTableStatement, "pub", "t1"));
        TestHelper.execute(TestHelper.createReplicationSlotStatement);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderWithPublication("yugabyte", "pub", "test_replication_slot");
        configBuilder.with(YugabyteDBConnectorConfig.PUBLICATION_AUTOCREATE_MODE, "filtered");
        configBuilder.with(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST, "public.t2, public.t3");

        startEngine(configBuilder);
        final int recordsCount = 10;

        awaitUntilConnectorIsReady();

        // Introducing sleep to ensure that we insert the records after bootstrap is completed.
        Thread.sleep(30000);

        TestHelper.executeBulk(insertStatementFormatfort2, recordsCount);
        TestHelper.executeBulk(insertStatementFormatfort3, recordsCount);

        verifyRecordCount(recordsCount * 2);

    }

    @Test
    public void testFilteredAutocreateModeWithTableWithoutPrimaryKey() throws Exception {
        TestHelper.execute("CREATE TABLE test_table (id int, name text);");
        TestHelper.execute(TestHelper.createReplicationSlotStatement);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderWithPublication("yugabyte", "pub", "test_replication_slot");
        configBuilder.with(YugabyteDBConnectorConfig.PUBLICATION_AUTOCREATE_MODE, "filtered");
        configBuilder.with(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST, "public.t2, public.test_table");

        Configuration config = configBuilder.build();

        ConnectException e = assertThrows(ConnectException.class, () -> YugabyteDBConnectorConfig.initPublication(config));
        String errorMessage = "Unable to create filtered publication pub";
        assertTrue(e.getMessage().contains(errorMessage));

    }

    @Test
    public void testReplicaIdentityFull() throws Exception {
        TestHelper.initDB("yugabyte_create_tables.ddl");

        TestHelper.execute("ALTER TABLE t1 REPLICA IDENTITY FULL;");
        TestHelper.execute(String.format(TestHelper.createPublicationForTableStatement, PUB_NAME, "t1"));
        TestHelper.execute(TestHelper.createReplicationSlotStatement);

        startEngine(getPublicationConfig());
        awaitUntilConnectorIsReady();

        TestHelper.execute(String.format(formatInsertString, 1));
        TestHelper.execute("UPDATE t1 SET first_name='VKVK', hours=56.78 WHERE id = 1;");
        TestHelper.execute("DELETE FROM t1 WHERE id = 1;");

        List<SourceRecord> records = new ArrayList<>();
        CompletableFuture.runAsync(() -> getRecords(records, 4, 20000)).get();

        // INSERT: no before image
        SourceRecord insertRecord = records.get(0);
        assertValueField(insertRecord, "before", null);
        assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

        // UPDATE: full before image with all columns
        SourceRecord updateRecord = records.get(1);
        assertBeforeImage(updateRecord, 1, "Vaibhav", "Kushwaha", 12.345);
        assertAfterImage(updateRecord, 1, "VKVK", "Kushwaha", 56.78);

        // DELETE: full before image, null after
        SourceRecord deleteRecord = records.get(2);
        assertBeforeImage(deleteRecord, 1, "VKVK", "Kushwaha", 56.78);
        assertValueField(deleteRecord, "after", null);

        assertTombstone(records.get(3));
    }

    /**
     * REPLICA IDENTITY DEFAULT: before image is null for UPDATE and
     *  contains only the primary key columns for DELETE.
     */
    @Test
    public void testReplicaIdentityDefault() throws Exception {
        TestHelper.initDB("yugabyte_create_tables.ddl");

        TestHelper.execute("ALTER TABLE t1 REPLICA IDENTITY DEFAULT;");
        TestHelper.execute(String.format(TestHelper.createPublicationForTableStatement, PUB_NAME, "t1"));
        TestHelper.execute(TestHelper.createReplicationSlotStatement);

        startEngine(getPublicationConfig());
        awaitUntilConnectorIsReady();

        TestHelper.execute(String.format(formatInsertString, 1));
        TestHelper.execute("UPDATE t1 SET first_name='VKVK', hours=56.78 WHERE id = 1;");
        TestHelper.execute("DELETE FROM t1 WHERE id = 1;");

        List<SourceRecord> records = new ArrayList<>();
        CompletableFuture.runAsync(() -> getRecords(records, 4, 20000)).get();

        // INSERT: no before image
        SourceRecord insertRecord = records.get(0);
        assertValueField(insertRecord, "before", null);
        assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

        // UPDATE: before image is null
        SourceRecord updateRecord = records.get(1);
        assertValueField(insertRecord, "before", null);
        assertAfterImage(updateRecord, 1, "VKVK", "Kushwaha", 56.78);

        // DELETE: before image has only the PK, null after
        SourceRecord deleteRecord = records.get(2);
        assertValueField(deleteRecord, "before/id/value", 1);
        assertValueField(deleteRecord, "after", null);

        assertTombstone(records.get(3));
    }

    /**
     * REPLICA IDENTITY NOTHING: UPDATE and DELETE are disallowed by the database
     * because the table has no replica identity and publishes updates/deletes.
     */
    @Test
    public void testReplicaIdentityNothing() throws Exception {
        TestHelper.initDB("yugabyte_create_tables.ddl");

        TestHelper.execute("ALTER TABLE t1 REPLICA IDENTITY NOTHING;");
        TestHelper.execute(String.format(TestHelper.createPublicationForTableStatement, PUB_NAME, "t1"));
        TestHelper.execute(TestHelper.createReplicationSlotStatement);

        startEngine(getPublicationConfig());
        awaitUntilConnectorIsReady();

        TestHelper.execute(String.format(formatInsertString, 1));

        RuntimeException updateEx = assertThrows(RuntimeException.class,
            () -> TestHelper.execute("UPDATE t1 SET first_name='VKVK', hours=56.78 WHERE id = 1;"));
        assertTrue(updateEx.getMessage().contains(
            "cannot update table \"t1\" because it does not have a replica identity and publishes updates"));

        RuntimeException deleteEx = assertThrows(RuntimeException.class,
            () -> TestHelper.execute("DELETE FROM t1 WHERE id = 1;"));
        assertTrue(deleteEx.getMessage().contains(
            "cannot delete from table \"t1\" because it does not have a replica identity and publishes deletes"));

        List<SourceRecord> records = new ArrayList<>();
        CompletableFuture.runAsync(() -> getRecords(records, 1, 20000)).get();

        // INSERT: no before image
        SourceRecord insertRecord = records.get(0);
        assertValueField(insertRecord, "before", null);
        assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);
    }

    /**
     * REPLICA IDENTITY CHANGE:
     * - INSERT: before=null, after=full new row
     * - UPDATE: before=null, after=PK + changed columns
     * - DELETE: before=PK only, after=null
     */
    @Test
    public void testReplicaIdentityChange() throws Exception {
        TestHelper.initDB("yugabyte_create_tables.ddl");

        TestHelper.execute("ALTER TABLE t1 REPLICA IDENTITY CHANGE;");
        TestHelper.execute(String.format(TestHelper.createPublicationForTableStatement, PUB_NAME, "t1"));
        TestHelper.execute(TestHelper.createReplicationSlotStatement);

        startEngine(getPublicationConfig());
        awaitUntilConnectorIsReady();

        TestHelper.execute(String.format(formatInsertString, 1));
        TestHelper.execute("UPDATE t1 SET last_name='KK', hours=56.78 WHERE id = 1;");
        TestHelper.execute("DELETE FROM t1 WHERE id = 1;");

        List<SourceRecord> records = new ArrayList<>();
        CompletableFuture.runAsync(() -> getRecords(records, 4, 20000)).get();

        // INSERT: no before image, full new row in after
        SourceRecord insertRecord = records.get(0);
        assertValueField(insertRecord, "before", null);
        assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

        // UPDATE: no before image, after has PK + changed columns
        SourceRecord updateRecord = records.get(1);
        assertValueField(updateRecord, "before", null);
        assertValueField(updateRecord, "after/id/value", 1);
        assertValueField(updateRecord, "after/last_name/value", "KK");
        assertValueField(updateRecord, "after/hours/value", 56.78);

        // DELETE: before has PK only, null after
        SourceRecord deleteRecord = records.get(2);
        assertValueField(deleteRecord, "before/id/value", 1);
        assertValueField(deleteRecord, "after", null);

        assertTombstone(records.get(3));
    }

    /**
     * Combined test with 4 tables, each having a different REPLICA IDENTITY
     * (FULL, DEFAULT, NOTHING, CHANGE), all part of the same publication
     * and replication slot.
     */
    @Test
    public void testMixedReplicaIdentitiesInSameStream() throws Exception {
        String createTableFmt =
            "CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, first_name TEXT NOT NULL, last_name VARCHAR(40), hours DOUBLE PRECISION);";
        TestHelper.execute(String.format(createTableFmt, "ri_full"));
        TestHelper.execute(String.format(createTableFmt, "ri_default"));
        TestHelper.execute(String.format(createTableFmt, "ri_nothing"));
        TestHelper.execute(String.format(createTableFmt, "ri_change"));

        TestHelper.execute("ALTER TABLE ri_full REPLICA IDENTITY FULL;");
        TestHelper.execute("ALTER TABLE ri_default REPLICA IDENTITY DEFAULT;");
        TestHelper.execute("ALTER TABLE ri_nothing REPLICA IDENTITY NOTHING;");
        TestHelper.execute("ALTER TABLE ri_change REPLICA IDENTITY CHANGE;");

        TestHelper.execute(String.format(
            "CREATE PUBLICATION %s FOR TABLE ri_full, ri_default, ri_nothing, ri_change;", PUB_NAME));
        TestHelper.execute(TestHelper.createReplicationSlotStatement);

        startEngine(getPublicationConfig());
        awaitUntilConnectorIsReady();

        String insertFmt = "INSERT INTO %s VALUES (1, 'Vaibhav', 'Kushwaha', 12.345);";
        String updateFmt = "UPDATE %s SET first_name='VKVK', hours=56.78 WHERE id = 1;";
        String deleteFmt = "DELETE FROM %s WHERE id = 1;";

        // ri_full: INSERT, UPDATE, DELETE
        TestHelper.execute(String.format(insertFmt, "ri_full"));
        TestHelper.execute(String.format(updateFmt, "ri_full"));
        TestHelper.execute(String.format(deleteFmt, "ri_full"));

        // ri_default: INSERT, UPDATE, DELETE
        TestHelper.execute(String.format(insertFmt, "ri_default"));
        TestHelper.execute(String.format(updateFmt, "ri_default"));
        TestHelper.execute(String.format(deleteFmt, "ri_default"));

        // ri_nothing: only INSERT is allowed
        TestHelper.execute(String.format(insertFmt, "ri_nothing"));

        RuntimeException updateEx = assertThrows(RuntimeException.class,
            () -> TestHelper.execute(String.format(updateFmt, "ri_nothing")));
        assertTrue(updateEx.getMessage().contains(
            "cannot update table \"ri_nothing\" because it does not have a replica identity and publishes updates"));

        RuntimeException deleteEx = assertThrows(RuntimeException.class,
            () -> TestHelper.execute(String.format(deleteFmt, "ri_nothing")));
        assertTrue(deleteEx.getMessage().contains(
            "cannot delete from table \"ri_nothing\" because it does not have a replica identity and publishes deletes"));

        // ri_change: INSERT, UPDATE, DELETE
        TestHelper.execute(String.format(insertFmt, "ri_change"));
        TestHelper.execute("UPDATE ri_change SET last_name='KK', hours=56.78 WHERE id = 1;");
        TestHelper.execute(String.format(deleteFmt, "ri_change"));

        // Expect: ri_full(4) + ri_default(4) + ri_nothing(1) + ri_change(4) = 13 records
        List<SourceRecord> allRecords = new ArrayList<>();
        CompletableFuture.runAsync(() -> getRecords(allRecords, 13, 30000)).get();

        String topicPrefix = TestHelper.TEST_SERVER + ".public.";
        Map<String, List<SourceRecord>> byTopic = new HashMap<>();
        for (SourceRecord r : allRecords) {
            byTopic.computeIfAbsent(r.topic(), k -> new ArrayList<>()).add(r);
        }

        // --- ri_full: FULL before image on UPDATE and DELETE ---
        List<SourceRecord> fullRecords = byTopic.get(topicPrefix + "ri_full");
        assertNotNull(fullRecords, "Expected records for ri_full");
        assertEquals(4, fullRecords.size());

        assertValueField(fullRecords.get(0), "before", null);
        assertAfterImage(fullRecords.get(0), 1, "Vaibhav", "Kushwaha", 12.345);

        assertBeforeImage(fullRecords.get(1), 1, "Vaibhav", "Kushwaha", 12.345);
        assertAfterImage(fullRecords.get(1), 1, "VKVK", "Kushwaha", 56.78);

        assertBeforeImage(fullRecords.get(2), 1, "VKVK", "Kushwaha", 56.78);
        assertValueField(fullRecords.get(2), "after", null);

        assertTombstone(fullRecords.get(3));

        // --- ri_default: null before on UPDATE, PK-only before on DELETE ---
        List<SourceRecord> defaultRecords = byTopic.get(topicPrefix + "ri_default");
        assertNotNull(defaultRecords, "Expected records for ri_default");
        assertEquals(4, defaultRecords.size());

        assertValueField(defaultRecords.get(0), "before", null);
        assertAfterImage(defaultRecords.get(0), 1, "Vaibhav", "Kushwaha", 12.345);

        assertValueField(defaultRecords.get(1), "before", null);
        assertAfterImage(defaultRecords.get(1), 1, "VKVK", "Kushwaha", 56.78);

        assertValueField(defaultRecords.get(2), "before/id/value", 1);
        assertValueField(defaultRecords.get(2), "after", null);

        assertTombstone(defaultRecords.get(3));

        // --- ri_nothing: only INSERT ---
        List<SourceRecord> nothingRecords = byTopic.get(topicPrefix + "ri_nothing");
        assertNotNull(nothingRecords, "Expected records for ri_nothing");
        assertEquals(1, nothingRecords.size());

        assertValueField(nothingRecords.get(0), "before", null);
        assertAfterImage(nothingRecords.get(0), 1, "Vaibhav", "Kushwaha", 12.345);

        // --- ri_change: null before on UPDATE (after has PK + changed cols), PK-only before on DELETE ---
        List<SourceRecord> changeRecords = byTopic.get(topicPrefix + "ri_change");
        assertNotNull(changeRecords, "Expected records for ri_change");
        assertEquals(4, changeRecords.size());

        assertValueField(changeRecords.get(0), "before", null);
        assertAfterImage(changeRecords.get(0), 1, "Vaibhav", "Kushwaha", 12.345);

        assertValueField(changeRecords.get(1), "before", null);
        assertValueField(changeRecords.get(1), "after/id/value", 1);
        assertValueField(changeRecords.get(1), "after/last_name/value", "KK");
        assertValueField(changeRecords.get(1), "after/hours/value", 56.78);

        assertValueField(changeRecords.get(2), "before/id/value", 1);
        assertValueField(changeRecords.get(2), "after", null);

        assertTombstone(changeRecords.get(3));
    }

    // ---- Helpers ----

    private Configuration.Builder getPublicationConfig() throws Exception {
        return TestHelper.getConfigBuilderWithPublication("yugabyte", PUB_NAME, SLOT_NAME);
    }

    private void assertBeforeImage(SourceRecord record, Integer id, String firstName,
                                   String lastName, Double hours) {
        assertValueField(record, "before/id/value", id);
        assertValueField(record, "before/first_name/value", firstName);
        assertValueField(record, "before/last_name/value", lastName);
        assertValueField(record, "before/hours/value", hours);
    }

    private void assertAfterImage(SourceRecord record, Integer id, String firstName,
                                  String lastName, Double hours) {
        assertValueField(record, "after/id/value", id);
        assertValueField(record, "after/first_name/value", firstName);
        assertValueField(record, "after/last_name/value", lastName);
        assertValueField(record, "after/hours/value", hours);
    }

    private void insertRecords(long numOfRowsToBeInserted) throws Exception {
        String formatInsertString = "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 30);";
        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < numOfRowsToBeInserted; i++) {
                TestHelper.execute(String.format(formatInsertString, i));
            }

        }).exceptionally(throwable -> {
            throw new RuntimeException(throwable);
        }).get();
    }

    

    private void verifyPrimaryKeyOnly(long recordsCount) {
        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsCount);

        for (int i = 0; i < records.size(); ++i) {
            // verify the records
            assertValueField(records.get(i), "after/id/value", i);
        }
    }
    
    private boolean dropReplicationSlot() throws Exception {
        try (YugabyteDBConnection ybConnection = TestHelper.create();
             Connection connection = ybConnection.connection()) {
            final Statement statement = connection.createStatement();

            ResultSet check = statement.executeQuery(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = 'test_replication_slot';");
            if (!check.next()) {
                return true;
            }

            statement.execute(TestHelper.dropReplicationSlotStatement);

            ResultSet verify = statement.executeQuery(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = 'test_replication_slot';");
            return !verify.next();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void verifyRecordCount(long recordsCount) {
        waitAndFailIfCannotConsume(new ArrayList<>(), recordsCount);
    }

    private void getRecords(List<SourceRecord> records, long totalRecordsToConsume,
                            long milliSecondsToWait) {
        AtomicLong totalConsumedRecords = new AtomicLong();
        long seconds = milliSecondsToWait / 1000;
        try {
            Awaitility.await()
                .atMost(Duration.ofSeconds(seconds))
                .until(() -> {
                    int consumed = consumeAvailableRecords(record -> {
                        LOGGER.debug("The record being consumed is " + record);
                        records.add(record);
                    });
                    if (consumed > 0) {
                        totalConsumedRecords.addAndGet(consumed);
                        LOGGER.debug("Consumed " + totalConsumedRecords + " records");
                    }

                    return totalConsumedRecords.get() == totalRecordsToConsume;
                });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + totalRecordsToConsume + " records in " + seconds
                 + " seconds", exception);
        }

        assertEquals(totalRecordsToConsume, totalConsumedRecords.get());
    }

}
