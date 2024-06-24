package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.junit.logging.LogInterceptor;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.yb.client.*;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic unit tests to check the behaviour with YugabyteDB datatypes
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */

public class YugabyteDBDatatypesTest extends YugabytedTestBase {
    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
            "INSERT INTO s2.a (aa) VALUES (1);";
    private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "DROP SCHEMA IF EXISTS s2 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));";

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

    private void updateRecords(long numOfRowsToBeUpdated) throws Exception {
        String formatUpdateString = "UPDATE t1 SET hours = 10 WHERE id = %d";
        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < numOfRowsToBeUpdated; i++) {
                TestHelper.execute(String.format(formatUpdateString, i));
            }
        }).exceptionally(throwable -> {
            throw new RuntimeException(throwable);
        }).get();
    }

    private void deleteRecords(long numOfRowsToBeDeleted) throws Exception {
        String formatDeleteString = "DELETE FROM t1 WHERE id = %d;";
        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < numOfRowsToBeDeleted; i++) {
                TestHelper.execute(String.format(formatDeleteString, i));
            }
        }).exceptionally(throwable -> {
            throw new RuntimeException(throwable);
        }).get();
    }

    private void insertRecordsInSchema(long numOfRowsToBeInserted) throws Exception {
        String formatInsertString = "INSERT INTO test_schema.table_in_schema VALUES (%d, 'Vaibhav', 'Kushwaha', 30);";
        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < numOfRowsToBeInserted; i++) {
                TestHelper.execute(String.format(formatInsertString, i));
            }
        }).exceptionally(throwable -> {
            throw new RuntimeException(throwable);
        }).get();
    }

    private void verifyDeletedFieldPresentInValue(long recordsCount, YBExtractNewRecordState<SourceRecord> transformation) {
        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsCount);

        // According to the test, we are expecting that the last record is going to be a tombstone
        // record whose value part is null, so do not assert that record.
        for (int i = 0; i < recordsCount - 1; ++i) {
            SourceRecord transformedRecrod = transformation.apply(records.get(i));
            Struct transformedRecrodValue = (Struct) transformedRecrod.value();
            Object deleteFieldValue = transformedRecrodValue.get("__deleted");
            if (deleteFieldValue == null) {
                throw new RuntimeException("Required field: '__deleted', dropped from value of source record");
            }

            LOGGER.debug("'__deleted' field's value in source recrod: " + deleteFieldValue.toString());
        }
    }

    private void verifyPrimaryKeyOnly(long recordsCount) {
        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsCount);

        for (int i = 0; i < records.size(); ++i) {
            // verify the records
            assertValueField(records.get(i), "after/id/value", i);
        }
    }

    private void verifyValue(long recordsCount) {
        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsCount);

        try {
            for (int i = 0; i < records.size(); ++i) {
                assertValueField(records.get(i), "after/id/value", i);
                assertValueField(records.get(i), "after/first_name/value", "Vaibhav");
                assertValueField(records.get(i), "after/last_name/value", "Kushwaha");
            }
        }
        catch (Exception e) {
            LOGGER.error("Exception caught while parsing records: " + e);
            fail();
        }
    }

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

    // This test will just verify that the TestContainers are up and running
    // and it will also verify that the unit tests are able to make API calls.
    @Test
    public void testTestContainers() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        insertRecords(2);
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
        assertNotNull(dbStreamId);
        assertTrue(dbStreamId.length() > 0);

        dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", true, false);
        assertNotNull(dbStreamId);
        assertTrue(dbStreamId.length() > 0);

        dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", true, true);
        assertNotNull(dbStreamId);
        assertTrue(dbStreamId.length() > 0);
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void testRecordConsumption(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        startEngine(configBuilder);
        final long recordsCount = 1;

        awaitUntilConnectorIsReady();

        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        insertRecords(recordsCount);

        CompletableFuture.runAsync(() -> verifyPrimaryKeyOnly(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();

        // Since this table has no datatypes which require a JDBC connection to be decoded,
        // we shouldn't have opened any connections.
        assertEquals(0, TestHelper.getConnectionCount(YugabyteDBConnection.CONNECTION_GENERAL));

        LOGGER.info("Waiting for 2 minutes");
        TestHelper.waitFor(Duration.ofMinutes(2));
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void shouldNotCauseDataLossIfThereIsErrorWhileProcessingBatch(
        boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        LogInterceptor logInterceptor = new LogInterceptor(YugabyteDBStreamingChangeEventSource.class);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, 60_000);
        startEngine(configBuilder);
        final long recordsCount = 1;

        awaitUntilConnectorIsReady();
        insertRecords(recordsCount);

        YugabyteDBStreamingChangeEventSource.TEST_FAIL_WHILE_PROCESSING_BATCH = true;

        // Do not change the error message.
        final String errorMessage = "[TEST ONLY] Failing while processing the batch of records";

        // Wait till we have received the failure message.
        Awaitility.await()
          .atMost(Duration.ofSeconds(60))
          .until(() -> logInterceptor.containsStacktraceElement(errorMessage));

        // Revert the flag so that processing can resume normally.
        YugabyteDBStreamingChangeEventSource.TEST_FAIL_WHILE_PROCESSING_BATCH = false;

        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsCount);
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void testRangeSplitTables(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1_range", consistentSnapshot, useSnapshot);
        LOGGER.info("Created stream ID: {}", dbStreamId);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1_range", dbStreamId);
        startEngine(configBuilder);
        final int recordsCount = 100;

        awaitUntilConnectorIsReady();

        TestHelper.executeBulk("INSERT INTO t1_range values (%d);", recordsCount);

        waitAndFailIfCannotConsume(new ArrayList<>(), recordsCount);
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void verifyConsumptionAfterRestart(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        YBClient ybClient = TestHelper.getYbClient(getMasterAddress());

        // Stop YugabyteDB instance, this should result in failure of yb-client APIs as well
        restartYugabyteDB(5000);

        GetDBStreamInfoResponse response = ybClient.getDBStreamInfo(dbStreamId);
        assertNotNull(response.getNamespaceId());

        final int recordsCount = 1;
        insertRecords(recordsCount);
        CompletableFuture.runAsync(() -> verifyPrimaryKeyOnly(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();

        ybClient.close();
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void testRecordDeleteFieldWithYBExtractNewRecordState(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        YBExtractNewRecordState<SourceRecord> transformation = new YBExtractNewRecordState<>();

        Map<String, Object> configs = new HashMap<String, Object>();
        configs.put(ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES.toString(), "false");
        configs.put(ExtractNewRecordStateConfigDefinition.HANDLE_DELETES.toString(), "rewrite");
        transformation.configure(configs);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        startEngine(configBuilder);
        final long rowsCount = 1;

        awaitUntilConnectorIsReady();

        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        insertRecords(rowsCount);
        // update rows in the table t1 where id is <some-pk>
        updateRecords(rowsCount);
        // delete rows in the table t1 where id is <some-pk>
        deleteRecords(rowsCount);

        // We have called 'insert', 'update' and 'delete' on each row. Thus we expect
        // (rowsCount * 3) number of recrods, additionaly, with a delete record, we will see
        // a tombstone record so we will 1 record for that as well.
        final long recordsCount = 1 /* insert */ + 1 /* update */ + 1 /* delete */ + 1 /* tombstone */;
        CompletableFuture.runAsync(() -> verifyDeletedFieldPresentInValue(recordsCount, transformation))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();

        transformation.close();
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void testSmallLoad(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        startEngine(configBuilder);
        final long recordsCount = 75;

        awaitUntilConnectorIsReady();
        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        insertRecords(recordsCount);

        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsCount);
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void testVerifyValue(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        startEngine(configBuilder);
        final long recordsCount = 1;

        awaitUntilConnectorIsReady();

        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        insertRecords(recordsCount);

        CompletableFuture.runAsync(() -> verifyValue(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void testNonPublicSchema(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("tables_in_non_public_schema.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "table_in_schema", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("test_schema.table_in_schema", dbStreamId);
        startEngine(configBuilder);
        final long recordsCount = 1;

        awaitUntilConnectorIsReady();
        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        insertRecordsInSchema(recordsCount);

        CompletableFuture.runAsync(() -> verifyValue(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }

    // GitHub issue: https://github.com/yugabyte/debezium-connector-yugabytedb/issues/134
    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void updatePrimaryKeyToSameValue(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);

        // Ignore tombstones since we will not need them for verification.
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.TOMBSTONES_ON_DELETE, false);
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        List<SourceRecord> records = new ArrayList<>();

        // Insert a record and then update the primary key to the same value.
        insertRecords(1); // This will insert a record with id = 0.
        TestHelper.execute("UPDATE t1 SET id = 0 WHERE id = 0;");

        // There should be 3 records - INSERT + DELETE + UPDATE
        waitAndFailIfCannotConsume(records, 3);

        assertEquals("c", TestHelper.getOpValue(records.get(0)));
        assertEquals("d", TestHelper.getOpValue(records.get(1)));
        assertEquals("c", TestHelper.getOpValue(records.get(2)));
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void shouldWorkWithNullValues(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1", consistentSnapshot, useSnapshot);

        String insertFormatString = "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha');";

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        TestHelper.waitFor(Duration.ofSeconds(15));

        List<SourceRecord> records = new ArrayList<>();

        int recordsToBeInserted = 100;
        for (int i = 0; i < recordsToBeInserted; ++i) {
            TestHelper.execute(String.format(insertFormatString, i));
        }

        waitAndFailIfCannotConsume(records, recordsToBeInserted);

        for (int i = 0; i < records.size(); ++i) {
            assertValueField(records.get(i), "after/id/value", i);
            assertValueField(records.get(i), "after/hours/value", null);
        }
    }
}
