package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

import io.debezium.connector.yugabytedb.connection.HashPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.yb.cdc.CdcService;
import org.yb.client.*;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;
import org.yb.master.MasterClientOuterClass;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic unit tests to check the behaviour with YugabyteDB datatypes
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */

public class YugabyteDBDatatypesTest extends YugabyteDBContainerTestBase {
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
        initializeYBContainer("enable_tablet_split_of_cdcsdk_streamed_tables=true", null);
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
    }

    @Test
    public void testRecordConsumption() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
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
    }

    @Test
    public void testKeyInRange() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        YBClient ybClient = TestHelper.getYbClient(getMasterAddress());
        YBTable ybTable = TestHelper.getYbTable(ybClient, "t1");
        Objects.requireNonNull(ybTable);

        insertRecords(100);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
        LOGGER.info("Created stream ID: {}", dbStreamId);

        GetTabletListToPollForCDCResponse resp1 = ybClient.getTabletListToPollForCdc(ybTable, dbStreamId, ybTable.getTableId());
        LOGGER.info("Got get tablet list to poll response with tablet pair size: {}", resp1.getTabletCheckpointPairListSize());

        assertEquals(1, resp1.getTabletCheckpointPairListSize());

        CdcService.TabletCheckpointPair tcp = resp1.getTabletCheckpointPairList().get(0);
        HashPartition originalPartition = new HashPartition(tcp.getTabletLocations().getTableId().toStringUtf8(),
          tcp.getTabletLocations().getTabletId().toStringUtf8(),
          tcp.getTabletLocations().getPartition().getPartitionKeyStart().toByteArray(),
          tcp.getTabletLocations().getPartition().getPartitionKeyEnd().toByteArray(), tcp.getTabletLocations().getPartition().getHashBucketsList());

        ybClient.flushTable(ybTable.getTableId());
        LOGGER.info("Waiting for 30s for table to be flushed");
        TestHelper.waitFor(Duration.ofSeconds(30));

        ybClient.splitTablet(tcp.getTabletLocations().getTabletId().toStringUtf8());
        LOGGER.info("Waiting for children for tablet {} to be split", tcp.getTabletLocations().getTabletId().toStringUtf8());
        TestHelper.waitForTablets(ybClient, ybTable, 2);

        List<String> children = new ArrayList<>(ybClient.getTabletUUIDs(ybTable));

//        Awaitility.await()
//          .pollDelay(Duration.ofSeconds(2))
//          .atMost(Duration.ofSeconds(20))
//          .until(() -> {
//              GetTabletLocationsResponse r = ybClient.getTabletLocations(List.of(tcp.getTabletLocations().getTabletId().toStringUtf8()), ybTable.getTableId(), true, false);
//              LOGGER.info("Got response size: {}", r.getTabletLocations().size());
//              return r.getTabletLocations().size() == 2;
//          });

        GetTabletLocationsResponse tResp = ybClient.getTabletLocations(children, ybTable.getTableId(), true, false);
//        GetTabletListToPollForCDCResponse resp2 = ybClient.getTabletListToPollForCdc(ybTable, dbStreamId, ybTable.getTableId());
        assertEquals(2, tResp.getTabletLocations().size());

        LOGGER.info("Hash buckets list size in parent: {}", originalPartition.getHashBuckets());

        MasterClientOuterClass.TabletLocationsPB c1 = tResp.getTabletLocations().get(0);
        HashPartition child1 = new HashPartition(c1.getTableId().toStringUtf8(), c1.getTabletId().toStringUtf8(),  c1.getPartition().getPartitionKeyStart().toByteArray(),
          c1.getPartition().getPartitionKeyEnd().toByteArray(), c1.getPartition().getHashBucketsList());
        LOGGER.info("Child 1 hash buckets count: {}", child1.getHashBuckets());

        MasterClientOuterClass.TabletLocationsPB c2 = tResp.getTabletLocations().get(1);
        HashPartition child2 = new HashPartition(c2.getTableId().toStringUtf8(), c2.getTabletId().toStringUtf8(), c2.getPartition().getPartitionKeyStart().toByteArray(),
          c2.getPartition().getPartitionKeyEnd().toByteArray(), c2.getPartition().getHashBucketsList());
        LOGGER.info("Child 2 hash buckets count: {}", child2.getHashBuckets());

        LOGGER.info("Got 2 tablets, comparing their partition now");

        LOGGER.info("Child 1 contain status: {}", originalPartition.containsPartition(child1));
        LOGGER.info("Child 2 contain status: {}", originalPartition.containsPartition(child2));

        LOGGER.info("Child 1 conflict status: {}", originalPartition.isConflictingWith(child1));
        LOGGER.info("Child 2 conflict status: {}", originalPartition.isConflictingWith(child2));

        // Split both the children too now.
        ybClient.flushTable(ybTable.getTableId());
        LOGGER.info("Waiting for 30s for table to be flushed");
        TestHelper.waitFor(Duration.ofSeconds(30));
        ybClient.splitTablet(children.get(0));
        ybClient.splitTablet(children.get(1));
        LOGGER.info("Waiting for children to be split further");
        TestHelper.waitForTablets(ybClient, ybTable, 4);

        List<String> finalChildren = new ArrayList<>(ybClient.getTabletUUIDs(ybTable));
        GetTabletLocationsResponse r = ybClient.getTabletLocations(finalChildren, ybTable.getTableId(), true, false);
        assertEquals(4, r.getTabletLocations().size());

        MasterClientOuterClass.TabletLocationsPB finalC1 = r.getTabletLocations().get(0);
        HashPartition finalChild1 = new HashPartition(finalC1.getTableId().toStringUtf8(), finalC1.getTableId().toStringUtf8(), finalC1.getPartition().getPartitionKeyStart().toByteArray(),
          finalC1.getPartition().getPartitionKeyEnd().toByteArray(), finalC1.getPartition().getHashBucketsList());

        MasterClientOuterClass.TabletLocationsPB finalC2 = r.getTabletLocations().get(1);
        HashPartition finalChild2 = new HashPartition(finalC2.getTableId().toStringUtf8(), finalC2.getTableId().toStringUtf8(), finalC2.getPartition().getPartitionKeyStart().toByteArray(),
          finalC2.getPartition().getPartitionKeyEnd().toByteArray(), finalC2.getPartition().getHashBucketsList());

        MasterClientOuterClass.TabletLocationsPB finalC3 = r.getTabletLocations().get(2);
        HashPartition finalChild3 = new HashPartition(finalC3.getTableId().toStringUtf8(), finalC3.getTableId().toStringUtf8(), finalC3.getPartition().getPartitionKeyStart().toByteArray(),
          finalC3.getPartition().getPartitionKeyEnd().toByteArray(), finalC3.getPartition().getHashBucketsList());

        MasterClientOuterClass.TabletLocationsPB finalC4 = r.getTabletLocations().get(3);
        HashPartition finalChild4 = new HashPartition(finalC4.getTableId().toStringUtf8(), finalC4.getTableId().toStringUtf8(), finalC4.getPartition().getPartitionKeyStart().toByteArray(),
          finalC4.getPartition().getPartitionKeyEnd().toByteArray(), finalC4.getPartition().getHashBucketsList());

        List<String> e = List.of(finalChild1.toString(), finalChild2.toString(), finalChild3.toString(), finalChild4.toString());
        List<String> elements = new ArrayList<>(e);
        Collections.sort(elements);

        for (String sorted : elements) {
            LOGGER.info(sorted);
        }

        LOGGER.info("All the final partitions are: {} {} {} {}", finalChild1, finalChild2, finalChild3, finalChild4);
    }

    @Test
    public void verifyConsumptionAfterRestart() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1");
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

    @Test
    public void testRecordDeleteFieldWithYBExtractNewRecordState() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        YBExtractNewRecordState<SourceRecord> transformation = new YBExtractNewRecordState<>();

        Map<String, Object> configs = new HashMap<String, Object>();
        configs.put(ExtractNewRecordStateConfigDefinition.DROP_TOMBSTONES.toString(), "false");
        configs.put(ExtractNewRecordStateConfigDefinition.HANDLE_DELETES.toString(), "rewrite");
        transformation.configure(configs);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
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

    @Test
    public void testSmallLoad() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        startEngine(configBuilder);
        final long recordsCount = 75;

        awaitUntilConnectorIsReady();
        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        insertRecords(recordsCount);

        CompletableFuture.runAsync(() -> verifyPrimaryKeyOnly(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }

    @Test
    public void testVerifyValue() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
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

    @Test
    public void testNonPublicSchema() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("tables_in_non_public_schema.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "table_in_schema");
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
    @Test
    public void updatePrimaryKeyToSameValue() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");

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

    @Test
    public void shouldWorkWithNullValues() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1");

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
