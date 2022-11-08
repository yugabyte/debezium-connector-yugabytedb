package io.debezium.connector.yugabytedb;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.log4j.Logger;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.YugabyteYSQLContainer;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;
import io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;
import io.debezium.util.Strings;

/**
 * Basic unit tests to check the behaviour with YugabyteDB datatypes
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */

public class YugabyteDBDatatypesTest extends YugabyteDBTestBase {
    private final static Logger LOGGER = Logger.getLogger(YugabyteDBDatatypesTest.class);
    private static YugabyteYSQLContainer ybContainer;

    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
            "INSERT INTO s2.a (aa) VALUES (1);";
    private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "DROP SCHEMA IF EXISTS s2 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));";
    private static final String SETUP_TABLES_STMT = CREATE_TABLES_STMT + INSERT_STMT;

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

  private void insertBulkRecords(int numRecords) throws Exception {
    String formatInsertString = "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 30);";
    CompletableFuture.runAsync(() -> {
        TestHelper.executeBulk(formatInsertString, numRecords);
    }).exceptionally(throwable -> {
      throw new RuntimeException(throwable);
    }).get();
  }
    // This function will one row each of the specified enum labels
    private void insertEnumRecords() throws Exception {
        String[] enumLabels = {"ZERO", "ONE", "TWO"};
        String formatInsertString = "INSERT INTO test_enum VALUES (%d, '%s');";
        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < enumLabels.length; i++) {
                TestHelper.execute(String.format(formatInsertString, i, enumLabels[i]));
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
        int totalConsumedRecords = 0;
        long start = System.currentTimeMillis();
        List<SourceRecord> records = new ArrayList<>();
        while (totalConsumedRecords < recordsCount) {
            int consumed = super.consumeAvailableRecords(record -> {
                LOGGER.debug("The record being consumed is " + record);
                records.add(record);
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
                LOGGER.debug("Consumed " + totalConsumedRecords + " records");
            }
        }
        LOGGER.info("Total duration to consume " + recordsCount + " records: " + Strings.duration(System.currentTimeMillis() - start));

        for (int i = 0; i < recordsCount; ++i) {
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
        int totalConsumedRecords = 0;
        long start = System.currentTimeMillis();
        List<SourceRecord> records = new ArrayList<>();
        while (totalConsumedRecords < recordsCount) {
            int consumed = super.consumeAvailableRecords(record -> {
                LOGGER.debug("The record being consumed is " + record);
                records.add(record);
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
                LOGGER.debug("Consumed " + totalConsumedRecords + " records");
            }
        }
        LOGGER.info("Total duration to consume " + recordsCount + " records: " + Strings.duration(System.currentTimeMillis() - start));

        for (int i = 0; i < records.size(); ++i) {
            // verify the records
            assertValueField(records.get(i), "after/id/value", i);
        }
    }

    private void verifyRecordCount(long recordsCount) {
        int totalConsumedRecords = 0;
        long start = System.currentTimeMillis();
        while (totalConsumedRecords < recordsCount) {
            int consumed = super.consumeAvailableRecords(record -> {
                LOGGER.debug("The record being consumed is " + record);
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
                LOGGER.debug("Consumed " + totalConsumedRecords + " records");
            }
        }
        LOGGER.info("Total duration to consume " + recordsCount + " records: " + Strings.duration(System.currentTimeMillis() - start));

        assertEquals(recordsCount, totalConsumedRecords);
    }

    private void verifyRecordCountUntilTimeExpires(long recordsCount, long milliSecondsToWait) {
        int totalConsumedRecords = 0;
        long start = System.currentTimeMillis();
        while (totalConsumedRecords < recordsCount && ((System.currentTimeMillis() - start) < milliSecondsToWait)) {
            int consumed = super.consumeAvailableRecords(record -> {
                LOGGER.debug("The record being consumed is " + record);
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
                LOGGER.debug("Consumed " + totalConsumedRecords + " records");
            }
        }
        LOGGER.info("Total duration to consume " + recordsCount + " records: " + Strings.duration(System.currentTimeMillis() - start));

        assertEquals(recordsCount, totalConsumedRecords);
    }

    private void func(long recordsCount, long milliSecondsToWait) {
        int totalConsumedRecords = 0;
        long start = System.currentTimeMillis();
        while (totalConsumedRecords < recordsCount && ((System.currentTimeMillis() - start) < milliSecondsToWait)) {
            int consumed = super.consumeAvailableRecords(record -> {
                LOGGER.debug("The record being consumed is " + record);
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
                LOGGER.debug("Consumed " + totalConsumedRecords + " records");
            }
        }
        LOGGER.info("Total duration to consume " + recordsCount + " records: " + Strings.duration(System.currentTimeMillis() - start));

        assertEquals(recordsCount, totalConsumedRecords);
    }

    private void verifyValue(long recordsCount) {
        int totalConsumedRecords = 0;
        long start = System.currentTimeMillis();
        List<SourceRecord> records = new ArrayList<>();
        while (totalConsumedRecords < recordsCount) {
            int consumed = super.consumeAvailableRecords(record -> {
                LOGGER.debug("The record being consumed is " + record);
                records.add(record);
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
                LOGGER.debug("Consumed " + totalConsumedRecords + " records");
            }
        }
        LOGGER.info("Total duration to consume " + recordsCount + " records: " + Strings.duration(System.currentTimeMillis() - start));

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

  private void verifyEnumValue(long recordsCount) {
    int totalConsumedRecords = 0;
    long start = System.currentTimeMillis();
    List<SourceRecord> records = new ArrayList<>();
    while (totalConsumedRecords < recordsCount) {
      int consumed = super.consumeAvailableRecords(record -> {
        LOGGER.debug("The record being consumed is " + record);
        records.add(record);
      });
      if (consumed > 0) {
        totalConsumedRecords += consumed;
        LOGGER.debug("Consumed " + totalConsumedRecords + " records");
      }
    }
    LOGGER.info("Total duration to consume " + recordsCount + " records: " + Strings.duration(System.currentTimeMillis() - start));
    String[] enum_val = {"ZERO", "ONE", "TWO"};

    try {
      for (int i = 0; i < records.size(); ++i) {
        assertValueField(records.get(i), "after/id/value", i);
        assertValueField(records.get(i), "after/enum_col/value", enum_val[i]);
      }
    }
    catch (Exception e) {
      LOGGER.error("Exception caught while parsing records: " + e);
      fail();
    }
  }
    @BeforeClass
    public static void beforeClass() throws SQLException {
        ybContainer = TestHelper.getYbContainer();
        ybContainer.start();

        TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433));
        TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));
        TestHelper.dropAllSchemas();
    }

    @Before
    public void before() {
        initializeConnectorTestFramework();
    }

    @After
    public void after() throws Exception {
        stopConnector();
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ybContainer.stop();
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
        start(YugabyteDBConnector.class, configBuilder.build());
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

    // Only verifying the record count since the snapshot records are not ordered so it may be
    // a little complex to verify them in the sorted order at the moment
    CompletableFuture.runAsync(() -> verifyRecordCount(recordsCount))
      .exceptionally(throwable -> {
        throw new RuntimeException(throwable);
      }).get();
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
        start(YugabyteDBConnector.class, configBuilder.build());
        final long rowsCount = 1;

        awaitUntilConnectorIsReady();

        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        insertRecords(rowsCount);
        // update rows in the table t1 where id is <some-pk>
        updateRecords(rowsCount);
        // delete rows in the table t1 where id is <some-pk>
        deleteRecords(rowsCount);

        // We have called 'insert', 'update' and 'delete' on each row. Thus we expect (rowsCount * 3) number of recrods
        final long recordsCount = rowsCount * 3;
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
        start(YugabyteDBConnector.class, configBuilder.build());
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
        start(YugabyteDBConnector.class, configBuilder.build());
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
    public void testEnumValue() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
        Thread.sleep(1000);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "test_enum");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.test_enum", dbStreamId);
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        // 3 because there are 3 enum values in the enum type
        final long recordsCount = 3;

        awaitUntilConnectorIsReady();

        // 3 records will be inserted in the table test_enum
        insertEnumRecords();

        CompletableFuture.runAsync(() -> verifyEnumValue(recordsCount))
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
        start(YugabyteDBConnector.class, configBuilder.build());
        final long recordsCount = 1;

        awaitUntilConnectorIsReady();
        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        insertRecordsInSchema(recordsCount);

        CompletableFuture.runAsync(() -> verifyValue(recordsCount))
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

        // Dummy wait condition to wait for another 10 seconds
        Awaitility.await()
            .pollDelay(Duration.ofSeconds(10))
            .atMost(Duration.ofSeconds(15))
            .until(() -> {
                return true;
            });   
        
        SourceRecords records = consumeRecordsByTopic(recordCountT1);

        assertNotNull(records);

        // Assert that there are the expected number of records in the snapshotted table
        assertEquals(recordCountT1, records.recordsForTopic("test_server.public.t1").size());

        // Since there are no records for this topic, the topic itself won't be created
        // so if the topic simply doesn't exist then the test should pass
        assertFalse(records.topics().contains("test_server.public.all_types"));
    }
}
