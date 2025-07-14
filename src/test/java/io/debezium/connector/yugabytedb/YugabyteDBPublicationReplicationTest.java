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

    @BeforeAll
    public static void beforeClass() throws SQLException {
        setMasterFlags("ysql_yb_enable_replication_commands=true");
        setTserverFlags("ysql_yb_enable_replication_commands=true");
        initializeYBContainer( );
        TestHelper.dropAllSchemas();
    }

    @BeforeEach
    public void before() throws Exception {
        initializeConnectorTestFramework();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
    }

    @AfterEach
    public void after() throws Exception {
        stopConnector();
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
        String insertStatement = "INSERT INTO t2 values (%d);";
        final int recordsCount = 1000;
        TestHelper.executeBulk(insertStatement, recordsCount);

        TestHelper.execute(String.format(TestHelper.createPublicationForTableStatement, "pub", "t2"));
        TestHelper.execute(TestHelper.createReplicationSlotStatement);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderWithPublication("yugabyte", "pub", "test_replication_slot"); 
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        verifyRecordCount(recordsCount);
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
        TestHelper.execute(String.format(TestHelper.createPublicationForTableStatement, "pub", "t1"));
        TestHelper.execute(TestHelper.createReplicationSlotStatement);
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

    private void insertRecords(long numOfRowsToBeInserted) throws Exception {
        String formatInsertString = "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 30);";
        for (int i = 0; i < numOfRowsToBeInserted; i++) {
            TestHelper.execute(String.format(formatInsertString, i));
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
    
    private boolean dropReplicationSlot() throws Exception {
        try (YugabyteDBConnection ybConnection = TestHelper.create();
             Connection connection = ybConnection.connection()) {
            final Statement statement = connection.createStatement();
            final ResultSet rs = statement.executeQuery(TestHelper.dropReplicationSlotStatement);
            return rs.next();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void verifyRecordCount(long recordsCount) {
        waitAndFailIfCannotConsume(new ArrayList<>(), recordsCount);
    }

}
