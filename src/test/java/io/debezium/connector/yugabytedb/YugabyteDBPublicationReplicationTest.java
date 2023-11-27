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
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.yb.client.GetDBStreamInfoResponse;
import org.yb.client.YBClient;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;

import static org.junit.jupiter.api.Assertions.*;

public class YugabyteDBPublicationReplicationTest extends YugabyteDBContainerTestBase {

    public static String insertStatementFormatfort2 = "INSERT INTO t2 values (%d);";
    public static String insertStatementFormatfort3 = "INSERT INTO t3 values (%d);";

    @BeforeAll
    public static void beforeClass() throws SQLException {
        initializeYBContainer();
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
        configBuilder.with(YugabyteDBConnectorConfig.PUBLICATION_AUTOCREATE_MODE, "all tables");
        startEngine(configBuilder);
        final int recordsCount = 10;

        awaitUntilConnectorIsReady();

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

        Thread.sleep(30000);

        TestHelper.executeBulk(insertStatementFormatfort2, recordsCount);
        TestHelper.executeBulk(insertStatementFormatfort3, recordsCount);

        verifyRecordCount(recordsCount);

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
