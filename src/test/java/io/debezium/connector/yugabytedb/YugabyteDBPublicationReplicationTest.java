package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;
import org.yb.client.GetDBStreamInfoResponse;
import org.yb.client.YBClient;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;

import static org.junit.jupiter.api.Assertions.*;

public class YugabyteDBPublicationReplicationTest extends YugabytedTestBase {
    @BeforeAll
    public static void beforeClass() throws SQLException {
        initializeYBContainer();
        // TestHelper.dropAllSchemas();
    }

    @BeforeEach
    public void before() {
        initializeConnectorTestFramework();
    }

    @AfterEach
    public void after() throws Exception {
        stopConnector();
        // TestHelper.executeDDL("drop_tables_and_databases.ddl");
    }

    @AfterAll
    public static void afterClass() {
        shutdownYBContainer();
    }

    @Test
    public void testPublicationReplication() throws Exception {
        Configuration.Builder configBuilder = TestHelper.getConfigBuilderWithPublication("yugabyte", "pub", "slot_2"); 
        startEngine(configBuilder);
        final long recordsCount = 1;

        awaitUntilConnectorIsReady();

        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        Thread.sleep(30000);
        LOGGER.info("iNSERTING RECORD");
        insertRecords(recordsCount);

        CompletableFuture.runAsync(() -> verifyPrimaryKeyOnly(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();

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
}
