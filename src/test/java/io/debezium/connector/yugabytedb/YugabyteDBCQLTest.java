package io.debezium.connector.yugabytedb;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.*;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class YugabyteDBCQLTest extends YugabytedTestBase/*YugabyteDBContainerTestBase*/ {

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

    private void waitAndFailIfCannotConsume(List<SourceRecord> records, long recordsCount) {
        waitAndFailIfCannotConsume(records, recordsCount, 300 * 1000 /* 5 minutes */);
    }
    private void verifyPrimaryKeyOnly(long recordsCount) {
        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsCount);

        for (int i = 0; i < records.size(); ++i) {
            // verify the records
            assertValueField(records.get(i), "after/id/value", i);
        }
    }

    @Test
    public void testRecordConsumption() throws Exception {
        CqlSession session = CqlSession
                .builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .build();

        // String dbStreamId = TestHelper.getNewDbStreamId("cdctest", "test_cdc");
        String dbStreamId = "";

        // Create keyspace 'ybdemo' if it does not exist.
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS cdctest;";
        session.execute(createKeyspace);
        session.execute("drop table if exists cdctest.test_cdc;");
        session.execute("create table cdctest.test_cdc(a int primary key, b int);");
        session.execute("insert into cdctest.test_cdc(a,b) values (1,2);");
        String select_stmt =
                "select * from cdctest.test_cdc where a = 1;";
        PreparedStatement stmt = session.prepare(select_stmt);
        ResultSet rs = session.execute(stmt.bind());
        Row row = rs.one();
        System.out.println("Row " + row);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("cdctest.test_cdc", dbStreamId);
        start(YugabyteDBConnector.class, configBuilder.build());
        final long recordsCount = 1;

        awaitUntilConnectorIsReady();

        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        insertRecords(recordsCount);

        CompletableFuture.runAsync(() -> verifyPrimaryKeyOnly(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();

        System.out.println("Done");
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


}
