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
    CqlSession session;

    @BeforeAll
    public static void beforeClass() throws SQLException {
        initializeYBContainer();
        // TestHelper.dropAllSchemas();


    }

    @BeforeEach
    public void before() {
        initializeConnectorTestFramework();
        session = CqlSession
                .builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .build();

    }

    @AfterEach
    public void after() throws Exception {
        // session.execute("drop table cdctest.test_cdc;");
        stopConnector();
        // TestHelper.executeDDL("drop_tables_and_databases.ddl");
    }

    @AfterAll
    public static void afterClass() {
        shutdownYBContainer();
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
        // String dbStreamId = "6ecbf7a7617d495e8cb0b5cf19193f39";

        // Create keyspace 'ybdemo' if it does not exist.
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS cdctest;";
        session.execute(createKeyspace);
        LOGGER.info("Sumukh: keyspace created");

        session.execute("create table if not exists cdctest.test_cdc(a int primary key, b int);");
        LOGGER.info("Sumukh: table created");

        String dbStreamId = TestHelper.getNewDbStreamId("cdctest", "test_cdc", false, false,true);
        LOGGER.info("Sumukh Stream ID created " + dbStreamId);
        session.execute("insert into cdctest.test_cdc(a,b) values (1,2);");
        LOGGER.info("Sumukh: Insert successful");

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderForCQL("cdctest","cdctest.test_cdc", dbStreamId);
        LOGGER.info("Sumukh before start");
        // start(YugabyteDBConnector.class, configBuilder.build());
        startEngine(configBuilder);
        final long recordsCount = 1;
        LOGGER.info("Sumukh after start");


        awaitUntilConnectorIsReady();

        session.execute("insert into cdctest.test_cdc(a,b) values (2,3);");
        session.execute("update cdctest.test_cdc set b = 4 where a = 2;");
        session.execute("delete from cdctest.test_cdc where a = 2;");
        verifyRecordCount(4);

        LOGGER.info("Done");
    }

    private void verifyRecordCount(long recordsCount) {
        waitAndFailIfCannotConsume(new ArrayList<>(), recordsCount);
    }


}
