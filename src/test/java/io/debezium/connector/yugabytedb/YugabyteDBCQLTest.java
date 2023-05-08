package io.debezium.connector.yugabytedb;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import org.junit.jupiter.api.*;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

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

    @Test
    public void testRecordConsumption() throws Exception {
        CqlSession session = CqlSession
                .builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .build();

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
