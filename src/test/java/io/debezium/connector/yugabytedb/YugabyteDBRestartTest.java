package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;

public class YugabyteDBRestartTest extends YugabyteDBContainerTestBase {
    final String formatInsertString = "INSERT INTO t1 VALUES (generate_series(%d,%d), "
                                        + "'Vaibhav', 'Kushwaha', 30);";
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
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
    }

    @AfterAll
    public static void afterClass() {
        shutdownYBContainer();
    }

    @Test
    public void verifyStateAfterRestart() throws Exception {
        final int totalInsertedRecords = 10;
        // Insert data into the table
        TestHelper.execute(String.format(formatInsertString, 1, totalInsertedRecords));

        // Obtain a connection and verify the number of records.
        try (YugabyteDBConnection ybConnection = TestHelper.create()) {
            Statement st = ybConnection.connection().createStatement();

            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM t1;");
            
            if (rs.next()) {
                assertEquals(totalInsertedRecords, rs.getInt("count"));
            } else {
                fail("Cannot obtain a result set from the database");
            }
        }

        // Stop YugabyteDB, wait and verify that it is stopped and then verify record count
        // after starting it.
        stopYugabyteDB();
        
        boolean queryFailedWhileYugabytedStopped = false;
        try {
            TestHelper.execute("SELECT COUNT(*) FROM t1;");
        } catch (Exception e) {
            // The above query will fail since YugabyteDB is not running.
            assertTrue(e.getMessage().contains("The connection attempt failed"));
            queryFailedWhileYugabytedStopped = true;
        }

        assertTrue(queryFailedWhileYugabytedStopped);

        // Start YugabyteDB.
        startYugabyteDB();


        // Verify that the record count is the same after restarting the YugabyteDB process.
        try (YugabyteDBConnection ybConnection = TestHelper.create()) {
            Statement st = ybConnection.connection().createStatement();

            ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM t1;");
            
            if (rs.next()) {
                assertEquals(totalInsertedRecords, rs.getInt("count"));
            } else {
                fail("Cannot obtain a result set from the database");
            }
        }
    }
}
