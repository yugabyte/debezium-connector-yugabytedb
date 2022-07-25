package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.testcontainers.containers.YugabyteYSQLContainer;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;

public class YugabyteDBConfigTest extends YugabyteDBTestBase {
  private final static Logger LOGGER = Logger.getLogger(YugabyteDBConnectorIT.class);

    private static YugabyteYSQLContainer ybContainer;

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

    private void insertRecords(int numOfRowsToBeInserted) throws Exception {
        String formatInsertString = "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 30);";
        CompletableFuture.runAsync(() -> {
            for (int i = 0; i < numOfRowsToBeInserted; i++) {
                TestHelper.execute(String.format(formatInsertString, i));
            }
        }).exceptionally(throwable -> {
            throw new RuntimeException(throwable);
        }).get();
    }

    private void verifyRecordCount(int recordsCount) {
        int totalConsumedRecords = 0;
        while (totalConsumedRecords < recordsCount) {
            int consumed = super.consumeAvailableRecords(record -> {
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
                LOGGER.debug("Consumed " + totalConsumedRecords + " records");
            }
        }
        assertEquals(recordsCount, totalConsumedRecords);
    }

    // This verifies that the connector should not be running if a wrong table.include.list is provided
    @Test
    public void shouldThrowExceptionWithWrongIncludeList() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "all_types");

        // Create another table which will not be a part of the DB stream ID
        TestHelper.execute("CREATE TABLE not_part_of_stream (id INT PRIMARY KEY);");

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.all_types,public.not_part_of_stream", dbStreamId);

        // This should throw a DebeziumException saying the table not_part_of_stream is not a part of stream ID
        start(YugabyteDBConnector.class, configBuilder.build(), (success, msg, error) -> {
            assertFalse(success);
            assertTrue(error instanceof DebeziumException);

            assertTrue(error.getMessage().contains("is not a part of the stream ID " + dbStreamId));
        });

        assertConnectorNotRunning();
    }

    // This test verifies that the connector configuration works properly even if there are tables of the same name
    // in another database
    @Test
    public void shouldWorkWithSameNameTablePresentInAnotherDatabase() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");

        // Create a same table in another database
        // This is to ensure that when the yb-client returns all the tables, then the YugabyteDBConnector
        // is filtering them properly
        String createNewTableStatement = "CREATE TABLE t1 (id INT PRIMARY KEY, first_name TEXT NOT NULL, last_name VARCHAR(40), hours DOUBLE PRECISION);";
        
        // The name of the secondary database is secondary_database
        TestHelper.createTableInSecondaryDatabase(createNewTableStatement);

        // The config builder returns a default config with the database as "postgres"
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

        start(YugabyteDBConnector.class, configBuilder.build());

        awaitUntilConnectorIsReady();

        int recordsCount = 10;
        insertRecords(recordsCount);

        CompletableFuture.runAsync(() -> verifyRecordCount(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }

    @Test
    public void shouldNotWorkWithWrongStreamId() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String invalidStreamId = "someInvalidDbStreamId";

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", invalidStreamId);
        start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
            assertFalse(success);

            String expectedErrorMessageLine = String.format("Could not find CDC stream: db_stream_id: \"%s\"", invalidStreamId);
            assertTrue(error.getCause().getMessage().contains(expectedErrorMessageLine));
        });

        assertConnectorNotRunning();
    }

    @Test
    public void shouldNotWorkWithWrongDatabaseName() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        // Create a dbStreamId on the default namespace but provide a different namespace to the configuration
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");

        String wrongNamespaceName = "wrong_namespace";
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(wrongNamespaceName, "public.t1", dbStreamId);
        start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
            assertFalse(success);

            String expectedErrorMessageLine = String.format("FATAL: database \"%s\" does not exist", wrongNamespaceName);
            assertTrue(error.getCause().getMessage().contains(expectedErrorMessageLine));
        });

        assertConnectorNotRunning();
    }

    @Test
    public void shouldNotAuthenticateWithWrongUsername() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        // Create a dbStreamId on the default namespace
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");

        Configuration.Builder configBuilderWithWrongUser = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        configBuilderWithWrongUser.with(YugabyteDBConnectorConfig.USER, "wrong_username");
        start(YugabyteDBConnector.class, configBuilderWithWrongUser.build(), (success, message, error) -> {
            assertFalse(success);

            assertTrue(error.getCause().getMessage().contains("authentication failed for user \"wrong_username\""));
        });

        assertConnectorNotRunning();
    }

    @Test
    public void shouldNotAuthenticateWithWrongPassword() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        // Create a dbStreamId on the default namespace
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");

        Configuration.Builder configBuilderWithWrongPassword = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        configBuilderWithWrongPassword.with(YugabyteDBConnectorConfig.PASSWORD, "wrong_password");
        start(YugabyteDBConnector.class, configBuilderWithWrongPassword.build(), (success, message, error) -> {
            assertFalse(success);

            assertTrue(error.getCause().getMessage().contains("password authentication failed for user \"yugabyte\""));
        });

        assertConnectorNotRunning();
    }

    @Test
    public void shouldThrowProperErrorMessageWithEmptyTableList() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        // Create a dbStreamId on the default namespace
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");

        Configuration.Builder configBuilderWithWrongPassword = TestHelper.getConfigBuilder("", dbStreamId);
        start(YugabyteDBConnector.class, configBuilderWithWrongPassword.build(), (success, message, error) -> {
            assertFalse(success);

            assertTrue(error.getMessage().contains("The table.include.list is empty, please provide a list of tables to get the changes from"));
        });

        assertConnectorNotRunning();
    }

    @Test
    public void shouldThrowExceptionIfTheProvidedTableDoesNotExist() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        // Create a dbStreamId on the default namespace
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");

        Configuration.Builder configBuilderWithWrongPassword = TestHelper.getConfigBuilder("public.non_existent_table", dbStreamId);
        start(YugabyteDBConnector.class, configBuilderWithWrongPassword.build(), (success, message, error) -> {
            assertFalse(success);

            assertTrue(error.getMessage().contains("The tables provided in table.include.list do not exist"));
        });

        assertConnectorNotRunning();
    }

    @Test
    public void shouldThrowProperErrorMessageWithEmptyStream() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        // Provide a empty stream ID as the configuration to verify for the error message
        String dbStreamId = "";

        Configuration.Builder configBuilderWithWrongPassword = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        start(YugabyteDBConnector.class, configBuilderWithWrongPassword.build(), (success, message, error) -> {
            assertFalse(success);

            assertTrue(error.getMessage().contains("DB Stream ID not provided, please provide a DB stream ID to proceed"));
        });

        assertConnectorNotRunning();
    }
}
