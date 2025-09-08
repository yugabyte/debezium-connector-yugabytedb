package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests to verify various configuration settings.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBConfigTest extends YugabyteDBContainerTestBase {

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

    private void insertRecords(int numOfRowsToBeInserted) throws Exception {
        String formatInsertString = "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 30);";
        for (int i = 0; i < numOfRowsToBeInserted; i++) {
            TestHelper.execute(String.format(formatInsertString, i));
        }
    }

    private void verifyRecordCount(int recordsCount) {
        int totalConsumedRecords = 0;
        while (totalConsumedRecords < recordsCount) {
            int consumed = consumeAvailableRecords(record -> {
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
                LOGGER.debug("Consumed " + totalConsumedRecords + " records");
            }
        }
        assertEquals(recordsCount, totalConsumedRecords);
    }

    // This verifies that the connector should not be running if a wrong table.include.list is provided
    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void shouldThrowExceptionWithWrongIncludeList(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "all_types", consistentSnapshot, useSnapshot);

        // Create another table which will not be a part of the DB stream ID
        TestHelper.execute("CREATE TABLE not_part_of_stream (id INT PRIMARY KEY);");

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.all_types,public.not_part_of_stream", dbStreamId);

        // This should throw a DebeziumException saying the table not_part_of_stream is not a part of stream ID
        startEngine(configBuilder, (success, msg, error) -> {
            assertFalse(success);
            assertTrue(error instanceof DebeziumException);

            assertTrue(error.getMessage().contains("is not a part of the stream ID " + dbStreamId));
        });

        assertConnectorNotRunning();
    }

    // This test verifies that the connector configuration works properly even if there are tables of the same name
    // in another database
    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void shouldWorkWithSameNameTablePresentInAnotherDatabase(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);

        // Create a same table in another database
        // This is to ensure that when the yb-client returns all the tables, then the YugabyteDBgRPCConnector
        // is filtering them properly
        String createNewTableStatement = "CREATE TABLE t1 (id INT PRIMARY KEY, first_name TEXT NOT NULL, last_name VARCHAR(40), hours DOUBLE PRECISION);";
        
        // The name of the secondary database is secondary_database
        TestHelper.createTableInSecondaryDatabase(createNewTableStatement);

        // The config builder returns a default config with the database as "postgres"
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        int recordsCount = 10;
        insertRecords(recordsCount);
        verifyRecordCount(recordsCount);
    }

    @Test
    public void shouldNotWorkWithWrongStreamId() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        String invalidStreamId = "someInvalidDbStreamId";

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", invalidStreamId);
        startEngine(configBuilder, (success, message, error) -> {
            assertFalse(success);

            String expectedErrorMessageLine = String.format("Could not find CDC stream: db_stream_id: \"%s\"", invalidStreamId);
            assertTrue(error.getCause().getMessage().contains(expectedErrorMessageLine));
        });

        assertConnectorNotRunning();
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void shouldNotWorkWithWrongDatabaseName(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        // Create a dbStreamId on the default namespace but provide a different namespace to the configuration
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);

        String wrongNamespaceName = "wrong_namespace";
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(wrongNamespaceName, "public.t1", dbStreamId);
        startEngine(configBuilder, (success, message, error) -> {
            assertFalse(success);

            String expectedErrorMessageLine = String.format("FATAL: database \"%s\" does not exist", wrongNamespaceName);
            assertTrue(error.getCause().getMessage().contains(expectedErrorMessageLine));
        });

        assertConnectorNotRunning();
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void shouldNotAuthenticateWithWrongUsername(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        // Create a dbStreamId on the default namespace
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);

        Configuration.Builder configBuilderWithWrongUser = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        configBuilderWithWrongUser.with(YugabyteDBConnectorConfig.USER, "wrong_username");
        startEngine(configBuilderWithWrongUser, (success, message, error) -> {
            assertFalse(success);

            assertTrue(error.getCause().getMessage().contains("authentication failed for user \"wrong_username\""));
        });

        assertConnectorNotRunning();
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void shouldNotAuthenticateWithWrongPassword(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        // Create a dbStreamId on the default namespace
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);

        Configuration.Builder configBuilderWithWrongPassword = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        configBuilderWithWrongPassword.with(YugabyteDBConnectorConfig.PASSWORD, "wrong_password");
        startEngine(configBuilderWithWrongPassword, (success, message, error) -> {
            assertFalse(success);

            assertTrue(error.getCause().getMessage().contains("password authentication failed for user \"yugabyte\""));
        });

        assertConnectorNotRunning();
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void shouldThrowProperErrorMessageWithEmptyTableList(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        // Create a dbStreamId on the default namespace
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);

        Configuration.Builder configBuilderWithWrongPassword = TestHelper.getConfigBuilder("", dbStreamId);
        startEngine(configBuilderWithWrongPassword, (success, message, error) -> {
            assertFalse(success);

            assertTrue(error.getMessage().contains("The table.include.list is empty, please provide a list of tables to get the changes from"));
        });

        assertConnectorNotRunning();
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void shouldThrowExceptionIfTheProvidedTableDoesNotExist(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.executeDDL("yugabyte_create_tables.ddl");

        // Create a dbStreamId on the default namespace
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);

        Configuration.Builder configBuilderWithWrongPassword = TestHelper.getConfigBuilder("public.non_existent_table", dbStreamId);
        startEngine(configBuilderWithWrongPassword, (success, message, error) -> {
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
        startEngine(configBuilderWithWrongPassword, (success, message, error) -> {
            assertFalse(success);

            assertTrue(error.getMessage().contains("DB Stream ID not provided, please provide a DB stream ID to proceed"));
        });

        assertConnectorNotRunning();
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void throwProperErrorMessageIfStreamIdIsNotAssociatedWithAnyTable(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.execute("CREATE TABLE dummy_table (id INT);");
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "dummy_table", consistentSnapshot, useSnapshot);

        Configuration.Builder configBuilderWithHollowStreamId = 
            TestHelper.getConfigBuilder("public.dummy_table", dbStreamId);
        
        startEngine(configBuilderWithHollowStreamId, (success, message, error) -> {
            assertFalse(success);

            assertTrue(error.getMessage().contains("The provided stream ID is not associated with any table"));
        });

        assertConnectorNotRunning();
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    @Disabled("Disabled in lieu of transaction ordering with logical replication")
    public void throwExceptionIfExplicitCheckpointingNotConfiguredWithConsistency(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();

        // Create a stream ID with IMPLICIT checkpointing and then deploy it in a consistent streaming setup.
        TestHelper.execute("CREATE TABLE dummy_table (id INT PRIMARY KEY);");
        final String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "dummy_table", false,
                false, consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.dummy_table", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.TRANSACTION_ORDERING, true);

        start(YugabyteDBgRPCConnector.class, configBuilder.build(), (success, message, error) -> {
           assertFalse(success);

           assertTrue(error.getMessage().contains("Explicit checkpointing not enabled in consistent streaming mode"));
        });

        assertConnectorNotRunning();
    }

    @Test
    public void shouldAllowConnectorDeploymentWhenTransactionOrderingDeprecationIsOverridden() throws Exception {
        TestHelper.dropAllSchemas();

        TestHelper.execute("CREATE TABLE dummy_table (id INT PRIMARY KEY);");
        final String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "dummy_table");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.dummy_table", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.TRANSACTION_ORDERING, true);
        configBuilder.with(YugabyteDBConnectorConfig.OVERRIDE_TRANSACTION_ORDERING_DEPRECATION, true);

        start(YugabyteDBgRPCConnector.class, configBuilder.build(), (success, message, error) -> {
           assertTrue(success);

           assertFalse(error.getMessage().contains("transaction.ordering is disabled with gRPC connector"));
        });

        assertConnectorIsRunning();
    }

    @Test
    public void throwExceptionIfImplicitStreamSpecified() throws Exception {
        TestHelper.dropAllSchemas();

        // Create a stream ID with IMPLICIT checkpointing and then deploy it in a consistent streaming setup.
        TestHelper.execute("CREATE TABLE dummy_table (id INT PRIMARY KEY);");
        final String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "dummy_table", false,
          false, false, false);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.dummy_table", dbStreamId);

        start(YugabyteDBgRPCConnector.class, configBuilder.build(), (success, message, error) -> {
            assertFalse(success);

            assertTrue(error.getMessage().contains("is an IMPLICIT stream, create a stream with EXPLICIT checkpointing and try again"));
        });

        assertConnectorNotRunning();
    }

    @Test
    public void shouldWorkIfForceUseImplicitStreamIsSet() throws Exception {
        TestHelper.dropAllSchemas();

        // Create a stream ID with IMPLICIT checkpointing and then deploy it in a consistent streaming setup.
        TestHelper.execute("CREATE TABLE dummy_table (id INT PRIMARY KEY);");
        final String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "dummy_table", false,
          false, false, false);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.dummy_table", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.FORCE_USE_IMPLICIT_STREAM, true);

        start(YugabyteDBgRPCConnector.class, configBuilder.build(), (success, message, error) -> {
            assertTrue(success);
        });
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    @Disabled("Disabled in lieu of transaction ordering with logical replication")
    public void throwExceptionWithIncorrectTaskCountWithTransactionOrdering(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();

        // Create a stream ID with IMPLICIT checkpointing and then deploy it in a consistent streaming setup.
        TestHelper.execute("CREATE TABLE dummy_table (id INT PRIMARY KEY);");
        final String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "dummy_table", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.dummy_table", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.TRANSACTION_ORDERING, true);
        configBuilder.with("tasks.max", 2);

        start(YugabyteDBgRPCConnector.class, configBuilder.build(), (success, message, error) -> {
           assertFalse(success);

           assertTrue(error.getMessage().contains("Transaction ordering is only supported with 1 task"));
        });

        assertConnectorNotRunning();
    }
}
