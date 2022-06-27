package io.debezium.connector.yugabytedb;

import static org.fest.assertions.Assertions.*;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.YugabyteYSQLContainer;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;

public class YugabyteDBConnectorIT extends YugabyteDBTestBase {
    private final static Logger LOGGER = Logger.getLogger(YugabyteDBConnectorIT.class);

    private static YugabyteYSQLContainer ybContainer;

    private YugabyteDBConnector connector;

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
    public void afterClass() throws Exception {
        ybContainer.stop();
    }

    private void validateFieldDef(Field expected) {
        ConfigDef configDef = connector.config();
        assertThat(configDef.names()).contains(expected.name());
        ConfigDef.ConfigKey key = configDef.configKeys().get(expected.name());
        assertThat(key).isNotNull();
        assertThat(key.name).isEqualTo(expected.name());
        assertThat(key.displayName).isEqualTo(expected.displayName());
        assertThat(key.importance).isEqualTo(expected.importance());
        assertThat(key.documentation).isEqualTo(expected.description());
        assertThat(key.type).isEqualTo(expected.type());
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
        assertThat(totalConsumedRecords).isEqualTo(recordsCount);
    }

    @Test
    public void shouldValidateConnectorConfigDef() {
        connector = new YugabyteDBConnector();
        ConfigDef configDef = connector.config();
        assertThat(configDef).isNotNull();
        YugabyteDBConnectorConfig.ALL_FIELDS.forEach(this::validateFieldDef);
    }

    @Test
    public void shouldNotStartWithInvalidConfiguration() throws Exception {
        // use an empty configuration which should be invalid because of the lack of DB connection details
        Configuration config = Configuration.create().build();

        // we expect the engine will log at least one error, so preface it ...
        LOGGER.info("Attempting to start the connector with an INVALID configuration, so MULTIPLE error messages & one exception will appear in the log");
        start(YugabyteDBConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
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
            assertThat(success).isFalse();
            assertThat(error).isInstanceOf(DebeziumException.class);

            assertThat(error.getMessage().contains("is not a part of the stream ID " + dbStreamId)).isTrue();
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
}
