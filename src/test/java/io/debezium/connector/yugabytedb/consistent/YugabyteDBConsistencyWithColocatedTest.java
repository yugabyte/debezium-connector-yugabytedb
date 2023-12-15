package io.debezium.connector.yugabytedb.consistent;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.TestHelper;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;

public class YugabyteDBConsistencyWithColocatedTest extends YugabyteDBContainerTestBase {
    @BeforeAll
    public static void beforeClass() throws SQLException {
        setMasterFlags("enable_automatic_tablet_splitting=false");
        setTserverFlags("cdc_populate_safepoint_record=true", "enable_automatic_tablet_splitting=false");
        TestHelper.dropAllSchemas();

        // Create colocated database for usage.
        TestHelper.execute(String.format("CREATE DATABASE %s WITH COLOCATED = true;", DEFAULT_COLOCATED_DB_NAME));
    }

    @BeforeEach
    public void before() throws Exception {
        initializeConnectorTestFramework();
        TestHelper.executeInDatabase("DROP TABLE IF EXISTS employee;", DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase("DROP TABLE IF EXISTS department;", DEFAULT_COLOCATED_DB_NAME);
    }

    @AfterEach
    public void after() throws Exception {
        stopConnector();

        TestHelper.executeInDatabase("DROP TABLE IF EXISTS employee;", DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase("DROP TABLE IF EXISTS department;", DEFAULT_COLOCATED_DB_NAME);
    }

    @AfterAll
    public static void afterClass() throws Exception {
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
        shutdownYBContainer();
    }
    
    @Test
    public void consistencyWithColocatedTables() throws Exception {
        TestHelper.executeInDatabase("CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT, serial_no INT) WITH (colocated = true);", DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase("CREATE TABLE employee (id INT PRIMARY KEY, emp_name TEXT, d_id INT, serial_no INT) WITH (colocated = true);", DEFAULT_COLOCATED_DB_NAME);

        YugabyteDBConnection c = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME);
        Connection conn = c.connection();
        conn.setAutoCommit(false);

        final String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "department", false, true);
        Configuration.Builder configBuilder = getConsistentConfigurationBuilder(DEFAULT_COLOCATED_DB_NAME, "public.department,public.employee",dbStreamId);
        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        final boolean runIndefinitely = false;
        final int iterations = runIndefinitely ? Integer.MAX_VALUE : 20;
        final int seconds = 900;

        final int employeeBatchSize = 25;

        final long totalRecords = iterations /* department */
                                  + iterations * employeeBatchSize; /* employee */

        ExecutorService exec = Executors.newFixedThreadPool(1);
        Future<?> future = exec.submit(() -> {
            int employeeId = 1;
            try (Statement st = conn.createStatement()) {
                long serial = 0;

                for (int i = 0; i < iterations; ++i) {
                    st.execute(String.format("INSERT INTO department VALUES (%d, 'my department no %d', %d);", i, i, serial++));
                    for (int j = employeeId; j <= employeeId + employeeBatchSize - 1; ++j) {
                        st.execute(String.format("INSERT INTO employee VALUES (%d, 'emp no %d', %d, %d);", j, j, i, serial++));
                    }

                    conn.commit();

                    employeeId += employeeBatchSize;
                }
            } catch (Exception e) {
                LOGGER.error("Exception caught: ", e);
                throw new RuntimeException(e);
            }
        });

        List<SourceRecord> recordsToAssert = new ArrayList<>();

        AtomicLong totalConsumedRecords = new AtomicLong();
        try {
            LOGGER.info("Started consuming");
            Awaitility.await()
              .atMost(Duration.ofSeconds(seconds))
              .until(() -> {
                  int consumed = super.consumeAvailableRecords(record -> {
                      LOGGER.debug("The record being consumed is " + record);
                      Struct value = (Struct) record.value();
                      final int serialVal = value.getStruct("after").getStruct("serial_no").getInt32("value");
                      Assertions.assertEquals(recordsToAssert.size(), serialVal,
                        "Expected serial: " + recordsToAssert.size() + " but received "
                          + serialVal + " at index " + recordsToAssert.size()
                          + " with record " + record);

                      recordsToAssert.add(record);
                  });
                  if (consumed > 0) {
                      totalConsumedRecords.addAndGet(consumed);
                      LOGGER.info("Consumed " + totalConsumedRecords.get() + " records");
                  }

                  return recordsToAssert.size() == (long) totalRecords && future.isDone();
              });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + (long) totalRecords + " records in " + seconds + " seconds, consumed " + totalConsumedRecords.get(), exception);
        }
    }

    private Configuration.Builder getConsistentConfigurationBuilder(String databaseName, String tableIncludeList, String dbStreamId) throws Exception {
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(databaseName, tableIncludeList, dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.TRANSACTION_ORDERING, true);
        configBuilder.with("transforms", "Reroute");
        configBuilder.with("transforms.Reroute.type", "io.debezium.transforms.ByLogicalTableRouter");
        configBuilder.with("transforms.Reroute.topic.regex", "(.*)");
        configBuilder.with("transforms.Reroute.topic.replacement", "test_server_all_events");
        configBuilder.with("transforms.Reroute.key.field.regex", "test_server.public.(.*)");
        configBuilder.with("transforms.Reroute.key.field.replacement", "\\$1");

        return configBuilder;
    }
}
