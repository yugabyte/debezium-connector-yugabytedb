package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig.SnapshotMode;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;

/**
 * Integration test that exercises snapshot filtering behaviour for colocated tables.
 * The test validates that Debezium Filter SMT correctly filters records based on
 * field values in both snapshot mode (op='r') and streaming mode (op='c').
 * 
 * Test Scenarios:
 * 1. INITIAL mode: Snapshots existing data, then streams new changes
 * 2. NEVER mode: Only streams new changes, no snapshot
 * 
 */
public class YugabyteDBSnapshotFilterTest extends YugabyteDBContainerTestBase {

    private static final int SNAPSHOT_RECORDS = 1010;
    private static final int STREAM_RECORDS = 10;

    @BeforeAll
    public static void beforeClass() throws Exception {
        initializeYBContainer();
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
    }

    @BeforeEach
    public void before() throws Exception {
        initializeConnectorTestFramework();
        TestHelper.dropAllSchemas();
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_SETTING_INITIAL_CHECKPOINT = false;
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_BOOTSTRAP_GET_CHANGES = false;
        YugabyteDBSnapshotChangeEventSource.FAIL_WHEN_MARKING_SNAPSHOT_DONE = false;
    }

    @AfterEach
    public void after() throws Exception {
        stopConnector();
        dropAllTablesInColocatedDB();
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
        TestHelper.dropAllSchemas();
        resetCommitCallbackDelay();
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_SETTING_INITIAL_CHECKPOINT = false;
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_BOOTSTRAP_GET_CHANGES = false;
        YugabyteDBSnapshotChangeEventSource.FAIL_WHEN_MARKING_SNAPSHOT_DONE = false;
    }

    @AfterAll
    public static void afterClass() {
        shutdownYBContainer();
    }

    @Test
    public void shouldSnapshotAndStreamAccordingToMode() throws Exception {
        // Test INITIAL mode: snapshot + streaming
        runScenario(SnapshotMode.INITIAL);
        cleanupForNextScenario();

        // Test NEVER mode: streaming only
        runScenario(SnapshotMode.NEVER);
        cleanupForNextScenario();
    }

    /**
     * Runs a test scenario with the specified snapshot mode.
     * 
     * Test Flow:
     * 1. Insert 1010 records with empty name (to be filtered out in snapshot)
     * 2. Insert 10 records with name='Shishir' (to pass filter in snapshot)
     * 3. Create CDC stream and start connector with filter
     * 4. Insert 10 more records with name='Shishir' (streaming records to pass filter)
     * 5. Verify expected number of records consumed based on snapshot mode
     * 
     * @param snapshotMode The snapshot mode to test (INITIAL or NEVER)
     */
    private void runScenario(SnapshotMode snapshotMode) throws Exception {
        createTablesInColocatedDB(true);

        // Insert records with empty name - these will be filtered out
        String formatInsertString = "INSERT INTO public.test_1(id, name) VALUES (%d, '');";
        TestHelper.executeBulk(formatInsertString, SNAPSHOT_RECORDS, DEFAULT_COLOCATED_DB_NAME);

        // Insert 10 records with non-empty name - these should pass the filter in snapshot
        for (int i = SNAPSHOT_RECORDS; i < SNAPSHOT_RECORDS + STREAM_RECORDS; i++) {
            TestHelper.executeInDatabase(
                "INSERT INTO public.test_1(id, name) VALUES (" + i + ", 'Shishir');",
                DEFAULT_COLOCATED_DB_NAME
            );
        }

        // Create CDC stream and configure connector with filter
        String dbStreamId = TestHelper.getNewDbStreamId(
            DEFAULT_COLOCATED_DB_NAME, "test_1", false, true
        );
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(
            DEFAULT_COLOCATED_DB_NAME, "public.test_1", dbStreamId
        );
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, snapshotMode.getValue());
        
        // Configure Debezium Filter SMT to filter records with non-empty name
        configBuilder.with("transforms", "filter");
        configBuilder.with("transforms.filter.type", "io.debezium.transforms.Filter");
        configBuilder.with("transforms.filter.language", "jsr223.groovy");
        configBuilder.with("transforms.filter.condition",
            "(value.op == 'r' && value.after.name?.value?.trim() != null && value.after.name?.value?.trim() != '') || " +
            "(value.op == 'c' && value.after.name?.value?.trim() != null && value.after.name?.value?.trim() != '')"
        );
        
        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        // Insert 10 streaming records with non-empty name - these should pass the filter
        for (int i = SNAPSHOT_RECORDS + STREAM_RECORDS; i < SNAPSHOT_RECORDS + STREAM_RECORDS + 10; i++) {
            TestHelper.executeInDatabase(
                "INSERT INTO public.test_1(id, name) VALUES (" + i + ", 'Shishir');",
                DEFAULT_COLOCATED_DB_NAME
            );
        }

        List<SourceRecord> records = new ArrayList<>();

        if (snapshotMode.getValue().equals(SnapshotMode.INITIAL.getValue())) {
            // INITIAL mode: Expect 10 snapshot records + 10 streaming records
            waitAndFailIfCannotConsume(records, 20);
            assertEquals(20, records.size(), 
                "Expected 20 records (10 from snapshot + 10 from streaming) in INITIAL mode");
        } else {
            // NEVER mode: Expect only 10 streaming records
            waitAndFailIfCannotConsume(records, 10);
            assertEquals(10, records.size(), 
                "Expected 10 records (only streaming) in NEVER mode");
        }
    }

    private void cleanupForNextScenario() throws Exception {
        stopConnector();
        dropAllTablesInColocatedDB();
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
        TestHelper.dropAllSchemas();
        resetCommitCallbackDelay();
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_SETTING_INITIAL_CHECKPOINT = false;
        YugabyteDBSnapshotChangeEventSource.FAIL_AFTER_BOOTSTRAP_GET_CHANGES = false;
        YugabyteDBSnapshotChangeEventSource.FAIL_WHEN_MARKING_SNAPSHOT_DONE = false;
        initializeConnectorTestFramework();
    }
}
