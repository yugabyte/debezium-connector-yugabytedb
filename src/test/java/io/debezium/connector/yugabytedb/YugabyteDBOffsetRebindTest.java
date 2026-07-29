package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the stream rebind feature controlled by the
 * `yugabytedb.enable.offset.rebind` configuration property.
 *
 * The test validates that when a connector is initially started against one
 * stream, stopped, and then restarted against a NEW stream id with rebind
 * enabled, it resumes from the last committed offsets and therefore emits
 * change events that were produced after the last committed offset.
 */
public class YugabyteDBOffsetRebindTest extends YugabyteDBContainerTestBase {

    private static final String INSERT_STMT_FMT = "INSERT INTO t1 VALUES (%d, 'John', 'Doe', 32);";

    @BeforeAll
    public static void beforeAll() throws SQLException {
        initializeYBContainer();
        TestHelper.dropAllSchemas();
    }

    @AfterAll
    public static void afterAll() {
        shutdownYBContainer();
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        initializeConnectorTestFramework();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
    }

    @AfterEach
    public void afterEach() throws Exception {
        stopConnector();
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
    }

    @Test
    public void testResumeFromOffsetsAcrossStreamRebind() throws Exception {
        // 1) Create original stream and start the connector without rebind.
        String originalStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", /*consistentSnapshot=*/false, /*useSnapshot=*/false);
        Configuration.Builder initialConfig = TestHelper.getConfigBuilder("public.t1", originalStreamId);
        startEngine(initialConfig);
        awaitUntilConnectorIsReady();

        // Produce 100 rows.
        TestHelper.executeBulkWithRange(INSERT_STMT_FMT, 1, 101);

        // Consume all 100 change events and assert.
        List<SourceRecord> initialRecords = new ArrayList<>();
        waitAndFailIfCannotConsume(initialRecords, 100);
        assertEquals(100, initialRecords.size(), "Did not receive expected initial records");
        assertNoRecordsToConsume();

        // 2) Stop connector – offsets are now committed in the embedded engine.
        stopConnector();

        // 3) Drop the old stream implicitly (delete.on.stop=true in config builder) and create a NEW one.
        String newStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", /*consistentSnapshot=*/false, /*useSnapshot=*/false);

        // 4) Insert additional 20 rows (101-120) after restart.
        TestHelper.executeBulkWithRange(INSERT_STMT_FMT, 101, 121);

        // 5) Start connector again, pointing at the *new* stream id and enabling offset rebind.
        Configuration.Builder rebindConfig = TestHelper.getConfigBuilder("public.t1", newStreamId)
                .with(YugabyteDBConnectorConfig.ENABLE_OFFSET_REBIND, true);
        startEngine(rebindConfig);
        awaitUntilConnectorIsReady();

        // 6) Verify that the new 20 change events are emitted.
        List<SourceRecord> rebindRecords = new ArrayList<>();
        waitAndFailIfCannotConsume(rebindRecords, 20);
        assertEquals(20, rebindRecords.size(), "Unexpected number of records after rebind – offsets not honoured?");
        assertNoRecordsToConsume();
    }
}
