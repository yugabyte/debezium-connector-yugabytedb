package io.debezium.connector.yugabytedb;

import static org.fest.assertions.Assertions.*;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class YugabyteDBgRPCConnectorIT extends YugabyteDBContainerTestBase {

    private YugabyteDBgRPCConnector connector;

    @BeforeAll
    public static void beforeClass() throws Exception {
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
    }

    @AfterAll
    public static void afterClass() {
        shutdownYBContainer();
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForSnapshot")
    public void shouldDeliverEverySnapshotRowAndStreamAfterRestartWithUnacknowledgedRecords(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        // Four range-sharded tablets (key order = id order), several batches each. Only the first
        // 1,000 records are acknowledged before a restart mid-snapshot; afterwards no snapshot row may
        // be missing, and a restart during streaming must lose nothing either.
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
        TestHelper.execute("CREATE TABLE t_split (id INT, name TEXT, PRIMARY KEY (id ASC)) SPLIT AT VALUES ((3001), (6001), (9001));");

        final int snapshotRows = 12_000;
        final int acknowledgedCount = 1_000;
        final int streamedRows = 100;
        insertRows(1, snapshotRows, "repeat('x', 2000)");

        YugabyteDBSnapshotChangeEventSource.TRACK_EXPLICIT_CHECKPOINTS = true;
        long requestsBefore = YugabyteDBSnapshotChangeEventSource.SNAPSHOT_GET_CHANGES_REQUESTS.get();
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t_split", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t_split", dbStreamId)
            .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());

        startEngineWithPartialConsumptionOfLastBatch(configBuilder, acknowledgedCount, (success, message, error) -> {
            if (error != null) {
                LOGGER.error("Engine stopped: {}", message, error);
            }
        });
        awaitUntilConnectorIsReady();
        Awaitility.await()
            .atMost(Duration.ofSeconds(120))
            .until(() -> getNonConsumedRecordCount() == acknowledgedCount);
        waitUntilSnapshotStopsRequesting(requestsBefore);

        Set<Integer> ids = new HashSet<>();
        consumeAvailableRecords(record -> ids.add(idOf(record)));
        assertThat(ids.size()).isEqualTo(acknowledgedCount);
        stopConnector();
        YugabyteDBSnapshotChangeEventSource.TRACK_EXPLICIT_CHECKPOINTS = false;

        // Restart mid-snapshot and let the snapshot delivery settle, complete or not.
        startEngine(configBuilder);
        awaitUntilConnectorIsReady();
        waitUntilNoNewRecords(ids);

        // Streaming after the snapshot, with a restart in between: both halves must arrive.
        insertRows(snapshotRows + 1, snapshotRows + streamedRows, "'streamed'");
        awaitIds(ids, snapshotRows + 1, snapshotRows + streamedRows);
        stopConnector();
        startEngine(configBuilder);
        awaitUntilConnectorIsReady();
        insertRows(snapshotRows + streamedRows + 1, snapshotRows + 2 * streamedRows, "'streamed'");
        awaitIds(ids, snapshotRows + streamedRows + 1, snapshotRows + 2 * streamedRows);

        String delivery = describeDelivery(ids, 1, snapshotRows);
        assertTrue(!delivery.contains("missing"), "snapshot rows after the restart mid-snapshot: " + delivery
            + "; streamed rows " + (snapshotRows + 1) + "-" + (snapshotRows + 2 * streamedRows) + " all delivered");
    }

    @Test
    public void shouldRedeliverATransactionWhoseRowsWereOnlyPartlyAcknowledged() throws Exception {
        // All rows of one transaction come back in one GetChanges response, and only the first of them is
        // acknowledged. The tablet's checkpoint must stay before the transaction and a restart must deliver
        // every row again.
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
        TestHelper.execute("CREATE TABLE t_txn (id INT PRIMARY KEY, name TEXT) SPLIT INTO 1 TABLETS;");

        final int rows = 100;
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t_txn");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t_txn", dbStreamId)
            .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.NEVER.getValue());

        startEngineWithPartialConsumptionOfLastBatch(configBuilder, 1);
        awaitUntilConnectorIsReady();
        TestHelper.execute(String.format("INSERT INTO t_txn SELECT i, 'txn' FROM generate_series(1, %d) i;", rows));
        Awaitility.await()
            .atMost(Duration.ofSeconds(60))
            .until(() -> getNonConsumedRecordCount() == 1);
        // Time to commit the acknowledged row and hand the checkpoint to the service.
        TestHelper.waitFor(Duration.ofSeconds(10));
        stopConnector();
        consumeAvailableRecords(record -> {});

        startEngine(configBuilder);
        awaitUntilConnectorIsReady();
        Set<Integer> ids = new HashSet<>();
        Awaitility.await()
            .atMost(Duration.ofSeconds(120))
            .untilAsserted(() -> {
                consumeAvailableRecords(record -> ids.add(idOf(record)));
                assertEquals(rows, ids.size(), "t_txn rows after the restart: " + describeDelivery(ids, 1, rows));
            });
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.YugabyteDBSnapshotTest#streamTypeProviderForSnapshotWithColocation")
    public void shouldDeliverEverySnapshotRowWhenOnlyASiblingTableIsAcknowledged(boolean consistentSnapshot, boolean useSnapshot, boolean colocation) throws Exception {
        // test_1 fits one snapshot batch while ~4 KB keys split test_2 over several, so test_2 keeps
        // dispatching after test_1 has been read completely. Only test_2's records are acknowledged before a
        // restart; test_1 must then be snapshotted again, whether the two tables share a colocated tablet or not.
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
        createTablesInColocatedDB(colocation);

        final int unacknowledgedRows = 1_000;
        final int acknowledgedRows = 3_000;
        TestHelper.executeInDatabase(String.format(
            "INSERT INTO test_1 SELECT i FROM generate_series(1, %d) i;", unacknowledgedRows), DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase(String.format(
            "INSERT INTO test_2 SELECT i::text || repeat('x', 4000) FROM generate_series(1, %d) i;", acknowledgedRows),
            DEFAULT_COLOCATED_DB_NAME);

        YugabyteDBSnapshotChangeEventSource.TRACK_EXPLICIT_CHECKPOINTS = true;
        long requestsBefore = YugabyteDBSnapshotChangeEventSource.SNAPSHOT_GET_CHANGES_REQUESTS.get();
        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1,public.test_2", dbStreamId)
            .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());

        startEngineAcknowledgingOnly(configBuilder, record -> record.topic().endsWith(".test_2"));
        awaitUntilConnectorIsReady();
        Awaitility.await()
            .atMost(Duration.ofSeconds(120))
            .until(() -> getNonConsumedRecordCount() == acknowledgedRows);
        waitUntilSnapshotStopsRequesting(requestsBefore);
        stopConnector();
        consumeAvailableRecords(record -> {});
        YugabyteDBSnapshotChangeEventSource.TRACK_EXPLICIT_CHECKPOINTS = false;

        startEngine(configBuilder);
        awaitUntilConnectorIsReady();
        Set<Integer> ids = new HashSet<>();
        Awaitility.await()
            .atMost(Duration.ofSeconds(120))
            .untilAsserted(() -> {
                consumeAvailableRecords(record -> {
                    if (record.topic().endsWith(".test_1")) {
                        ids.add(idOf(record));
                    }
                });
                assertEquals(unacknowledgedRows, ids.size(),
                    "test_1 rows after the restart: " + describeDelivery(ids, 1, unacknowledgedRows));
            });
    }

    @Test
    public void shouldDeliverStreamedRowsOfAColocatedTableWhileOnlyItsSiblingIsAcknowledged() throws Exception {
        // test_1's records are held back as if its topic were down while test_2's keep being acknowledged.
        // Both tables stream from one colocated tablet, whichever table's tablet entry polls a row, so the
        // tablet's checkpoint must not pass test_1's rows and a restart must deliver them.
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
        createTablesInColocatedDB(true);

        final int heldBackRows = 100;
        String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1,public.test_2", dbStreamId)
            .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.NEVER.getValue());

        startEngineAcknowledgingOnly(configBuilder, inSourcePartitionOrder(record -> !record.topic().endsWith(".test_1")));
        awaitUntilConnectorIsReady();
        TestHelper.executeInDatabase(String.format(
            "INSERT INTO test_1 SELECT i FROM generate_series(1, %d) i;", heldBackRows), DEFAULT_COLOCATED_DB_NAME);
        try (YugabyteDBConnection connection = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME)) {
            for (int i = 1; i <= 200; i++) {
                connection.execute("INSERT INTO test_2 VALUES ('" + i + "');");
            }
        }
        // Time to commit what was acknowledged and hand the checkpoint to the service.
        TestHelper.waitFor(Duration.ofSeconds(10));
        stopConnector();
        consumeAvailableRecords(record -> {});

        startEngine(configBuilder);
        awaitUntilConnectorIsReady();
        Set<Integer> ids = new HashSet<>();
        Awaitility.await()
            .atMost(Duration.ofSeconds(120))
            .untilAsserted(() -> {
                consumeAvailableRecords(record -> {
                    if (record.topic().endsWith(".test_1")) {
                        ids.add(idOf(record));
                    }
                });
                assertEquals(heldBackRows, ids.size(),
                    "test_1 rows after the restart: " + describeDelivery(ids, 1, heldBackRows));
            });
    }

    private static void insertRows(int from, int to, String nameExpression) {
        TestHelper.execute(String.format(
            "INSERT INTO t_split SELECT i, %s FROM generate_series(%d, %d) i;", nameExpression, from, to));
    }

    private void awaitIds(Set<Integer> ids, int from, int to) {
        Awaitility.await()
            .atMost(Duration.ofSeconds(300))
            .until(() -> {
                consumeAvailableRecords(record -> ids.add(idOf(record)));
                for (int i = from; i <= to; i++) {
                    if (!ids.contains(i)) {
                        return false;
                    }
                }
                return true;
            });
    }

    // Consumes until no record has arrived for 15 s.
    private void waitUntilNoNewRecords(Set<Integer> ids) {
        AtomicInteger lastSize = new AtomicInteger(-1);
        AtomicInteger quietPolls = new AtomicInteger();
        Awaitility.await()
            .atMost(Duration.ofSeconds(300))
            .pollInterval(Duration.ofSeconds(1))
            .until(() -> {
                consumeAvailableRecords(record -> ids.add(idOf(record)));
                if (ids.size() != lastSize.getAndSet(ids.size())) {
                    quietPolls.set(0);
                }
                return quietPolls.incrementAndGet() >= 15;
            });
    }

    // "delivered 1-1000, missing 1001-1900, delivered 1901-3000, ..." for the ids in [from, to].
    private static String describeDelivery(Set<Integer> ids, int from, int to) {
        List<String> parts = new ArrayList<>();
        int start = from;
        boolean delivered = ids.contains(from);
        for (int i = from + 1; i <= to + 1; i++) {
            boolean next = i <= to && ids.contains(i);
            if (i > to || next != delivered) {
                parts.add((delivered ? "delivered " : "missing ") + start + "-" + (i - 1));
                start = i;
                delivered = next;
            }
        }
        return String.join(", ", parts);
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

    @Test
    public void shouldValidateConnectorConfigDef() {
        connector = new YugabyteDBgRPCConnector();
        ConfigDef configDef = connector.config();
        assertThat(configDef).isNotNull();
        YugabyteDBConnectorConfig.ALL_FIELDS.forEach(this::validateFieldDef);
    }

    @Test
    public void shouldNotStartWithInvalidConfiguration() {
        // use an empty configuration which should be invalid because of the lack of DB connection details
        Configuration.Builder configBuilder = Configuration.create();

        // we expect the engine will log at least one error, so preface it ...
        LOGGER.info("Attempting to start the connector with an INVALID configuration, so MULTIPLE error messages & one exception will appear in the log");
        startEngine(configBuilder, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }
}
