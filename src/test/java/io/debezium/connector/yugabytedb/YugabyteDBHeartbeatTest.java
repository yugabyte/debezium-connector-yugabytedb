package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.util.LoggingContext;
import io.debezium.util.Testing;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.yb.client.GetCheckpointResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies the per-tablet timer heartbeat: with updates filtered out (skipped.operations=u), the tablet
 * keeps advancing its checkpoint, the heartbeats carry an offset past the last delivered insert, and
 * streaming still resumes for later inserts.
 */
public class YugabyteDBHeartbeatTest extends YugabyteDBContainerTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBHeartbeatTest.class);
    private final List<SourceRecord> captured = Collections.synchronizedList(new ArrayList<>());

    @BeforeAll
    public static void beforeAll() throws SQLException {
        initializeYBContainer();
        TestHelper.dropAllSchemas();
    }

    @BeforeEach
    public void beforeEach() {
        captured.clear();
    }

    @AfterEach
    public void afterEach() throws Exception {
        stopConnector();
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
    }

    @AfterAll
    public static void afterAll() {
        shutdownYBContainer();
    }

    private void runEngine(Configuration config) {
        CountDownLatch latch = new CountDownLatch(1);
        engine = EmbeddedEngine.create()
                .using(config)
                .using(OffsetCommitPolicy.always())
                .notifying((records, committer) -> {
                    for (SourceRecord record : records) {
                        committer.markProcessed(record);
                        captured.add(record);
                    }
                    committer.markBatchFinished();
                })
                .using(this.getClass().getClassLoader())
                .using((success, message, error) -> {
                    if (error != null) {
                        LOGGER.error("Engine error", error);
                    }
                    latch.countDown();
                })
                .build();
        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });
    }

    private List<SourceRecord> snapshotOf(String suffix) {
        return new ArrayList<>(captured).stream()
                .filter(r -> r.topic() != null && r.topic().endsWith(suffix))
                .collect(Collectors.toList());
    }

    private List<SourceRecord> dataRecords() {
        return snapshotOf(".t1");
    }

    private List<SourceRecord> heartbeats() {
        return new ArrayList<>(captured).stream()
                .filter(r -> r.topic() != null && r.topic().startsWith("__debezium-heartbeat"))
                .collect(Collectors.toList());
    }

    // Highest tablet checkpoint index found in a record's sourceOffset. Offset values are OpId
    // strings "term:index:key:write_id:time"; index is the second field.
    private static long tabletMaxIndex(SourceRecord record) {
        long max = -1;
        Map<String, ?> offset = record.sourceOffset();
        if (offset == null) {
            return max;
        }
        for (Map.Entry<String, ?> e : offset.entrySet()) {
            if ("transaction_id".equals(e.getKey()) || !(e.getValue() instanceof String)) {
                continue;
            }
            String[] parts = ((String) e.getValue()).split(":");
            if (parts.length >= 2) {
                try {
                    max = Math.max(max, Long.parseLong(parts[1].trim()));
                }
                catch (NumberFormatException ignored) {
                }
            }
        }
        return max;
    }

    @Test
    public void heartbeatCarriesCheckpointPastFilteredUpdatesAndStreamingResumes() throws Exception {
        TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT);");
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false /* before image */,
                true /* explicit checkpointing */, false, false);
        Configuration config = TestHelper.getConfigBuilder("public.t1", dbStreamId)
                .with(EmbeddedEngine.ENGINE_NAME, "heartbeat-test")
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                        Testing.Files.createTestingFile("heartbeat-offsets.txt").getAbsolutePath())
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .with(EmbeddedEngine.CONNECTOR_CLASS, YugabyteDBgRPCConnector.class)
                .with("heartbeat.interval.ms", 10000)
                .with("skipped.operations", "u")
                .build();

        runEngine(config);
        awaitUntilConnectorIsReady();

        // 10 inserts are delivered (op=c).
        for (int i = 1; i <= 10; ++i) {
            TestHelper.execute("INSERT INTO t1 VALUES (" + i + ", 'ins" + i + "');");
        }
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> snapshotOf(".t1").size() >= 10);
        long lastInsertIndex = snapshotOf(".t1").stream().mapToLong(YugabyteDBHeartbeatTest::tabletMaxIndex).max().orElse(-1);
        assertTrue(lastInsertIndex > 0, "expected insert offsets to be captured");

        long heartbeatsBeforeIdle = heartbeats().size();

        YBClient ybClient = TestHelper.getYbClient(getMasterAddress());
        YBTable table = TestHelper.getYbTable(ybClient, "t1");
        String tabletId = ybClient.getTabletUUIDs(table).iterator().next();
        long checkpointBeforeIdle = serverCheckpointIndex(ybClient, table, dbStreamId, tabletId);

        // 10 updates are filtered out (op=u) but still advance the tablet position.
        for (int i = 1; i <= 10; ++i) {
            TestHelper.execute("UPDATE t1 SET name = 'upd" + i + "' WHERE id = " + i + ";");
        }

        // Idle for ~65s: with a 10s interval, only heartbeats should flow.
        TestHelper.waitFor(Duration.ofSeconds(65));

        List<SourceRecord> hbs = heartbeats();
        long duringIdle = hbs.size() - heartbeatsBeforeIdle;
        assertTrue(duringIdle >= 5,
                "expected >=5 heartbeats over ~65s at 10s interval, got " + duringIdle);

        long maxHeartbeatIndex = hbs.stream().mapToLong(YugabyteDBHeartbeatTest::tabletMaxIndex).max().orElse(-1);
        assertTrue(maxHeartbeatIndex > lastInsertIndex,
                "heartbeat checkpoint index " + maxHeartbeatIndex
                        + " should be past the last delivered insert index " + lastInsertIndex
                        + " (filtered updates must advance the checkpoint)");

        // Carrying the position on the record is only half of it: the acknowledged heartbeat has to
        // reach the server, so cdc_state must have moved past where it was before the idle window.
        Awaitility.await().atMost(Duration.ofSeconds(60)).until(
                () -> serverCheckpointIndex(ybClient, table, dbStreamId, tabletId) > checkpointBeforeIdle);
        ybClient.close();

        // Streaming still works after the filtered/idle stretch: later inserts are delivered.
        long dataBeforeResume = snapshotOf(".t1").size();
        for (int i = 11; i <= 15; ++i) {
            TestHelper.execute("INSERT INTO t1 VALUES (" + i + ", 'ins" + i + "');");
        }
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> snapshotOf(".t1").size() >= dataBeforeResume + 5);

        engine.stop();
    }

    private static Set<String> distinctTablets(List<SourceRecord> records) {
        Set<String> tablets = new HashSet<>();
        for (SourceRecord r : records) {
            Map<String, ?> offset = r.sourceOffset();
            if (offset == null) {
                continue;
            }
            for (String key : offset.keySet()) {
                if ("transaction_id".equals(key)) {
                    continue;
                }
                int dot = key.lastIndexOf('.');
                tablets.add(dot < 0 ? key : key.substring(dot + 1));
            }
        }
        return tablets;
    }

    @Test
    public void snapshotCompletionEmitsHeartbeatPerTablet() throws Exception {
        TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 3 TABLETS;");
        for (int i = 1; i <= 30; ++i) {
            TestHelper.execute("INSERT INTO t1 VALUES (" + i + ", 'snap" + i + "');");
        }
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false /* before image */,
                true /* explicit checkpointing */, false, false);
        Configuration config = TestHelper.getConfigBuilder("public.t1", dbStreamId)
                .with(EmbeddedEngine.ENGINE_NAME, "heartbeat-snapshot-test")
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                        Testing.Files.createTestingFile("heartbeat-snapshot-offsets.txt").getAbsolutePath())
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .with(EmbeddedEngine.CONNECTOR_CLASS, YugabyteDBgRPCConnector.class)
                .with("heartbeat.interval.ms", 600000)
                .with("snapshot.mode", "initial")
                .build();

        runEngine(config);
        awaitUntilConnectorIsReady();

        // All 30 pre-inserted rows are delivered by the snapshot.
        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> snapshotOf(".t1").size() >= 30);
        long lastSnapshotIndex = snapshotOf(".t1").stream()
                .mapToLong(YugabyteDBHeartbeatTest::tabletMaxIndex).max().orElse(-1);
        assertTrue(lastSnapshotIndex > 0, "expected snapshot offsets to be captured");

        int tabletCount = distinctTablets(new ArrayList<>(captured)).size();
        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> heartbeats().size() >= tabletCount);

        List<SourceRecord> hbs = heartbeats();
        LOGGER.info("snapshot-completion heartbeats={} tabletCount={}", hbs.size(), tabletCount);
        assertEquals(tabletCount, hbs.size(),
                "expected exactly one snapshot-completion heartbeat per tablet (" + tabletCount
                        + " tablets), got " + hbs.size());

        long maxHeartbeatIndex = hbs.stream().mapToLong(YugabyteDBHeartbeatTest::tabletMaxIndex).max().orElse(-1);
        assertTrue(maxHeartbeatIndex >= lastSnapshotIndex,
                "snapshot-completion heartbeat checkpoint index " + maxHeartbeatIndex
                        + " should be at least the last snapshot record index " + lastSnapshotIndex);

        // The snapshot boundary beat has to land in cdc_state, otherwise the checkpoint the snapshot
        // reached is lost the moment streaming takes over.
        YBClient ybClient = TestHelper.getYbClient(getMasterAddress());
        try {
            YBTable table = TestHelper.getYbTable(ybClient, "t1");
            Set<String> tablets = ybClient.getTabletUUIDs(table);
            Awaitility.await().atMost(Duration.ofSeconds(90)).until(() -> {
                for (String tabletId : tablets) {
                    if (serverCheckpointIndex(ybClient, table, dbStreamId, tabletId) <= 0) {
                        return false;
                    }
                }
                return true;
            });
        }
        finally {
            ybClient.close();
        }

        engine.stop();
    }

    private static long serverCheckpointIndex(YBClient client, YBTable table, String streamId, String tabletId)
            throws Exception {
        GetCheckpointResponse response = client.getCheckpoint(table, streamId, tabletId);
        return response.getIndex();
    }


    @Test
    public void heartbeatsAdvanceATabletWhoseEveryRecordIsFiltered() throws Exception {
        // Range sharded with a split point, seeded on both sides before the stream exists. The
        // connector never snapshots, so the only records it ever sees are the updates below, every one
        // of which the filter drops: both tablets have WAL of their own and nothing shippable.
        TestHelper.execute("CREATE TABLE t1 (id INT, name TEXT, PRIMARY KEY (id ASC)) SPLIT AT VALUES ((5000));");
        for (int i = 0; i < 30; ++i) {
            TestHelper.execute("INSERT INTO t1 VALUES (" + i + ", 'pre-existing');");
            TestHelper.execute("INSERT INTO t1 VALUES (" + (6000 + i) + ", 'pre-existing');");
        }

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false /* before image */,
                true /* explicit checkpointing */, false, false);
        Configuration config = TestHelper.getConfigBuilder("public.t1", dbStreamId)
                .with(EmbeddedEngine.ENGINE_NAME, "heartbeat-filtered-tablet-test")
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                        Testing.Files.createTestingFile("heartbeat-filtered-tablet.txt").getAbsolutePath())
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .with("heartbeat.interval.ms", 5000)
                .with("skipped.operations", "u")
                .with(EmbeddedEngine.CONNECTOR_CLASS, YugabyteDBgRPCConnector.class)
                .build();

        runEngine(config);
        awaitUntilConnectorIsReady();

        YBClient ybClient = TestHelper.getYbClient(getMasterAddress());
        try {
            YBTable table = TestHelper.getYbTable(ybClient, "t1");
            Set<String> tablets = ybClient.getTabletUUIDs(table);
            assertEquals(2, tablets.size(), "expected the split point to produce two tablets");

            Map<String, Long> before = new HashMap<>();
            for (String tabletId : tablets) {
                before.put(tabletId, serverCheckpointIndex(ybClient, table, dbStreamId, tabletId));
            }

            for (int round = 0; round < 5; ++round) {
                TestHelper.execute("UPDATE t1 SET name = 'filtered-" + round + "';");
                TestHelper.waitFor(Duration.ofSeconds(3));
            }

            assertTrue(dataRecords().isEmpty(),
                    "expected every record to be filtered, saw " + dataRecords().size());

            // Heartbeats have to reach both tablets: the one whose every record was filtered and the
            // one that never had a record at all.
            Awaitility.await().atMost(Duration.ofSeconds(90))
                    .until(() -> distinctTablets(heartbeats()).size() >= 2);

            // Emitting a heartbeat is not enough; the checkpoint has to move on the server.
            Awaitility.await().atMost(Duration.ofSeconds(90)).until(() -> {
                for (String tabletId : tablets) {
                    if (serverCheckpointIndex(ybClient, table, dbStreamId, tabletId) <= before.get(tabletId)) {
                        return false;
                    }
                }
                return true;
            });
        }
        finally {
            ybClient.close();
        }

        engine.stop();
    }

    @Test
    public void heartbeatsFlowWhenEverySnapshotRecordIsFiltered() throws Exception {
        TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 3 TABLETS;");
        for (int i = 0; i < 60; ++i) {
            TestHelper.execute("INSERT INTO t1 VALUES (" + i + ", 'pre-existing');");
        }

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false /* before image */,
                true /* explicit checkpointing */, true, true);
        Configuration config = TestHelper.getConfigBuilder("public.t1", dbStreamId)
                .with(EmbeddedEngine.ENGINE_NAME, "heartbeat-filtered-snapshot-test")
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                        Testing.Files.createTestingFile("heartbeat-filtered-snapshot.txt").getAbsolutePath())
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .with("heartbeat.interval.ms", 5000)
                .with("snapshot.mode", "initial")
                // Snapshot rows are read events, so filtering them leaves the snapshot with nothing
                // to produce for any tablet.
                .with("skipped.operations", "r")
                .with(EmbeddedEngine.CONNECTOR_CLASS, YugabyteDBgRPCConnector.class)
                .build();

        runEngine(config);
        awaitUntilConnectorIsReady();

        YBClient ybClient = TestHelper.getYbClient(getMasterAddress());
        try {
            YBTable table = TestHelper.getYbTable(ybClient, "t1");
            Set<String> tablets = ybClient.getTabletUUIDs(table);

            // No snapshot record reaches Kafka, so only heartbeats can report progress; one per
            // tablet has to arrive by the time the snapshot boundary is crossed.
            Awaitility.await().atMost(Duration.ofSeconds(120))
                    .until(() -> distinctTablets(heartbeats()).size() >= tablets.size());

            assertTrue(dataRecords().isEmpty(),
                    "expected every snapshot record to be filtered, saw " + dataRecords().size());

            // Nothing was produced from the snapshot, so only the heartbeats can have moved these
            // tablets off the position the stream started from.
            Awaitility.await().atMost(Duration.ofSeconds(90)).until(() -> {
                for (String tabletId : tablets) {
                    if (serverCheckpointIndex(ybClient, table, dbStreamId, tabletId) <= 0) {
                        return false;
                    }
                }
                return true;
            });
        }
        finally {
            ybClient.close();
        }

        engine.stop();
    }

    @Test
    public void heartbeatOffsetCarriesOnlyItsOwnTablet() throws Exception {
        TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 3 TABLETS;");
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false /* before image */,
                true /* explicit checkpointing */, false, false);
        Configuration config = TestHelper.getConfigBuilder("public.t1", dbStreamId)
                .with(EmbeddedEngine.ENGINE_NAME, "heartbeat-own-tablet-test")
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                        Testing.Files.createTestingFile("heartbeat-own-tablet.txt").getAbsolutePath())
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .with("heartbeat.interval.ms", 5000)
                .with(EmbeddedEngine.CONNECTOR_CLASS, YugabyteDBgRPCConnector.class)
                .build();

        runEngine(config);
        awaitUntilConnectorIsReady();

        for (int i = 0; i < 60; ++i) {
            TestHelper.execute("INSERT INTO t1 VALUES (" + i + ", " + i + "::text);");
        }
        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> dataRecords().size() >= 40);

        // Every tablet has to appear across the heartbeats. Since a heartbeat carries only its own
        // tablet, that can only happen if each tablet emitted one for itself.
        Awaitility.await().atMost(Duration.ofSeconds(90))
                .until(() -> distinctTablets(heartbeats()).size() >= 3);

        for (SourceRecord heartbeat : heartbeats()) {
            Set<String> tablets = heartbeat.sourceOffset().keySet().stream()
                    .filter(k -> !k.equals("transaction_id"))
                    .collect(Collectors.toSet());
            assertEquals(1, tablets.size(),
                    "heartbeat offset must reference exactly one tablet, got: " + heartbeat.sourceOffset());
        }

        engine.stop();
    }

    @Test
    public void heartbeatsAreOffByDefault() throws Exception {
        TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 2 TABLETS;");
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false /* before image */,
                true /* explicit checkpointing */, false, false);
        // No heartbeat.interval.ms: the upstream default of 0 must leave heartbeats disabled.
        Configuration config = TestHelper.getConfigBuilder("public.t1", dbStreamId)
                .with(EmbeddedEngine.ENGINE_NAME, "heartbeat-default-off-test")
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                        Testing.Files.createTestingFile("heartbeat-default-off.txt").getAbsolutePath())
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .with(EmbeddedEngine.CONNECTOR_CLASS, YugabyteDBgRPCConnector.class)
                .build();

        runEngine(config);
        awaitUntilConnectorIsReady();

        for (int i = 0; i < 20; ++i) {
            TestHelper.execute("INSERT INTO t1 VALUES (" + i + ", 'row" + i + "');");
        }
        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> dataRecords().size() >= 20);

        // Idle long enough that any accidental non-zero default would have fired several times.
        TestHelper.waitFor(Duration.ofSeconds(20));

        assertTrue(heartbeats().isEmpty(),
                "heartbeats must stay disabled unless heartbeat.interval.ms is set, got "
                        + heartbeats().size());

        engine.stop();
    }
}
