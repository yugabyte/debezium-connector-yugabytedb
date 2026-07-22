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

        engine.stop();
    }
}
