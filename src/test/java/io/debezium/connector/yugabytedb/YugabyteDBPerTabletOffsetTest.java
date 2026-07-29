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
import org.yb.client.GetCheckpointResponse;
import org.yb.client.YBClient;

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
 * Verifies that each record's sourceOffset carries only its own tablet's checkpoint, not the whole
 * per-task tablet map, so a record can never advance a sibling tablet's checkpoint.
 */
public class YugabyteDBPerTabletOffsetTest extends YugabyteDBContainerTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBPerTabletOffsetTest.class);
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

    private List<SourceRecord> dataRecords() {
        return new ArrayList<>(captured).stream()
                .filter(r -> r.topic() != null && r.topic().endsWith(".t1"))
                .collect(Collectors.toList());
    }

    // A tablet is keyed in the offset as "tabletId" or "tableId.tabletId"; reduce to the tabletId.
    private static String tabletOf(String key) {
        int dot = key.lastIndexOf('.');
        return dot < 0 ? key : key.substring(dot + 1);
    }

    @Test
    public void recordOffsetCarriesOnlyItsOwnTablet() throws Exception {
        TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 2 TABLETS;");
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false /* before image */,
                true /* explicit checkpointing */, false, false);
        Configuration config = TestHelper.getConfigBuilder("public.t1", dbStreamId)
                .with(EmbeddedEngine.ENGINE_NAME, "per-tablet-offset-test")
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                        Testing.Files.createTestingFile("per-tablet-offset.txt").getAbsolutePath())
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .with(EmbeddedEngine.CONNECTOR_CLASS, YugabyteDBgRPCConnector.class)
                .build();

        runEngine(config);
        awaitUntilConnectorIsReady();

        // Wide key spread so both tablets receive rows.
        for (int i = 0; i < 400; ++i) {
            TestHelper.execute("INSERT INTO t1 VALUES (" + i + ", " + i + "::text);");
        }
        Awaitility.await().atMost(Duration.ofSeconds(40)).until(() -> dataRecords().size() >= 300);

        Set<String> tabletsSeen = new HashSet<>();
        for (SourceRecord record : dataRecords()) {
            Map<String, ?> offset = record.sourceOffset();
            Set<String> tabletsInRecord = offset.keySet().stream()
                    .filter(k -> !k.equals("transaction_id"))
                    .map(YugabyteDBPerTabletOffsetTest::tabletOf)
                    .collect(Collectors.toSet());
            assertEquals(1, tabletsInRecord.size(),
                    "record offset must reference exactly one tablet, got: " + offset);
            tabletsSeen.addAll(tabletsInRecord);
        }

        // Test is only meaningful if both tablets actually produced records.
        assertTrue(tabletsSeen.size() >= 2, "expected records from both tablets, saw: " + tabletsSeen);

        engine.stop();
    }

    // Server-side explicit checkpoint (term, index) for a tablet, read straight from the tserver.
    private long[] serverCheckpoint(YBClient client, String dbStreamId, String tabletId) throws Exception {
        GetCheckpointResponse resp = client.getCheckpoint(TestHelper.getYbTable(client, "t1"), dbStreamId, tabletId);
        return new long[] { resp.getTerm(), resp.getIndex() };
    }

    private static boolean advanced(long[] before, long[] after) {
        return after[0] > before[0] || (after[0] == before[0] && after[1] > before[1]);
    }

    private Set<String> tabletsIn(List<SourceRecord> records) {
        Set<String> tablets = new HashSet<>();
        for (SourceRecord r : records) {
            r.sourceOffset().keySet().stream()
                    .filter(k -> !k.equals("transaction_id"))
                    .map(YugabyteDBPerTabletOffsetTest::tabletOf)
                    .forEach(tablets::add);
        }
        return tablets;
    }

    /**
     * The point of the own-only offset: acking an active tablet must not drag a sibling's checkpoint
     * forward. One tablet keeps taking inserts while the other receives only updates (all dropped by
     * skipped.operations=u), so the sibling delivers nothing. On the server, the active tablet's
     * explicit checkpoint must move forward and the filtered sibling's must stay put. With the old
     * whole-map offset this failed: the active tablet's records carried the sibling's position, so
     * committing them advanced the sibling past data it never delivered.
     */
    @Test
    public void activeTabletAdvancesCheckpointWithoutDraggingFilteredSibling() throws Exception {
        // Range split so a key range maps deterministically to a tablet: [min,500) and [500,max).
        TestHelper.execute("CREATE TABLE t1 (id INT, name TEXT, PRIMARY KEY(id ASC)) SPLIT AT VALUES ((500));");
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false /* before image */,
                true /* explicit checkpointing */, false, false);
        Configuration config = TestHelper.getConfigBuilder("public.t1", dbStreamId)
                .with(EmbeddedEngine.ENGINE_NAME, "per-tablet-offset-filtered-test")
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                        Testing.Files.createTestingFile("per-tablet-offset-filtered.txt").getAbsolutePath())
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .with(EmbeddedEngine.CONNECTOR_CLASS, YugabyteDBgRPCConnector.class)
                .with("skipped.operations", "u")
                .build();

        runEngine(config);
        awaitUntilConnectorIsReady();

        // Baseline: insert into both ranges so both tablets deliver and get a real checkpoint.
        for (int i = 100; i < 140; ++i) {
            TestHelper.execute("INSERT INTO t1 VALUES (" + i + ", 'a" + i + "');");
        }
        for (int i = 600; i < 640; ++i) {
            TestHelper.execute("INSERT INTO t1 VALUES (" + i + ", 'b" + i + "');");
        }
        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> dataRecords().size() >= 80);
        TestHelper.waitFor(Duration.ofSeconds(10));

        Set<String> baselineTablets = tabletsIn(dataRecords());
        assertEquals(2, baselineTablets.size(),
                "expected both tablets to deliver in the baseline, saw: " + baselineTablets);

        YBClient ybClient = TestHelper.getYbClient(getMasterAddress());
        Map<String, long[]> baseline = new HashMap<>();
        for (String t : baselineTablets) {
            baseline.put(t, serverCheckpoint(ybClient, dbStreamId, t));
        }

        // Phase 2: one tablet stays active (inserts, delivered), the other is update-only (all filtered).
        int deliveredBeforePhase2 = dataRecords().size();
        for (int i = 140; i < 180; ++i) {
            TestHelper.execute("INSERT INTO t1 VALUES (" + i + ", 'a" + i + "');");
        }
        for (int i = 600; i < 640; ++i) {
            TestHelper.execute("UPDATE t1 SET name = 'u" + i + "' WHERE id = " + i + ";");
        }
        Awaitility.await().atMost(Duration.ofSeconds(60))
                .until(() -> dataRecords().size() >= deliveredBeforePhase2 + 40);
        TestHelper.waitFor(Duration.ofSeconds(10));

        // Whichever tablet delivered the phase-2 inserts is the active one; the other is the filtered one.
        List<SourceRecord> all = dataRecords();
        Set<String> activeTablets = tabletsIn(all.subList(deliveredBeforePhase2, all.size()));
        assertEquals(1, activeTablets.size(),
                "phase-2 inserts should come from exactly one tablet, saw: " + activeTablets);
        String activeTablet = activeTablets.iterator().next();
        String filteredTablet = baselineTablets.stream()
                .filter(t -> !t.equals(activeTablet)).findFirst().orElseThrow();

        long[] activeAfter = serverCheckpoint(ybClient, dbStreamId, activeTablet);
        long[] filteredAfter = serverCheckpoint(ybClient, dbStreamId, filteredTablet);
        ybClient.close();

        assertTrue(advanced(baseline.get(activeTablet), activeAfter),
                "active tablet explicit checkpoint should move forward: "
                        + Arrays.toString(baseline.get(activeTablet)) + " -> " + Arrays.toString(activeAfter));
        // The sibling had 40 updates filtered away, so nothing of its own could be acknowledged. Being
        // dragged by the active tablet would confirm it past all 40, while the empty-poll self advance
        // can only nudge it by the handful of entries it saw before the filtering began.
        long filteredGain = filteredAfter[1] - baseline.get(filteredTablet)[1];
        assertTrue(filteredGain < 20,
                "filtered sibling must not be confirmed past its filtered updates, gained " + filteredGain
                        + ": " + Arrays.toString(baseline.get(filteredTablet)) + " -> "
                        + Arrays.toString(filteredAfter));

        engine.stop();
    }

    private static long serverCheckpointIndex(YBClient client, YBTable table, String streamId, String tabletId)
            throws Exception {
        GetCheckpointResponse response = client.getCheckpoint(table, streamId, tabletId);
        return response.getIndex();
    }

    private Set<String> tabletsInRecordOffsets() {
        return dataRecords().stream()
                .flatMap(r -> r.sourceOffset().keySet().stream())
                .filter(k -> !k.equals("transaction_id"))
                .map(YugabyteDBPerTabletOffsetTest::tabletOf)
                .collect(Collectors.toSet());
    }

    @Test
    public void everyTabletAdvancesItsOwnCheckpointOnServer() throws Exception {
        TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 3 TABLETS;");
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false /* before image */,
                true /* explicit checkpointing */, false, false);
        Configuration config = TestHelper.getConfigBuilder("public.t1", dbStreamId)
                .with(EmbeddedEngine.ENGINE_NAME, "per-tablet-checkpoint-test")
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                        Testing.Files.createTestingFile("per-tablet-checkpoint.txt").getAbsolutePath())
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .with(EmbeddedEngine.CONNECTOR_CLASS, YugabyteDBgRPCConnector.class)
                .build();

        runEngine(config);
        awaitUntilConnectorIsReady();

        for (int i = 0; i < 400; ++i) {
            TestHelper.execute("INSERT INTO t1 VALUES (" + i + ", " + i + "::text);");
        }
        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> dataRecords().size() >= 300);

        YBClient ybClient = TestHelper.getYbClient(getMasterAddress());
        try {
            YBTable table = TestHelper.getYbTable(ybClient, "t1");
            Set<String> tablets = ybClient.getTabletUUIDs(table);

            // With per-tablet offsets a tablet's checkpoint can only be moved by its own
            // acknowledgements, so every tablet has to end up past the stream's starting position.
            Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
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
    public void checkpointAdvancesForTabletThatProducedNoRecords() throws Exception {
        // Range sharded with an explicit split point so all writes can be confined to one tablet,
        // leaving the other with nothing to stream.
        TestHelper.execute("CREATE TABLE t1 (id INT, name TEXT, PRIMARY KEY (id ASC)) SPLIT AT VALUES ((5000));");
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false /* before image */,
                true /* explicit checkpointing */, false, false);
        Configuration config = TestHelper.getConfigBuilder("public.t1", dbStreamId)
                .with(EmbeddedEngine.ENGINE_NAME, "idle-tablet-checkpoint-test")
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                        Testing.Files.createTestingFile("idle-tablet-checkpoint.txt").getAbsolutePath())
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .with(EmbeddedEngine.CONNECTOR_CLASS, YugabyteDBgRPCConnector.class)
                .build();

        runEngine(config);
        awaitUntilConnectorIsReady();

        for (int i = 0; i < 200; ++i) {
            TestHelper.execute("INSERT INTO t1 VALUES (" + i + ", 'below-split');");
        }
        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> dataRecords().size() >= 150);

        // Only the tablet holding keys below the split point produced anything.
        assertEquals(1, tabletsInRecordOffsets().size(),
                "expected records from a single tablet, saw: " + tabletsInRecordOffsets());

        YBClient ybClient = TestHelper.getYbClient(getMasterAddress());
        try {
            YBTable table = TestHelper.getYbTable(ybClient, "t1");
            Set<String> tablets = ybClient.getTabletUUIDs(table);
            assertEquals(2, tablets.size(), "expected the split point to produce two tablets");

            // The silent tablet has no records of its own to acknowledge and, with per-tablet
            // offsets, no sibling can carry its position, so only the empty-poll self advance can
            // move it. It still has to move, otherwise its WAL would be pinned forever.
            Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
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
    public void recordOffsetNeverCarriesASiblingTabletUnderTransactionMetadata() throws Exception {
        TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 3 TABLETS;");
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false /* before image */,
                true /* explicit checkpointing */, false, false);
        Configuration config = TestHelper.getConfigBuilder("public.t1", dbStreamId)
                .with(EmbeddedEngine.ENGINE_NAME, "per-tablet-txn-metadata-test")
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                        Testing.Files.createTestingFile("per-tablet-txn-metadata.txt").getAbsolutePath())
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .with("provide.transaction.metadata", true)
                .with(EmbeddedEngine.CONNECTOR_CLASS, YugabyteDBgRPCConnector.class)
                .build();

        runEngine(config);
        awaitUntilConnectorIsReady();

        for (int i = 0; i < 200; ++i) {
            TestHelper.execute("INSERT INTO t1 VALUES (" + i + ", " + i + "::text);");
        }
        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> dataRecords().size() >= 150);

        // Transaction metadata records are emitted through a different path than data records;
        // they must respect the same single-tablet offset rule.
        for (SourceRecord record : new ArrayList<>(captured)) {
            Set<String> tablets = record.sourceOffset().keySet().stream()
                    .filter(k -> !k.equals("transaction_id"))
                    .map(YugabyteDBPerTabletOffsetTest::tabletOf)
                    .collect(Collectors.toSet());
            assertTrue(tablets.size() <= 1,
                    "record on topic " + record.topic() + " referenced multiple tablets: "
                            + record.sourceOffset());
        }

        engine.stop();
    }
}
