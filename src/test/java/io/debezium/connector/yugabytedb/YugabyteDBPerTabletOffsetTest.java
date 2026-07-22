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
}
