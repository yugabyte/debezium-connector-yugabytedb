package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;

import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.async.AsyncEmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.util.LoggingContext;
import io.debezium.util.Testing;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.client.CdcSdkCheckpoint;
import org.yb.client.GetCheckpointResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic unit tests to verify the behavior of {@link YugabyteDBStreamingChangeEventSource#commitOffset(Map)}
 * and {@link YugabyteDBSnapshotChangeEventSource#commitOffset(Map)} with the server.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBExplicitCheckpointingTest extends YugabyteDBContainerTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBExplicitCheckpointingTest.class);

    private final String CONNECTOR_NAME = "explicit-checkpointing-test-connector";
    protected Map<String, ?> offsetMap = new HashMap<>();

    @BeforeAll
    public static void beforeAll() throws SQLException {
        initializeYBContainer();
        TestHelper.dropAllSchemas();
    }

    @BeforeEach
    public void beforeEach() throws Exception {
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
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

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void verifyCommitOffsetCheckpointAndGetCheckpointBehaviour(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false /* before image */,
                true /* explicit checkpointing */, consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId)
                .with(AsyncEmbeddedEngine.ENGINE_NAME, CONNECTOR_NAME)
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, Testing.Files.createTestingFile("file-connector-offsets.txt").getAbsolutePath())
                .with(AsyncEmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .with(AsyncEmbeddedEngine.CONNECTOR_CLASS, YugabyteDBgRPCConnector.class);
        final Configuration config = configBuilder.build();

        CountDownLatch firstLatch = new CountDownLatch(1);

        DebeziumEngine.Builder<SourceRecord> builder = createEngineBuilder();
            builder
                .using(config.asProperties())
                .using(OffsetCommitPolicy.always())
                .notifying((records, committer) -> {
                    for (SourceRecord record : records) {
                        committer.markProcessed(record);

                        // Use this offset map and call GetCheckpoint on the server
                        // to see if this offset and the one set on the cdc_state table are the same.
                        this.offsetMap = record.sourceOffset();
                    }

                    // This function call is responsible for calling task.commit() later on which
                    // then invokes the callback commitOffset().
                    committer.markBatchFinished();
                })
                .using(this.getClass().getClassLoader())
                .using((success, message, error) -> {
                    if (error != null) {
                        LOGGER.error("Error while shutting down", error);
                    }
                    firstLatch.countDown();
                })
                .build();

        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
            engine.run();
        });

        awaitUntilConnectorIsReady();

        TestHelper.execute("INSERT INTO t1 VALUES (1, 'Vaibhav', 'Kushwaha', 12.34)");
        TestHelper.waitFor(Duration.ofSeconds(15));

        // The last update to the offsetMap will be the offset being committed on the server side.
        // Get the checkpoints from the server and match them with the value from offset map.
        YBClient ybClient = TestHelper.getYbClient(getMasterAddress());
        for (Map.Entry<String, ?> entry : offsetMap.entrySet()) {
            if (!entry.getKey().equals("transaction_id")) {
                String[] splitString = entry.getKey().split(Pattern.quote("."));

                // If string doesn't split, that means we have only received the tabletId in the
                // response, if it splits then we will have two elements - tableId and tabletId.
                String tabletId = splitString.length == 1 ? splitString[0] : splitString[1];
                CdcSdkCheckpoint cp = OpId.valueOf((String) entry.getValue()).toCdcSdkCheckpoint();

                GetCheckpointResponse resp = ybClient.getCheckpoint(
                        TestHelper.getYbTable(ybClient, "t1"), dbStreamId, tabletId);
                LOGGER.info("Offset op_id: {}.{} and response op_id: {}.{}", cp.getTerm(),
                        cp.getIndex(), resp.getTerm(), resp.getIndex());
                assertTrue(resp.getTerm() >= cp.getTerm());
                assertTrue(resp.getIndex() >= cp.getIndex());
            }
        }

        // Close the YBClient instance.
        ybClient.close();

        // Stop the engine started in this test.
        engine.close();;
    }
}
