package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import com.google.common.net.HostAndPort;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.yb.client.CDCErrorException;
import org.yb.client.DeleteCDCStreamResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import ch.qos.logback.classic.Logger;

import org.testcontainers.containers.Container.ExecResult;

import io.debezium.config.Configuration;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;

/**
 * End-to-end fail-fast tests on a Testcontainers YugabyteDB.
 * Code and message rules are covered by {@link YugabyteDBCdcErrorClassifierTest}.
 * <ul>
 *   <li>{@link #deleteStreamDuringSnapshotShouldFailFast}: a deleted stream stops the connector
 *       during the snapshot.</li>
 *   <li>{@link #streamExpiredForTabletAfterStopShouldFailFast} and
 *       {@link #streamExpiredForTabletDuringConsistentStreamingShouldFailFast}:
 *       an expired tablet stops the connector.</li>
 *   <li>{@link #walGarbageCollectedAfterStopShouldFailFast}: a missing WAL stops the connector
 *       with {@code CHECKPOINT_TOO_OLD}.</li>
 *   <li>The {@code inject*} tests: transient GetChanges errors are retried.</li>
 * </ul>
 */
public class YugabyteDBCdcFailFastTest extends YugabyteDBContainerTestBase {

    private static final String SIMULATE_GET_CHANGES_ERROR_FLAG = "TEST_cdc_simulate_error_for_get_changes";

    private static final long FAIL_FAST_RETRY_DELAY_MS = 500L;
    private static final long INJECT_RETRY_DELAY_MS = 2_500L;
    /**
     * High RPC attempts and a bounded operation timeout so yb-client exhausts in
     * {@code delayedSendRpcToTablet} with the last {@link CDCErrorException} on the cause chain,
     * not {@code tooManyAttemptsOrTimeout(rpc, null)} from low attempt limits.
     * <p>
     * The timeout has to stay above the GetChanges safe-deadline slack:
     * {@code cdc_read_rpc_timeout_ms} (30s) × {@code cdc_read_safe_deadline_ratio} (0.1) = 3s.
     * A client deadline that short makes GetChanges return {@code safe_hybrid_time = 0}, and
     * explicit checkpointing then keeps polling {@code 1.3} with time 0, so the streaming-ready
     * insert is never delivered.
     */
    private static final long FAIL_FAST_TEST_OPERATION_TIMEOUT_MS = 15_000L;
    private static final long STREAMING_READY_CONSUME_TIMEOUT_MS = 120_000L;
    private static final String STREAMING_LOOP_LOG = "Processing messages";
    private static final String CONSISTENT_STREAMING_LOOP_LOG = "Processing consistent messages";
    private static final String FAIL_FAST_LOG = "Failing fast";
    private static final String CONNECTOR_LOGGER = "io.debezium.connector.yugabytedb";

    private LogInterceptor logs;
    private final AtomicReference<String> completionMessage = new AtomicReference<>();

    @BeforeAll
    public static void beforeClass() throws Exception {
        initializeYBContainer();
        TestHelper.dropAllSchemas();
        resetServerFlags();
    }

    @BeforeEach
    public void before() {
        initializeConnectorTestFramework();
        logs = new LogInterceptor(CONNECTOR_LOGGER);
    }

    @AfterEach
    public void after() throws Exception {
        stopConnector();
        detachLogs();
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
    }

    @AfterAll
    public static void afterClass() throws Exception {
        try {
            resetServerFlags();
        }
        finally {
            shutdownYBContainer();
        }
    }

    @Test
    public void streamExpiredForTabletDuringConsistentStreamingShouldFailFast() throws Exception {
        String table = "t_expired_ord";
        TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);


        AtomicReference<Throwable> error = start(failFastConfig("public." + table, streamId)
                .with(YugabyteDBConnectorConfig.TRANSACTION_ORDERING, true)
                .with(YugabyteDBConnectorConfig.OVERRIDE_TRANSACTION_ORDERING_DEPRECATION, true)
                .with("tasks.max", 1));
        waitUntilStreamingReady(error, table, true);
        assertTrue(logs.containsMessage("Transaction ordering enabled: true"),
                "Expected ConsistentStreamingSource path.");
        assertTrue(logs.containsMessage("Processing consistent messages"),
                "Expected consistent streaming loop.");

        applyShortIntentRetentionFlags();
        try {
            stopConnector();
            // Drop the first run's logs. "Processing consistent messages" then shows the
            // restart entered consistent streaming; it does not identify which throwIfFailFast fired.
            resetLogs();
            TestHelper.waitFor(Duration.ofSeconds(8));

            error = start(failFastConfig("public." + table, streamId)
                    .with(YugabyteDBConnectorConfig.TRANSACTION_ORDERING, true)
                    .with(YugabyteDBConnectorConfig.OVERRIDE_TRANSACTION_ORDERING_DEPRECATION, true)
                    .with("tasks.max", 1));
            waitUntilStopped(error, Duration.ofSeconds(60));
            assertFailFastTaskStopped(error.get());
            assertTrue(logsContainExpiredForTablet(),
                    "Expected Stream ID … is expired for Tablet ID ….");
            assertTrue(logs.containsMessage("Processing consistent messages"),
                    "Expected fail-fast on ConsistentStreamingSource path.");
        }
        finally {
            resetIntentRetentionFlags();
        }
    }

    @Test
    public void deleteStreamDuringSnapshotShouldFailFast() throws Exception {
        String table = "t_del_stream_snap";
        TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
        TestHelper.executeBulkWithRange(
                "INSERT INTO " + table + " VALUES (%d, 'snap');", 0, 200);
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);
        AtomicReference<Throwable> error = start(failFastConfig("public." + table, streamId)
                .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial")
                .with(YugabyteDBConnectorConfig.LOG_GET_CHANGES, true));
        // Wait until the connector has entered the snapshot phase.
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> {
                    if (error.get() != null) {
                        throw new AssertionError("Connector failed before snapshot GetChanges.", error.get());
                    }
                    return logs.containsMessage("Requesting snapshot for table " + table);
                });

        // Delete the stream while the connector is in the snapshot GetChanges loop.
        resetLogs();
        deleteStreamOrFail(streamId);

        waitUntilStopped(error, Duration.ofSeconds(60));
        assertFailFastTaskStopped(error.get());
        assertFalse(logs.containsMessage(STREAMING_LOOP_LOG),
                "Fail-fast happened after streaming started.");
        // Each snapshot retry catch logs "will attempt retry" when it will retry.
        // No such log means a snapshot catch block failed fast instead of retrying.
        assertFalse(logs.containsMessage("will attempt retry"),
                "Snapshot retry loop ran instead of failing fast.");
        assertFalse(logs.containsMessage("Too many errors while trying to stream the snapshot"),
                "Snapshot exhausted its retries instead of failing fast.");
    }

    @Test
    public void streamExpiredForTabletAfterStopShouldFailFast() throws Exception {
        String table = "t_expired_tablet";
        TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);


        AtomicReference<Throwable> error = start(failFastConfig("public." + table, streamId));
        waitUntilStreamingReady(error, table);

        applyShortIntentRetentionFlags();
        try {
            stopConnector();
            TestHelper.waitFor(Duration.ofSeconds(8));

            error = start(failFastConfig("public." + table, streamId));
            waitUntilStopped(error, Duration.ofSeconds(60));
            assertFailFastTaskStopped(error.get());
            assertTrue(logsContainExpiredForTablet(),
                    "Expected Stream ID … is expired for Tablet ID ….");
        }
        finally {
            resetIntentRetentionFlags();
        }
    }

    @Test
    public void walGarbageCollectedAfterStopShouldFailFast() throws Exception {
        applyWalGcFlags();
        try {
            String table = "t_wal_gc";
            TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
            String streamId = TestHelper.getNewDbStreamId("yugabyte", table);


            AtomicReference<Throwable> error = start(failFastConfig("public." + table, streamId));
            waitUntilStreamingReady(error, table);
            String tabletId;
            try (YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress())) {
                YBTable ybTable = TestHelper.getYbTable(ybClient, table);
                tabletId = ybClient.getTabletUUIDs(ybTable).iterator().next();
            }
            Set<String> walBeforeStop = listTserverWalSegments(tabletId);
            if (walBeforeStop.isEmpty()) {
                throw new AssertionError("No wal-* segments for tablet " + tabletId);
            }
            stopConnector();

            TestHelper.execute("INSERT INTO " + table
                    + " SELECT g, repeat('x', 4096) FROM generate_series(100, 20100) g;");
            try (YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress())) {
                YBTable ybTable = TestHelper.getYbTable(ybClient, table);
                ybClient.flushTable(ybTable.getTableId());
            }
            Awaitility.await()
                    .atMost(Duration.ofSeconds(120))
                    .pollInterval(Duration.ofSeconds(1))
                    .until(() -> {
                        Set<String> remaining = new HashSet<>(walBeforeStop);
                        remaining.retainAll(listTserverWalSegments(tabletId));
                        return remaining.isEmpty();
                    });

            // rpc.retry.sleep.time.ms=20_000 with operation.timeout.ms=15_000: the first GetChanges
            // reads the WAL and returns CHECKPOINT_TOO_OLD. The retry sleep would be 20s, but the
            // 15s deadline expires first, so yb-client gives up with CDCErrorException on the cause
            // chain and throwIfFailFast stops the task.
            // Do not open a YBClient between this start and the assert: AsyncYBClient.sleepTime is
            // static and a new client resets it to 500ms.
            error = start(failFastConfig("public." + table, streamId)
                    .with(YugabyteDBConnectorConfig.RPC_RETRY_SLEEP_TIME, 20_000));
            waitUntilStopped(error, Duration.ofSeconds(90));
            assertFailFastTaskStopped(error.get());
            assertTrue(logs.containsMessage("code=CHECKPOINT_TOO_OLD"),
                    "Expected CHECKPOINT_TOO_OLD in Failing fast log.");
        }
        finally {
            resetWalGcFlags();
        }
    }

    @Test
    public void injectPeerNotStartedShouldRetry() throws Exception {
        assertRetryOnInjectedError("t_inj_peer_not_started", 0);
    }

    @Test
    public void injectTabletUnavailableShouldRetry() throws Exception {
        assertRetryOnInjectedError("t_inj_tablet_unavailable", 1);
    }

    @Test
    public void injectPeerNotReadyToServeShouldRetry() throws Exception {
        assertRetryOnInjectedError("t_inj_leader_not_ready", 3);
    }

    @Test
    public void injectPeerNotLeaderShouldRetry() throws Exception {
        assertRetryOnInjectedError("t_inj_not_leader", 2);
    }

    @Test
    public void injectLogFooterNotFoundShouldRetry() throws Exception {
        assertRetryOnInjectedError("t_inj_log_footer", 4);
    }

    @Test
    public void injectLogReaderNotInitializedShouldRetry() throws Exception {
        assertRetryOnInjectedError("t_inj_log_reader", 5);
    }

    /** Server text uses {@code Tablet}; accept the lowercase form too. */
    private boolean logsContainExpiredForTablet() {
        return logs.containsMessage("is expired for Tablet")
                || logs.containsMessage("is expired for tablet")
                || logs.containsStacktraceElement("is expired for Tablet")
                || logs.containsStacktraceElement("is expired for tablet");
    }

    private static void deleteStreamOrFail(String streamId) throws Exception {
        try (YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress())) {
            DeleteCDCStreamResponse response =
                    ybClient.deleteCDCStream(Collections.singleton(streamId), false, true);
            if (response.hasError()) {
                throw new AssertionError("deleteCDCStream(" + streamId + ") failed: "
                        + response.errorMessage());
            }
            if (response.getNotFoundStreamIds() != null
                    && response.getNotFoundStreamIds().contains(streamId)) {
                throw new AssertionError("deleteCDCStream did not find " + streamId);
            }
        }
    }

    private void assertFailFastTaskStopped(Throwable error) {
        assertNotNull(error);
        assertTrue(logs.containsMessage(FAIL_FAST_LOG),
                "Expected YugabyteDBCdcErrorClassifier.throwIfFailFast log.");
        assertFalse(logs.containsMessage("will attempt retry"),
                "Should not enter connector retry loop.");
    }

    private void assertRetryOnInjectedError(String table, int simulateCode) throws Exception {
        TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);

        AtomicReference<Throwable> error = start(TestHelper.getConfigBuilder("public." + table, streamId)
                .with(YugabyteDBConnectorConfig.MAX_CONNECTOR_RETRIES, 2)
                .with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, INJECT_RETRY_DELAY_MS)
                .with(YugabyteDBConnectorConfig.MAX_RPC_RETRY_ATTEMPTS, 2)
                .with(YugabyteDBConnectorConfig.RPC_RETRY_SLEEP_TIME, 50)
                .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "never"));
        waitUntilStreamingReady(error, table);

        setTserverFlag(SIMULATE_GET_CHANGES_ERROR_FLAG, String.valueOf(simulateCode));
        try {
            Awaitility.await()
                    .atMost(Duration.ofSeconds(20))
                    .pollInterval(Duration.ofMillis(200))
                    .until(() -> logs.containsMessage("will attempt retry"));
        }
        finally {
            setTserverFlag(SIMULATE_GET_CHANGES_ERROR_FLAG, "99");
        }
        TestHelper.execute("INSERT INTO " + table + " VALUES (2, 'inject-retry');");

        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    if (error.get() != null) {
                        throw new AssertionError("Task failed before inject-retry row.", error.get());
                    }
                    List<SourceRecord> batch = new ArrayList<>();
                    consumeAvailableRecords(batch::add);
                    return batch.stream().anyMatch(record -> ((Struct) record.value())
                            .getStruct("after").getStruct("id").getInt32("value") == 2);
                });

        assertFalse(logs.containsMessage(FAIL_FAST_LOG),
                "Injected code " + simulateCode + " should not fail fast.");
        assertTrue(engine.isRunning(),
                "Injected code " + simulateCode + " should recover after retry.");
        assertNull(error.get(),
                "Task should stay up after injected retryable error.");
    }

    private Configuration.Builder failFastConfig(String tableInclude, String streamId) throws Exception {
        return TestHelper.getConfigBuilder(tableInclude, streamId)
                .with(YugabyteDBConnectorConfig.MAX_CONNECTOR_RETRIES, 2)
                .with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, FAIL_FAST_RETRY_DELAY_MS)
                .with(YugabyteDBConnectorConfig.MAX_RPC_RETRY_ATTEMPTS,
                        YugabyteDBConnectorConfig.DEFAULT_MAX_RPC_RETRY_ATTEMPTS)
                .with(YugabyteDBConnectorConfig.OPERATION_TIMEOUT_MS, FAIL_FAST_TEST_OPERATION_TIMEOUT_MS)
                .with(YugabyteDBConnectorConfig.ADMIN_OPERATION_TIMEOUT_MS, FAIL_FAST_TEST_OPERATION_TIMEOUT_MS)
                .with(YugabyteDBConnectorConfig.RPC_RETRY_SLEEP_TIME, 100)
                .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "never");
    }

    private AtomicReference<Throwable> start(Configuration.Builder configBuilder) throws Exception {
        AtomicReference<Throwable> completionError = new AtomicReference<>();
        startEngine(configBuilder, (success, message, error) -> {
            completionError.set(error);
            completionMessage.set(message);
            if (!success) {
                LOGGER.info("Connector completed unsuccessfully: {}", message, error);
            }
        });
        return completionError;
    }

    /**
     * Waits until GetChanges is running. Vanilla streaming also waits for one consumed row.
     * Consistent streaming ({@code transaction.ordering}) logs {@code Processing consistent messages}
     * and may not emit a row until a safepoint, so the consumed-row wait is skipped there.
     */
    private void waitUntilStreamingReady(AtomicReference<Throwable> error, String table) throws Exception {
        waitUntilStreamingReady(error, table, false);
    }

    private void waitUntilStreamingReady(
            AtomicReference<Throwable> error, String table, boolean transactionOrdering)
            throws Exception {
        if (error.get() != null) {
            throw new AssertionError("Connector failed before streaming ready.", error.get());
        }
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> {
                    if (error.get() != null) {
                        throw new AssertionError("Connector failed before streaming loop.", error.get());
                    }
                    return logs.containsMessage(STREAMING_LOOP_LOG)
                            || logs.containsMessage(CONSISTENT_STREAMING_LOOP_LOG)
                            || logs.containsMessage("Requesting changes for table " + table);
                });
        if (transactionOrdering) {
            // Drain schema/DDL already in the change-event queue. A non-empty queue makes
            // ConsistentStreamingSource skip GetChanges, so stream-delete would never be seen.
            for (int i = 0; i < 20; i++) {
                if (consumeAvailableRecords(record -> {
                }) == 0) {
                    break;
                }
            }
            return;
        }
        TestHelper.execute("INSERT INTO " + table + " VALUES (1, 'streaming-ready');");
        AtomicLong consumed = new AtomicLong();
        Awaitility.await()
                .atMost(Duration.ofMillis(STREAMING_READY_CONSUME_TIMEOUT_MS))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    consumed.addAndGet(consumeAvailableRecords(record -> {
                    }));
                    return consumed.get() >= 1;
                });
    }

    private void waitUntilStopped(AtomicReference<Throwable> error, Duration timeout) {
        Awaitility.await()
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(250))
                .until(() -> error.get() != null);
    }

    private void resetLogs() {
        detachLogs();
        logs = new LogInterceptor(CONNECTOR_LOGGER);
    }

    private void detachLogs() {
        if (logs == null) {
            return;
        }
        Logger logger = (Logger) LoggerFactory.getLogger(CONNECTOR_LOGGER);
        logger.detachAppender(logs);
        logs.stop();
        logs = null;
    }

    /** Defaults for every tserver flag this class changes. */
    private static void resetServerFlags() throws Exception {
        setTserverFlag(SIMULATE_GET_CHANGES_ERROR_FLAG, "99");
        setTserverFlag("cdc_intent_retention_ms", "28800000");
        setTserverFlag("update_metrics_interval_ms", "15000");
        setTserverFlag("enable_log_retention_by_op_idx", "true");
        setTserverFlag("TEST_disable_wal_retention_time", "false");
        setTserverFlag("log_min_segments_to_retain", "2");
        setTserverFlag("retryable_request_timeout_secs", "660");
    }

    private static void setTserverFlag(String name, String value) throws Exception {
        HostAndPort tserver = HostAndPort.fromParts(ybContainer.getHost(), ybContainer.getMappedPort(9100));
        try (YBClient client = TestHelper.getYbClient(TestHelper.getMasterAddress())) {
            if (!client.setFlag(tserver, name, value, true)) {
                throw new RuntimeException("setFlag failed for " + name + " on " + tserver);
            }
        }
    }

    /**
     * Flags must be set before {@code CREATE TABLE} so the tablet is created without a CDC
     * op-index pin or time-based WAL floor. {@code retryable_request_timeout_secs} is shortened
     * so retryable-RPC WAL retention does not keep the old segment.
     */
    private static void applyWalGcFlags() throws Exception {
        setTserverFlag("enable_log_retention_by_op_idx", "false");
        setTserverFlag("TEST_disable_wal_retention_time", "true");
        setTserverFlag("log_min_segments_to_retain", "1");
        setTserverFlag("retryable_request_timeout_secs", "5");
    }

    private static void resetWalGcFlags() throws Exception {
        setTserverFlag("enable_log_retention_by_op_idx", "true");
        setTserverFlag("TEST_disable_wal_retention_time", "false");
        setTserverFlag("log_min_segments_to_retain", "2");
        setTserverFlag("retryable_request_timeout_secs", "660");
    }

    private static Set<String> listTserverWalSegments(String tabletId) throws Exception {
        ExecResult result = ybContainer.execInContainer(
                "sh", "-c",
                "find /home/yugabyte/var/data -name 'wal-*' -path '*tablet-" + tabletId + "*' -type f 2>/dev/null");
        return Arrays.stream(result.getStdout().split("\\R"))
                .map(String::trim)
                .filter(line -> !line.isEmpty())
                .collect(Collectors.toCollection(HashSet::new));
    }

    private static void applyShortIntentRetentionFlags() throws Exception {
        setTserverFlag("cdc_intent_retention_ms", "5000");
        setTserverFlag("update_metrics_interval_ms", "1000");
    }

    /** Server defaults: 8h intent retention, 15s metrics interval. */
    private static void resetIntentRetentionFlags() throws Exception {
        setTserverFlag("cdc_intent_retention_ms", "28800000");
        setTserverFlag("update_metrics_interval_ms", "15000");
    }

}
