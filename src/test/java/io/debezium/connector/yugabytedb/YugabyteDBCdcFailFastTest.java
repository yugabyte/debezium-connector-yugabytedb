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
import java.util.concurrent.CopyOnWriteArrayList;
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
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.read.ListAppender;

import org.testcontainers.containers.Container.ExecResult;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;

/**
 * End-to-end tests for {@link YugabyteDBCdcErrorClassifier#throwIfFailFast} on a Testcontainers
 * YugabyteDB instance (started via {@link #initializeYBContainer()}).
 * Code/message rules are covered by {@link YugabyteDBCdcErrorClassifierTest}.
 * <p>
 * <b>After stop with deleted stream</b> ({@link #deleteStreamAfterStopShouldFailFast}): streams
 * to ready, stops the connector, deletes the stream, then restarts. The connector's
 * {@code taskConfigs()} calls {@code validateTServerConnection(..., raiseOnPermanentError=true)},
 * which calls {@code getDBStreamInfo} to fetch stream metadata. That master RPC detects the
 * deleted stream (OBJECT_NOT_FOUND) and calls {@code context().raiseError()}, stopping the task
 * fail-fast. Live stream-delete during active {@code GetChanges} streaming polling is not
 * reliably testable: {@code deleteCDCStream} (a master RPC) removes the stream's {@code cdc_state}
 * entries, but the tserver's CDC service may return a retryable error code (e.g.
 * {@code TABLE_NOT_FOUND}) rather than the fatal {@code "could not find cdc stream"} message on
 * the streaming-checkpoint code path, so the connector retries indefinitely instead of stopping.
 * {@link #deleteStreamDuringSnapshotShouldFailFast} works because snapshot-phase checkpoint values
 * appear to trigger the fatal classification from the same deletion; the exact tserver-side
 * distinction requires reading the C++ {@code CDCServiceImpl::GetChanges} handler.
 * {@code DROP TABLE} on a table in an active CDCSDK stream is not covered here: master hides
 * the table for CDC ({@code hide_only}), so streaming keeps polling known tablets and typical
 * tserver errors ({@code TABLET_NOT_FOUND}, expired, not-in-stream) are retried by
 * {@link YugabyteDBCdcErrorClassifier}, not fail-fast.
 * Removing a table from the stream before connector start is a bootstrap hard stop
 * ({@code DebeziumException} from {@code getDBStreamInfo} / {@code fetchTableList}), not a
 * GetTabletList retry loop; {@code TABLE_NOT_FOUND} → retry is covered by
 * {@link YugabyteDBCdcErrorClassifierTest}.
 * <p>
 * {@link #streamExpiredForTabletDuringConsistentStreamingShouldFailFast} enters the
 * {@link YugabyteDBConsistentStreamingSource} path after intent-retention expiry and the
 * connector fails fast; the assertions do not pinpoint which {@code throwIfFailFast} call site
 * fired (bootstrap retries also run before the GetChanges loop).
 * Live stream-delete during streaming is not reliably testable — see above.
 * {@link #tabletSplitDuringConsistentStreamingShouldNotFailFast} splits a tablet on that path
 * so {@code isTabletSplit} handles it in-stream.
 * <p>
 * {@link #walGarbageCollectedAfterStopShouldFailFast} drops the CDC op-index pin and time-based
 * WAL retention, flushes after a few MB of writes, waits until a pre-stop WAL segment is gone,
 * then resumes. The resumed connector uses {@code rpc.retry.sleep.time.ms=20_000} with
 * {@code operation.timeout.ms=15_000}: the first attempt still has enough time for the server to
 * read the WAL and return {@code CHECKPOINT_TOO_OLD}; the retry sleep overshoots the deadline
 * so yb-client gives up with the {@link org.yb.client.CDCErrorException} on the cause chain,
 * which {@link YugabyteDBCdcErrorClassifier#throwIfFailFast} classifies. Expect fail-fast
 * ({@code code=CHECKPOINT_TOO_OLD} in log), not stream-expired.
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

    private ListAppender<ILoggingEvent> logAppender;

    @BeforeAll
    public static void beforeClass() throws Exception {
        initializeYBContainer();
        TestHelper.dropAllSchemas();
        resetSimulateFlag();
    }

    @BeforeEach
    public void before() {
        initializeConnectorTestFramework();
        Logger logger = (Logger) LoggerFactory.getLogger("io.debezium.connector.yugabytedb");
        logAppender = new CopyOnWriteListAppender();
        logAppender.start();
        logger.addAppender(logAppender);
    }

    @AfterEach
    public void after() throws Exception {
        // Run every reset independently so a single failure does not leave other flags dirty
        // on the cluster and contaminate subsequent tests (e.g. cdc_intent_retention_ms=5000
        // or WAL-GC flags staying set).
        Exception firstFailure = null;
        try { resetSimulateFlag(); }         catch (Exception e) { if (firstFailure == null) firstFailure = e; }
        try { resetIntentRetentionFlags(); } catch (Exception e) { if (firstFailure == null) firstFailure = e; }
        try { resetWalGcFlags(); }           catch (Exception e) { if (firstFailure == null) firstFailure = e; }
        try {
            if (firstFailure != null) throw firstFailure;
        }
        finally {
            stopConnector();
            Logger logger = (Logger) LoggerFactory.getLogger("io.debezium.connector.yugabytedb");
            if (logAppender != null) {
                logger.detachAppender(logAppender);
                logAppender.stop();
            }
            TestHelper.executeDDL("drop_tables_and_databases.ddl");
        }
    }

    @AfterAll
    public static void afterClass() throws Exception {
        // Same pattern: attempt all three resets before shutting down the container.
        Exception firstFailure = null;
        try { resetSimulateFlag(); }         catch (Exception e) { if (firstFailure == null) firstFailure = e; }
        try { resetIntentRetentionFlags(); } catch (Exception e) { if (firstFailure == null) firstFailure = e; }
        try { resetWalGcFlags(); }           catch (Exception e) { if (firstFailure == null) firstFailure = e; }
        try {
            if (firstFailure != null) throw firstFailure;
        }
        finally {
            shutdownYBContainer();
        }
    }

    @Test
    public void deleteStreamAfterStopShouldFailFast() throws Exception {
        String table = "t_del_stream";
        TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);


        AtomicReference<Throwable> error = startConnectorForFailFast(
                "public." + table, streamId, FAIL_FAST_RETRY_DELAY_MS);
        waitUntilStreamingReady(error, table);
        stopConnector();

        deleteStreamOrFail(streamId);

        // taskConfigs() calls validateTServerConnection(..., raiseOnPermanentError=true), which calls
        // getDBStreamInfo (master RPC). The deleted stream (OBJECT_NOT_FOUND) is detected immediately,
        // and the catch at YugabyteDBgRPCConnector.java:398-409 calls context().raiseError() directly,
        // not via throwIfFailFast. We assert isClassifiedFailFast rather than the "Failing fast" log
        // line because raiseError marks the connector FAILED without logging that specific message.
        error = startConnectorForFailFast("public." + table, streamId, FAIL_FAST_RETRY_DELAY_MS);
        waitUntilStopped(error, Duration.ofSeconds(60));
        assertNotNull(error.get(), "Expected connector to stop with an error");
        assertTrue(isClassifiedFailFast(error.get()),
                "Expected fail-fast classification on deleted stream. error=" + flatten(error.get())
                        + " logs=" + joinedLogs());
        assertFalse(logsContain("will attempt retry"),
                "Should not enter connector retry loop: " + joinedLogs());
    }

    @Test
    public void streamExpiredForTabletDuringConsistentStreamingShouldFailFast() throws Exception {
        String table = "t_expired_ord";
        TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);


        AtomicReference<Throwable> error = startConnectorForFailFast(
                "public." + table, streamId, FAIL_FAST_RETRY_DELAY_MS, true);
        waitUntilStreamingReady(error, table, true);
        assertTrue(logsContain("Transaction ordering enabled: true"),
                "Expected ConsistentStreamingSource path. logs=" + joinedLogs());
        assertTrue(logsContain("Processing consistent messages"),
                "Expected consistent streaming loop. logs=" + joinedLogs());

        applyShortIntentRetentionFlags();
        stopConnector();
        // Clear the log so assertions below reflect only the restarted connector's run
        // and not any "Processing consistent messages" from the first run.
        // The assertion on "Processing consistent messages" proves the restarted connector
        // entered the ConsistentStreamingSource path before the expiry was detected; it
        // does not pinpoint which throwIfFailFast call site fired (bootstrap retries in
        // YugabyteDBStreamingChangeEventSource:213/259 also run before the GetChanges
        // loop at ConsistentStreamingSource:287).
        logAppender.list.clear();
        TestHelper.waitFor(Duration.ofSeconds(8));

        error = startConnectorForFailFast(
                "public." + table, streamId, FAIL_FAST_RETRY_DELAY_MS, true);
        waitUntilStopped(error, Duration.ofSeconds(60));
        assertFailFastTaskStopped(error.get());
        String flattened = flatten(error.get()).toLowerCase();
        assertTrue(flattened.contains("is expired for tablet") || logsContain("is expired for tablet"),
                "Expected Stream ID … is expired for Tablet ID …. error=" + flatten(error.get())
                        + " logs=" + joinedLogs());
        assertTrue(logsContain("Processing consistent messages"),
                "Expected fail-fast on ConsistentStreamingSource path. logs=" + joinedLogs());
    }

    @Test
    public void tabletSplitDuringConsistentStreamingShouldNotFailFast() throws Exception {
        String table = "t_split_ord";
        TestHelper.execute("CREATE TABLE " + table
                + " (id INT PRIMARY KEY, name TEXT) SPLIT INTO 1 TABLETS;");
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);


        AtomicReference<Throwable> error = startConnectorForFailFast(
                "public." + table, streamId, FAIL_FAST_RETRY_DELAY_MS, true);
        waitUntilStreamingReady(error, table, true);
        for (int i = 0; i < 50; i++) {
            TestHelper.execute("INSERT INTO " + table + " VALUES (" + i + ", 'split');");
        }

        try (YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress())) {
            YBTable ybTable = TestHelper.getYbTable(ybClient, table);
            ybClient.flushTable(ybTable.getTableId());
            TestHelper.waitFor(Duration.ofSeconds(5));
            ybClient.splitTablet(ybClient.getTabletUUIDs(ybTable).iterator().next());
            TestHelper.waitForTablets(ybClient, ybTable, 2);
        }

        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> {
                    if (error.get() != null) {
                        throw new AssertionError("Task failed on tablet split: "
                                + flatten(error.get()), error.get());
                    }
                    return logsContain("Encountered a tablet split, handling it gracefully");
                });
        assertFalse(logsContain(FAIL_FAST_LOG),
                "Tablet split should be handled in-stream. logs=" + joinedLogs());
        assertTrue(engine.isRunning(),
                "Task should stay up after consistent-source tablet split. logs=" + joinedLogs());
        assertNull(error.get());

        // Verify that post-split rows are actually consumed (not silently stalled by frozen parent safetime).
        TestHelper.execute("INSERT INTO " + table + " VALUES (9999, 'post-split');");
        AtomicLong consumed = new AtomicLong();
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    if (error.get() != null) {
                        throw new AssertionError("Task failed after split before post-split row consumed: "
                                + flatten(error.get()), error.get());
                    }
                    consumeAvailableRecords(record -> {
                        Integer id = afterId(record);
                        if (Integer.valueOf(9999).equals(id)) {
                            consumed.incrementAndGet();
                        }
                    });
                    return consumed.get() >= 1;
                });
    }

    @Test
    public void deleteStreamDuringSnapshotShouldFailFast() throws Exception {
        String table = "t_del_stream_snap";
        TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
        TestHelper.executeBulkWithRange(
                "INSERT INTO " + table + " VALUES (%d, 'snap');", 0, 200);
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);


        AtomicReference<Throwable> error = startConnectorForFailFastSnapshot(
                "public." + table, streamId, FAIL_FAST_RETRY_DELAY_MS);

        // Wait until the connector has entered the snapshot phase.
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(50))
                .until(() -> {
                    if (error.get() != null) {
                        throw new AssertionError("Connector failed before snapshot: "
                                + flatten(error.get()), error.get());
                    }
                    return logsContain("Starting the snapshot process now")
                            || logsContain("Requesting snapshot for table " + table);
                });

        // Delete the stream while the connector is in snapshot mode.
        logAppender.list.clear();
        deleteStreamOrFail(streamId);

        // The connector must fail fast once it detects the deleted stream.
        // On fast local storage YugabyteDB's CDCSDK snapshot GetChanges serves buffered tablet
        // data even after the stream is deleted; the NOT_FOUND error surfaces when the connector
        // transitions to streaming and calls GetTabletListToPollForCDC.  Either way the
        // connector detects the deletion and stops, which is the behaviour we are testing.
        waitUntilStopped(error, Duration.ofSeconds(60));
        assertFailFastTaskStopped(error.get());
    }

    @Test
    public void streamExpiredForTabletAfterStopShouldFailFast() throws Exception {
        String table = "t_expired_tablet";
        TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);


        AtomicReference<Throwable> error = startConnectorForFailFast(
                "public." + table, streamId, FAIL_FAST_RETRY_DELAY_MS);
        waitUntilStreamingReady(error, table);

        applyShortIntentRetentionFlags();
        stopConnector();
        TestHelper.waitFor(Duration.ofSeconds(8));

        error = startConnectorForFailFast("public." + table, streamId, FAIL_FAST_RETRY_DELAY_MS);
        waitUntilStopped(error, Duration.ofSeconds(60));
        assertFailFastTaskStopped(error.get());
        String flattened = flatten(error.get()).toLowerCase();
        assertTrue(flattened.contains("is expired for tablet") || logsContain("is expired for tablet"),
                "Expected Stream ID … is expired for Tablet ID …. error=" + flatten(error.get())
                        + " logs=" + joinedLogs());
    }

    @Test
    public void walGarbageCollectedAfterStopShouldFailFast() throws Exception {
        applyWalGcFlags();
        String table = "t_wal_gc";
        TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);


        AtomicReference<Throwable> error = startConnectorForFailFast(
                "public." + table, streamId, FAIL_FAST_RETRY_DELAY_MS);
        waitUntilStreamingReady(error, table);
        String tabletId = cdcTabletId(table);
        Set<String> walBeforeStop = listTserverWalSegments(tabletId);
        if (walBeforeStop.isEmpty()) {
            throw new AssertionError("No wal-* segments for tablet " + tabletId);
        }
        stopConnector();

        TestHelper.execute("INSERT INTO " + table
                + " SELECT g, repeat('x', 4096) FROM generate_series(100, 20100) g;");
        flushTable(table);
        Awaitility.await()
                .atMost(Duration.ofSeconds(120))
                .pollInterval(Duration.ofSeconds(1))
                .until(() -> {
                    Set<String> remaining = new HashSet<>(walBeforeStop);
                    remaining.retainAll(listTserverWalSegments(tabletId));
                    return remaining.isEmpty();
                });

        // Do NOT open any YBClient between here and assertFailFastTaskStopped: AsyncYBClient.sleepTime
        // is static; a new client resets it to 500ms which defeats the overshooting-sleep approach.
        error = startConnectorForWalGc("public." + table, streamId);
        waitUntilStopped(error, Duration.ofSeconds(90));
        assertFailFastTaskStopped(error.get());
        assertTrue(logsContain("code=CHECKPOINT_TOO_OLD"),
                "Expected CHECKPOINT_TOO_OLD in Failing fast log. logs=" + joinedLogs());
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
        assertTrue(logsContain(FAIL_FAST_LOG),
                "Expected YugabyteDBCdcErrorClassifier.throwIfFailFast log. isFailFast="
                        + isClassifiedFailFast(error) + " error=" + flatten(error) + " logs=" + joinedLogs());
        assertFalse(logsContain("will attempt retry"),
                "Should not enter connector retry loop: " + joinedLogs());
    }

    private void assertRetryOnInjectedError(String table, int simulateCode) throws Exception {
        TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);

        AtomicReference<Throwable> error = startConnector("public." + table, streamId, INJECT_RETRY_DELAY_MS);
        waitUntilStreamingReady(error, table);

        setSimulateFlag(simulateCode);
        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> logsContain("will attempt retry"));

        resetSimulateFlag();
        TestHelper.execute("INSERT INTO " + table + " VALUES (2, 'inject-retry');");

        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> {
                    if (error.get() != null) {
                        throw new AssertionError("Task failed before inject-retry row: "
                                + flatten(error.get()), error.get());
                    }
                    List<SourceRecord> batch = new ArrayList<>();
                    consumeAvailableRecords(batch::add);
                    return batch.stream().anyMatch(record -> Integer.valueOf(2).equals(afterId(record)));
                });

        assertFalse(logsContain(FAIL_FAST_LOG),
                "Injected code " + simulateCode + " should not fail fast: " + joinedLogs());
        assertTrue(engine.isRunning(),
                "Injected code " + simulateCode + " should recover after retry. logs="
                        + joinedLogs() + " error=" + (error.get() == null ? "none" : flatten(error.get())));
        assertNull(error.get(),
                "Task should stay up after injected retryable error. logs=" + joinedLogs());
    }

    private AtomicReference<Throwable> startConnectorForFailFast(
            String tableInclude, String streamId, long connectorRetryDelayMs) throws Exception {
        return startConnectorForFailFast(tableInclude, streamId, connectorRetryDelayMs, false);
    }

    private AtomicReference<Throwable> startConnectorForFailFast(
            String tableInclude, String streamId, long connectorRetryDelayMs,
            boolean transactionOrdering) throws Exception {
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(tableInclude, streamId)
                .with(YugabyteDBConnectorConfig.MAX_CONNECTOR_RETRIES, 2)
                .with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, connectorRetryDelayMs)
                .with(YugabyteDBConnectorConfig.MAX_RPC_RETRY_ATTEMPTS,
                        YugabyteDBConnectorConfig.DEFAULT_MAX_RPC_RETRY_ATTEMPTS)
                .with(YugabyteDBConnectorConfig.OPERATION_TIMEOUT_MS, FAIL_FAST_TEST_OPERATION_TIMEOUT_MS)
                .with(YugabyteDBConnectorConfig.ADMIN_OPERATION_TIMEOUT_MS, FAIL_FAST_TEST_OPERATION_TIMEOUT_MS)
                .with(YugabyteDBConnectorConfig.RPC_RETRY_SLEEP_TIME, 100)
                .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "never");
        if (transactionOrdering) {
            configBuilder
                    .with(YugabyteDBConnectorConfig.TRANSACTION_ORDERING, true)
                    .with(YugabyteDBConnectorConfig.OVERRIDE_TRANSACTION_ORDERING_DEPRECATION, true)
                    .with("tasks.max", 1);
        }

        AtomicReference<Throwable> completionError = new AtomicReference<>();
        startEngine(configBuilder, (success, message, error) -> {
            completionError.set(error);
            if (!success) {
                LOGGER.info("Connector completed unsuccessfully: {}", message, error);
            }
        });
        return completionError;
    }

    /**
     * Resumed-connector config for the WAL GC test.
     * <p>
     * {@code rpc.retry.sleep.time.ms=20_000} with {@code operation.timeout.ms=15_000}: the first
     * GetChanges attempt has >8s left so the server reads the WAL and returns
     * {@code CHECKPOINT_TOO_OLD (NOT_FOUND)}; the retry sleep would be 20s but the 15s deadline
     * expires first, so yb-client gives up carrying the {@link org.yb.client.CDCErrorException}
     * on the cause chain. {@link YugabyteDBCdcErrorClassifier#throwIfFailFast} sees it and stops
     * the task.
     * <p>
     * Do NOT create any other YBClient between this call and the assert: AsyncYBClient.sleepTime
     * is a static field and a new client resets it to the default 500ms.
     */
    private AtomicReference<Throwable> startConnectorForWalGc(
            String tableInclude, String streamId) throws Exception {
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(tableInclude, streamId)
                .with(YugabyteDBConnectorConfig.MAX_CONNECTOR_RETRIES, 2)
                .with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, FAIL_FAST_RETRY_DELAY_MS)
                .with(YugabyteDBConnectorConfig.MAX_RPC_RETRY_ATTEMPTS,
                        YugabyteDBConnectorConfig.DEFAULT_MAX_RPC_RETRY_ATTEMPTS)
                .with(YugabyteDBConnectorConfig.OPERATION_TIMEOUT_MS, 15_000)
                .with(YugabyteDBConnectorConfig.ADMIN_OPERATION_TIMEOUT_MS, 15_000)
                .with(YugabyteDBConnectorConfig.RPC_RETRY_SLEEP_TIME, 20_000)
                .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "never");

        AtomicReference<Throwable> completionError = new AtomicReference<>();
        startEngine(configBuilder, (success, message, error) -> {
            completionError.set(error);
            if (!success) {
                LOGGER.info("Connector completed unsuccessfully: {}", message, error);
            }
        });
        return completionError;
    }

    private AtomicReference<Throwable> startConnectorForFailFastSnapshot(
            String tableInclude, String streamId, long connectorRetryDelayMs) throws Exception {
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(tableInclude, streamId)
                .with(YugabyteDBConnectorConfig.MAX_CONNECTOR_RETRIES, 2)
                .with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, connectorRetryDelayMs)
                .with(YugabyteDBConnectorConfig.MAX_RPC_RETRY_ATTEMPTS,
                        YugabyteDBConnectorConfig.DEFAULT_MAX_RPC_RETRY_ATTEMPTS)
                .with(YugabyteDBConnectorConfig.OPERATION_TIMEOUT_MS, FAIL_FAST_TEST_OPERATION_TIMEOUT_MS)
                .with(YugabyteDBConnectorConfig.ADMIN_OPERATION_TIMEOUT_MS, FAIL_FAST_TEST_OPERATION_TIMEOUT_MS)
                .with(YugabyteDBConnectorConfig.RPC_RETRY_SLEEP_TIME, 100)
                .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "initial");

        AtomicReference<Throwable> completionError = new AtomicReference<>();
        startEngine(configBuilder, (success, message, error) -> {
            completionError.set(error);
            if (!success) {
                LOGGER.info("Connector completed unsuccessfully: {}", message, error);
            }
        });
        return completionError;
    }

    private AtomicReference<Throwable> startConnector(
            String tableInclude, String streamId, long connectorRetryDelayMs) throws Exception {
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(tableInclude, streamId)
                .with(YugabyteDBConnectorConfig.MAX_CONNECTOR_RETRIES, 2)
                .with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, connectorRetryDelayMs)
                .with(YugabyteDBConnectorConfig.MAX_RPC_RETRY_ATTEMPTS, 2)
                .with(YugabyteDBConnectorConfig.RPC_RETRY_SLEEP_TIME, 50)
                .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, "never");

        AtomicReference<Throwable> completionError = new AtomicReference<>();
        startEngine(configBuilder, (success, message, error) -> {
            completionError.set(error);
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
            throw new AssertionError("Connector failed before streaming ready: "
                    + flatten(error.get()), error.get());
        }
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> {
                    if (error.get() != null) {
                        throw new AssertionError("Connector failed before streaming loop: "
                                + flatten(error.get()), error.get());
                    }
                    return logsContain(STREAMING_LOOP_LOG)
                            || logsContain(CONSISTENT_STREAMING_LOOP_LOG)
                            || logsContain("Requesting changes for table " + table);
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

    private boolean isClassifiedFailFast(Throwable error) {
        return YugabyteDBCdcErrorClassifier.isFailFast(error);
    }

    private boolean logsContain(String token) {
        return logMessages().stream().anyMatch(message -> message.contains(token));
    }

    private String joinedLogs() {
        return String.join(" | ", logMessages());
    }

    /** Snapshot of events; the appender list is a {@link CopyOnWriteArrayList}. */
    private List<String> logMessages() {
        List<String> messages = new ArrayList<>();
        for (ILoggingEvent event : logAppender.list) {
            messages.add(event.getFormattedMessage());
            appendThrowable(messages, event.getThrowableProxy());
        }
        return messages;
    }

    private static void appendThrowable(List<String> messages, IThrowableProxy proxy) {
        while (proxy != null) {
            if (proxy.getMessage() != null) {
                messages.add(proxy.getMessage());
            }
            proxy = proxy.getCause();
        }
    }

    private static Integer afterId(SourceRecord record) {
        if (!(record.value() instanceof Struct)) {
            return null;
        }
        Struct envelope = (Struct) record.value();
        if (envelope.schema().field("after") == null || envelope.get("after") == null
                || !(envelope.get("after") instanceof Struct)) {
            return null;
        }
        Struct after = envelope.getStruct("after");
        if (after.schema().field("id") == null || !(after.get("id") instanceof Struct)) {
            return null;
        }
        Struct id = after.getStruct("id");
        if (id.schema().field("value") == null || id.get("value") == null) {
            return null;
        }
        Object value = id.get("value");
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return null;
    }

    private static String flatten(Throwable error) {
        StringBuilder builder = new StringBuilder();
        Throwable current = error;
        while (current != null) {
            builder.append(current.getClass().getSimpleName())
                    .append(':')
                    .append(current.getMessage())
                    .append(" -> ");
            current = current.getCause();
        }
        return builder.toString();
    }

    private static void setSimulateFlag(int value) throws Exception {
        HostAndPort tserver = tserverRpcAddress();
        String flagValue = String.valueOf(value);
        try (YBClient client = TestHelper.getYbClient(TestHelper.getMasterAddress())) {
            if (client.setFlag(tserver, SIMULATE_GET_CHANGES_ERROR_FLAG, flagValue, true)) {
                return;
            }
            if (client.setFlag(tserver, SIMULATE_GET_CHANGES_ERROR_FLAG, flagValue)) {
                return;
            }
            throw new RuntimeException("setFlag failed for " + SIMULATE_GET_CHANGES_ERROR_FLAG + " on " + tserver);
        }
    }

    private static HostAndPort tserverRpc;

    /**
     * Tserver RPC address for {@link YBClient#setFlag}.
     * Derived from the container's mapped port for the tserver RPC port 9100.
     */
    private static HostAndPort tserverRpcAddress() throws Exception {
        if (tserverRpc != null) {
            return tserverRpc;
        }
        tserverRpc = HostAndPort.fromParts(ybContainer.getHost(), rpcPortOf(9100));
        return tserverRpc;
    }

    private static int rpcPortOf(int containerPort) {
        return ybContainer.getMappedPort(containerPort);
    }

    private static void resetSimulateFlag() throws Exception {
        setSimulateFlag(99);
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

    private static void setTserverFlag(String name, String value) throws Exception {
        HostAndPort tserver = tserverRpcAddress();
        try (YBClient client = TestHelper.getYbClient(TestHelper.getMasterAddress())) {
            if (!client.setFlag(tserver, name, value, true) && !client.setFlag(tserver, name, value)) {
                throw new RuntimeException("setFlag failed for " + name + " on " + tserver);
            }
        }
    }

    private String cdcTabletId(String table) {
        String prefix = "Requesting changes for table " + table + " tablet ";
        for (String message : logMessages()) {
            int index = message.indexOf(prefix);
            if (index < 0) {
                continue;
            }
            String rest = message.substring(index + prefix.length()).trim();
            int comma = rest.indexOf(',');
            return (comma >= 0 ? rest.substring(0, comma) : rest.split("\\s+")[0]).trim();
        }
        throw new AssertionError("No GetChanges tablet id for " + table + " in logs: " + joinedLogs());
    }

    private static void flushTable(String table) throws Exception {
        ExecResult result = ybContainer.execInContainer(
                "/home/yugabyte/bin/yb-admin",
                "--master_addresses", containerIpAddress + ":7100",
                "flush_table", "ysql.yugabyte", table);
        if (result.getExitCode() != 0) {
            throw new RuntimeException("flush_table failed for " + table + ": " + result.getStderr());
        }
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
        HostAndPort tserver = tserverRpcAddress();
        try (YBClient client = TestHelper.getYbClient(TestHelper.getMasterAddress())) {
            if (!client.setFlag(tserver, "cdc_intent_retention_ms", "28800000", true)
                    && !client.setFlag(tserver, "cdc_intent_retention_ms", "28800000")) {
                throw new RuntimeException("setFlag failed for cdc_intent_retention_ms on " + tserver);
            }
            if (!client.setFlag(tserver, "update_metrics_interval_ms", "15000", true)
                    && !client.setFlag(tserver, "update_metrics_interval_ms", "15000")) {
                throw new RuntimeException("setFlag failed for update_metrics_interval_ms on " + tserver);
            }
        }
    }

    /**
     * {@link ListAppender} stores events in an {@link ArrayList}. Readers copy that list without
     * the appender lock, so a concurrent {@code add} can expose a null slot. Store events in a
     * {@link CopyOnWriteArrayList} instead.
     */
    private static final class CopyOnWriteListAppender extends ListAppender<ILoggingEvent> {
        CopyOnWriteListAppender() {
            this.list = new CopyOnWriteArrayList<>();
        }

        @Override
        protected void append(ILoggingEvent event) {
            list.add(event);
        }
    }
}
