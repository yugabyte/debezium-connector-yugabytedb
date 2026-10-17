package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.awaitility.Awaitility;
import com.google.common.net.HostAndPort;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.yb.client.CDCErrorException;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.core.read.ListAppender;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;

/**
 * End-to-end tests for {@link YugabyteDBCdcErrorClassifier#throwIfFailFast} on real RPC paths (Testcontainers).
 * Code/message rules are covered by {@link YugabyteDBCdcErrorClassifierTest}.
 * <p>
 * <b>During streaming</b> ({@link #deleteStreamDuringStreamingShouldFailFast}): waits for
 * {@link #waitUntilStreamingReady} (engine delay, {@code Processing messages}, and one consumed change) before
 * the fault, then requires a {@code Failing fast} log from the classifier. {@code DROP TABLE} on a table in an
 * active CDCSDK stream is not covered here: master hides the table for CDC ({@code hide_only}), so streaming
 * keeps polling known tablets and typical tserver errors ({@code TABLET_NOT_FOUND}, expired, not-in-stream) are
 * retried by {@link YugabyteDBCdcErrorClassifier}, not fail-fast.
 * <p>
 * <b>Bootstrap / tablet list</b> ({@link #removeTableFromStreamBeforeStartShouldRetryUntilExhausted}): yb-admin
 * removal before start yields {@code TABLE_NOT_FOUND} / {@code not found under stream} on
 * {@code GetTabletListToPollForCDC} and connector retries until exhausted (not fail-fast). GetChanges
 * {@code Tablet ID … is not part of stream ID …} remains retry (unit test).
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
     * The timeout has to stay above the tserver {@code consensus_rpc_timeout_ms} (3s). A client
     * deadline that short makes GetChanges return {@code safe_hybrid_time = 0}, and explicit
     * checkpointing then keeps polling {@code 1.3} with time 0, so the streaming-ready insert is
     * never delivered.
     */
    private static final long FAIL_FAST_TEST_OPERATION_TIMEOUT_MS = 15_000L;
    private static final long STREAMING_READY_CONSUME_TIMEOUT_MS = 120_000L;
    private static final String STREAMING_LOOP_LOG = "Processing messages";
    private static final String FAIL_FAST_LOG = "Failing fast";

    private ListAppender<ILoggingEvent> logAppender;

    @BeforeAll
    public static void beforeClass() throws SQLException {
        initializeYBContainer();
        TestHelper.dropAllSchemas();
        resetSimulateFlag();
    }

    @BeforeEach
    public void before() {
        initializeConnectorTestFramework();
        Logger logger = (Logger) LoggerFactory.getLogger("io.debezium.connector.yugabytedb");
        logAppender = new ListAppender<>();
        logAppender.start();
        logger.addAppender(logAppender);
    }

    @AfterEach
    public void after() throws Exception {
        resetSimulateFlag();
        stopConnector();
        Logger logger = (Logger) LoggerFactory.getLogger("io.debezium.connector.yugabytedb");
        if (logAppender != null) {
            logger.detachAppender(logAppender);
            logAppender.stop();
        }
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
    }

    @AfterAll
    public static void afterClass() throws Exception {
        resetSimulateFlag();
        shutdownYBContainer();
    }

    @Test
    public void deleteStreamDuringStreamingShouldFailFast() throws Exception {
        assertFailFastAfterStreamingReady("t_del_stream", FAIL_FAST_RETRY_DELAY_MS, Duration.ofSeconds(60), () -> {
            TestHelper.execute("INSERT INTO t_del_stream VALUES (2, 'fail-fast');");
            try (YBClient ybClient = TestHelper.getYbClient(getMasterAddress())) {
                ybClient.deleteCDCStream(Collections.singleton(currentStreamId()), false, true);
            }
        });
    }

    @Test
    public void removeTableFromStreamBeforeStartShouldRetryUntilExhausted() throws Exception {
        String table = "t_remove_stream";
        TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);
        streamIdRef.set(streamId);

        try (YBClient ybClient = TestHelper.getYbClient(getMasterAddress())) {
            YBTable ybTable = TestHelper.getYbTable(ybClient, table);
            assertNotNull(ybTable);
            removeUserTableFromChangeDataStream(streamId, ybTable.getTableId());
        }

        AtomicReference<Throwable> error = startConnectorForFailFast("public." + table, streamId, FAIL_FAST_RETRY_DELAY_MS);
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> logsContain("will attempt retry"));

        waitUntilStopped(error, Duration.ofSeconds(60));

        assertNotNull(error.get());
        assertFalse(logsContain(FAIL_FAST_LOG),
                "TABLE_NOT_FOUND should retry, not fail fast. logs=" + joinedLogs());
        assertFalse(isClassifiedFailFast(error.get()),
                "TABLE_NOT_FOUND should not be classified fail-fast. error=" + flatten(error.get()));
        String flattened = flatten(error.get()).toLowerCase();
        boolean removalError = flattened.contains("not found under stream") || logsContain("not found under stream");
        assertTrue(removalError,
                "Expected GetTabletListToPollForCDC-style removal error, not GetChanges membership retry. error="
                        + flatten(error.get()) + " logs=" + joinedLogs());
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

    private final AtomicReference<String> streamIdRef = new AtomicReference<>();

    private String currentStreamId() {
        return streamIdRef.get();
    }

    private void assertFailFastAfterStreamingReady(
            String table,
            long connectorRetryDelayMs,
            Duration stopTimeout,
            ThrowingRunnable trigger) throws Exception {
        TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);
        streamIdRef.set(streamId);
        AtomicReference<Throwable> error = startConnectorForFailFast("public." + table, streamId, connectorRetryDelayMs);
        waitUntilStreamingReady(error, table);
        trigger.run();

        waitUntilStopped(error, stopTimeout);
        assertFailFastTaskStopped(error.get());
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
        streamIdRef.set(streamId);
        AtomicReference<Throwable> error = startConnector("public." + table, streamId, INJECT_RETRY_DELAY_MS);
        waitUntilStreamingReady(error, table);

        setSimulateFlag(simulateCode);
        TestHelper.execute("INSERT INTO " + table + " VALUES (2, 'inject-retry');");

        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> logsContain("will attempt retry"));

        assertFalse(logsContain(FAIL_FAST_LOG),
                "Injected code " + simulateCode + " should not fail fast: " + joinedLogs());
        assertTrue(engine.isRunning(),
                "Injected code " + simulateCode + " should be retried without stopping the task. logs="
                        + joinedLogs() + " error=" + (error.get() == null ? "none" : flatten(error.get())));
        assertNull(error.get(),
                "Task should not have completed after injected retryable error. logs=" + joinedLogs());
    }

    private AtomicReference<Throwable> startConnectorForFailFast(
            String tableInclude, String streamId, long connectorRetryDelayMs) throws Exception {
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(tableInclude, streamId)
                .with(YugabyteDBConnectorConfig.MAX_CONNECTOR_RETRIES, 2)
                .with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, connectorRetryDelayMs)
                .with(YugabyteDBConnectorConfig.MAX_RPC_RETRY_ATTEMPTS,
                        YugabyteDBConnectorConfig.DEFAULT_MAX_RPC_RETRY_ATTEMPTS)
                .with(YugabyteDBConnectorConfig.OPERATION_TIMEOUT_MS, FAIL_FAST_TEST_OPERATION_TIMEOUT_MS)
                .with(YugabyteDBConnectorConfig.ADMIN_OPERATION_TIMEOUT_MS, FAIL_FAST_TEST_OPERATION_TIMEOUT_MS)
                .with(YugabyteDBConnectorConfig.RPC_RETRY_SLEEP_TIME, 100)
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
     * After {@link #awaitUntilConnectorIsReady()}, waits for the streaming loop to start and at least one
     * change to be delivered so {@code GetChanges} is active before faults or injection.
     */
    private void waitUntilStreamingReady(AtomicReference<Throwable> error, String table) throws Exception {
        if (error.get() != null) {
            throw new AssertionError("Connector failed before streaming ready: "
                    + flatten(error.get()), error.get());
        }
        awaitUntilConnectorIsReady();
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> {
                    if (error.get() != null) {
                        throw new AssertionError("Connector failed before streaming loop: "
                                + flatten(error.get()), error.get());
                    }
                    return logsContain(STREAMING_LOOP_LOG);
                });
        TestHelper.execute("INSERT INTO " + table + " VALUES (1, 'streaming-ready');");
        waitAndFailIfCannotConsume(new ArrayList<>(), 1, STREAMING_READY_CONSUME_TIMEOUT_MS);
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

    /** The appender list is mutated by the logging thread while tests scan it. */
    private List<String> logMessages() {
        while (true) {
            try {
                List<String> messages = new ArrayList<>();
                for (ILoggingEvent event : new ArrayList<>(logAppender.list)) {
                    messages.add(event.getFormattedMessage());
                    appendThrowable(messages, event.getThrowableProxy());
                }
                return messages;
            }
            catch (java.util.ConcurrentModificationException ignored) {
                // Appender added a line during the copy. Retry.
            }
        }
    }

    private static void appendThrowable(List<String> messages, IThrowableProxy proxy) {
        while (proxy != null) {
            if (proxy.getMessage() != null) {
                messages.add(proxy.getMessage());
            }
            proxy = proxy.getCause();
        }
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

    /**
     * yb-client has no master RPC for {@code remove_user_table_from_change_data_stream}; yb-admin only.
     * Runs {@code yb-admin} inside the Testcontainers YB image.
     */
    private static void removeUserTableFromChangeDataStream(String streamId, String tableId) throws Exception {
        ExecResult result = ybContainer.execInContainer(
                ybAdminBinary(),
                "--master_addresses", inContainerMasterAddress(),
                "remove_user_table_from_change_data_stream",
                streamId,
                tableId);
        if (result.getExitCode() != 0) {
            throw new RuntimeException("yb-admin remove_user_table_from_change_data_stream failed (exit "
                    + result.getExitCode() + "): " + result.getStdout() + result.getStderr());
        }
    }

    private static void setSimulateFlag(int value) throws Exception {
        HostAndPort tserver = tserverRpcAddress();
        String flagValue = String.valueOf(value);
        try (YBClient client = TestHelper.getYbClient(getMasterAddress())) {
            if (client.setFlag(tserver, SIMULATE_GET_CHANGES_ERROR_FLAG, flagValue, true)) {
                return;
            }
            if (client.setFlag(tserver, SIMULATE_GET_CHANGES_ERROR_FLAG, flagValue)) {
                return;
            }
            throw new RuntimeException("setFlag failed for " + SIMULATE_GET_CHANGES_ERROR_FLAG + " on " + tserver);
        }
    }

    private static String ybAdminPath;
    private static String masterAddressInContainer;
    private static HostAndPort tserverRpc;

    /** {@code yb-admin} on the image PATH, or beside the {@code yugabyted} this harness starts. */
    private static String ybAdminBinary() throws Exception {
        if (ybAdminPath != null) {
            return ybAdminPath;
        }
        ExecResult which = ybContainer.execInContainer("sh", "-c", "command -v yb-admin");
        if (which.getExitCode() == 0 && which.getStdout() != null && !which.getStdout().isBlank()) {
            ybAdminPath = which.getStdout().trim().split("\\s+")[0];
            return ybAdminPath;
        }
        int slash = yugabytedLocation.lastIndexOf('/');
        ybAdminPath = slash < 0 ? "yb-admin" : yugabytedLocation.substring(0, slash + 1) + "yb-admin";
        return ybAdminPath;
    }

    /** Master RPC as seen from inside the container, from the {@code yb-master} process. */
    private static String inContainerMasterAddress() throws Exception {
        if (masterAddressInContainer == null) {
            masterAddressInContainer = containerIpAddress + ":" + rpcPortOf("yb-master");
        }
        return masterAddressInContainer;
    }

    /**
     * Tserver RPC for {@link YBClient#setFlag}, using the host and mapped port the same way
     * {@link #getMasterAddress()} does when that container port is published.
     */
    private static HostAndPort tserverRpcAddress() throws Exception {
        if (tserverRpc != null) {
            return tserverRpc;
        }
        int containerPort = rpcPortOf("yb-tserver");
        String master = getMasterAddress();
        String host = master.substring(0, master.lastIndexOf(':'));
        int port = ybContainer.getExposedPorts().contains(containerPort)
                ? ybContainer.getMappedPort(containerPort)
                : containerPort;
        if (!ybContainer.getExposedPorts().contains(containerPort)) {
            host = containerIpAddress;
        }
        tserverRpc = HostAndPort.fromParts(host, port);
        return tserverRpc;
    }

    private static int rpcPortOf(String processName) throws Exception {
        String needle = processName.replace("'", "");
        ExecResult cmdline = ybContainer.execInContainer("sh", "-c",
                "for f in /proc/[0-9]*/cmdline; do tr '\\0' ' ' < \"$f\" 2>/dev/null; echo; done | grep -F '"
                        + needle + "' | grep rpc_bind_addresses | head -n 1");
        String args = cmdline.getStdout() == null ? "" : cmdline.getStdout();
        Matcher matcher = Pattern.compile("rpc_bind_addresses(?:=|\\s+)\\S*:(\\d+)").matcher(args);
        if (!matcher.find()) {
            throw new IllegalStateException("No rpc_bind_addresses for " + processName + " in: " + args);
        }
        return Integer.parseInt(matcher.group(1));
    }

    private static void resetSimulateFlag() {
        try {
            setSimulateFlag(99);
        }
        catch (Exception ignored) {
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
