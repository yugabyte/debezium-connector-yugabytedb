package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.slf4j.LoggerFactory;
import org.yb.client.CDCErrorException;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

/**
 * Service-side validation of CDC error classification. Disabled in CI unless
 * {@code -Dyb.service.it=true} is set. Optional {@code -Dyb.bin.dir=/path/to/yugabyte/bin}
 * (or {@code YB_BIN_DIR}) locates {@code yb-admin} / {@code yb-ts-cli}; otherwise PATH is used.
 */
@EnabledIfSystemProperty(named = "yb.service.it", matches = "true")
public class YugabyteDBCdcServiceErrorIT extends YugabytedTestBase {

    private static final long RETRY_DELAY_MS = 2_500L;

    private ListAppender<ILoggingEvent> logAppender;

    @BeforeAll
    public static void beforeClass() throws SQLException {
        initializeYBContainer();
        assumeTrue(isYugabyteReachable(), "Local yugabyted is not reachable on 127.0.0.1:5433/7100");
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
    public static void afterClass() {
        resetSimulateFlag();
        shutdownYBContainer();
    }

    @Test
    public void deleteStreamShouldFailFast() throws Exception {
        assertFailFastAfterRunning("t_del_stream", () -> {
            try (YBClient ybClient = TestHelper.getYbClient(getMasterAddress())) {
                String streamId = currentStreamId();
                ybClient.deleteCDCStream(Collections.singleton(streamId), false, true);
            }
        });
    }

    @Test
    public void dropTableShouldFailFast() throws Exception {
        assertFailFastAfterRunning("t_drop_table", () -> TestHelper.execute("DROP TABLE t_drop_table;"));
    }

    @Test
    public void removeTableFromStreamShouldFailFast() throws Exception {
        assertFailFastAfterRunning("t_remove_stream", () -> {
            try (YBClient ybClient = TestHelper.getYbClient(getMasterAddress())) {
                YBTable table = TestHelper.getYbTable(ybClient, "t_remove_stream");
                assertNotNull(table);
                runCommand(ybTool("yb-admin"),
                        "--master_addresses", getMasterAddress(),
                        "remove_user_table_from_change_data_stream",
                        currentStreamId(),
                        table.getTableId());
            }
        });
    }

    @Test
    public void nonexistentStreamIdShouldFailFast() throws Exception {
        TestHelper.execute("CREATE TABLE t_bad_stream (id INT PRIMARY KEY, name TEXT);");
        AtomicReference<Throwable> error = startConnector("public.t_bad_stream",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        waitUntilStopped(error, Duration.ofSeconds(25));
        assertConnectorNotRunning();
        assertNotNull(error.get());
        assertFalse(logsContain("will attempt retry"),
                "Nonexistent stream should not enter connector retry: " + joinedLogs());
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
    public void injectPeerNotLeaderIsRetriedBecauseYbClientDropsCdcCode() throws Exception {
        // TEST_SimulateError returns a Status, not CDCErrorPB. After yb-client exhausts
        // RPC attempts the connector sees NonRecoverableException and retries.
        assertRetryOnInjectedError("t_inj_not_leader", 2);
    }

    @Test
    public void injectLogFooterNotFoundIsRetriedBecauseYbClientDropsCdcCode() throws Exception {
        assertRetryOnInjectedError("t_inj_log_footer", 4);
    }

    private final AtomicReference<String> streamIdRef = new AtomicReference<>();

    private String currentStreamId() {
        return streamIdRef.get();
    }

    private void assertFailFastAfterRunning(String table, ThrowingRunnable trigger) throws Exception {
        TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);
        streamIdRef.set(streamId);
        AtomicReference<Throwable> error = startConnector("public." + table, streamId);
        waitUntilRunning(error);
        trigger.run();

        waitUntilStopped(error, Duration.ofSeconds(30));

        assertConnectorNotRunning();
        assertNotNull(error.get());
        assertTrue(logsContain("Failing fast") || isClassifiedFailFast(error.get()),
                "Expected fail-fast. logs=" + joinedLogs() + " error=" + flatten(error.get()));
        assertFalse(logsContain("will attempt retry"),
                "Should not enter connector retry loop: " + joinedLogs());
    }

    private void assertRetryOnInjectedError(String table, int simulateCode) throws Exception {
        TestHelper.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name TEXT);");
        String streamId = TestHelper.getNewDbStreamId("yugabyte", table);
        streamIdRef.set(streamId);
        AtomicReference<Throwable> error = startConnector("public." + table, streamId);
        waitUntilRunning(error);

        setSimulateFlag(simulateCode);

        Awaitility.await()
                .atMost(Duration.ofSeconds(20))
                .pollInterval(Duration.ofMillis(200))
                .until(() -> logsContain("will attempt retry"));

        assertTrue(engine.isRunning() || error.get() == null || !isClassifiedFailFast(error.get()),
                "Injected code " + simulateCode + " should be retried, not fail-fast. logs="
                        + joinedLogs() + " error=" + (error.get() == null ? "none" : flatten(error.get())));
        assertFalse(logsContain("Failing fast"),
                "Injected code " + simulateCode + " should not fail fast: " + joinedLogs());
    }

    private AtomicReference<Throwable> startConnector(String tableInclude, String streamId) throws Exception {
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(tableInclude, streamId)
                .with(YugabyteDBConnectorConfig.MAX_CONNECTOR_RETRIES, 2)
                .with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, RETRY_DELAY_MS)
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

    private void waitUntilRunning(AtomicReference<Throwable> error) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> {
                    if (error.get() != null) {
                        throw new AssertionError("Connector failed before becoming ready: "
                                + flatten(error.get()), error.get());
                    }
                    return engine.isRunning();
                });
    }

    private void waitUntilStopped(AtomicReference<Throwable> error, Duration timeout) {
        Awaitility.await()
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(200))
                .until(() -> !engine.isRunning() && error.get() != null);
    }

    private boolean isClassifiedFailFast(Throwable error) {
        CDCErrorException cdcError = YugabyteDBCdcErrorClassifier.findCdcError(error);
        if (cdcError != null) {
            return YugabyteDBCdcErrorClassifier.actionFor(
                    cdcError.getCDCError(),
                    YugabyteDBConnectorConfig.AutoCreateMode.DISABLED)
                    == YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST;
        }
        return YugabyteDBCdcErrorClassifier.isFatalMasterError(error);
    }

    private boolean logsContain(String token) {
        return logAppender.list.stream().anyMatch(event -> event.getFormattedMessage().contains(token));
    }

    private String joinedLogs() {
        return logAppender.list.stream()
                .map(ILoggingEvent::getFormattedMessage)
                .collect(Collectors.joining(" | "));
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

    private static boolean isYugabyteReachable() {
        try (YBClient client = TestHelper.getYbClient("127.0.0.1:7100")) {
            client.waitForMasterLeader(TimeUnit.SECONDS.toMillis(5));
            TestHelper.execute("SELECT 1;");
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    private static void setSimulateFlag(int value) {
        runCommand(ybTool("yb-ts-cli"),
                "--server_address=127.0.0.1:9100",
                "set_flag",
                "-force",
                "TEST_cdc_simulate_error_for_get_changes",
                String.valueOf(value));
    }

    private static void resetSimulateFlag() {
        try {
            // -1 is parsed as a gflag by yb-ts-cli; any unused code disables injection.
            setSimulateFlag(99);
        }
        catch (RuntimeException ignored) {
        }
    }

    private static String ybTool(String name) {
        String dir = System.getProperty("yb.bin.dir");
        if (dir == null || dir.isEmpty()) {
            dir = System.getenv("YB_BIN_DIR");
        }
        if (dir != null && !dir.isEmpty()) {
            return dir + "/" + name;
        }
        return name;
    }

    private static void runCommand(String... command) {
        try {
            Process process = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .start();
            String output;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                output = reader.lines().collect(Collectors.joining("\n"));
            }
            if (!process.waitFor(20, TimeUnit.SECONDS) || process.exitValue() != 0) {
                throw new RuntimeException("Command failed: " + List.of(command) + "\n" + output);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    private interface ThrowingRunnable {
        void run() throws Exception;
    }
}
