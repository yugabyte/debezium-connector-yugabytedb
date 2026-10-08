package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.yb.client.CDCErrorException;
import org.yb.client.YBClient;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

/**
 * Integration tests for connector fail-fast on non-retriable CDC errors while streaming.
 * Requires a local yugabyted instance, same as {@link YugabyteDBTabletSplitTest}.
 * <p>
 * Invalid {@code database.stream.id} at startup is not covered here: config {@code validate()} does
 * not check stream existence, and the task typically fails only after the yb-client exhausts
 * {@code max.rpc.retry.attempts} (15+ minutes with defaults). That path is covered by
 * {@link YugabyteDBCdcErrorClassifierTest} for classified CDC responses.
 */
public class YugabyteDBCdcFailFastIT extends YugabytedTestBase {

    /** Short delay so delete-stream path finishes within Awaitility bounds when retries run. */
    private static final long FAIL_FAST_IT_RETRY_DELAY_MS = 500L;

    private ListAppender<ILoggingEvent> logAppender;

    @BeforeAll
    public static void beforeClass() throws SQLException {
        initializeYBContainer();
        TestHelper.dropAllSchemas();
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
        shutdownYBContainer();
    }

    @Test
    public void shouldFailFastOnNonRetriableCdcError() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT);");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
        AtomicReference<Throwable> completionError = startConnector("public.t1", dbStreamId,
                FAIL_FAST_IT_RETRY_DELAY_MS);

        waitUntilRunning(completionError);

        TestHelper.execute("INSERT INTO t1 VALUES (1, 'fail-fast');");
        try (YBClient ybClient = TestHelper.getYbClient(getMasterAddress())) {
            ybClient.deleteCDCStream(Collections.singleton(dbStreamId), false, true);
        }

        waitUntilStopped(completionError, Duration.ofSeconds(60));

        assertConnectorNotRunning();
        assertNotNull(completionError.get(), "Connector should complete with a non-retriable CDC error");
        assertTrue(logsContain("Failing fast") || isClassifiedFailFast(completionError.get()),
                "Expected fail-fast. logs=" + joinedLogs() + " error=" + flatten(completionError.get()));
        assertFalse(logsContain("will attempt retry"),
                "Deleted stream should not enter connector retry loop: " + joinedLogs());
    }

    private AtomicReference<Throwable> startConnector(
            String tableInclude, String streamId, long retryDelayMs) throws Exception {
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(tableInclude, streamId)
                .with(YugabyteDBConnectorConfig.MAX_CONNECTOR_RETRIES, 2)
                .with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, retryDelayMs)
                .with(YugabyteDBConnectorConfig.MAX_RPC_RETRY_ATTEMPTS, 5)
                .with(YugabyteDBConnectorConfig.RPC_RETRY_SLEEP_TIME, 100);

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
                .atMost(Duration.ofSeconds(60))
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
                .pollInterval(Duration.ofMillis(250))
                .until(() -> !engine.isRunning() && error.get() != null);
    }

    private boolean isClassifiedFailFast(Throwable error) {
        CDCErrorException cdcError = YugabyteDBCdcErrorClassifier.findCdcError(error);
        if (cdcError == null) {
            return false;
        }
        return YugabyteDBCdcErrorClassifier.actionFor(cdcError.getCDCError())
                == YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST;
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
}
