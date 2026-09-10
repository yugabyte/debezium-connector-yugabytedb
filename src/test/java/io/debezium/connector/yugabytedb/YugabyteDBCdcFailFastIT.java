package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.yb.client.CDCErrorException;
import org.yb.client.YBClient;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

/**
 * Integration tests for connector fail-fast on non-retriable CDC errors.
 * Requires a local yugabyted instance, same as {@link YugabyteDBTabletSplitTest}.
 */
public class YugabyteDBCdcFailFastIT extends YugabytedTestBase {

    private static final long CONNECTOR_RETRY_DELAY_MS = 20_000L;

    @BeforeAll
    public static void beforeClass() throws SQLException {
        initializeYBContainer();
        TestHelper.dropAllSchemas();
    }

    @BeforeEach
    public void before() {
        initializeConnectorTestFramework();
    }

    @AfterEach
    public void after() throws Exception {
        stopConnector();
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
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId)
                .with(YugabyteDBConnectorConfig.MAX_CONNECTOR_RETRIES, 2)
                .with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, CONNECTOR_RETRY_DELAY_MS)
                .with(YugabyteDBConnectorConfig.MAX_RPC_RETRY_ATTEMPTS, 5)
                .with(YugabyteDBConnectorConfig.RPC_RETRY_SLEEP_TIME, 100);

        AtomicReference<Throwable> completionError = new AtomicReference<>();
        startEngine(configBuilder, (success, message, error) -> {
            completionError.set(error);
            if (!success) {
                LOGGER.info("Connector completed unsuccessfully: {}", message, error);
            }
        });

        // Either the server already returns a fatal CDC error, or streaming starts and we
        // delete the stream to produce one. Do not use awaitUntilConnectorIsReady(): that
        // waits 15s even when the task already failed.
        Awaitility.await()
                .atMost(Duration.ofSeconds(25))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> completionError.get() != null || engine.isRunning());

        if (completionError.get() == null && engine.isRunning()) {
            TestHelper.execute("INSERT INTO t1 VALUES (1, 'fail-fast');");
            try (YBClient ybClient = TestHelper.getYbClient(getMasterAddress())) {
                ybClient.deleteCDCStream(Collections.singleton(dbStreamId), false, true);
            }
        }

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> !engine.isRunning() && completionError.get() != null);

        assertConnectorNotRunning();
        assertNotNull(completionError.get(), "Connector should complete with a non-retriable CDC error");

        CDCErrorException cdcError = YugabyteDBCdcErrorClassifier.findCdcError(completionError.get());
        if (cdcError != null) {
            assertTrue(
                    YugabyteDBCdcErrorClassifier.actionFor(
                            cdcError.getCDCError(),
                            YugabyteDBConnectorConfig.AutoCreateMode.DISABLED)
                            == YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                    "CDC error should classify as FAIL_FAST, got code="
                            + cdcError.getCDCError().getCode());
        } else {
            String message = flattenMessages(completionError.get());
            assertTrue(message.contains("stream") || message.contains("checkpoint"),
                    "Expected a stream or checkpoint failure without connector retries, got: "
                            + completionError.get());
        }
    }

    @Test
    public void shouldFailWhenConfiguredWithNonexistentStreamId() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT);");

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(
                "public.t1",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .with(YugabyteDBConnectorConfig.MAX_CONNECTOR_RETRIES, 2)
                .with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, CONNECTOR_RETRY_DELAY_MS)
                .with(YugabyteDBConnectorConfig.MAX_RPC_RETRY_ATTEMPTS, 5)
                .with(YugabyteDBConnectorConfig.RPC_RETRY_SLEEP_TIME, 100);

        AtomicReference<Boolean> succeeded = new AtomicReference<>();
        AtomicReference<Throwable> completionError = new AtomicReference<>();
        startEngine(configBuilder, (success, message, error) -> {
            succeeded.set(success);
            completionError.set(error);
        });

        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> succeeded.get() != null || !engine.isRunning());

        assertConnectorNotRunning();
        assertFalse(Boolean.TRUE.equals(succeeded.get()),
                "Connector should not start successfully with a nonexistent stream id");
        assertNotNull(completionError.get());
    }

    private static String flattenMessages(Throwable error) {
        StringBuilder builder = new StringBuilder();
        Throwable current = error;
        while (current != null) {
            builder.append(String.valueOf(current.getMessage()).toLowerCase());
            builder.append(' ');
            current = current.getCause();
        }
        return builder.toString();
    }
}
