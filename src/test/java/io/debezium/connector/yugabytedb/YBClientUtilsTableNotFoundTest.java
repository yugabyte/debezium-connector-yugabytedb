package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.yb.cdc.CdcService.CDCErrorPB;
import org.yb.client.AsyncYBClient;
import org.yb.client.CDCErrorException;
import org.yb.client.GetDBStreamInfoResponse;
import org.yb.client.GetTabletListToPollForCDCResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * Unit tests for how {@link YBClientUtils#getTabletListToPollForCDCWithRetry} handles
 * {@code TABLE_NOT_FOUND}. They run the real retry loop against a fake {@link YBClient}, so no
 * YugabyteDB cluster is needed.
 */
public class YBClientUtilsTableNotFoundTest {

    private static final String STREAM_ID = "stream-1";
    private static final String TABLE_ID = "table-1";

    /** {@code TABLE_NOT_FOUND} with the table absent from the stream fails fast, with no retry. */
    @Test
    public void tableNotFoundWithTableAbsentFromStreamShouldFailFast() throws Exception {
        CDCErrorException tableNotFound = tableNotFoundError();
        FakeClient client = new FakeClient(tableNotFound, null);

        DebeziumException e = assertThrows(DebeziumException.class,
                () -> YBClientUtils.getTabletListToPollForCDCWithRetry(client, null, TABLE_ID, config(5)));

        assertTrue(e.getMessage().contains("is no longer part of stream"), e.getMessage());
        assertSame(tableNotFound, e.getCause());
        assertEquals(1, client.tabletListCalls.get(), "must not retry the tablet list call");
        assertEquals(1, client.streamInfoCalls.get());
    }

    /**
     * If the {@code getDBStreamInfo} check itself fails transiently, we fall back to the normal
     * retry handling. With no retries left, that rethrows the original tablet-list error.
     */
    @Test
    public void streamInfoFailureFallsBackToNormalRetryHandling() throws Exception {
        CDCErrorException tableNotFound = tableNotFoundError();
        FakeClient client = new FakeClient(tableNotFound, new RuntimeException("master unreachable"));

        Exception e = assertThrows(Exception.class,
                () -> YBClientUtils.getTabletListToPollForCDCWithRetry(client, null, TABLE_ID, config(0)));

        assertSame(tableNotFound, e);
        assertEquals(1, client.streamInfoCalls.get());
    }

    /**
     * Building a {@link YugabyteDBConnectorConfig} asks the master whether the stream is YSQL and
     * caches the answer per stream ID. Seed that cache so these tests never contact a server.
     */
    @BeforeAll
    @SuppressWarnings("unchecked")
    public static void seedIsYsqlCache() throws Exception {
        Field cacheField = YBClientUtils.class.getDeclaredField("isYSQLCache");
        cacheField.setAccessible(true);
        ((Map<String, Boolean>) cacheField.get(null)).put(STREAM_ID, true);
    }

    private static YugabyteDBConnectorConfig config(int maxConnectorRetries) {
        return new YugabyteDBConnectorConfig(Configuration.create()
                .with(RelationalDatabaseConnectorConfig.SERVER_NAME, "test-server")
                .with(YugabyteDBConnectorConfig.STREAM_ID, STREAM_ID)
                .with(YugabyteDBConnectorConfig.MAX_CONNECTOR_RETRIES, maxConnectorRetries)
                .with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, 1L)
                .build());
    }

    private static CDCErrorException tableNotFoundError() throws Exception {
        Constructor<CDCErrorException> constructor =
                CDCErrorException.class.getDeclaredConstructor(String.class, CDCErrorPB.class);
        constructor.setAccessible(true);
        return constructor.newInstance("tserver-1",
                CDCErrorPB.newBuilder().setCode(CDCErrorPB.Code.TABLE_NOT_FOUND).build());
    }

    /**
     * A {@link YBClient} whose tablet-list call always throws {@code TABLE_NOT_FOUND}, and whose
     * {@code getDBStreamInfo} either returns a stream with no tables or throws.
     */
    private static class FakeClient extends YBClient {
        final AtomicInteger tabletListCalls = new AtomicInteger();
        final AtomicInteger streamInfoCalls = new AtomicInteger();
        private final Exception tabletListError;
        private final Exception streamInfoError;

        FakeClient(Exception tabletListError, Exception streamInfoError) {
            // The async client is only built, never connected, so no master is needed.
            super(new AsyncYBClient.AsyncYBClientBuilder("127.0.0.1:1").build());
            this.tabletListError = tabletListError;
            this.streamInfoError = streamInfoError;
        }

        @Override
        public GetTabletListToPollForCDCResponse getTabletListToPollForCdc(
                YBTable table, String streamId, String tableId) throws Exception {
            tabletListCalls.incrementAndGet();
            throw tabletListError;
        }

        @Override
        public GetDBStreamInfoResponse getDBStreamInfo(String streamId) throws Exception {
            streamInfoCalls.incrementAndGet();
            if (streamInfoError != null) {
                throw streamInfoError;
            }
            return new GetDBStreamInfoResponse(0L, "ts-uuid", Collections.emptyList(), "ns-uuid");
        }
    }
}
