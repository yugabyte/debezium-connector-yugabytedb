package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.yb.client.GetDBStreamInfoResponse;
import org.yb.master.MasterReplicationOuterClass.GetCDCDBStreamInfoResponsePB.TableInfo;

import com.google.protobuf.ByteString;

import io.debezium.DebeziumException;

/**
 * Unit tests for {@link YBClientUtils#throwIfTableNotInStream}, which decides whether a
 * {@code TABLE_NOT_FOUND} from {@code GetTabletListToPollForCDC} is retried or fails fast.
 */
public class YBClientUtilsTableNotFoundTest {

    private static final String STREAM_ID = "stream-1";

    @Test
    public void tableStillInStreamShouldRetry() {
        assertDoesNotThrow(() -> YBClientUtils.throwIfTableNotInStream(
                streamWith("table-1", "table-2"), "table-1", STREAM_ID, new Exception("TABLE_NOT_FOUND")));
    }

    @Test
    public void tableGoneFromStreamShouldFailFast() {
        Exception cause = new Exception("TABLE_NOT_FOUND");
        DebeziumException ex = assertThrows(DebeziumException.class,
                () -> YBClientUtils.throwIfTableNotInStream(streamWith("table-2"), "table-1", STREAM_ID, cause));
        assertTrue(ex.getMessage().contains("is no longer part of stream " + STREAM_ID), ex.getMessage());
        assertSame(cause, ex.getCause(), "Expected original error as cause");
    }

    private static GetDBStreamInfoResponse streamWith(String... tableIds) {
        List<TableInfo> tables = Arrays.stream(tableIds)
                .map(id -> TableInfo.newBuilder()
                        .setTableId(ByteString.copyFromUtf8(id))
                        .setStreamId(ByteString.copyFromUtf8(STREAM_ID))
                        .build())
                .collect(Collectors.toList());
        return new GetDBStreamInfoResponse(0L, "ts-uuid", tables, "ns-uuid");
    }
}
