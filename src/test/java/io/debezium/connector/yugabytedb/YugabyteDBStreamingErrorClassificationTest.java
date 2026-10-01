package io.debezium.connector.yugabytedb;

import java.lang.reflect.Constructor;

import org.junit.jupiter.api.Test;
import org.yb.cdc.CdcService;
import org.yb.cdc.CdcService.CDCErrorPB.Code;
import org.yb.client.CDCErrorException;
import org.yb.client.YBException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the terminal classification of GC'd-WAL (CHECKPOINT_TOO_OLD) GetChanges
 * failures, see {@link YugabyteDBStreamingChangeEventSource#isCheckpointTooOldFailure(Throwable)}.
 */
public class YugabyteDBStreamingErrorClassificationTest {

    // Message text as surfaced by AsyncYBClient when the retry budget is exhausted, with the
    // last attempt's server error appended.
    private static final String GCED_WAL_MESSAGE =
            "Too many attempts: YRpc(method=GetChanges, service=yb.cdc.CDCService, "
            + "tablet=5850030e94824b1a9b163b42583c865d, attempt=68, maxAttempts=1800, "
            + "maxTimeoutMs=900000, elapsedTimeMs=879055). "
            + "Server[7c590f5172fa471bb897c8df6596ef9f] NOT_FOUND[code 1]: "
            + "The logs from index 69914790 have been garbage collected and cannot be read : "
            + "Failed to read ops 69914791..74395351: op index 69914791 has been already GCed "
            + "from log index cache, max_gced_op_index: 71300580";

    @Test
    public void shouldClassifyCheckpointTooOldCdcErrorAsTerminal() throws Exception {
        assertTrue(YugabyteDBStreamingChangeEventSource.isCheckpointTooOldFailure(
                cdcErrorException("checkpoint too old", Code.CHECKPOINT_TOO_OLD)));
    }

    @Test
    public void shouldClassifyCheckpointTooOldCdcErrorOnCauseChainAsTerminal() throws Exception {
        // AsyncYBClient chains the last attempt's CDCErrorException as the cause of the
        // NonRecoverableException it surfaces once the retry budget is exhausted.
        Exception wrapped = new RuntimeException("Too many attempts",
                cdcErrorException("checkpoint too old", Code.CHECKPOINT_TOO_OLD));
        assertTrue(YugabyteDBStreamingChangeEventSource.isCheckpointTooOldFailure(wrapped));
    }

    @Test
    public void shouldClassifyGcedWalMessageWithoutCauseAsTerminal() {
        // Some retry-exhaustion paths surface no cause, leaving only the message text.
        assertTrue(YugabyteDBStreamingChangeEventSource.isCheckpointTooOldFailure(
                ybException(GCED_WAL_MESSAGE)));
    }

    @Test
    public void shouldNotClassifyOtherCdcErrorsAsTerminal() throws Exception {
        assertFalse(YugabyteDBStreamingChangeEventSource.isCheckpointTooOldFailure(
                cdcErrorException("tablet split detected", Code.TABLET_SPLIT)));
        assertFalse(YugabyteDBStreamingChangeEventSource.isCheckpointTooOldFailure(
                cdcErrorException("leader not ready", Code.LEADER_NOT_READY)));
    }

    @Test
    public void shouldNotClassifyOtherYbClientFailuresAsTerminal() {
        assertFalse(YugabyteDBStreamingChangeEventSource.isCheckpointTooOldFailure(
                ybException("Too many attempts: YRpc(method=GetChanges, ...). TimedOut[code 15]")));
        assertFalse(YugabyteDBStreamingChangeEventSource.isCheckpointTooOldFailure(
                ybException(null)));
    }

    @Test
    public void shouldNotClassifyNonYbClientExceptionsByMessageText() {
        // The message backstop only applies to yb-client exceptions.
        assertFalse(YugabyteDBStreamingChangeEventSource.isCheckpointTooOldFailure(
                new RuntimeException(GCED_WAL_MESSAGE)));
    }

    private static YBException ybException(String message) {
        return new YBException(message) {
        };
    }

    private static CDCErrorException cdcErrorException(String message, Code code) throws Exception {
        CdcService.CDCErrorPB error = CdcService.CDCErrorPB.newBuilder().setCode(code).buildPartial();
        Constructor<CDCErrorException> constructor =
                CDCErrorException.class.getDeclaredConstructor(String.class, CdcService.CDCErrorPB.class);
        constructor.setAccessible(true);
        return constructor.newInstance(message, error);
    }
}
