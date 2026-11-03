package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.yb.WireProtocol.AppStatusPB;
import org.yb.WireProtocol.AppStatusPB.ErrorCode;
import org.yb.cdc.CdcService.CDCErrorPB;
import org.yb.cdc.CdcService.CDCErrorPB.Code;
import org.yb.client.CDCErrorException;
import org.yb.client.MasterErrorException;
import org.yb.client.NonRecoverableException;
import org.yb.master.MasterTypes.MasterErrorPB;

/**
 * Unit tests for {@link YugabyteDBCdcErrorClassifier}. These tests construct CDC proto errors
 * in-memory and do not start YugabyteDB or the connector.
 */
public class YugabyteDBCdcErrorClassifierTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("classificationCases")
    public void classification(String name, YugabyteDBCdcErrorClassifier.CdcErrorAction expected, CDCErrorPB error) {
        assertEquals(expected, classify(error), name);
    }

    @Test
    public void tableNotFoundWithNotFoundUnderStreamMessageShouldRetry() throws Exception {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.TABLE_NOT_FOUND, ErrorCode.NOT_FOUND,
                        "Table 00000000-0000-0000-0000-000000000000 not found under stream")));
        assertFalse(YugabyteDBCdcErrorClassifier.isFailFast(
                cdcException(error(Code.TABLE_NOT_FOUND, ErrorCode.NOT_FOUND,
                        "Table abc not found under stream"))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("permanentCdcFailures")
    public void permanentCdcFailureShouldFailFast(String name, Code code, ErrorCode status, String message)
            throws Exception {
        CDCErrorPB cdcError = error(code, status, message);

        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST, classify(cdcError), name);

        CDCErrorException cdcException = cdcException(cdcError);
        Exception wrapped = new Exception("GetChanges failed", cdcException);
        assertSame(cdcException, YugabyteDBCdcErrorClassifier.findCdcError(wrapped), name);
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                YugabyteDBCdcErrorClassifier.actionFor(cdcException.getCDCError()), name);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("retryableLookalikes")
    public void similarCdcMessageShouldRetry(String name, String message) {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR, message)), name);
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.UNKNOWN_ERROR, ErrorCode.NOT_FOUND, message)), name);
    }

    @Test
    public void findCdcErrorReturnsNullWhenAbsent() {
        assertNull(YugabyteDBCdcErrorClassifier.findCdcError(new IllegalStateException("leader moved")));
    }

    @Test
    public void isFailFastFalseForNonCdcException() {
        assertFalse(YugabyteDBCdcErrorClassifier.isFailFast(new IllegalStateException("leader moved")));
    }

    @Test
    public void isFailFastTrueWhenDroppedTableObjectNotFoundOnMaster() throws Exception {
        MasterErrorException masterError = masterErrorException(
                MasterErrorPB.Code.OBJECT_NOT_FOUND,
                "The object 'yugabyte.public.t_drop_table' does not exist");
        assertTrue(YugabyteDBCdcErrorClassifier.isFailFast(masterError));
        assertTrue(YugabyteDBCdcErrorClassifier.isFailFast(
                new RuntimeException("bootstrap failed", masterError)));
    }

    @Test
    public void isFailFastFalseForMasterErrorOtherThanObjectNotFound() throws Exception {
        MasterErrorException masterError = masterErrorException(
                MasterErrorPB.Code.NOT_THE_LEADER,
                "not the leader");
        assertFalse(YugabyteDBCdcErrorClassifier.isFailFast(masterError));
    }

    @Test
    public void isFailFastFalseForRpcExhaustedWithoutFatalText() {
        RuntimeException error = new RuntimeException(
                "Too many attempts: YRpc(method=GetChanges). "
                        + "LEADER_NOT_READY_TO_SERVE[code 23]: Simulated error");
        assertFalse(YugabyteDBCdcErrorClassifier.isFailFast(error));
    }

    @Test
    public void isFailFastFalseWhenGetTabletListExhaustsWithoutStatusText() throws Exception {
        NonRecoverableException error = nonRecoverable(
                "Too many attempts: YRpc(method=GetTabletListToPollForCDC, service=yb.cdc.CDCService, "
                        + "tablet=abc, attempt=5, maxAttempts=5, maxTimeoutMs=900000, elapsedTimeMs=704)");
        assertFalse(YugabyteDBCdcErrorClassifier.isFailFast(error));
    }

    @Test
    public void isFailFastFalseWhenGetChangesExhaustsWithoutStatusText() throws Exception {
        NonRecoverableException error = nonRecoverable(
                "Too many attempts: YRpc(method=GetChanges, service=yb.cdc.CDCService, tablet=abc, "
                        + "attempt=2, maxAttempts=2, maxTimeoutMs=900000, elapsedTimeMs=107)");
        assertFalse(YugabyteDBCdcErrorClassifier.isFailFast(error));
    }

    @Test
    public void isTabletSplitTrueForTabletSplitCode() {
        assertTrue(YugabyteDBCdcErrorClassifier.isTabletSplit(error(Code.TABLET_SPLIT)));
    }

    @Test
    public void isTabletSplitFalseForInvalidRequestWithTabletSplitStatus() {
        assertFalse(YugabyteDBCdcErrorClassifier.isTabletSplit(
                error(Code.INVALID_REQUEST, ErrorCode.TABLET_SPLIT, "tablet split detected")));
    }

    @Test
    public void isTabletSplitFalseForBareInvalidRequest() {
        assertFalse(YugabyteDBCdcErrorClassifier.isTabletSplit(error(Code.INVALID_REQUEST)));
    }

    @Test
    public void isTabletSplitFalseForPermanentInvalidRequest() {
        assertFalse(YugabyteDBCdcErrorClassifier.isTabletSplit(
                error(Code.INVALID_REQUEST, ErrorCode.INVALID_ARGUMENT, "invalid stream id")));
    }

    private static Stream<Arguments> classificationCases() {
        YugabyteDBCdcErrorClassifier.CdcErrorAction handle = YugabyteDBCdcErrorClassifier.CdcErrorAction.HANDLE_IN_STREAM;
        YugabyteDBCdcErrorClassifier.CdcErrorAction fail = YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST;
        YugabyteDBCdcErrorClassifier.CdcErrorAction retry = YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY;
        return Stream.of(
                classificationCase("tablet split handled in stream", handle, error(Code.TABLET_SPLIT)),
                classificationCase("checkpoint too old fails fast", fail, error(Code.CHECKPOINT_TOO_OLD)),
                classificationCase("subscriber not found retries because the code is not listed", retry,
                        error(Code.SUBSCRIBER_NOT_FOUND)),
                classificationCase("operation disallowed fails fast", fail, error(Code.OPERATION_DISALLOWED)),
                classificationCase("auto flags mismatch retries because the code is not listed", retry,
                        error(Code.AUTO_FLAGS_CONFIG_VERSION_MISMATCH)),
                classificationCase("table not found retries", retry, error(Code.TABLE_NOT_FOUND)),
                classificationCase("bare INVALID_REQUEST is not a tablet split", retry, error(Code.INVALID_REQUEST)),
                classificationCase("RUNTIME_ERROR is permanent, message text is not a split", fail,
                        error(Code.INVALID_REQUEST, ErrorCode.RUNTIME_ERROR, "unexpected split of request")),
                classificationCase("tablet not found retries", retry, error(Code.TABLET_NOT_FOUND)),
                classificationCase("leader not ready retries", retry, error(Code.LEADER_NOT_READY)),
                classificationCase("invalid stream id fails fast", fail,
                        error(Code.INVALID_REQUEST, ErrorCode.INVALID_ARGUMENT, "invalid stream id")),
                classificationCase("deleted stream fails fast", fail,
                        error(Code.INVALID_REQUEST, ErrorCode.DELETED, "deleted stream")),
                classificationCase("NOT_FOUND is not fatal by itself", retry,
                        error(Code.INVALID_REQUEST, ErrorCode.NOT_FOUND, "table is not part of publication")),
                classificationCase("invalid checkpoint fails fast", fail,
                        error(Code.INVALID_REQUEST, ErrorCode.INVALID_ARGUMENT, "invalid checkpoint")),
                classificationCase("unsupported request fails fast", fail,
                        error(Code.INVALID_REQUEST, ErrorCode.NOT_SUPPORTED, "unsupported request")),
                classificationCase("permission denied fails fast", fail,
                        error(Code.INVALID_REQUEST, ErrorCode.NOT_AUTHORIZED, "permission denied")),
                classificationCase("INVALID_REQUEST with AppStatus TABLET_SPLIT is not a split", retry,
                        error(Code.INVALID_REQUEST, ErrorCode.TABLET_SPLIT, "tablet split detected")),
                classificationCase("RUNTIME_ERROR with a split message is not a split", fail,
                        error(Code.INVALID_REQUEST, ErrorCode.RUNTIME_ERROR, "tablet was split")),
                classificationCase("transient INVALID_REQUEST retries", retry,
                        error(Code.INVALID_REQUEST, ErrorCode.SERVICE_UNAVAILABLE, "leader unavailable")),
                classificationCase("GetChanges membership miss retries", retry,
                        error(Code.INVALID_REQUEST, ErrorCode.INVALID_ARGUMENT,
                                "Tablet ID xyz is not part of stream ID abc")),
                classificationCase("UNKNOWN_ERROR with AppStatus TABLET_SPLIT retries", retry,
                        error(Code.UNKNOWN_ERROR, ErrorCode.TABLET_SPLIT, "tablet split")),
                classificationCase("UNKNOWN_ERROR split message with RUNTIME_ERROR fails fast", fail,
                        error(Code.UNKNOWN_ERROR, ErrorCode.RUNTIME_ERROR, "tablet was split")),
                classificationCase("missing CDC stream on UNKNOWN_ERROR fails fast", fail,
                        error(Code.UNKNOWN_ERROR, ErrorCode.NOT_FOUND,
                                "Could not find CDC stream: stream_id: \"abc\"")),
                classificationCase("expired for tablet matches regardless of case", fail,
                        error(Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR,
                                "STREAM ID ABC IS EXPIRED FOR TABLET ID XYZ")),
                classificationCase("UNKNOWN_ERROR with RUNTIME_ERROR fails fast", fail,
                        error(Code.UNKNOWN_ERROR, ErrorCode.RUNTIME_ERROR, "internal failure")),
                classificationCase("UNKNOWN_ERROR timed out retries", retry,
                        error(Code.UNKNOWN_ERROR, ErrorCode.TIMED_OUT, "rpc timed out")),
                classificationCase("UNKNOWN_ERROR without status retries", retry, error(Code.UNKNOWN_ERROR)));
    }

    private static Arguments classificationCase(String name, YugabyteDBCdcErrorClassifier.CdcErrorAction action, CDCErrorPB error) {
        return Arguments.of(name, action, error);
    }

    private static Stream<Arguments> permanentCdcFailures() {
        String walGarbageCollected = "The logs from index 10 have been garbage collected and cannot be read: "
                + "op index 10 has been already GCed";
        String gcedIntents = "CDCSDK Trying to fetch already GCed intents for transaction 123";
        String beforeImage = "Failed to get the beforeimage for tablet_id: abc due to compaction";
        String missingWal = "Unexpectedly did not find a WAL message corresponding to from_op_id: 1.2";
        String unpolled = "Stream ID abc unpolled for too long for Tablet ID xyz";
        String expiredForTablet = "Stream ID abc is expired for Tablet ID xyz";
        String streamMissing = "Could not find CDC stream: stream_id: \"abc\"";
        String unsupportedReplicaIdentity = "Unknown or unsupported replica identity for table t1";
        String replicaIdentityNotFound = "Replica Identity not found for table t1";

        return Stream.of(
                failure("wal garbage collected", Code.CHECKPOINT_TOO_OLD, ErrorCode.NOT_FOUND, walGarbageCollected),
                failure("wal garbage collected reported as unknown error", Code.UNKNOWN_ERROR, ErrorCode.NOT_FOUND, walGarbageCollected),
                failure("wal garbage collected reported as internal error", Code.INTERNAL_ERROR, ErrorCode.NOT_FOUND, walGarbageCollected),
                failure("already gced intents as unknown error", Code.UNKNOWN_ERROR, ErrorCode.INTERNAL_ERROR, gcedIntents),
                failure("already gced intents as internal error", Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR, gcedIntents),
                failure("before image lost to compaction as unknown error", Code.UNKNOWN_ERROR, ErrorCode.INTERNAL_ERROR, beforeImage),
                failure("before image lost to compaction as internal error", Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR, beforeImage),
                failure("missing wal message as unknown error", Code.UNKNOWN_ERROR, ErrorCode.INTERNAL_ERROR, missingWal),
                failure("missing wal message as internal error", Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR, missingWal),
                failure("tablet unpolled too long", Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR, unpolled),
                failure("stream expired for tablet as internal error", Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR,
                        expiredForTablet),
                failure("stream expired for tablet as unknown error", Code.UNKNOWN_ERROR, ErrorCode.INTERNAL_ERROR,
                        expiredForTablet),
                failure("cdc stream not found on invalid request", Code.INVALID_REQUEST, ErrorCode.NOT_FOUND, streamMissing),
                failure("cdc stream not found on internal error", Code.INTERNAL_ERROR, ErrorCode.NOT_FOUND, streamMissing),
                failure("unsupported replica identity as unknown error", Code.UNKNOWN_ERROR, ErrorCode.INTERNAL_ERROR,
                        unsupportedReplicaIdentity),
                failure("unsupported replica identity as internal error", Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR,
                        unsupportedReplicaIdentity),
                failure("replica identity not found as unknown error", Code.UNKNOWN_ERROR, ErrorCode.INTERNAL_ERROR,
                        replicaIdentityNotFound),
                failure("replica identity not found as internal error", Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR,
                        replicaIdentityNotFound));
    }

    private static Stream<Arguments> retryableLookalikes() {
        return Stream.of(
                Arguments.of("unrelated internal error", "temporary leader change"),
                Arguments.of("gced without intents", "already GCed"),
                Arguments.of("before image without compaction", "Failed to get the beforeimage for tablet_id: abc"),
                Arguments.of("from_op_id without a missing wal record", "from_op_id: 1.2 is not yet available"),
                Arguments.of("expired without a tablet", "stream expired"),
                Arguments.of("unpolled without the timeout phrase", "tablet was not polled"),
                Arguments.of("missing stream without cdc", "Could not find stream: stream_id: \"abc\""),
                Arguments.of("tablet not part of a publication", "Tablet ID xyz is not part of publication abc"),
                Arguments.of("tablet not part of stream membership", "Tablet ID xyz is not part of stream ID abc"));
    }

    private static Arguments failure(String name, Code code, ErrorCode status, String message) {
        return Arguments.of(name, code, status, message);
    }

    private static CDCErrorException cdcException(CDCErrorPB error) throws Exception {
        Constructor<CDCErrorException> constructor =
                CDCErrorException.class.getDeclaredConstructor(String.class, CDCErrorPB.class);
        constructor.setAccessible(true);
        return constructor.newInstance("tserver-1", error);
    }

    private static NonRecoverableException nonRecoverable(String message) throws Exception {
        Constructor<NonRecoverableException> constructor =
                NonRecoverableException.class.getDeclaredConstructor(String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(message);
    }

    private static MasterErrorException masterErrorException(MasterErrorPB.Code code, String message)
            throws Exception {
        MasterErrorPB error = MasterErrorPB.newBuilder()
                .setCode(code)
                .setStatus(AppStatusPB.newBuilder()
                        .setCode(ErrorCode.NOT_FOUND)
                        .setMessage(message)
                        .build())
                .build();
        Constructor<MasterErrorException> constructor =
                MasterErrorException.class.getDeclaredConstructor(String.class, MasterErrorPB.class);
        constructor.setAccessible(true);
        return constructor.newInstance("master-1", error);
    }

    private static YugabyteDBCdcErrorClassifier.CdcErrorAction classify(CDCErrorPB error) {
        return YugabyteDBCdcErrorClassifier.actionFor(error);
    }

    private static CDCErrorPB error(Code code) {
        return CDCErrorPB.newBuilder().setCode(code).build();
    }

    private static CDCErrorPB error(Code code, ErrorCode appStatusCode, String message) {
        return CDCErrorPB.newBuilder()
                .setCode(code)
                .setStatus(AppStatusPB.newBuilder()
                        .setCode(appStatusCode)
                        .setMessage(message)
                        .build())
                .build();
    }
}
