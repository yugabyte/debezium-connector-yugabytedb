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

/**
 * Unit tests for {@link YugabyteDBCdcErrorClassifier}. These tests construct CDC proto errors
 * in-memory and do not start YugabyteDB or the connector.
 */
public class YugabyteDBCdcErrorClassifierTest {

    private static final YugabyteDBConnectorConfig.AutoCreateMode DISABLED =
            YugabyteDBConnectorConfig.AutoCreateMode.DISABLED;
    private static final YugabyteDBConnectorConfig.AutoCreateMode FILTERED =
            YugabyteDBConnectorConfig.AutoCreateMode.FILTERED;
    private static final YugabyteDBConnectorConfig.AutoCreateMode ALL_TABLES =
            YugabyteDBConnectorConfig.AutoCreateMode.ALL_TABLES;

    @Test
    public void tabletSplitShouldBeHandledInStream() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.HANDLE_IN_STREAM,
                classify(error(Code.TABLET_SPLIT), DISABLED));
    }

    @Test
    public void checkpointTooOldShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.CHECKPOINT_TOO_OLD), DISABLED));
    }

    @Test
    public void subscriberNotFoundShouldRetry() {
        // Not a CDC-stream failure. The switch does not list this code, so it retries.
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.SUBSCRIBER_NOT_FOUND), DISABLED));
    }

    @Test
    public void operationDisallowedShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.OPERATION_DISALLOWED), DISABLED));
    }

    @Test
    public void autoFlagsMismatchShouldRetry() {
        // Not a CDC-stream failure. The switch does not list this code, so it retries.
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.AUTO_FLAGS_CONFIG_VERSION_MISMATCH), DISABLED));
    }

    @Test
    public void tableNotFoundShouldRetryForFilteredPublication() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.TABLE_NOT_FOUND), FILTERED));
    }

    @Test
    public void tableNotFoundShouldRetryForAllTablesPublication() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.TABLE_NOT_FOUND), ALL_TABLES));
    }

    @Test
    public void tableNotFoundShouldRetryWhenNotUsingPublication() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.TABLE_NOT_FOUND), DISABLED));
    }

    @Test
    public void invalidRequestWithoutStatusShouldRetry() {
        // Align with main: bare INVALID_REQUEST is not assumed to be a tablet split.
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.INVALID_REQUEST), DISABLED));
    }

    @Test
    public void invalidRequestWithGenericSplitWordShouldFailFast() {
        // Message text alone is not enough; RUNTIME_ERROR AppStatus is permanent.
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.INVALID_REQUEST, ErrorCode.RUNTIME_ERROR, "unexpected split of request"), DISABLED));
    }

    @Test
    public void tabletNotFoundShouldRetry() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.TABLET_NOT_FOUND), DISABLED));
    }

    @Test
    public void leaderNotReadyShouldRetry() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.LEADER_NOT_READY), DISABLED));
    }

    @Test
    public void invalidRequestWithInvalidStreamIdShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.INVALID_REQUEST, ErrorCode.INVALID_ARGUMENT, "invalid stream id"), DISABLED));
    }

    @Test
    public void invalidRequestWithDeletedStreamShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.INVALID_REQUEST, ErrorCode.DELETED, "deleted stream"), DISABLED));
    }

    @Test
    public void invalidRequestWithIncorrectTabletIdShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.INVALID_REQUEST, ErrorCode.INVALID_ARGUMENT, "incorrect tablet id"), DISABLED));
    }

    @Test
    public void invalidRequestWithTableNotInPublicationAndNotFoundStatusShouldRetry() {
        // AppStatus NOT_FOUND is not fatal by itself (leader/tablet/footer also use it).
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.INVALID_REQUEST, ErrorCode.NOT_FOUND, "table is not part of publication"), DISABLED));
    }

    @Test
    public void invalidRequestWithBadCheckpointShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.INVALID_REQUEST, ErrorCode.INVALID_ARGUMENT, "invalid checkpoint"), DISABLED));
    }

    @Test
    public void invalidRequestWithUnsupportedConfigShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.INVALID_REQUEST, ErrorCode.NOT_SUPPORTED, "unsupported request"), DISABLED));
    }

    @Test
    public void invalidRequestWithPermissionMismatchShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.INVALID_REQUEST, ErrorCode.NOT_AUTHORIZED, "permission denied"), DISABLED));
    }

    @Test
    public void invalidRequestWithTabletSplitStatusShouldRetry() {
        // Only CDC TABLET_SPLIT is a split, not INVALID_REQUEST + AppStatus TABLET_SPLIT.
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.INVALID_REQUEST, ErrorCode.TABLET_SPLIT, "tablet split detected"), DISABLED));
    }

    @Test
    public void invalidRequestWithSplitInMessageShouldFailFast() {
        // RUNTIME_ERROR is a fatal AppStatus; message text is not a split.
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.INVALID_REQUEST, ErrorCode.RUNTIME_ERROR, "tablet was split"), DISABLED));
    }

    @Test
    public void invalidRequestWithTransientStatusShouldRetry() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.INVALID_REQUEST, ErrorCode.SERVICE_UNAVAILABLE, "leader unavailable"), DISABLED));
    }

    @Test
    public void unknownErrorWithTabletSplitStatusShouldRetry() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.UNKNOWN_ERROR, ErrorCode.TABLET_SPLIT, "tablet split"), DISABLED));
    }

    @Test
    public void unknownErrorWithSplitMessageAloneShouldNotBeHandledAsSplit() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.UNKNOWN_ERROR, ErrorCode.RUNTIME_ERROR, "tablet was split"), DISABLED));
    }

    @Test
    public void unknownErrorWithMissingStreamShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.UNKNOWN_ERROR, ErrorCode.NOT_FOUND,
                        "Could not find CDC stream: stream_id: \"abc\""), DISABLED));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("permanentCdcFailures")
    public void permanentCdcFailureShouldFailFast(String name, Code code, ErrorCode status, String message)
            throws Exception {
        CDCErrorPB cdcError = error(code, status, message);

        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST, classify(cdcError, DISABLED), name);
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST, classify(cdcError, FILTERED), name);

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
                classify(error(Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR, message), DISABLED), name);
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.UNKNOWN_ERROR, ErrorCode.NOT_FOUND, message), DISABLED), name);
    }

    @Test
    public void findCdcErrorReturnsNullWhenAbsent() {
        assertNull(YugabyteDBCdcErrorClassifier.findCdcError(new IllegalStateException("leader moved")));
    }

    @Test
    public void unknownErrorWithRuntimeStatusShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.UNKNOWN_ERROR, ErrorCode.RUNTIME_ERROR, "internal failure"), DISABLED));
    }

    @Test
    public void unknownErrorWithTimedOutShouldRetry() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.UNKNOWN_ERROR, ErrorCode.TIMED_OUT, "rpc timed out"), DISABLED));
    }

    @Test
    public void unknownErrorWithoutStatusShouldRetry() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.UNKNOWN_ERROR), DISABLED));
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

    private static Stream<Arguments> permanentCdcFailures() {
        String walGarbageCollected = "The logs from index 10 have been garbage collected and cannot be read: "
                + "op index 10 has been already GCed";
        String gcedIntents = "CDCSDK Trying to fetch already GCed intents for transaction 123";
        String beforeImage = "Failed to get the beforeimage for tablet_id: abc due to compaction";
        String missingWal = "Unexpectedly did not find a WAL message corresponding to from_op_id: 1.2";
        String streamExpired = "Stream ID abc is expired for Tablet ID xyz";
        String unpolled = "Stream ID abc unpolled for too long for Tablet ID xyz";
        String streamMissing = "Could not find CDC stream: stream_id: \"abc\"";
        String tabletNotInStream = "Tablet ID xyz is not part of stream ID abc";
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
                failure("stream expired for tablet", Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR, streamExpired),
                failure("stream expired for tablet is case insensitive", Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR,
                        "STREAM ID ABC IS EXPIRED FOR TABLET ID XYZ"),
                failure("tablet unpolled too long", Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR, unpolled),
                failure("cdc stream not found on invalid request", Code.INVALID_REQUEST, ErrorCode.NOT_FOUND, streamMissing),
                failure("cdc stream not found on internal error", Code.INTERNAL_ERROR, ErrorCode.NOT_FOUND, streamMissing),
                failure("tablet not part of stream", Code.INVALID_REQUEST, ErrorCode.INVALID_ARGUMENT, tabletNotInStream),
                // INVALID_ARGUMENT is fatal on its own. This row keeps the message match under test.
                failure("tablet not part of stream by message", Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR, tabletNotInStream),
                failure("tablet not found under stream", Code.INTERNAL_ERROR, ErrorCode.NOT_FOUND,
                        "Tablet ID xyz not found under stream ID abc"),
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
                Arguments.of("tablet not part of a publication", "Tablet ID xyz is not part of publication abc"));
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

    private static YugabyteDBCdcErrorClassifier.CdcErrorAction classify(
            CDCErrorPB error,
            YugabyteDBConnectorConfig.AutoCreateMode publicationMode) {
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
