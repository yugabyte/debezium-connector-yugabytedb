package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;

import org.junit.jupiter.api.Test;
import org.yb.WireProtocol.AppStatusPB;
import org.yb.WireProtocol.AppStatusPB.ErrorCode;
import org.yb.cdc.CdcService.CDCErrorPB;
import org.yb.cdc.CdcService.CDCErrorPB.Code;
import org.yb.client.MasterErrorException;
import org.yb.master.MasterTypes.MasterErrorPB;

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
    public void subscriberNotFoundShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.SUBSCRIBER_NOT_FOUND), DISABLED));
    }

    @Test
    public void operationDisallowedShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.OPERATION_DISALLOWED), DISABLED));
    }

    @Test
    public void autoFlagsMismatchShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
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
    public void tableNotFoundShouldFailFastWhenPublicationIsDisabled() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.TABLE_NOT_FOUND), DISABLED));
    }

    @Test
    public void tableNotFoundShouldFailFastWhenNotUsingPublication() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                YugabyteDBCdcErrorClassifier.actionFor(error(Code.TABLE_NOT_FOUND), ALL_TABLES, false));
    }

    @Test
    public void invalidRequestWithoutStatusShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.INVALID_REQUEST), DISABLED));
    }

    @Test
    public void invalidRequestWithGenericSplitWordShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.INVALID_REQUEST, ErrorCode.RUNTIME_ERROR, "unexpected split of request"), DISABLED));
    }

    @Test
    public void internalErrorStreamExpiredShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR,
                        "Stream ID abc is expired for Tablet ID xyz"), DISABLED));
    }

    @Test
    public void masterObjectNotFoundShouldFailFast() throws Exception {
        MasterErrorPB masterError = MasterErrorPB.newBuilder()
                .setCode(MasterErrorPB.Code.OBJECT_NOT_FOUND)
                .setStatus(AppStatusPB.newBuilder()
                        .setCode(ErrorCode.NOT_FOUND)
                        .setMessage("The object does not exist")
                        .build())
                .build();
        Constructor<MasterErrorException> constructor =
                MasterErrorException.class.getDeclaredConstructor(String.class, MasterErrorPB.class);
        constructor.setAccessible(true);
        MasterErrorException exception = constructor.newInstance("tserver", masterError);
        assertTrue(YugabyteDBCdcErrorClassifier.isFatalMasterError(exception));
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
    public void invalidRequestWithTableNotInPublicationShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
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
    public void invalidRequestWithTabletSplitStatusShouldBeHandledInStream() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.HANDLE_IN_STREAM,
                classify(error(Code.INVALID_REQUEST, ErrorCode.TABLET_SPLIT, "tablet split detected"), DISABLED));
    }

    @Test
    public void invalidRequestWithSplitInMessageShouldBeHandledInStream() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.HANDLE_IN_STREAM,
                classify(error(Code.INVALID_REQUEST, ErrorCode.RUNTIME_ERROR, "tablet was split"), DISABLED));
    }

    @Test
    public void invalidRequestWithTransientStatusShouldRetry() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.INVALID_REQUEST, ErrorCode.SERVICE_UNAVAILABLE, "leader unavailable"), DISABLED));
    }

    @Test
    public void unknownErrorWithMissingStreamShouldFailFast() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.UNKNOWN_ERROR, ErrorCode.NOT_FOUND,
                        "Could not find CDC stream: stream_id: \"abc\""), DISABLED));
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

    private static YugabyteDBCdcErrorClassifier.CdcErrorAction classify(
            CDCErrorPB error,
            YugabyteDBConnectorConfig.AutoCreateMode publicationMode) {
        return YugabyteDBCdcErrorClassifier.actionFor(error, publicationMode);
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
