package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.debezium.config.Configuration;

import org.junit.jupiter.api.Test;
import org.yb.WireProtocol.AppStatusPB;
import org.yb.WireProtocol.AppStatusPB.ErrorCode;
import org.yb.cdc.CdcService.CDCErrorPB;
import org.yb.cdc.CdcService.CDCErrorPB.Code;

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
    public void tableNotFoundShouldFailFastWhenNotUsingPublication() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                classify(error(Code.TABLE_NOT_FOUND), DISABLED));
    }

    @Test
    public void tableNotFoundShouldRetryForPublicationEvenWithAutocreateDisabled() {
        // Supported: publication path with publication.autocreate.mode=disabled.
        // Operator adds tables by hand; table poller still reconfigures and can hit
        // the same brief TABLE_NOT_FOUND window as ALL_TABLES/FILTERED.
        Configuration config = Configuration.create()
                .with(YugabyteDBConnectorConfig.STREAM_ID, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .with(YugabyteDBConnectorConfig.TASK_USE_PUBLICATION, true)
                .with(YugabyteDBConnectorConfig.PUBLICATION_AUTOCREATE_MODE, DISABLED.getValue())
                .build();
        assertTrue(YugabyteDBConnectorConfig.usesPublication(config));
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                YugabyteDBCdcErrorClassifier.actionFor(error(Code.TABLE_NOT_FOUND), config));
    }

    @Test
    public void tableNotFoundShouldRetryForPublicationEvenAfterStreamIdIsInjected() {
        // Mimics task props: stream id already filled from the slot, plus the flag the
        // connector writes so the classifier still knows this is a publication deployment.
        Configuration config = Configuration.create()
                .with(YugabyteDBConnectorConfig.STREAM_ID, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .with(YugabyteDBConnectorConfig.TASK_USE_PUBLICATION, true)
                .with(YugabyteDBConnectorConfig.PUBLICATION_AUTOCREATE_MODE, ALL_TABLES.getValue())
                .build();
        assertTrue(YugabyteDBConnectorConfig.usesPublication(config));
        // Must go through the config-based overload — same entry used at task runtime.
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                YugabyteDBCdcErrorClassifier.actionFor(error(Code.TABLE_NOT_FOUND), config));
    }

    @Test
    public void tableNotFoundShouldFailFastForGrpcStreamWithInjectedStreamId() {
        Configuration config = Configuration.create()
                .with(YugabyteDBConnectorConfig.STREAM_ID, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .with(YugabyteDBConnectorConfig.TASK_USE_PUBLICATION, false)
                .with(YugabyteDBConnectorConfig.PUBLICATION_AUTOCREATE_MODE, ALL_TABLES.getValue())
                .build();
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                YugabyteDBCdcErrorClassifier.actionFor(error(Code.TABLE_NOT_FOUND), config));
    }

    @Test
    public void tableNotFoundShouldFailFastWhenStreamIdPresentButPublicationFlagMissing() {
        // Old bug: stream id alone made shouldUsePublication() false → always FAIL_FAST.
        Configuration config = Configuration.create()
                .with(YugabyteDBConnectorConfig.STREAM_ID, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                .with(YugabyteDBConnectorConfig.PUBLICATION_AUTOCREATE_MODE, ALL_TABLES.getValue())
                .build();
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.FAIL_FAST,
                YugabyteDBCdcErrorClassifier.actionFor(error(Code.TABLE_NOT_FOUND), config));
    }

    @Test
    public void tableNotFoundDuringAutoAddWindowShouldRetryOnPublicationTask() {
        // auto.add.new.tables / table poller: task already has a stream id, and during
        // reconfiguration GetTabletList/GetChanges can briefly return TABLE_NOT_FOUND.
        // Publication tasks must RETRY so the task is not killed in that window.
        Configuration config = Configuration.create()
                .with(YugabyteDBConnectorConfig.STREAM_ID, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
                .with(YugabyteDBConnectorConfig.TASK_USE_PUBLICATION, true)
                .with(YugabyteDBConnectorConfig.PUBLICATION_AUTOCREATE_MODE, FILTERED.getValue())
                .with(YugabyteDBConnectorConfig.AUTO_ADD_NEW_TABLES, true)
                .build();
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                YugabyteDBCdcErrorClassifier.actionFor(error(Code.TABLE_NOT_FOUND), config));
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
    public void internalErrorStreamExpiredForTabletShouldRetry() {
        // Curable during bootstrap: SetCheckpoint not issued yet / stale tserver cache.
        // Must not fail-fast so bootstrapTabletWithRetry and markNoSnapshotNeeded can recover.
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.RETRY,
                classify(error(Code.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR,
                        "Stream ID abc is expired for Tablet ID xyz"), DISABLED));
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

    private static YugabyteDBCdcErrorClassifier.CdcErrorAction classify(
            CDCErrorPB error,
            YugabyteDBConnectorConfig.AutoCreateMode publicationMode) {
        boolean usePublication = publicationMode != DISABLED;
        return YugabyteDBCdcErrorClassifier.actionFor(error, usePublication);
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
