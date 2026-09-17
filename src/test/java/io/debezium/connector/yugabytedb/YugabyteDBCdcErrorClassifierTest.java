package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;

import io.debezium.config.Configuration;

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
    public void tableNotFoundShouldFailFastWhenNotUsingPublication() {
        // Convenience overload treats AutoCreateMode.DISABLED as usePublication=false.
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
    public void invalidRequestWithoutStatusShouldBeHandledInStream() {
        // Legacy YB used bare CDC INVALID_REQUEST for tablet splits.
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.HANDLE_IN_STREAM,
                classify(error(Code.INVALID_REQUEST), DISABLED));
    }

    @Test
    public void invalidRequestWithGenericSplitWordShouldBeHandledInStream() {
        // Split decision is keyed on CDC INVALID_REQUEST, not message text.
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.HANDLE_IN_STREAM,
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
    public void masterInvalidRequestShouldNotFailFast() throws Exception {
        // Stale tserver-cache lookups during bootstrap can surface INVALID_REQUEST;
        // bootstrap uses maxConnectorRetries * 5 and must be allowed to spend that budget.
        MasterErrorPB masterError = MasterErrorPB.newBuilder()
                .setCode(MasterErrorPB.Code.INVALID_REQUEST)
                .setStatus(AppStatusPB.newBuilder()
                        .setCode(ErrorCode.INVALID_ARGUMENT)
                        .setMessage("Invalid request against stale cache entry")
                        .build())
                .build();
        Constructor<MasterErrorException> constructor =
                MasterErrorException.class.getDeclaredConstructor(String.class, MasterErrorPB.class);
        constructor.setAccessible(true);
        MasterErrorException exception = constructor.newInstance("tserver", masterError);
        assertFalse(YugabyteDBCdcErrorClassifier.isFatalMasterError(exception));
    }

    @Test
    public void masterErrorWithDoesNotExistTextAloneShouldNotFailFast() throws Exception {
        // Free-text "does not exist" must not be enough without a permanent master code.
        MasterErrorPB masterError = MasterErrorPB.newBuilder()
                .setCode(MasterErrorPB.Code.UNKNOWN_ERROR)
                .setStatus(AppStatusPB.newBuilder()
                        .setCode(ErrorCode.RUNTIME_ERROR)
                        .setMessage("tablet peer does not exist yet")
                        .build())
                .build();
        Constructor<MasterErrorException> constructor =
                MasterErrorException.class.getDeclaredConstructor(String.class, MasterErrorPB.class);
        constructor.setAccessible(true);
        MasterErrorException exception = constructor.newInstance("tserver", masterError);
        assertFalse(YugabyteDBCdcErrorClassifier.isFatalMasterError(exception));
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
    public void invalidRequestWithInvalidStreamIdShouldBeHandledInStream() {
        // Same CDC code as legacy splits; streaming runs handleTabletSplit and
        // a non-split failure surfaces from that path / outer catch.
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.HANDLE_IN_STREAM,
                classify(error(Code.INVALID_REQUEST, ErrorCode.INVALID_ARGUMENT, "invalid stream id"), DISABLED));
    }

    @Test
    public void invalidRequestWithDeletedStreamShouldBeHandledInStream() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.HANDLE_IN_STREAM,
                classify(error(Code.INVALID_REQUEST, ErrorCode.DELETED, "deleted stream"), DISABLED));
    }

    @Test
    public void invalidRequestWithIncorrectTabletIdShouldBeHandledInStream() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.HANDLE_IN_STREAM,
                classify(error(Code.INVALID_REQUEST, ErrorCode.INVALID_ARGUMENT, "incorrect tablet id"), DISABLED));
    }

    @Test
    public void invalidRequestWithTableNotInPublicationShouldBeHandledInStream() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.HANDLE_IN_STREAM,
                classify(error(Code.INVALID_REQUEST, ErrorCode.NOT_FOUND, "table is not part of publication"), DISABLED));
    }

    @Test
    public void invalidRequestWithBadCheckpointShouldBeHandledInStream() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.HANDLE_IN_STREAM,
                classify(error(Code.INVALID_REQUEST, ErrorCode.INVALID_ARGUMENT, "invalid checkpoint"), DISABLED));
    }

    @Test
    public void invalidRequestWithUnsupportedConfigShouldBeHandledInStream() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.HANDLE_IN_STREAM,
                classify(error(Code.INVALID_REQUEST, ErrorCode.NOT_SUPPORTED, "unsupported request"), DISABLED));
    }

    @Test
    public void invalidRequestWithPermissionMismatchShouldBeHandledInStream() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.HANDLE_IN_STREAM,
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
    public void unknownErrorWithTabletSplitStatusShouldBeHandledInStream() {
        assertEquals(YugabyteDBCdcErrorClassifier.CdcErrorAction.HANDLE_IN_STREAM,
                classify(error(Code.UNKNOWN_ERROR, ErrorCode.TABLET_SPLIT, "tablet split"), DISABLED));
    }

    @Test
    public void unknownErrorWithSplitMessageAloneShouldNotBeHandledAsSplit() {
        // Split for UNKNOWN/INTERNAL is keyed on AppStatus TABLET_SPLIT, not message text.
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
