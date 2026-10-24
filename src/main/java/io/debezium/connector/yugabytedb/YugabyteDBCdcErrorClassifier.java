/*
 * Copyright YugabyteDB Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.util.EnumSet;
import java.util.Locale;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.WireProtocol.AppStatusPB;
import org.yb.WireProtocol.AppStatusPB.ErrorCode;
import org.yb.cdc.CdcService.CDCErrorPB;
import org.yb.client.CDCErrorException;
import org.yb.client.MasterErrorException;
import org.yb.master.MasterTypes.MasterErrorPB;

/**
 * Classifies CDC errors before the generic connector retry loop.
 * <p>
 * Two different membership-related texts are not interchangeable:
 * <ul>
 *   <li>{@code Tablet ID … is not part of stream ID …} on {@code GetChanges} (often
 *       {@code INVALID_REQUEST} / {@code InvalidArgument}) is intentionally {@link CdcErrorAction#RETRY retried}:
 *       the tserver can build that error from an incomplete tablet list when
 *       {@code GetTablets(ignore_errors=true)} skips failed master lookups (cache miss,
 *       restart). The server does not distinguish that from a real removal on this RPC.</li>
 *   <li>{@code Table … not found under stream} on {@code GetTabletListToPollForCDC} is
 *       {@code CDCErrorPB.TABLE_NOT_FOUND}. Because the same code/text is used when stream
 *       metadata has not caught up after adding a table as when a table was removed, the
 *       bootstrap path ({@code YBClientUtils.getTabletListToPollForCDCWithRetry}) calls
 *       {@code getDBStreamInfo} to check membership. If the table is still listed in the
 *       stream, the error is {@link CdcErrorAction#RETRY retried}; if not, a
 *       {@code DebeziumException} is thrown to fail-fast. The split-child path
 *       ({@code getTabletListResponseWithRetry}) does not perform this check and retries
 *       on {@code TABLE_NOT_FOUND} like any other transient error.</li>
 * </ul>
 * Stream delete uses {@code NOT_FOUND} (e.g. could not find CDC stream), not the GetChanges
 * membership string above.
 */
final class YugabyteDBCdcErrorClassifier {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBCdcErrorClassifier.class);

    private static final EnumSet<ErrorCode> FATAL_APP_STATUS = EnumSet.of(
            ErrorCode.NOT_AUTHORIZED,
            ErrorCode.NOT_SUPPORTED,
            ErrorCode.CONFIGURATION_ERROR,
            ErrorCode.DELETED,
            ErrorCode.EXPIRED,
            ErrorCode.RUNTIME_ERROR);

    enum CdcErrorAction {
        /** Tablet split: refresh children in the streaming loop (do not fail/retry the task) */
        HANDLE_IN_STREAM,
        /** Permanent error: stop the task immediately (no connector retry) */
        FAIL_FAST,
        /** Transient error: let the existing connector retry loop run its course */
        RETRY
    }

    private YugabyteDBCdcErrorClassifier() {
    }

    static CDCErrorException findCdcError(Throwable error) {
        return ExceptionUtils.throwableOfType(error, CDCErrorException.class);
    }

    static boolean isFailFast(Throwable error) {
        if (isMasterObjectNotFound(error)) {
            return true;
        }
        CDCErrorException cdcError = findCdcError(error);
        if (cdcError != null) {
            return actionFor(cdcError.getCDCError()) == CdcErrorAction.FAIL_FAST;
        }
        return false;
    }

    static void throwIfFailFast(Exception error) throws Exception {
        if (!isFailFast(error)) {
            return;
        }

        MasterErrorException masterException = ExceptionUtils.throwableOfType(error, MasterErrorException.class);
        if (masterException != null && isMasterObjectNotFound(error)) {
            LOGGER.warn(
                    "Failing fast for non-retriable master error from YugabyteDB. masterError={}, message={}",
                    masterException.error.getCode(),
                    masterException.getMessage());
        }
        else {
            CDCErrorException cdcException = findCdcError(error);
            if (cdcException != null) {
                CDCErrorPB cdcError = cdcException.getCDCError();
                String appStatusCode = "none";
                String appStatusMessage = "";
                if (cdcError.hasStatus()) {
                    AppStatusPB status = cdcError.getStatus();
                    appStatusCode = status.getCode().name();
                    if (status.hasMessage()) {
                        appStatusMessage = status.getMessage();
                    }
                }
                LOGGER.warn(
                        "Failing fast for non-retriable CDC error from YugabyteDB. code={}, appStatus={}, message={}",
                        cdcError.getCode(),
                        appStatusCode,
                        appStatusMessage);
            }
        }

        throw error;
    }

    /**
     * True only for CDC {@code TABLET_SPLIT}. {@code INVALID_REQUEST} is not a split.
     */
    static boolean isTabletSplit(CDCErrorPB error) {
        return error.getCode() == CDCErrorPB.Code.TABLET_SPLIT;
    }

    static CdcErrorAction actionFor(CDCErrorPB error) {
        if (!error.hasCode()) {
            LOGGER.warn("CDC error has no code set; proto2 getCode() defaults to UNKNOWN_ERROR");
        }
        switch (error.getCode()) {
            case TABLET_SPLIT:
                return CdcErrorAction.HANDLE_IN_STREAM;
            case TABLE_NOT_FOUND:
                // Transient while CDC metadata catches up after adding a table, or permanent
                // if table was removed from stream. getTabletListToPollForCDCWithRetry disambiguates
                // by calling getDBStreamInfo. This returns RETRY; disambiguation throws if not in stream.
                return CdcErrorAction.RETRY;
            case INVALID_REQUEST:
                // Not a tablet split (only CDC TABLET_SPLIT is). Classified via actionForAmbiguousCdcCode:
                // FAIL_FAST on fatal AppStatus/message; RETRY otherwise (e.g. transient pre-producer failure).
                return actionForAmbiguousCdcCode(error);
            case CHECKPOINT_TOO_OLD:
            case OPERATION_DISALLOWED:
                return CdcErrorAction.FAIL_FAST;
            case TABLET_NOT_FOUND:
            case TABLET_NOT_RUNNING:
            case NOT_LEADER:
            case NOT_RUNNING:
            case LEADER_NOT_READY:
                return CdcErrorAction.RETRY;
            case UNKNOWN_ERROR:
            case INTERNAL_ERROR:
                return actionForAmbiguousCdcCode(error);
            default:
                // Reached for codes not explicitly listed above (e.g., SUBSCRIBER_NOT_FOUND,
                // AUTO_FLAGS_CONFIG_VERSION_MISMATCH). Kept so any future Code constant is
                // retried by default rather than causing a compilation error or accidental fail-fast.
                return CdcErrorAction.RETRY;
        }
    }

    /** Shared by INVALID_REQUEST, UNKNOWN_ERROR, and INTERNAL_ERROR. Split is CDC TABLET_SPLIT only. */
    private static CdcErrorAction actionForAmbiguousCdcCode(CDCErrorPB error) {
        if (hasFatalMessage(error) || isFatalAppStatus(error)) {
            return CdcErrorAction.FAIL_FAST;
        }
        return CdcErrorAction.RETRY;
    }

    private static boolean isFatalAppStatus(CDCErrorPB error) {
        return error.hasStatus() && FATAL_APP_STATUS.contains(error.getStatus().getCode());
    }

    /**
     * Permanent CDC failures that often arrive as {@code UNKNOWN_ERROR} or {@code INTERNAL_ERROR},
     * so the AppStatus message on {@link CDCErrorPB} is what distinguishes them from a retryable
     * error with the same code. Matched only via {@link #hasFatalMessage(CDCErrorPB)}, not on the
     * exception message chain (stripped yb-client errors carry no server text to match).
     * <p>
     * {@code Stream ID … is expired for Tablet ID …} is fatal. GetChanges runs
     * {@code CheckStreamActive} before it updates active time, so retrying GetChanges cannot
     * recover. Normal connector startup always calls SetCDCCheckpoint / bootstrap before the
     * first GetChanges on a tablet. Exception: child tablets after a tablet split are added
     * directly to the streaming loop ({@code YugabyteDBStreamingChangeEventSource:1183})
     * without a prior bootstrap call. Even so, expiry is irrecoverable: CheckStreamActive
     * marks the tablet expired at the server and no subsequent GetChanges call can undo it.
     * {@code Stream ID … unpolled for too long for Tablet ID …} is a different check and is
     * also a fatal fragment. A bare {@code stream expired} (no tablet) is not matched.
     * <p>
     * {@code not found under stream} is omitted: master returns it only as
     * {@link CDCErrorPB.Code#TABLE_NOT_FOUND}. The connector calls {@code getDBStreamInfo}
     * to check stream membership; if the table is still listed, the error is retried, otherwise
     * a {@code DebeziumException} is thrown to fail-fast.
     */
    private static final String[] FATAL_MESSAGE_FRAGMENTS = {
            "could not find cdc stream",
            "garbage collected",
            "already gced intents",
            "due to compaction",
            "did not find a wal message",
            "is expired for tablet",
            "unpolled for too long",
            "unsupported replica identity",
            "replica identity not found",
            "invalid stream id",
            "invalid checkpoint"
    };

    private static boolean hasFatalMessage(CDCErrorPB error) {
        return StringUtils.containsAny(statusMessage(error), FATAL_MESSAGE_FRAGMENTS);
    }

    /**
     * Master {@code OBJECT_NOT_FOUND} is treated as fail-fast wherever a {@code MasterErrorException}
     * propagates to a {@link #throwIfFailFast} call site or where {@link #isFailFast} is explicitly
     * checked. Three situations where this can happen:
     * <ol>
     *   <li>Stream validation: {@code validateTServerConnection} calls {@code getDBStreamInfo}
     *       ({@code YugabyteDBgRPCConnector:415}). The catch at {@code :416-428} checks
     *       {@code isFailFast(e)} at line {@code :424}. Under a real
     *       {@code SourceConnectorContext} (production), {@code raiseErrorSafely} at {@code :425}
     *       calls {@code context().raiseError()} to stop the {@code taskConfigs()} retry loop.
     *       Under {@code EmbeddedEngine} (tests), {@code context()} is not a
     *       {@code SourceConnectorContext}, so the {@code ClassCastException} is silently
     *       swallowed and the connector stops because {@code taskConfigs()} throws instead.
     *       Either way, this prevents endless retries on a permanently deleted stream.</li>
     *   <li>Tablet bootstrap: {@code bootstrapTabletWithRetry}
     *       ({@code YugabyteDBStreamingChangeEventSource:250}) and the {@code openTableByUUID}
     *       inside {@code YBClientUtils.setCheckpoint} (called via snapshot's
     *       {@code makeStreamActive} at {@code YugabyteDBSnapshotChangeEventSource:267-295})
     *       both call {@code openTableByUUID} inside retry loops whose catch blocks call
     *       {@code throwIfFailFast}. If the table is dropped between the preceding discovery step
     *       and that call, the master returns {@code OBJECT_NOT_FOUND} and the task stops
     *       immediately. This is a production-only concurrent-DDL race.
     *       Note: call sites such as {@code YugabyteDBgRPCConnector:494} and
     *       {@code YugabyteDBStreamingChangeEventSource:364} also call {@code openTableByUUID}
     *       but are <em>outside</em> {@code throwIfFailFast} catch. At {@code :494}, the catch at
     *       {@code :518-545} checks {@code isFailFast} and calls {@code raiseErrorSafely} at
     *       {@code :540} for permanent errors (e.g. {@code OBJECT_NOT_FOUND}) before wrapping as
     *       "Error while fetching all the tablets". At {@code :364}, the exception propagates
     *       uncaught.
     *       Note: a table explicitly excluded from the stream produces a {@link
     *       io.debezium.DebeziumException} from {@code fetchTableList} directly, not this
     *       error.</li>
     *   <li>Tablet-split handling during streaming: {@code handleTabletSplit} calls
     *       {@code syncClient.openTableByUUID} ({@code YugabyteDBStreamingChangeEventSource:1206})
     *       to locate the split tablet's parent table. If that table was dropped between the
     *       split event being observed and the handler running, the master returns
     *       {@code OBJECT_NOT_FOUND}. This propagates past the inner
     *       {@code CDCErrorException} catch to {@code throwIfFailFast} at the outer streaming
     *       catch ({@code YugabyteDBConsistentStreamingSource:287}). Intentionally fail-fast:
     *       a split on a dropped table cannot be recovered by retrying.</li>
     * </ol>
     * Note: a normal {@code DROP TABLE} on a table in an active CDCSDK stream uses the master's
     * {@code hide_only} path — the table UUID remains accessible via {@code openTableByUUID}
     * while CDC drains it — so that path does not produce {@code OBJECT_NOT_FOUND} during
     * {@code GetChanges} polling and is not affected by this classification.
     */
    private static boolean isMasterObjectNotFound(Throwable error) {
        MasterErrorException masterError = ExceptionUtils.throwableOfType(error, MasterErrorException.class);
        if (masterError == null || masterError.error == null || !masterError.error.hasCode()) {
            return false;
        }
        return masterError.error.getCode() == MasterErrorPB.Code.OBJECT_NOT_FOUND;
    }

    private static String statusMessage(CDCErrorPB error) {
        return error.hasStatus() ? error.getStatus().getMessage().toLowerCase(Locale.ROOT) : "";
    }
}
