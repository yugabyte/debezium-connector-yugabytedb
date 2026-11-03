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
 * Membership errors are handled differently by RPC:
 * <ul>
 *   <li>{@code GetChanges} "Tablet ID … is not part of stream ID …" is retried. The tserver can
 *       build it from an incomplete tablet list, so it can't be told apart from a real removal.</li>
 *   <li>{@code GetTabletListToPollForCDC} {@code TABLE_NOT_FOUND} is returned both while stream
 *       metadata catches up after adding a table and after a table is removed, so it is retried
 *       here. A caller that needs to tell the two apart has to check {@code getDBStreamInfo}
 *       itself.</li>
 * </ul>
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
                // if table was removed from stream. The code alone cannot tell them apart, so
                // this returns RETRY; a caller that can check getDBStreamInfo may fail fast.
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
     * {@code Stream ID … is expired for Tablet ID …} is fatal. GetChanges checks expiry
     * before it refreshes the active time, so a retry hits the same error.
     * {@code Stream ID … unpolled for too long for Tablet ID …} is a different check and is
     * also a fatal fragment. A bare {@code stream expired} (no tablet) is not matched.
     * <p>
     * {@code not found under stream} is {@code TABLE_NOT_FOUND}, not a fatal fragment.
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
     * True when a master {@code OBJECT_NOT_FOUND} reaches {@link #throwIfFailFast} or an
     * explicit {@link #isFailFast} check. A normal {@code DROP TABLE} uses the master's
     * {@code hide_only} path, so the table stays reachable and this error is not produced
     * during {@code GetChanges} polling.
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
