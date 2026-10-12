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
 * {@code INVALID_REQUEST} with AppStatus {@code InvalidArgument} and message
 * {@code Tablet ID … is not part of stream ID …} is intentionally retried: the
 * tserver can build that error from an incomplete tablet list when
 * {@code GetTablets(ignore_errors=true)} skips failed master lookups (cache miss,
 * restart). The same wire shape can mean a real removal; the server does not
 * distinguish them. Stream delete uses {@code NOT_FOUND} (e.g. could not find CDC
 * stream), not that membership string.
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
        // Fallback when yb-client wraps errors as NonRecoverableException without CDCErrorPB on
        // the chain (e.g. tooManyAttemptsOrTimeout(rpc, null)). Prefer CDC classification when
        // the cause is preserved (delayedSendRpcToTablet after operation timeout).
        return hasFatalExceptionMessage(error);
    }

    static void throwIfFailFast(Exception error) throws Exception {
        if (!isFailFast(error)) {
            return;
        }

        MasterErrorException masterException = ExceptionUtils.throwableOfType(error, MasterErrorException.class);
        if (masterException != null && isMasterObjectNotFound(error)) {
            LOGGER.warn(
                    "Failing fast for non-retriable CDC error from YugabyteDB. masterError={}, message={}",
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
            else {
                LOGGER.warn(
                        "Failing fast for non-retriable CDC error from YugabyteDB. message={}",
                        flattenMessages(error));
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
                // Transient while CDC metadata catches up, on both publication and
                // plain gRPC stream tasks. Do not fail the task.
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
                // Not reached with the current yb-client enum. Kept so a future
                // Code constant this switch does not list yet is retried.
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
     * so the status text is what distinguishes them from a retryable error with the same code.
     * Also matched on {@code NonRecoverableException} when yb-client omits {@code CDCErrorPB}.
     */
    private static final String[] FATAL_MESSAGE_FRAGMENTS = {
            "could not find cdc stream",
            "not found under stream",
            "garbage collected",
            "already gced intents",
            "due to compaction",
            "did not find a wal message",
            "is expired for tablet",
            "unpolled for too long",
            "unsupported replica identity",
            "replica identity not found",
            "invalid stream id",
            "incorrect tablet id",
            "invalid checkpoint"
    };

    private static boolean hasFatalMessage(CDCErrorPB error) {
        return StringUtils.containsAny(statusMessage(error), FATAL_MESSAGE_FRAGMENTS);
    }

    private static boolean hasFatalExceptionMessage(Throwable error) {
        return StringUtils.containsAny(flattenMessages(error), FATAL_MESSAGE_FRAGMENTS);
    }

    /** DROP TABLE during bootstrap/openTable: master returns {@code OBJECT_NOT_FOUND}, not a CDC error. */
    private static boolean isMasterObjectNotFound(Throwable error) {
        MasterErrorException masterError = ExceptionUtils.throwableOfType(error, MasterErrorException.class);
        if (masterError == null || masterError.error == null || !masterError.error.hasCode()) {
            return false;
        }
        return masterError.error.getCode() == MasterErrorPB.Code.OBJECT_NOT_FOUND;
    }

    private static String flattenMessages(Throwable error) {
        StringBuilder builder = new StringBuilder();
        Throwable current = error;
        while (current != null) {
            if (current.getMessage() != null) {
                builder.append(current.getMessage()).append(' ');
            }
            current = current.getCause();
        }
        return builder.toString().toLowerCase(Locale.ROOT);
    }

    private static String statusMessage(CDCErrorPB error) {
        return error.hasStatus() ? error.getStatus().getMessage().toLowerCase(Locale.ROOT) : "";
    }
}
