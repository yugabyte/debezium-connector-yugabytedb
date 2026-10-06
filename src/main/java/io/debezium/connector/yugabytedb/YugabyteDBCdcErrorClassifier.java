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
import org.yb.WireProtocol.AppStatusPB.ErrorCode;
import org.yb.cdc.CdcService.CDCErrorPB;
import org.yb.client.CDCErrorException;

/**
 * Classifies CDC errors before the generic connector retry loop.
 */
final class YugabyteDBCdcErrorClassifier {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBCdcErrorClassifier.class);

    private static final EnumSet<ErrorCode> FATAL_APP_STATUS = EnumSet.of(
            ErrorCode.INVALID_ARGUMENT,
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
        CDCErrorException cdcError = findCdcError(error);
        if (cdcError == null) {
            return false;
        }
        return actionFor(cdcError.getCDCError()) == CdcErrorAction.FAIL_FAST;
    }

    static void throwIfFailFast(Exception error) throws Exception {
        if (!isFailFast(error)) {
            return;
        }

        CDCErrorException cdcException = findCdcError(error);
        LOGGER.error("Failing fast for non-retriable CDC error from YugabyteDB. code={}, status={}",
                cdcException.getCDCError().getCode(),
                cdcException.getCDCError().hasStatus()
                        ? cdcException.getCDCError().getStatus()
                        : "none",
                error);

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
                // Not a tablet split. GetChanges uses this for bad requests
                // (InvalidArgument) and some pre-producer failures (e.g. master lookup).
                // A real split is CDC TABLET_SPLIT; retry here so the next poll can
                // receive that code instead of calling handleTabletSplit on a child.
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
     */
    private static boolean hasFatalMessage(CDCErrorPB error) {
        return StringUtils.containsAny(statusMessage(error),
                "could not find cdc stream",
                "is not part of stream",
                "not found under stream",
                "garbage collected",
                "already gced intents",
                "due to compaction",
                "did not find a wal message",
                "is expired for tablet",
                "unpolled for too long",
                "unsupported replica identity",
                "replica identity not found");
    }

    private static String statusMessage(CDCErrorPB error) {
        return error.hasStatus() ? error.getStatus().getMessage().toLowerCase(Locale.ROOT) : "";
    }
}
