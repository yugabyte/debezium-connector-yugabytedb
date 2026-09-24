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

import io.debezium.config.Configuration;

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

    static CdcErrorAction actionFor(CDCErrorPB error, YugabyteDBConnectorConfig connectorConfig) {
        return actionFor(error, connectorConfig.getConfig());
    }

    /**
     * Same decision as {@link #actionFor(CDCErrorPB, YugabyteDBConnectorConfig)} but reads
     * {@link YugabyteDBConnectorConfig#usesPublication(Configuration)} from raw config —
     * the path tasks use after stream id has been injected.
     */
    static CdcErrorAction actionFor(CDCErrorPB error, Configuration config) {
        return actionFor(error, YugabyteDBConnectorConfig.usesPublication(config));
    }

    static CDCErrorException findCdcError(Throwable error) {
        return ExceptionUtils.throwableOfType(error, CDCErrorException.class);
    }

    static boolean isFailFast(Throwable error, YugabyteDBConnectorConfig connectorConfig) {
        CDCErrorException cdcError = findCdcError(error);
        if (cdcError == null) {
            return false;
        }
        return actionFor(cdcError.getCDCError(), connectorConfig) == CdcErrorAction.FAIL_FAST;
    }

    static void throwIfFailFast(Exception error, YugabyteDBConnectorConfig connectorConfig) throws Exception {
        if (!isFailFast(error, connectorConfig)) {
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

    static CdcErrorAction actionFor(CDCErrorPB error, boolean usePublication) {
        if (!error.hasCode()) {
            LOGGER.warn("CDC error has no code set; proto2 getCode() defaults to UNKNOWN_ERROR");
        }
        switch (error.getCode()) {
            case TABLET_SPLIT:
                return CdcErrorAction.HANDLE_IN_STREAM;
            case TABLE_NOT_FOUND:
                // Any publication deployment can see transient TABLE_NOT_FOUND while
                // CDC metadata catches up (including autocreate.mode=disabled, where
                // operators add tables by hand and the table poller reconfigures).
                // Gate only on usePublication — not on autocreate mode.
                return usePublication ? CdcErrorAction.RETRY : CdcErrorAction.FAIL_FAST;
            case INVALID_REQUEST:
                // Not a tablet split. GetChanges uses this for bad requests
                // (InvalidArgument) and some pre-producer failures (e.g. master lookup).
                // A real split is CDC TABLET_SPLIT; retry here so the next poll can
                // receive that code instead of calling handleTabletSplit on a child.
                return actionForAmbiguousCdcCode(error);
            case CHECKPOINT_TOO_OLD:
            case SUBSCRIBER_NOT_FOUND:
            case OPERATION_DISALLOWED:
            case AUTO_FLAGS_CONFIG_VERSION_MISMATCH:
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
                // cdc_service.proto is proto2: an unknown enum value is not kept on
                // getCode(); the field falls back to UNKNOWN_ERROR and is handled
                // above. This branch is only for generated leftovers (e.g. UNRECOGNIZED).
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

    private static boolean hasFatalMessage(CDCErrorPB error) {
        
        return StringUtils.containsAny(statusMessage(error),
                "could not find cdc stream",
                "is not part of stream",
                "not found under stream");
    }

    private static String statusMessage(CDCErrorPB error) {
        return error.hasStatus() ? error.getStatus().getMessage().toLowerCase(Locale.ROOT) : "";
    }
}
