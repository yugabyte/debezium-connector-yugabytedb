/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.WireProtocol.AppStatusPB.ErrorCode;
import org.yb.cdc.CdcService.CDCErrorPB;
import org.yb.client.CDCErrorException;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;

/**
 * Classifies CDC errors before the generic connector retry loop.
 */
final class YugabyteDBCdcErrorClassifier {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBCdcErrorClassifier.class);

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
     * publication mode and {@link YugabyteDBConnectorConfig#usesPublication(Configuration)}
     * from raw config — the path tasks use after stream id has been injected.
     */
    static CdcErrorAction actionFor(CDCErrorPB error, Configuration config) {
        YugabyteDBConnectorConfig.AutoCreateMode publicationMode =
                YugabyteDBConnectorConfig.AutoCreateMode.parse(
                        config.getString(YugabyteDBConnectorConfig.PUBLICATION_AUTOCREATE_MODE),
                        YugabyteDBConnectorConfig.DEFAULT_PUBLICATION_AUTOCREATE_MODE);
        return actionFor(error, publicationMode, YugabyteDBConnectorConfig.usesPublication(config));
    }

    static CDCErrorException findCdcError(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof CDCErrorException) {
                return (CDCErrorException) current;
            }
            current = current.getCause();
        }
        return null;
    }

    static boolean isFailFast(Throwable error, YugabyteDBConnectorConfig connectorConfig) {
        CDCErrorException cdcError = findCdcError(error);
        if (cdcError == null) {
            return false;
        }
        return actionFor(cdcError.getCDCError(), connectorConfig) == CdcErrorAction.FAIL_FAST;
    }

    static void throwIfFailFast(Throwable error, YugabyteDBConnectorConfig connectorConfig) throws Exception {
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

        throw error instanceof Exception ? (Exception) error : new DebeziumException(error);
    }

    /**
     * Used by unit tests. {@code FILTERED} and {@code ALL_TABLES} are treated as publication paths.
     */
    static CdcErrorAction actionFor(CDCErrorPB error,
                                    YugabyteDBConnectorConfig.AutoCreateMode publicationMode) {
        return actionFor(error, publicationMode,
                publicationMode != YugabyteDBConnectorConfig.AutoCreateMode.DISABLED);
    }

    static CdcErrorAction actionFor(CDCErrorPB error,
                                    YugabyteDBConnectorConfig.AutoCreateMode publicationMode,
                                    boolean usePublication) {
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
                // Older YugabyteDB signalled tablet splits as CDC INVALID_REQUEST
                // (before CDCErrorPB.TABLET_SPLIT existed). Streaming treats
                // HANDLE_IN_STREAM as "run handleTabletSplit" — same as the
                // historical TABLET_SPLIT || INVALID_REQUEST check. Bare or
                // ambiguous INVALID_REQUEST must not fail-fast or the first
                // split kills the task. Clearly transient AppStatus still retries.
                return actionForInvalidRequest(error);
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
                return actionForUnknownOrInternal(error);
            default:
                // Forward-compatible: a newer server may send codes this connector
                // build does not know. Retry like the old generic catch loop.
                return CdcErrorAction.RETRY;
        }
    }

    private static CdcErrorAction actionForInvalidRequest(CDCErrorPB error) {
        // Prefer retry for unambiguous transport/leader blips. Otherwise hand off
        // to the streaming tablet-split path: keyed on CDC code INVALID_REQUEST
        // (and AppStatus TABLET_SPLIT when present), not free-text messages.
        if (isExplicitlyTransientAppStatus(error)) {
            return CdcErrorAction.RETRY;
        }
        return CdcErrorAction.HANDLE_IN_STREAM;
    }

    private static CdcErrorAction actionForUnknownOrInternal(CDCErrorPB error) {
        // Only trust the AppStatus tablet-split code here — not message substrings.
        if (error.hasStatus() && error.getStatus().getCode() == ErrorCode.TABLET_SPLIT) {
            return CdcErrorAction.HANDLE_IN_STREAM;
        }

        if (hasFatalMessage(error)) {
            return CdcErrorAction.FAIL_FAST;
        }

        return isTransientAppStatus(error)
                ? CdcErrorAction.RETRY
                : CdcErrorAction.FAIL_FAST;
    }

    private static boolean isExplicitlyTransientAppStatus(CDCErrorPB error) {
        if (!error.hasStatus()) {
            return false;
        }
        switch (error.getStatus().getCode()) {
            case SERVICE_UNAVAILABLE:
            case TIMED_OUT:
            case ABORTED:
            case TRY_AGAIN_CODE:
            case BUSY:
            case LEADER_NOT_READY_TO_SERVE:
            case LEADER_HAS_NO_LEASE:
            case CACHE_MISS_ERROR:
                return true;
            default:
                return false;
        }
    }

    private static boolean hasFatalMessage(CDCErrorPB error) {
        // Do not treat "is expired for tablet" as fatal: that message is also returned when
        // SetCheckpoint has not been issued yet and is cured by makeStreamActive / bootstrap
        // retries (see YugabyteDBSnapshotChangeEventSource#isSnapshotRequired).
        return containsAny(statusMessage(error),
                "could not find cdc stream",
                "is not part of stream",
                "not found under stream");
    }

    private static boolean isTransientAppStatus(CDCErrorPB error) {
        if (!error.hasStatus()) {
            return true;
        }

        switch (error.getStatus().getCode()) {
            case SERVICE_UNAVAILABLE:
            case TIMED_OUT:
            case ABORTED:
            case TRY_AGAIN_CODE:
            case BUSY:
            case LEADER_NOT_READY_TO_SERVE:
            case LEADER_HAS_NO_LEASE:
            case CACHE_MISS_ERROR:
                return true;
            case INVALID_ARGUMENT:
            case RUNTIME_ERROR:
            case NOT_AUTHORIZED:
            case NOT_SUPPORTED:
            case CONFIGURATION_ERROR:
            case DELETED:
            case EXPIRED:
            case NOT_FOUND:
                return false;
            default:
                return true;
        }
    }

    private static String statusMessage(CDCErrorPB error) {
        return error.hasStatus() ? error.getStatus().getMessage().toLowerCase(Locale.ROOT) : "";
    }

    private static boolean containsAny(String value, String... tokens) {
        for (String token : tokens) {
            if (value.contains(token)) {
                return true;
            }
        }
        return false;
    }
}
