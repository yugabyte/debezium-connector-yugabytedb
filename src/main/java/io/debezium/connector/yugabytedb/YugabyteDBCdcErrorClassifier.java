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
import org.yb.client.MasterErrorException;
import org.yb.master.MasterTypes.MasterErrorPB;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;

/**
 * Classifies CDC and master errors before the generic connector retry loop.
 */
final class YugabyteDBCdcErrorClassifier {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBCdcErrorClassifier.class);

    enum CdcErrorAction {
        HANDLE_IN_STREAM,
        FAIL_FAST,
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
        if (cdcError != null) {
            return actionFor(cdcError.getCDCError(), connectorConfig) == CdcErrorAction.FAIL_FAST;
        }
        return isFatalMasterError(error);
    }

    static void throwIfFailFast(Throwable error, YugabyteDBConnectorConfig connectorConfig) throws Exception {
        if (!isFailFast(error, connectorConfig)) {
            return;
        }

        CDCErrorException cdcException = findCdcError(error);
        if (cdcException != null) {
            LOGGER.error("Failing fast for non-retriable CDC error from YugabyteDB. code={}, status={}",
                    cdcException.getCDCError().getCode(),
                    cdcException.getCDCError().hasStatus()
                            ? cdcException.getCDCError().getStatus()
                            : "none",
                    error);
        }
        else {
            LOGGER.error("Failing fast for non-retriable YugabyteDB error", error);
        }

        throw error instanceof Exception ? (Exception) error : new DebeziumException(error);
    }

    /**
     * True only when the server reported an actual tablet split (CDC
     * {@code TABLET_SPLIT}, or AppStatus {@code TABLET_SPLIT}). Used by paths
     * that historically handled {@code TABLET_SPLIT} only and must not treat
     * bare {@code INVALID_REQUEST} as a split.
     */
    static boolean isTabletSplit(CDCErrorPB error) {
        if (error.getCode() == CDCErrorPB.Code.TABLET_SPLIT) {
            return true;
        }
        return error.hasStatus() && error.getStatus().getCode() == ErrorCode.TABLET_SPLIT;
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
                // main intentionally stopped treating bare INVALID_REQUEST as a
                // tablet split (transient GetChanges failures use this code).
                // Only HANDLE_IN_STREAM when AppStatus explicitly says TABLET_SPLIT;
                // otherwise retry (or fail-fast for clearly permanent AppStatus).
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
        // Confirmed split via AppStatus — handle in stream.
        if (error.hasStatus() && error.getStatus().getCode() == ErrorCode.TABLET_SPLIT) {
            return CdcErrorAction.HANDLE_IN_STREAM;
        }

        // Transient transport/leader issues — retry.
        if (isExplicitlyTransientAppStatus(error)) {
            return CdcErrorAction.RETRY;
        }

        // Permanent request failures — fail fast.
        if (error.hasStatus() && (isFatalAppStatus(error) || hasFatalMessage(error))) {
            return CdcErrorAction.FAIL_FAST;
        }

        // Bare / ambiguous INVALID_REQUEST: retry like main (do not assume split).
        return CdcErrorAction.RETRY;
    }

    private static boolean isFatalAppStatus(CDCErrorPB error) {
        if (!error.hasStatus()) {
            return false;
        }
        ErrorCode appStatusCode = error.getStatus().getCode();
        return appStatusCode == ErrorCode.INVALID_ARGUMENT
                || appStatusCode == ErrorCode.NOT_AUTHORIZED
                || appStatusCode == ErrorCode.NOT_SUPPORTED
                || appStatusCode == ErrorCode.CONFIGURATION_ERROR
                || appStatusCode == ErrorCode.DELETED
                || appStatusCode == ErrorCode.EXPIRED
                || appStatusCode == ErrorCode.NOT_FOUND
                || appStatusCode == ErrorCode.RUNTIME_ERROR;
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

    static boolean isFatalMasterError(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof MasterErrorException) {
                MasterErrorException masterError = (MasterErrorException) current;
                // Only trust explicit permanent master codes. Free-text matching
                // ("does not exist") is too broad: stale tserver-cache lookups during
                // bootstrap can surface similar wording and must still be retried.
                return masterError.error != null && isFatalMasterCode(masterError.error.getCode());
            }
            current = current.getCause();
        }
        return false;
    }

    private static boolean isFatalMasterCode(MasterErrorPB.Code code) {
        // INVALID_REQUEST is deliberately omitted: it can appear from transient
        // master/tserver cache inconsistency and should use the connector retry budget.
        return code == MasterErrorPB.Code.OBJECT_NOT_FOUND
                || code == MasterErrorPB.Code.NAMESPACE_NOT_FOUND
                || code == MasterErrorPB.Code.TYPE_NOT_FOUND
                || code == MasterErrorPB.Code.ROLE_NOT_FOUND
                || code == MasterErrorPB.Code.NOT_AUTHORIZED;
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
