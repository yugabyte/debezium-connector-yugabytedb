/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.util.Locale;

import org.yb.WireProtocol.AppStatusPB.ErrorCode;
import org.yb.cdc.CdcService.CDCErrorPB;
import org.yb.client.CDCErrorException;

/**
 * Classifies CDC server errors before the generic connector retry loop.
 */
final class YugabyteDBCdcErrorClassifier {

    enum CdcErrorAction {
        HANDLE_IN_STREAM,
        FAIL_FAST,
        RETRY
    }

    private YugabyteDBCdcErrorClassifier() {
    }

    static CdcErrorAction actionFor(CDCErrorPB error, YugabyteDBConnectorConfig connectorConfig) {
        return actionFor(error, connectorConfig.publicationAutocreateMode());
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
        return cdcError != null
                && actionFor(cdcError.getCDCError(), connectorConfig) == CdcErrorAction.FAIL_FAST;
    }

    static CdcErrorAction actionFor(CDCErrorPB error,
                                    YugabyteDBConnectorConfig.AutoCreateMode publicationMode) {
        switch (error.getCode()) {
            case TABLET_SPLIT:
                return CdcErrorAction.HANDLE_IN_STREAM;
            case TABLE_NOT_FOUND:
                return shouldRetryTableNotFound(publicationMode)
                        ? CdcErrorAction.RETRY
                        : CdcErrorAction.FAIL_FAST;
            case INVALID_REQUEST:
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
                return isTransientAppStatus(error)
                        ? CdcErrorAction.RETRY
                        : CdcErrorAction.FAIL_FAST;
            default:
                return CdcErrorAction.RETRY;
        }
    }

    private static boolean shouldRetryTableNotFound(YugabyteDBConnectorConfig.AutoCreateMode publicationMode) {
        return publicationMode == YugabyteDBConnectorConfig.AutoCreateMode.ALL_TABLES
                || publicationMode == YugabyteDBConnectorConfig.AutoCreateMode.FILTERED;
    }

    private static CdcErrorAction actionForInvalidRequest(CDCErrorPB error) {
        if (isTabletSplit(error)) {
            return CdcErrorAction.HANDLE_IN_STREAM;
        }

        if (isFatalInvalidRequest(error)) {
            return CdcErrorAction.FAIL_FAST;
        }

        return isTransientAppStatus(error)
                ? CdcErrorAction.RETRY
                : CdcErrorAction.FAIL_FAST;
    }

    private static boolean isTabletSplit(CDCErrorPB error) {
        return error.hasStatus()
                && (error.getStatus().getCode() == ErrorCode.TABLET_SPLIT
                        || statusMessage(error).contains("split"));
    }

    private static boolean isFatalInvalidRequest(CDCErrorPB error) {
        if (!error.hasStatus()) {
            return false;
        }

        ErrorCode appStatusCode = error.getStatus().getCode();
        if (appStatusCode == ErrorCode.INVALID_ARGUMENT
                || appStatusCode == ErrorCode.NOT_AUTHORIZED
                || appStatusCode == ErrorCode.NOT_SUPPORTED
                || appStatusCode == ErrorCode.CONFIGURATION_ERROR
                || appStatusCode == ErrorCode.DELETED
                || appStatusCode == ErrorCode.EXPIRED
                || appStatusCode == ErrorCode.NOT_FOUND) {
            return true;
        }

        String message = statusMessage(error);
        return containsAny(message,
                "could not find cdc stream",
                "invalid stream",
                "invalid cdc stream",
                "stream id",
                "stream_id",
                "deleted stream",
                "deleted subscriber",
                "subscriber not found",
                "invalid subscriber",
                "incorrect tablet",
                "invalid tablet",
                "tablet id",
                "tablet_id",
                "not part of publication",
                "not in publication",
                "not part of cdc stream",
                "not in cdc stream",
                "bad checkpoint",
                "invalid checkpoint",
                "checkpoint too old",
                "unsupported",
                "configuration",
                "permission",
                "not authorized",
                "unauthorized");
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
