/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.metrics;

/**
 * GetChanges pipelining metrics of one streaming task (see {@code cdc.poll.max.concurrent.tablets}).
 */
public interface GetChangesPipelineMetricsMXBean {

    /** The configured limit on GetChanges requests in flight for this task. */
    int getMaxConcurrentTablets();

    /** GetChanges requests currently outstanding, including the one being processed. */
    int getInFlightGetChanges();

    /** GetChanges requests issued since the task started. */
    long getTotalGetChangesRequests();

    /** Requests issued in an earlier pass and handed out again instead of being resent. */
    long getReusedGetChangesRequests();

    /** Requests dropped without being processed (tablet split away, task stopping). */
    long getAbandonedGetChangesRequests();

    /** Time from issuing the last processed request to its response arriving. */
    long getLastGetChangesRoundTripMs();

    /** Wall time of the last complete pass over the task's tablets. */
    long getLastPassDurationMs();
}
