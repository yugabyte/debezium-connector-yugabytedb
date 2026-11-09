/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.metrics;

import java.util.concurrent.atomic.AtomicLong;

import io.debezium.connector.yugabytedb.GetChangesPipelineStats;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig;

/**
 * Registered as {@code type=connector-metrics, server=..., task=..., context=getchanges}, next to the
 * task's streaming metrics, so existing JMX exporter rules pick it up.
 */
public class GetChangesPipelineMetrics extends YugabyteDBMetrics implements GetChangesPipelineMetricsMXBean, GetChangesPipelineStats {

    private final int maxConcurrentTablets;
    private volatile int inFlight;
    private final AtomicLong total = new AtomicLong();
    private final AtomicLong reused = new AtomicLong();
    private final AtomicLong abandoned = new AtomicLong();
    private volatile long lastRoundTripMs;
    private volatile long lastPassDurationMs;

    public GetChangesPipelineMetrics(String taskId, YugabyteDBConnectorConfig connectorConfig) {
        super(taskId, connectorConfig, "getchanges", true /* multiPartitionMode */);
        this.maxConcurrentTablets = connectorConfig.cdcPollMaxConcurrentTablets();
    }

    @Override
    public void inFlight(int count) {
        inFlight = count;
    }

    @Override
    public void issued() {
        total.incrementAndGet();
    }

    @Override
    public void reused() {
        reused.incrementAndGet();
    }

    @Override
    public void abandoned(int count) {
        abandoned.addAndGet(count);
    }

    public void roundTrip(long millis) {
        lastRoundTripMs = millis;
    }

    public void passDuration(long millis) {
        lastPassDurationMs = millis;
    }

    @Override
    public int getMaxConcurrentTablets() {
        return maxConcurrentTablets;
    }

    @Override
    public int getInFlightGetChanges() {
        return inFlight;
    }

    @Override
    public long getTotalGetChangesRequests() {
        return total.get();
    }

    @Override
    public long getReusedGetChangesRequests() {
        return reused.get();
    }

    @Override
    public long getAbandonedGetChangesRequests() {
        return abandoned.get();
    }

    @Override
    public long getLastGetChangesRoundTripMs() {
        return lastRoundTripMs;
    }

    @Override
    public long getLastPassDurationMs() {
        return lastPassDurationMs;
    }
}
