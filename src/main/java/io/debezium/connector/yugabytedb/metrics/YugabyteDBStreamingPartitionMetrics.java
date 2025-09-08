/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.metrics;

import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.data.Envelope;
import io.debezium.pipeline.meters.StreamingMeter;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBStreamingPartitionMetrics extends AbstractYugabyteDBPartitionMetrics
        implements YugabyteDBStreamingPartitionMetricsMXBean {

    private final StreamingMeter streamingMeter;

    public YugabyteDBStreamingPartitionMetrics(CdcSourceTaskContext taskContext,
                                               Map<String, String> tags,
                                               EventMetadataProvider metadataProvider) {
        super(taskContext, tags, metadataProvider);
        streamingMeter = new StreamingMeter(taskContext, metadataProvider);
    }

    @Override
    void onEvent(DataCollectionId source, OffsetContext offset, Object key, Struct value, Envelope.Operation operation) {
        super.onEvent(source, offset, key, value, operation);
        streamingMeter.onEvent(source, offset, key, value);
    }

    @Override
    public String[] getCapturedTables() {
        return streamingMeter.getCapturedTables();
    }

    @Override
    public long getMilliSecondsBehindSource() {
        return streamingMeter.getMilliSecondsBehindSource();
    }

    @Override
    public long getNumberOfCommittedTransactions() {
        return streamingMeter.getNumberOfCommittedTransactions();
    }

    @Override
    public Map<String, String> getSourceEventPosition() {
        return streamingMeter.getSourceEventPosition();
    }

    @Override
    public String getLastTransactionId() {
        return streamingMeter.getLastTransactionId();
    }

    @Override
    public void reset() {
        super.reset();
        streamingMeter.reset();
    }

    @Override
    public void resetLagBehindSource() {
        streamingMeter.resetLagBehindSource();
    }
}
