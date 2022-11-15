/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.metrics;

import java.util.Collection;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.yugabytedb.YBPartition;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

/**
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBMetricsFactory implements ChangeEventSourceMetricsFactory<YBPartition> {

    private final Collection<YBPartition> partitions;

    public YugabyteDBMetricsFactory(Collection<YBPartition> partitions) {
        this.partitions = partitions;
    }

    @Override
    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics<YBPartition> getSnapshotMetrics(T taskContext,
                                                                                                             ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                             EventMetadataProvider eventMetadataProvider) {
        return new YugabyteDBSnapshotTaskMetrics(taskContext, changeEventQueueMetrics,
                                                 eventMetadataProvider, partitions);
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<YBPartition> getStreamingMetrics(T taskContext,
                                                                                                               ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                               EventMetadataProvider eventMetadataProvider) {
        return new YugabyteDBStreamingTaskMetrics(taskContext, changeEventQueueMetrics,
                                                  eventMetadataProvider, partitions);
    }
}
