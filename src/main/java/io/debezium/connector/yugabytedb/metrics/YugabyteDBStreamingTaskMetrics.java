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
import io.debezium.pipeline.meters.ConnectionMeter;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.util.Collect;

/**
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBStreamingTaskMetrics extends AbstractYugabyteDBTaskMetrics<YugabyteDBStreamingPartitionMetrics>
        implements StreamingChangeEventSourceMetrics<YBPartition>, YugabyteDBStreamingTaskMetricsMXBean {

    private final ConnectionMeter connectionMeter;

    public YugabyteDBStreamingTaskMetrics(CdcSourceTaskContext taskContext,
                                          ChangeEventQueueMetrics changeEventQueueMetrics,
                                          EventMetadataProvider metadataProvider,
                                          Collection<YBPartition> partitions,
                                          YugabyteDBConnectorConfig connectorConfig,
                                          String taskId) {
        super(taskContext, "streaming", changeEventQueueMetrics, partitions,
                (YBPartition partition) -> new YugabyteDBStreamingPartitionMetrics(taskContext,
                    Collect.linkMapOf(
                        "server", taskContext.getConnectorName(),
                        "task", taskId,
                        "context", "streaming",
                        "tablet", partition.getTabletId()),
                    metadataProvider), connectorConfig, taskId);
        connectionMeter = new ConnectionMeter();
    }

    @Override
    public boolean isConnected() {
        return connectionMeter.isConnected();
    }

    @Override
    public void connected(boolean connected) {
        connectionMeter.connected(connected);
    }
}
