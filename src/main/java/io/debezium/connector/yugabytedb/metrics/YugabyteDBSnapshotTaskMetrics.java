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
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;

/**
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBSnapshotTaskMetrics extends AbstractYugabyteDBTaskMetrics<YugabyteDBSnapshotPartitionMetrics>
        implements SnapshotChangeEventSourceMetrics<YBPartition> {
    public YugabyteDBSnapshotTaskMetrics(CdcSourceTaskContext taskContext,
                                         ChangeEventQueueMetrics changeEventQueueMetrics,
                                         EventMetadataProvider metadataProvider,
                                         Collection<YBPartition> partitions,
                                         YugabyteDBConnectorConfig connectorConfig,
                                         String taskId) {
        super(taskContext, "snapshot", changeEventQueueMetrics, partitions,
                (YBPartition partition) -> new YugabyteDBSnapshotPartitionMetrics(taskContext,
                        Collect.linkMapOf(
                                "server", taskContext.getConnectorName(),
                                "task", taskId,
                                "context", "snapshot",
                                "partition", partition.getFullPartitionName()),
                        metadataProvider), connectorConfig, taskId);
    }

    @Override
    public void snapshotStarted(YBPartition partition) {
        onPartitionEvent(partition, YugabyteDBSnapshotPartitionMetrics::snapshotStarted);
    }

    @Override
    public void monitoredDataCollectionsDetermined(YBPartition partition, Iterable<? extends DataCollectionId> dataCollectionIds) {
        onPartitionEvent(partition, bean -> bean.monitoredDataCollectionsDetermined(dataCollectionIds));
    }

    @Override
    public void snapshotCompleted(YBPartition partition) {
        onPartitionEvent(partition, YugabyteDBSnapshotPartitionMetrics::snapshotCompleted);
    }

    @Override
    public void snapshotAborted(YBPartition partition) {
        onPartitionEvent(partition, YugabyteDBSnapshotPartitionMetrics::snapshotAborted);
    }

    @Override
    public void dataCollectionSnapshotCompleted(YBPartition partition, DataCollectionId dataCollectionId, long numRows) {
        onPartitionEvent(partition, bean -> bean.dataCollectionSnapshotCompleted(dataCollectionId, numRows));
    }

    @Override
    public void rowsScanned(YBPartition partition, TableId tableId, long numRows) {
        onPartitionEvent(partition, bean -> bean.rowsScanned(tableId, numRows));
    }

    @Override
    public void currentChunk(YBPartition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo) {
        onPartitionEvent(partition, bean -> bean.currentChunk(chunkId, chunkFrom, chunkTo));
    }

    @Override
    public void currentChunk(YBPartition partition, String chunkId, Object[] chunkFrom, Object[] chunkTo, Object[] tableTo) {
        onPartitionEvent(partition, bean -> bean.currentChunk(chunkId, chunkFrom, chunkTo, tableTo));
    }

    @Override
    public void snapshotPaused(YBPartition partition) {
        // No-op, as snapshot is not paused in YugabyteDB connector.
        // This method is provided to satisfy the interface contract.
    }

    @Override
    public void snapshotResumed(YBPartition partition) {
        // No-op, as snapshot is not paused and hence it cannot be
        // resumed in YugabyteDB connector.
    }
}
