/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.yugabytedb.spi.SlotState;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.LoggingContext.PreviousContext;

/**
 * Coordinates one or more {@link ChangeEventSource}s and executes them in order. Extends the base
 * {@link ChangeEventSourceCoordinator} to support a pre-snapshot catch up streaming phase.
 * 
 * @author Suranjan Kumar, Rajat Venkatesh, Vaibhav Kushwaha
 */
public class YugabyteDBChangeEventSourceCoordinator extends ChangeEventSourceCoordinator<YBPartition, YugabyteDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBChangeEventSourceCoordinator.class);

    private final Snapshotter snapshotter;
    private final SlotState slotInfo;

    private YugabyteDBSnapshotChangeEventSource snapshotSource;

    public YugabyteDBChangeEventSourceCoordinator(Offsets<YBPartition, YugabyteDBOffsetContext> previousOffsets,
                                                  ErrorHandler errorHandler,
                                                  Class<? extends SourceConnector> connectorType,
                                                  CommonConnectorConfig connectorConfig,
                                                  YugabyteDBChangeEventSourceFactory changeEventSourceFactory,
                                                  ChangeEventSourceMetricsFactory changeEventSourceMetricsFactory,
                                                  EventDispatcher<YBPartition, ?> eventDispatcher, DatabaseSchema<?> schema,
                                                  Snapshotter snapshotter, SlotState slotInfo) {
        super(previousOffsets, errorHandler, connectorType, connectorConfig, changeEventSourceFactory,
                changeEventSourceMetricsFactory, eventDispatcher, schema);
        this.snapshotter = snapshotter;
        this.slotInfo = slotInfo;
    }

    @Override
    protected CatchUpStreamingResult executeCatchUpStreaming(ChangeEventSourceContext context,
                                                             SnapshotChangeEventSource<YBPartition, YugabyteDBOffsetContext> snapshotSource,
                                                             YBPartition partition,
                                                             YugabyteDBOffsetContext previousOffset)
            throws InterruptedException {
        if (previousOffset != null && !snapshotter.shouldStreamEventsStartingFromSnapshot() && slotInfo != null) {
            try {
                setSnapshotStartLsn((YugabyteDBSnapshotChangeEventSource) snapshotSource,
                        previousOffset);
            }
            catch (SQLException e) {
                throw new DebeziumException("Failed to determine catch-up streaming stopping LSN");
            }
            LOGGER.info("Previous connector state exists and will stream events until {} then perform snapshot",
                    previousOffset.getStreamingStoppingLsn());
            streamEvents(context, partition, previousOffset);
            return new CatchUpStreamingResult(true);
        }

        return new CatchUpStreamingResult(false);
    }

    @Override
    protected void executeChangeEventSources(CdcSourceTaskContext taskContext,
      SnapshotChangeEventSource<YBPartition, YugabyteDBOffsetContext> snapshotSource,
      Offsets<YBPartition, YugabyteDBOffsetContext> previousOffsets,
      AtomicReference<PreviousContext> previousLogContext, ChangeEventSourceContext context)
      throws InterruptedException {
        Offsets<YBPartition, YugabyteDBOffsetContext> streamingOffsets =
            Offsets.of(new HashMap<>());
        this.snapshotSource = (YugabyteDBSnapshotChangeEventSource) snapshotSource;

        LOGGER.info("Performing the snapshot process now");
        for (Map.Entry<YBPartition, YugabyteDBOffsetContext> entry :
                 previousOffsets.getOffsets().entrySet()) {
            YBPartition partition = entry.getKey();
            YugabyteDBOffsetContext previousOffset = entry.getValue();

            LOGGER.info("YBPartition is {} and YugabyteDBOffsetContext while snapshot is {}",
                        partition, previousOffset);

            previousLogContext.set(taskContext.configureLoggingContext(
                String.format("snapshot|%s", taskContext.getTaskId()), partition));
            SnapshotResult<YugabyteDBOffsetContext> snapshotResult =
                doSnapshot(snapshotSource, context, partition, previousOffset);

            if (snapshotResult.isCompletedOrSkipped()) {
                streamingOffsets.getOffsets().put(partition, snapshotResult.getOffset());

                // Further down the processing unit, we are retrieving all the partitions even
                // though we pass just one at this level, so in case the snapshot gets completed
                // for one, it would be safe to break out of this loop to avoid processing things
                // again.
                break;
            }
        }

        // This is to handle the initial_only snapshot mode where we will not go to the streaming mode.
        if (!snapshotter.shouldStream()) {
            LOGGER.info("Snapshot complete for initial_only mode for task {}", taskContext.getTaskId());
            return;
        }

        previousLogContext.set(taskContext.configureLoggingContext(
            String.format("streaming|%s", taskContext.getTaskId())));

        LOGGER.info("Snapshot flag upon transition for task {}: {}", taskContext.getTaskId(), isSnapshotInProgress());

        for (Map.Entry<YBPartition, YugabyteDBOffsetContext> entry :
                streamingOffsets.getOffsets().entrySet()) {
            initStreamEvents(entry.getKey(), entry.getValue());
        }

        LOGGER.info("Performing the streaming process now");

        while (context.isRunning()) {
            for (Map.Entry<YBPartition, YugabyteDBOffsetContext> entry :
                     streamingOffsets.getOffsets().entrySet()) {
                YBPartition partition = entry.getKey();
                YugabyteDBOffsetContext previousOffset = entry.getValue();

                LOGGER.info("YBPartition is {} and YugabyteDBOffsetContext while streaming is {}",
                            partition, previousOffset);

                previousLogContext.set(taskContext.configureLoggingContext(
                    String.format("streaming|%s", taskContext.getTaskId()), partition));

                if (context.isRunning()) {
                    streamEvents(context, partition, previousOffset);
                }
            }
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // Check if snapshotter is enabled, if it is not then callback should go to the
        // streaming source only. If snapshot is complete, even then the callback should go to the
        // streaming source as in case of a finished snapshot, we do not want to do a duplicate call
        // for commitOffset.
        if (!commitOffsetLock.isLocked() && snapshotter.shouldSnapshot() && !snapshotSource.isSnapshotComplete()) {
            snapshotSource.commitOffset(offset);
            return;
        }

        if (!commitOffsetLock.isLocked() && streamingSource != null && offset != null) {
            streamingSource.commitOffset(offset);
        }
    }

    protected boolean isSnapshotInProgress() {
        return snapshotter.shouldSnapshot() && !snapshotSource.isSnapshotComplete();
    }

    private void setSnapshotStartLsn(YugabyteDBSnapshotChangeEventSource snapshotSource,
                                     YugabyteDBOffsetContext offsetContext)
            throws SQLException {
        snapshotSource.updateOffsetForPreSnapshotCatchUpStreaming(offsetContext);
    }

}
