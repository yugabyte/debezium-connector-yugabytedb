/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
    private YugabyteDBStreamingChangeEventSource streamingChangeEventSource;

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
    protected void streamEvents(ChangeEventSourceContext context, YBPartition partition,
            YugabyteDBOffsetContext offsetContext) throws InterruptedException {
        initStreamEvents(partition, offsetContext);
        LOGGER.info("Starting streaming");

        this.streamingChangeEventSource = (YugabyteDBStreamingChangeEventSource) streamingSource;

        streamingSource.execute(context, partition, offsetContext);
        LOGGER.info("Finished streaming");
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        if (snapshotSource == null) {
            return;
        }

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

    /**
     * @return the set of partitions i.e. {@link YBPartition} being in the streaming phase at a
     * given point in time. If streamingChangeEventSource is null that means we are still in the
     * snapshot phase and in that case it should be safe to return an {@link Optional#empty()}
     * which should be handled by the caller of this method.
     */
    public Optional<Set<YBPartition>> getPartitions() {
        if (this.streamingChangeEventSource == null) {
            LOGGER.info("Streaming change event source is null, returning empty value");
            return Optional.empty();
        }

        Optional<Set<YBPartition>> ybPartitions =
          Optional.of(this.streamingChangeEventSource.getActivePartitionsBeingPolled());

        // There's a small window during connector/task startup phase when the partitions being
        // returned from the streaming source can be empty owing to the fact that it has not been
        // populated yet. In that case, treat it as the streaming source itself has not been
        // initialised.
        if (ybPartitions.isPresent() && ybPartitions.get().size() == 0) {
            // todo: See if it is possible to add a task ID in the log statement.
            LOGGER.info("Received an empty set of partitions from streaming source, will use config to get partitions");
            ybPartitions = Optional.empty();
        }

        return ybPartitions;
    }

    /**
     * @return true if the connector is in snapshot phase, false otherwise
     */
    protected boolean isSnapshotInProgress() {
        return snapshotter.shouldSnapshot()
                 && (snapshotSource != null)
                 && !snapshotSource.isSnapshotComplete();
    }

    private void setSnapshotStartLsn(YugabyteDBSnapshotChangeEventSource snapshotSource,
                                     YugabyteDBOffsetContext offsetContext)
            throws SQLException {
        snapshotSource.updateOffsetForPreSnapshotCatchUpStreaming(offsetContext);
    }

}
