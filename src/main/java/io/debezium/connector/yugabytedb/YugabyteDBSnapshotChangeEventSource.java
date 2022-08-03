/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public class YugabyteDBSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource<YugabyteDBPartition, YugabyteDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBSnapshotChangeEventSource.class);
    private final YugabyteDBConnectorConfig connectorConfig;
    private final YugabyteDBSchema schema;
    private final SnapshotProgressListener snapshotProgressListener;
    private final YugabyteDBTaskContext taskContext;
    private final EventDispatcher<TableId> dispatcher;
    protected final Clock clock;
    private final Snapshotter snapshotter;

    public YugabyteDBSnapshotChangeEventSource(YugabyteDBConnectorConfig connectorConfig,
                                               YugabyteDBTaskContext taskContext,
                                               Snapshotter snapshotter,
                                               YugabyteDBSchema schema, EventDispatcher<TableId> dispatcher, Clock clock,
                                               SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.taskContext = taskContext;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotter = snapshotter;
        // this.errorHandler = errorHandler;
        this.snapshotProgressListener = snapshotProgressListener;
    }

    @Override
    public SnapshotResult<YugabyteDBOffsetContext> execute(ChangeEventSourceContext context, YugabyteDBPartition partition, YugabyteDBOffsetContext previousOffset)
            throws InterruptedException {
        SnapshottingTask snapshottingTask = getSnapshottingTask(previousOffset);
        if (snapshottingTask.shouldSkipSnapshot()) {
            LOGGER.debug("Skipping snapshotting");
            return SnapshotResult.skipped(previousOffset);
        }

        delaySnapshotIfNeeded(context);

        final SnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> ctx;
        try {
            ctx = prepare(partition);
        }
        catch (Exception e) {
            LOGGER.error("Failed to initialize snapshot context.", e);
            throw new RuntimeException(e);
        }

        boolean completedSuccessfully = true;

        try {
            snapshotProgressListener.snapshotStarted();
            return doExecute(context, partition, previousOffset, ctx, snapshottingTask);
        }
        catch (InterruptedException e) {
            completedSuccessfully = false;
            LOGGER.warn("Snapshot was interrupted before completion");
            throw e;
        }
        catch (Exception t) {
            completedSuccessfully = false;
            throw new DebeziumException(t);
        }
        finally {
            LOGGER.info("Snapshot - Final stage");
            complete(ctx);

            if (completedSuccessfully) {
                snapshotProgressListener.snapshotCompleted();
            }
            else {
                snapshotProgressListener.snapshotAborted();
            }
        }
    }

    private Stream<TableId> toTableIds(Set<TableId> tableIds, Pattern pattern) {
        return tableIds
                .stream()
                .filter(tid -> pattern.asPredicate().test(connectorConfig.getTableIdMapper().toString(tid)))
                .sorted();
    }

    private Set<TableId> sort(Set<TableId> capturedTables) throws Exception {
        String tableIncludeList = connectorConfig.tableIncludeList();
        if (tableIncludeList != null) {
            return Strings.listOfRegex(tableIncludeList, Pattern.CASE_INSENSITIVE)
                    .stream()
                    .flatMap(pattern -> toTableIds(capturedTables, pattern))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        }
        return capturedTables
                .stream()
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private void determineCapturedTables(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> ctx)
            throws Exception {
        Set<TableId> allTableIds = determineDataCollectionsToBeSnapshotted(getAllTableIds(ctx)).collect(Collectors.toSet());

        Set<TableId> capturedTables = new HashSet<>();
        Set<TableId> capturedSchemaTables = new HashSet<>();

        for (TableId tableId : allTableIds) {
            if (connectorConfig.getTableFilters().eligibleDataCollectionFilter().isIncluded(tableId)) {
                LOGGER.trace("Adding table {} to the list of capture schema tables", tableId);
                capturedSchemaTables.add(tableId);
            }
            if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
                LOGGER.trace("Adding table {} to the list of captured tables", tableId);
                capturedTables.add(tableId);
            }
            else {
                LOGGER.trace("Ignoring table {} as it's not included in the filter configuration", tableId);
            }
        }

        ctx.capturedTables = sort(capturedTables);
        ctx.capturedSchemaTables = capturedSchemaTables
                .stream()
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    protected SnapshotResult<YugabyteDBOffsetContext> doExecute(ChangeEventSourceContext context, YugabyteDBOffsetContext previousOffset,
                                                                SnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext,
                                                                SnapshottingTask snapshottingTask)
            throws Exception {
      return SnapshotResult.skipped(previousOffset);
    }

    protected SnapshotResult<YugabyteDBOffsetContext> doExecute(ChangeEventSourceContext context, YugabyteDBPartition partition, YugabyteDBOffsetContext previousOffset,
                                                                SnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext,
                                                                SnapshottingTask snapshottingTask)
            throws Exception {
      return SnapshotResult.skipped(previousOffset);
      //return null;
    }

    // public SnapshotResult<YugabyteDBOffsetContext> doExecute(
    // ChangeEventSourceContext context, YugabyteDBPartition partition, YugabyteDBOffsetContext offsetContext,
    // SnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext,
    // SnapshottingTask snapshottingTask)
    // throws Exception {
    //
    // // replication slot could exist at the time of starting Debezium so
    // // we will stream from the position in the slot
    // // instead of the last position in the database
    // // Get all partitions
    // // Get
    // Set<YBPartition> partitions = new YugabyteDBPartition.Provider(connectorConfig).getPartitions();
    // boolean hasStartLsnStoredInContext = offsetContext != null && !offsetContext.getTabletSourceInfo().isEmpty();
    //
    // LOGGER.info("SKSK The offset context is " + offsetContext + " partition is " + partition);
    // if (!hasStartLsnStoredInContext) {
    // LOGGER.info("No start opid found in the context.");
    // if (snapshotter.shouldSnapshot()) {
    // offsetContext = YugabyteDBOffsetContext.initialContextForSnapshot(connectorConfig, null, clock, partitions);
    // }
    // else {
    // offsetContext = YugabyteDBOffsetContext.initialContext(connectorConfig, null, clock, partitions);
    // }
    // }
    // /*
    // * if (snapshotter.shouldSnapshot()) {
    // * getSnapshotChanges();
    // * }
    // */
    //
    // try {
    // final WalPositionLocator walPosition;
    //
    // if (hasStartLsnStoredInContext) {
    // // start streaming from the last recorded position in the offset
    // final OpId lsn = offsetContext.lastCompletelyProcessedLsn() != null ? offsetContext.lastCompletelyProcessedLsn() : offsetContext.lsn();
    // LOGGER.info("Retrieved latest position from stored offset '{}'", lsn);
    // walPosition = new WalPositionLocator(offsetContext.lastCommitLsn(), lsn);
    // // replicationStream.compareAndSet(null, replicationConnection.startStreaming(lsn, walPosition));
    // }
    // else {
    // LOGGER.info("No previous LSN found in Kafka, streaming from the latest checkpoint" +
    // " in YugabyteDB");
    // walPosition = new WalPositionLocator();
    // // create snpashot offset.
    //
    // // replicationStream.compareAndSet(null, replicationConnection.startStreaming(walPosition));
    // }
    //
    // // this.lastCompletelyProcessedLsn = replicationStream.get().startLsn();
    //
    // // if (walPosition.searchingEnabled()) {
    // // searchWalPosition(context, stream, walPosition);
    // // try {
    // // if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
    // // connection.commit();
    // // }
    // // }
    // // catch (Exception e) {
    // // LOGGER.info("Commit failed while preparing for reconnect", e);
    // // }
    // // walPosition.enableFiltering();
    // // stream.stopKeepAlive();
    // // replicationConnection.reconnect();
    // // replicationStream.set(replicationConnection.startStreaming(walPosition.getLastEventStoredLsn(), walPosition));
    // // stream = this.replicationStream.get();
    // // stream.startKeepAlive(Threads.newSingleThreadExecutor(PostgresConnector.class, connectorConfig.getLogicalName(), KEEP_ALIVE_THREAD_NAME));
    // // }
    // // processMessages(context, partition, offsetContext, stream);
    // // if (!snapshotter.shouldStream()) {
    // // LOGGER.info("Streaming is not enabled in correct configuration");
    // // return;
    // // }
    // getChanges2(context, partition, offsetContext, hasStartLsnStoredInContext);
    // }
    // catch (Throwable e) {
    // errorHandler.setProducerThrowable(e);
    // }
    // finally {
    //
    // if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
    // // Need to CDCSDK see what can be done.
    // // try {
    // // connection.commit();
    // // }
    // // catch (SQLException throwables) {
    // // throwables.printStackTrace();
    // // }
    // }
    // if (asyncYBClient != null) {
    // try {
    // asyncYBClient.close();
    // }
    // catch (Exception e) {
    // e.printStackTrace();
    // }
    // }
    // if (syncClient != null) {
    // try {
    // syncClient.close();
    // }
    // catch (Exception e) {
    // e.printStackTrace();
    // }
    // }
    // // if (replicationConnection != null) {
    // // LOGGER.debug("stopping streaming...");
    // // // stop the keep alive thread, this also shuts down the
    // // // executor pool
    // // ReplicationStream stream = replicationStream.get();
    // // if (stream != null) {
    // // stream.stopKeepAlive();
    // // }
    // // // TODO author=Horia Chiorean date=08/11/2016 description=Ideally we'd close the stream, but it's not reliable atm (see javadoc)
    // // // replicationStream.close();
    // // // close the connection - this should also disconnect the current stream even if it's blocking
    // // try {
    // // if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
    // // connection.commit();
    // // }
    // // replicationConnection.close();
    // // }
    // // catch (Exception e) {
    // // LOGGER.debug("Exception while closing the connection", e);
    // // }
    // // replicationStream.set(null);
    // // }
    // }
    // return SnapshotResult.completed(offsetContext);
    // }

    @Override
    protected SnapshottingTask getSnapshottingTask(YugabyteDBOffsetContext previousOffset) {
        boolean snapshotSchema = true;
        boolean snapshotData = true;

        snapshotData = snapshotter.shouldSnapshot();
        if (snapshotData) {
            LOGGER.info("According to the connector configuration data will be snapshotted");
        }
        else {
            LOGGER.info("According to the connector configuration no snapshot will be executed");
            snapshotSchema = false;
        }

        return new SnapshottingTask(snapshotSchema, snapshotData);
    }

    @Override
    protected SnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> prepare(YugabyteDBPartition partition)
            throws Exception {
        return new YugabyteDBSnapshotContext(partition, connectorConfig.databaseName());
    }

    protected Set<TableId> getAllTableIds(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> ctx)
            throws Exception {
        // return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[]{ "TABLE" });
        return new HashSet<>();
    }

    protected void releaseSchemaSnapshotLocks(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext)
            throws SQLException {
    }

    protected void determineSnapshotOffset(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> ctx,
                                           YugabyteDBOffsetContext previousOffset)
            throws Exception {
        YugabyteDBOffsetContext offset = ctx.offset;
        if (offset == null) {
            // if (previousOffset != null && !snapshotter.shouldStreamEventsStartingFromSnapshot()) {
            // // The connect framework, not the connector, manages triggering committing offset state so the
            // // replication stream may not have flushed the latest offset state during catch up streaming.
            // // The previousOffset variable is shared between the catch up streaming and snapshot phases and
            // // has the latest known offset state.
            // offset = PostgresOffsetContext.initialContext(connectorConfig, jdbcConnection, getClock(),
            // previousOffset.lastCommitLsn(), previousOffset.lastCompletelyProcessedLsn());
            // }
            // else {
            // offset = PostgresOffsetContext.initialContext(connectorConfig, jdbcConnection, getClock());
            // }
            ctx.offset = offset;
        }

        updateOffsetForSnapshot(offset);
    }

    private void updateOffsetForSnapshot(YugabyteDBOffsetContext offset) throws SQLException {
        final OpId xlogStart = getTransactionStartLsn();
        // final long txId = jdbcConnection.currentTransactionId().longValue();
        // LOGGER.info("Read xlogStart at '{}' from transaction '{}'", xlogStart, txId);
        //
        // // use the old xmin, as we don't want to update it if in xmin recovery
        // offset.updateWalPosition(xlogStart, offset.lastCompletelyProcessedLsn(), clock.currentTime(), String.valueOf(txId), null, offset.xmin());
    }

    protected void updateOffsetForPreSnapshotCatchUpStreaming(YugabyteDBOffsetContext offset) throws SQLException {
        updateOffsetForSnapshot(offset);
        offset.setStreamingStoppingLsn(null/* OpId.valueOf(jdbcConnection.currentXLogLocation()) */);
    }

    // TOOD:CDCSDK get the offset from YB for snapshot.
    private OpId getTransactionStartLsn() throws SQLException {
        // if (slotCreatedInfo != null) {
        // // When performing an exported snapshot based on a newly created replication slot, the txLogStart position
        // // should be based on the replication slot snapshot transaction point. This is crucial so that if any
        // // SQL operations occur mid-snapshot that they'll be properly captured when streaming begins; otherwise
        // // they'll be lost.
        // return slotCreatedInfo.startLsn();
        // }
        // else if (!snapshotter.shouldStreamEventsStartingFromSnapshot() && startingSlotInfo != null) {
        // // Allow streaming to resume from where streaming stopped last rather than where the current snapshot starts.
        // SlotState currentSlotState = jdbcConnection.getReplicationSlotState(null/* connectorConfig.slotName() */,
        // connectorConfig.plugin().getPostgresPluginName());
        // return currentSlotState.slotLastFlushedLsn();
        // }

        return null;// OpId.valueOf(jdbcConnection.currentXLogLocation());
    }

    @Override
    protected void complete(SnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext) {
        snapshotter.snapshotCompleted();
    }

    /**
     * Generate a valid Postgres query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    protected Optional<String> getSnapshotSelect(
                                                 RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {
        return snapshotter.buildSnapshotQuery(tableId, columns);
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class YugabyteDBSnapshotContext extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> {

        public YugabyteDBSnapshotContext(YugabyteDBPartition partition, String catalogName) throws SQLException {
            super(partition, catalogName);
        }
    }
}
