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

import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
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
    public SnapshotResult<YugabyteDBOffsetContext> doExecute(
                                                             ChangeEventSourceContext context, YugabyteDBOffsetContext previousOffset,
                                                             SnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext,
                                                             SnapshottingTask snapshottingTask)
            throws Exception {
        final RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> ctx = (RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext>) snapshotContext;

        // Connection connection = null;
        try {
            LOGGER.info("Snapshot step 1 - Preparing");

            if (previousOffset != null && previousOffset.isSnapshotRunning()) {
                LOGGER.info("Previous snapshot was cancelled before completion; a new snapshot will be taken.");
            }

            // connection = createSnapshotConnection();
            // connectionCreated(ctx);

            LOGGER.info("Snapshot step 2 - Determining captured tables");

            // Note that there's a minor race condition here: a new table matching the filters could be created between
            // this call and the determination of the initial snapshot position below; this seems acceptable, though
            determineCapturedTables(ctx);
            snapshotProgressListener.monitoredDataCollectionsDetermined(ctx.capturedTables);

            LOGGER.info("Snapshot step 3 - Locking captured tables {}", ctx.capturedTables);

            if (snapshottingTask.snapshotSchema()) {
                // lockTablesForSchemaSnapshot(context, ctx);
            }

            LOGGER.info("Snapshot step 4 - Determining snapshot offset");
            determineSnapshotOffset(ctx, previousOffset);

            LOGGER.info("Snapshot step 5 - Reading structure of captured tables");
            readTableStructure(context, ctx, previousOffset);

            if (snapshottingTask.snapshotSchema()) {
                LOGGER.info("Snapshot step 6 - Persisting schema history");

                // createSchemaChangeEventsForTables(context, ctx, snapshottingTask);

                // if we've been interrupted before, the TX rollback will cause any locks to be released
                releaseSchemaSnapshotLocks(ctx);
            }
            else {
                LOGGER.info("Snapshot step 6 - Skipping persisting of schema history");
            }

            if (snapshottingTask.snapshotData()) {
                LOGGER.info("Snapshot step 7 - Snapshotting data");
                createDataEvents(context, ctx);
            }
            else {
                LOGGER.info("Snapshot step 7 - Skipping snapshotting of data");
                // releaseDataSnapshotLocks(ctx);
                ctx.offset.preSnapshotCompletion();
                ctx.offset.postSnapshotCompletion();
            }

            // postSnapshot();
            dispatcher.alwaysDispatchHeartbeatEvent(ctx.partition, ctx.offset);
            return SnapshotResult.completed(ctx.offset);
        }
        finally {
            // rollbackTransaction(connection);
        }
    }

    private void createDataEvents(ChangeEventSourceContext sourceContext,
                                  RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext)
            throws Exception {
        EventDispatcher.SnapshotReceiver snapshotReceiver = dispatcher.getSnapshotChangeEventReceiver();
        // tryStartingSnapshot(snapshotContext);

        final int tableCount = snapshotContext.capturedTables.size();
        int tableOrder = 1;
        LOGGER.info("Snapshotting contents of {} tables while still in transaction", tableCount);
        for (Iterator<TableId> tableIdIterator = snapshotContext.capturedTables.iterator(); tableIdIterator.hasNext();) {
            final TableId tableId = tableIdIterator.next();
            snapshotContext.lastTable = !tableIdIterator.hasNext();

            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while snapshotting table " + tableId);
            }

            LOGGER.debug("Snapshotting table {}", tableId);

            createDataEventsForTable(sourceContext, snapshotContext, snapshotReceiver,
                    snapshotContext.tables.forTable(tableId), tableOrder++, tableCount);
        }

        // releaseDataSnapshotLocks(snapshotContext);
        snapshotContext.offset.preSnapshotCompletion();
        snapshotReceiver.completeSnapshot();
        snapshotContext.offset.postSnapshotCompletion();
    }

    private void createDataEventsForTable(ChangeEventSourceContext sourceContext,
                                          RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext,
                                          EventDispatcher.SnapshotReceiver snapshotReceiver, Table table, int tableOrder,
                                          int tableCount)
            throws InterruptedException {

        long exportStart = clock.currentTimeInMillis();
        LOGGER.info("Exporting data from table '{}' ({} of {} tables)", table.id(), tableOrder, tableCount);

        // final Optional<String> selectStatement = determineSnapshotSelect(snapshotContext, table.id());
        if (true/* !selectStatement.isPresent() */) {
            LOGGER.warn("For table '{}' the select statement was not provided, skipping table", table.id());
            snapshotProgressListener.dataCollectionSnapshotCompleted(table.id(), 0);
            return;
        }
        // LOGGER.info("\t For table '{}' using select statement: '{}'", table.id(), selectStatement.get());
        final OptionalLong rowCount = OptionalLong.empty();// rowCountForTable(table.id());

        // try (Statement statement = readTableStatement(rowCount);
        // ResultSet rs = statement.executeQuery(selectStatement.get())) {

        // ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
        long rows = 0;
        // Threads.Timer logTimer = getTableScanLogTimer();
        snapshotContext.lastRecordInTable = false;

        // if (rs.next()) {
        // while (!snapshotContext.lastRecordInTable) {
        // if (!sourceContext.isRunning()) {
        // throw new InterruptedException("Interrupted while snapshotting table " + table.id());
        // }
        //
        // rows++;
        // final Object[] row = jdbcConnection.rowToArray(table, schema(), rs, columnArray);
        //
        // snapshotContext.lastRecordInTable = !rs.next();
        // if (logTimer.expired()) {
        // long stop = clock.currentTimeInMillis();
        // if (rowCount.isPresent()) {
        // LOGGER.info("\t Exported {} of {} records for table '{}' after {}", rows, rowCount.getAsLong(),
        // table.id(), Strings.duration(stop - exportStart));
        // }
        // else {
        // LOGGER.info("\t Exported {} records for table '{}' after {}", rows, table.id(),
        // Strings.duration(stop - exportStart));
        // }
        // snapshotProgressListener.rowsScanned(table.id(), rows);
        // logTimer = getTableScanLogTimer();
        // }
        //
        // if (snapshotContext.lastTable && snapshotContext.lastRecordInTable) {
        // lastSnapshotRecord(snapshotContext);
        // }
        // dispatcher.dispatchSnapshotEvent(table.id(), getChangeRecordEmitter(snapshotContext, table.id(), row), snapshotReceiver);
        // }
        // }
        // else if (snapshotContext.lastTable) {
        // lastSnapshotRecord(snapshotContext);
        // }

        LOGGER.info("\t Finished exporting {} records for table '{}'; total duration '{}'", rows,
                table.id(), Strings.duration(clock.currentTimeInMillis() - exportStart));
        snapshotProgressListener.dataCollectionSnapshotCompleted(table.id(), rows);
        // }
        // catch (SQLException e) {
        // throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
        // }
    }

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
        return new PostgresSnapshotContext(partition, connectorConfig.databaseName());
    }

    // @Override
    // protected void connectionCreated(RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext)
    // throws Exception {
    // // If using catch up streaming, the connector opens the transaction that the snapshot will eventually use
    // // before the catch up streaming starts. By looking at the current wal location, the transaction can determine
    // // where the catch up streaming should stop. The transaction is held open throughout the catch up
    // // streaming phase so that the snapshot is performed from a consistent view of the data. Since the isolation
    // // level on the transaction used in catch up streaming has already set the isolation level and executed
    // // statements, the transaction does not need to get set the level again here.
    // if (snapshotter.shouldStreamEventsStartingFromSnapshot() && startingSlotInfo == null) {
    // setSnapshotTransactionIsolationLevel();
    // }
    // schema.refresh(jdbcConnection, false);
    // }

    protected Set<TableId> getAllTableIds(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> ctx)
            throws Exception {
        // return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[]{ "TABLE" });
        return new HashSet<>();
    }

    // @Override
    // protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
    // RelationalSnapshotContext<PostgresPartition, PostgresOffsetContext> snapshotContext)
    // throws SQLException, InterruptedException {
    // final Duration lockTimeout = connectorConfig.snapshotLockTimeout();
    // final Optional<String> lockStatement = snapshotter.snapshotTableLockingStatement(lockTimeout, snapshotContext.capturedTables);
    //
    // if (lockStatement.isPresent()) {
    // LOGGER.info("Waiting a maximum of '{}' seconds for each table lock", lockTimeout.getSeconds());
    // jdbcConnection.executeWithoutCommitting(lockStatement.get());
    // // now that we have the locks, refresh the schema
    // schema.refresh(jdbcConnection, false);
    // }
    // }

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

    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext,
                                      YugabyteDBOffsetContext offsetContext)
            throws SQLException, InterruptedException {
        Set<String> schemas = snapshotContext.capturedTables.stream()
                .map(TableId::schema)
                .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        // for (String schema : schemas) {
        // if (!sourceContext.isRunning()) {
        // throw new InterruptedException("Interrupted while reading structure of schema " + schema);
        // }
        //
        // LOGGER.info("Reading structure of schema '{}'", snapshotContext.catalogName);
        // jdbcConnection.readSchema(
        // snapshotContext.tables,
        // snapshotContext.catalogName,
        // schema,
        // connectorConfig.getTableFilters().dataCollectionFilter(),
        // null,
        // false);
        // }
        // schema.refresh(jdbcConnection, false);
    }

    protected SchemaChangeEvent getCreateTableEvent(
                                                    RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> snapshotContext,
                                                    Table table)
            throws SQLException {
        return new SchemaChangeEvent(
                snapshotContext.partition.getSourcePartition(),
                snapshotContext.offset.getOffset(),
                snapshotContext.offset.getSourceInfo(),
                snapshotContext.catalogName,
                table.id().schema(),
                null,
                table,
                SchemaChangeEventType.CREATE,
                true);
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
    private static class PostgresSnapshotContext extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YugabyteDBPartition, YugabyteDBOffsetContext> {

        public PostgresSnapshotContext(YugabyteDBPartition partition, String catalogName) throws SQLException {
            super(partition, catalogName);
        }
    }
}
