/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;
import org.yb.client.AsyncYBClient;
import org.yb.client.GetChangesResponse;
import org.yb.client.GetCheckpointResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import io.debezium.DebeziumException;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.connection.ReplicationMessage.Operation;
import io.debezium.connector.yugabytedb.connection.pgproto.YbProtoReplicationMessage;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;

/**
 * Class to help processing the snapshot in YugabyteDB.
 *
 * @author Suranjan Kumar
 */

public class YugabyteDBSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource<YBPartition, YugabyteDBOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBSnapshotChangeEventSource.class);
    private final YugabyteDBConnectorConfig connectorConfig;
    private final YugabyteDBSchema schema;
    private final SnapshotProgressListener snapshotProgressListener;
    private final YugabyteDBTaskContext taskContext;
    private final EventDispatcher<YBPartition,TableId> dispatcher;
    protected final Clock clock;
    private final Snapshotter snapshotter;

    private final AsyncYBClient asyncClient;
    private final YBClient syncClient;

    private OpId lastCompletelyProcessedLsn;

    private YugabyteDBTypeRegistry yugabyteDbTypeRegistry;

    public YugabyteDBSnapshotChangeEventSource(YugabyteDBConnectorConfig connectorConfig,
                                               YugabyteDBTaskContext taskContext,
                                               Snapshotter snapshotter,
                                               YugabyteDBSchema schema, YugabyteDBEventDispatcher<TableId> dispatcher, Clock clock,
                                               SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.taskContext = taskContext;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotter = snapshotter;
        this.snapshotProgressListener = snapshotProgressListener;

        this.asyncClient = new AsyncYBClient.AsyncYBClientBuilder(connectorConfig.masterAddresses())
            .defaultAdminOperationTimeoutMs(connectorConfig.adminOperationTimeoutMs())
            .defaultOperationTimeoutMs(connectorConfig.operationTimeoutMs())
            .defaultSocketReadTimeoutMs(connectorConfig.socketReadTimeoutMs())
            .numTablets(connectorConfig.maxNumTablets())
            .sslCertFile(connectorConfig.sslRootCert())
            .sslClientCertFiles(connectorConfig.sslClientCert(), connectorConfig.sslClientKey())
            .build();
        
        this.syncClient = new YBClient(this.asyncClient);

        this.yugabyteDbTypeRegistry = taskContext.schema().getTypeRegistry();
    }

    @Override
    public SnapshotResult<YugabyteDBOffsetContext> execute(ChangeEventSourceContext context, YBPartition partition, YugabyteDBOffsetContext previousOffset)
            throws InterruptedException {
        SnapshottingTask snapshottingTask = getSnapshottingTask(partition, previousOffset);
        LOGGER.debug("Dispatcher in snapshot: " + dispatcher.toString());
        if (snapshottingTask.shouldSkipSnapshot()) {
            LOGGER.debug("Skipping snapshotting");
            return SnapshotResult.skipped(previousOffset);
        }

        delaySnapshotIfNeeded(context);

        final SnapshotContext<YBPartition, YugabyteDBOffsetContext> ctx;
        try {
            ctx = prepare(partition);
        }
        catch (Exception e) {
            LOGGER.error("Failed to initialize snapshot context.", e);
            throw new RuntimeException(e);
        }

        boolean completedSuccessfully = true;

        try {
            snapshotProgressListener.snapshotStarted(partition);
            Set<YBPartition> partitions = new YBPartition.Provider(connectorConfig).getPartitions();

            LOGGER.info("Setting offsetContext/previousOffset for snapshot...");
            previousOffset = YugabyteDBOffsetContext.initialContextForSnapshot(this.connectorConfig, clock, partitions);

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
                snapshotProgressListener.snapshotCompleted(partition);
            }
            else {
                snapshotProgressListener.snapshotAborted(partition);
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
        String tableIncludeList = this.connectorConfig.tableIncludeList();
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

    private Map<TableId, String> determineTablesForSnapshot(Map<String, YBTable> tableIdToTable) throws Exception {
      Map<TableId, String> res = new HashMap<>();

      Set<TableId> dbzTableIds = new HashSet<>();
      
      for (Entry<String, YBTable> entry : tableIdToTable.entrySet()) {
        res.put(
            YBClientUtils.getTableIdFromYbTable(this.syncClient, entry.getValue()), entry.getKey());
      }

      dbzTableIds = res.entrySet().stream().map(entry -> entry.getKey())
                        .collect(Collectors.toSet());

      // Get a list of filtered tables which are to be snapshotted
      Set<TableId> filteredTables = getDataCollectionsToBeSnapshotted(dbzTableIds)
                                        .collect(Collectors.toSet());

      res.keySet().removeIf(tableId -> !filteredTables.contains(tableId));
      
      return res;
    }

    @Override
    protected SnapshotResult<YugabyteDBOffsetContext> doExecute(ChangeEventSourceContext context, YugabyteDBOffsetContext previousOffset,
                                                                SnapshotContext<YBPartition, YugabyteDBOffsetContext> snapshotContext,
                                                                SnapshottingTask snapshottingTask)
            throws Exception {
      return SnapshotResult.skipped(previousOffset);
    }

    protected void setCheckpointWithRetryBeforeSnapshot(
        String tableId, String tabletId, Set<String> snapshotCompletedTablets, 
        Set<String> snapshotCompletedPreviously) throws Exception {
      short retryCount = 0;
      try {
        if (hasSnapshotCompletedPreviously(tableId, tabletId)) {
          LOGGER.info("Skipping snapshot for tablet {} since tablet has streamed some data before", 
                      tabletId);
          snapshotCompletedTablets.add(tabletId);
          snapshotCompletedPreviously.add(tabletId);
        } else {
          YBClientUtils.setCheckpoint(this.syncClient, 
                                      this.connectorConfig.streamId(), 
                                      tableId /* tableId */, 
                                      tabletId /* tabletId */, 
                                      0 /* term */, 0 /* index */,
                                      false /* initialCheckpoint */, false /* bootstrap */);
        }

        // Reaching this point would mean that the process went through without failure so reset
        // the retry counter here.
        retryCount = 0;
      } catch (Exception e) {
        ++retryCount;

        if (retryCount > this.connectorConfig.maxConnectorRetries()) {
          LOGGER.error("Too many errors while trying to set checkpoint, "
                        + "all {} retries failed.", this.connectorConfig.maxConnectorRetries());

          throw e;
        }

        LOGGER.warn("Error while trying to set the checkpoint; will attempt " 
                    + "retry {} of {} after {} milli-seconds. Exception message: {}", retryCount, 
                      this.connectorConfig.maxConnectorRetries(), 
                      this.connectorConfig.connectorRetryDelayMs(), e.getMessage());
        LOGGER.debug("Stacktrace: ", e);

        try {
          final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
          retryMetronome.pause();
        } catch (InterruptedException ie) {
          LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
          Thread.currentThread().interrupt();
        }
      }
    }

    protected SnapshotResult<YugabyteDBOffsetContext> doExecute(ChangeEventSourceContext context, YBPartition partition, YugabyteDBOffsetContext previousOffset,
                                                                SnapshotContext<YBPartition, YugabyteDBOffsetContext> snapshotContext,
                                                                SnapshottingTask snapshottingTask)
            throws Exception {
      LOGGER.info("Starting the snapshot process now");
      
      // Get the list of tablets
      List<Pair<String, String>> tableToTabletIds = null;
      try {
        String tabletList = this.connectorConfig.getConfig().getString(YugabyteDBConnectorConfig.TABLET_LIST);
        tableToTabletIds = (List<Pair<String, String>>) ObjectUtil.deserializeObjectFromString(tabletList);
      } catch (Exception e) {
        LOGGER.error("The tablet list cannot be deserialized");
        throw new DebeziumException(e);
      }

      Map<String, YBTable> tableIdToTable = new HashMap<>();
      Set<String> tableUUIDs = tableToTabletIds.stream()
                                  .map(pair -> pair.getLeft())
                                  .collect(Collectors.toSet());
      for (String tableUUID : tableUUIDs) {
        tableIdToTable.put(tableUUID, this.syncClient.openTableByUUID(tableUUID));
      }

      Map<TableId, String> filteredTableIdToUuid = determineTablesForSnapshot(tableIdToTable);

      Map<String, Boolean> schemaNeeded = new HashMap<>();
      Set<String> snapshotCompletedTablets = new HashSet<>();
      Set<String> snapshotCompletedPreviously = new HashSet<>();

      for (Pair<String, String> entry : tableToTabletIds) {
        schemaNeeded.put(entry.getValue(), Boolean.TRUE);

        previousOffset.initSourceInfo(entry.getValue(), this.connectorConfig);
        LOGGER.debug("Previous offset for tablet {} is {}", entry.getValue(), previousOffset.toString());
      }

      List<Pair<String, String>> tableToTabletForSnapshot = new ArrayList<>();
      List<Pair<String, String>> tableToTabletNoSnapshot = new ArrayList<>();

      for (Pair<String, String> entry : tableToTabletIds) {
        String tableUuid = entry.getKey();
        String tabletUuid = entry.getValue();

        if (filteredTableIdToUuid.containsValue(tableUuid)) {
          // This means we need to add this table/tablet for snapshotting
          tableToTabletForSnapshot.add(entry);
        } else {
          tableToTabletNoSnapshot.add(entry);
          // Bootstrap the tablets if they need not be snapshotted
          LOGGER.info(
            "Skipping the tablet {} since it is not a part of the snapshot collection include list",
            tabletUuid);
          YBClientUtils.setCheckpoint(this.syncClient, this.connectorConfig.streamId(), 
                                      tableUuid, tabletUuid, -1, -1, true, true);
        }
      }

      // Set checkpoint with bootstrap and initialCheckpoint as false.
      // A call to set the checkpoint is required first otherwise we will get an error 
      // from the server side saying:
      // INTERNAL_ERROR[code 21]: Stream ID {} is expired for Tablet ID {}
      for (Pair<String, String> entry : tableToTabletForSnapshot) {
        setCheckpointWithRetryBeforeSnapshot(entry.getKey() /*tableId*/,
                                             entry.getValue() /*tabletId*/,
                                             snapshotCompletedTablets,
                                             snapshotCompletedPreviously);
      }

      short retryCount = 0;

      // Helper internal variable to log GetChanges request at regular intervals.
      long lastLoggedTimeForGetChanges = System.currentTimeMillis();

      while (context.isRunning() && retryCount <= this.connectorConfig.maxConnectorRetries()) {
        try {
            while (context.isRunning() && (previousOffset.getStreamingStoppingLsn() == null)) {
              for (Pair<String, String> tableIdToTabletId : tableToTabletForSnapshot) {
                // Pause for the specified duration before asking for a new set of snapshot records from the server
                LOGGER.debug("Pausing for {} milliseconds before polling further", connectorConfig.cdcPollIntervalms());
                final Metronome pollIntervalMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.cdcPollIntervalms()), Clock.SYSTEM);
                pollIntervalMetronome.pause();

                String tableId = tableIdToTabletId.getKey();
                YBTable table = tableIdToTable.get(tableId);

                String tabletId = tableIdToTabletId.getValue();
                YBPartition part = new YBPartition(tabletId);
                
                 // Check if snapshot is completed here, if it is, then break out of the loop
                if (snapshotCompletedTablets.size() == tableToTabletForSnapshot.size()) {
                    LOGGER.info("Snapshot completed for all the tablets");
                    return SnapshotResult.completed(previousOffset);
                }

                // Skip the tablet if snapshot has already been taken for this tablet
                if (snapshotCompletedTablets.contains(tabletId)) {
                  continue;
                }

                OpId cp = previousOffset.snapshotLSN(tabletId);

                if (LOGGER.isDebugEnabled()
                    || (connectorConfig.logGetChanges() && System.currentTimeMillis() >= (lastLoggedTimeForGetChanges + connectorConfig.logGetChangesIntervalMs()))) {
                  LOGGER.info("Requesting changes for tablet {} from OpId {} for table {}",
                              tabletId, cp, table.getName());
                  lastLoggedTimeForGetChanges = System.currentTimeMillis();
                }

                if (!context.isRunning()) {
                  LOGGER.info("Connector has been stopped");
                  break;
                }
                
                GetChangesResponse resp = this.syncClient.getChangesCDCSDK(table, 
                    connectorConfig.streamId(), tabletId, cp.getTerm(), cp.getIndex(), cp.getKey(), 
                    cp.getWrite_id(), cp.getTime(), schemaNeeded.get(tabletId));
                
                // Process the response
                for (CdcService.CDCSDKProtoRecordPB record : 
                        resp.getResp().getCdcSdkProtoRecordsList()) {
                  CdcService.RowMessage m = record.getRowMessage();
                  YbProtoReplicationMessage message = 
                    new YbProtoReplicationMessage(m, this.yugabyteDbTypeRegistry);
                  
                  String pgSchemaName = m.getPgschemaName();

                  final OpId lsn = new OpId(record.getCdcSdkOpId().getTerm(), 
                                            record.getCdcSdkOpId().getIndex(), 
                                            record.getCdcSdkOpId().getWriteIdKey().toByteArray(), 
                                            record.getCdcSdkOpId().getWriteId(), 
                                            resp.getSnapshotTime());
                
                  if (message.isLastEventForLsn()) {
                    lastCompletelyProcessedLsn = lsn;
                  }

                  try {
                    if (message.isTransactionalMessage()) {
                      // Ideally there shouldn't be any BEGIN-COMMIT record while streaming
                      // the snapshot, if one is encountered then log a warning so the user knows
                      // that some debugging is required
                      LOGGER.warn("Transactional record of type {} encountered while snapshotting the table", message.getOperation().toString());
                    } else if (message.isDDLMessage()) {
                      LOGGER.debug("For table {}, received a DDL record {}", 
                                  message.getTable(), message.getSchema().toString());
                      
                      schemaNeeded.put(tabletId, Boolean.FALSE);

                      TableId tId = null;
                      if (message.getOperation() != Operation.NOOP) {
                        tId = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaName);
                        Objects.requireNonNull(tId);
                      }
                      // Getting the table with the help of the schema.
                      Table t = schema.tableForTablet(tId, tabletId);
                      if (YugabyteDBSchema.shouldRefreshSchema(t, message.getSchema())) {
                        // If we fail to achieve the table, that means we have not specified 
                        // correct schema information. Now try to refresh the schema.
                        schema.refreshSchemaWithTabletId(tId, message.getSchema(), pgSchemaName, tabletId);
                      }
                    } else {
                      // DML event
                      LOGGER.debug("For table {}, received a DML record {}", 
                                  message.getTable(), record);
                      
                      TableId tId = null;
                      if (message.getOperation() != Operation.NOOP) {
                        tId = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaName);
                        Objects.requireNonNull(tId);
                      }

                      previousOffset.updateWalPosition(tabletId, lsn, lastCompletelyProcessedLsn, 
                                                       message.getCommitTime(), 
                                                       String.valueOf(message.getTransactionId()), 
                                                       tId, null);
                      
                      boolean dispatched = (message.getOperation() != Operation.NOOP) && 
                          dispatcher.dispatchDataChangeEvent(part, tId, 
                              new YugabyteDBChangeRecordEmitter(part, previousOffset, clock, 
                                                                this.connectorConfig, schema,
                                                                tId, message,
                                                                pgSchemaName, tabletId,
                                                                taskContext.isBeforeImageEnabled()));

                      LOGGER.debug("Dispatched snapshot record successfully");
                    }
                  } catch (InterruptedException e) {
                    LOGGER.error("Exception while processing messages for snapshot: " + e);
                    throw e;
                  }
                }

                OpId finalOpId = new OpId(resp.getTerm(), resp.getIndex(), resp.getKey(), 
                                          resp.getWriteId(), resp.getSnapshotTime());
                LOGGER.debug("Final OpId is {}", finalOpId);
                
                previousOffset.getSourceInfo(tabletId).updateLastCommit(finalOpId);

                if (isSnapshotComplete(finalOpId)) {
                    // This will mark the snapshot completed for the tablet
                    snapshotCompletedTablets.add(tabletId);
                    LOGGER.info("Snapshot completed for tablet {} belonging to table {} ({})", 
                                tabletId, table.getName(), tableId);
                }

            }
            
            // Reset the retry count here indicating that if the flow has reached here then
            // everything succeeded without any exceptions
            retryCount = 0;
          }
        } catch (Exception e) {
          ++retryCount;

          if (retryCount > this.connectorConfig.maxConnectorRetries()) {
            LOGGER.error("Too many errors while trying to stream the snapshot, "
                         + "all {} retries failed.", this.connectorConfig.maxConnectorRetries());
            
            LOGGER.info("Tablets in the failed task:");
            for (Pair<String, String> entry : tableToTabletIds) {
              LOGGER.info("Tablet: {} table: {}({})", 
                          entry.getValue() /* tablet UUID */,
                          tableIdToTable.get(entry.getKey()).getName() /* table name */,
                          entry.getKey() /* table UUID */);
            }

            throw e;
          }

          LOGGER.warn("Error while trying to get the snapshot from the server; will attempt " 
                      + "retry {} of {} after {} milli-seconds. Exception message: {}", retryCount, 
                       this.connectorConfig.maxConnectorRetries(), 
                       this.connectorConfig.connectorRetryDelayMs(), e.getMessage());
          LOGGER.debug("Stacktrace: ", e);

          try {
            final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
            retryMetronome.pause();
          } catch (InterruptedException ie) {
            LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
            Thread.currentThread().interrupt();
          }
        }
      }
    
      // If the flow comes at this stage then it either failed or was aborted by 
      // some user interruption
      return SnapshotResult.aborted();
    }

    /**
     * Check if the passed OpId matches the conditions which signify that the snapshot has
     * been complete.
     *
     * @param opId the {@link OpId} to check for
     * @return true if the passed {@link OpId} means snapshot is complete, false otherwise
     */
    private boolean isSnapshotComplete(OpId opId) {
        return Arrays.equals(opId.getKey(), "".getBytes()) && opId.getWrite_id() == 0
                && opId.getTime() == 0;
    }

    protected Stream<TableId> getDataCollectionsToBeSnapshotted(Set<TableId> allDataCollections) {
      final Set<Pattern> snapshotAllowedDataCollections = 
          this.connectorConfig.getDataCollectionsToBeSnapshotted();
      if (snapshotAllowedDataCollections.size() == 0) {
          // If no snapshot.include.collection.list is specified then we should return all of it
          return allDataCollections.stream();
      }
      else {
          return allDataCollections.stream()
                  .filter(dataCollectionId -> snapshotAllowedDataCollections.stream()
                      .anyMatch(s -> s.matcher(dataCollectionId.schema()+"."+dataCollectionId.table())
                          .matches()));
      }
  }

    @Override
    protected SnapshottingTask getSnapshottingTask(YBPartition partition, 
                                                   YugabyteDBOffsetContext previousOffset) {
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
    protected SnapshotContext<YBPartition, YugabyteDBOffsetContext> prepare(YBPartition partition)
            throws Exception {
        return new YugabyteDBSnapshotContext(partition, connectorConfig.databaseName());
    }

    /**
     * Check on the server side if the tablet already has some checkpoint, if it does then do
     * not take a snapshot for it again since some data has already been streamed out of it
     * @param tableId the UUID of the table
     * @param tabletId the UUID of the tablet
     * @return true if snapshot has been taken or some data has streamed already
     * @throws Exception if checkpoint cannot be retreived from server side
     */
    protected boolean hasSnapshotCompletedPreviously(String tableId, String tabletId) 
        throws Exception {
      GetCheckpointResponse resp = this.syncClient.getCheckpoint(
                                       this.syncClient.openTableByUUID(tableId), 
                                       this.connectorConfig.streamId(), tabletId);

      return !(resp.getTerm() == -1 && resp.getIndex() == -1);
    }

    protected Set<TableId> getAllTableIds(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YBPartition, YugabyteDBOffsetContext> ctx)
            throws Exception {
        return new HashSet<>();
    }

    protected void releaseSchemaSnapshotLocks(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YBPartition, YugabyteDBOffsetContext> snapshotContext)
            throws SQLException {
    }

    protected void determineSnapshotOffset(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YBPartition, YugabyteDBOffsetContext> ctx,
                                           YugabyteDBOffsetContext previousOffset)
            throws Exception {
        YugabyteDBOffsetContext offset = ctx.offset;
        if (offset == null) {
            ctx.offset = offset;
        }

        updateOffsetForSnapshot(offset);
    }

    private void updateOffsetForSnapshot(YugabyteDBOffsetContext offset) throws SQLException {
        final OpId xlogStart = getTransactionStartLsn();
    }

    protected void updateOffsetForPreSnapshotCatchUpStreaming(YugabyteDBOffsetContext offset) throws SQLException {
        updateOffsetForSnapshot(offset);
        offset.setStreamingStoppingLsn(null/* OpId.valueOf(jdbcConnection.currentXLogLocation()) */);
    }

    // TOOD:CDCSDK get the offset from YB for snapshot.
    private OpId getTransactionStartLsn() throws SQLException {
        return null;
    }

    @Override
    protected void complete(SnapshotContext<YBPartition, YugabyteDBOffsetContext> snapshotContext) {
        snapshotter.snapshotCompleted();

        // Todo Vaibhav: Close the YBClient instances now
        // See if it can be closed anywhere else for the snapshotting tasks.
    }

    /**
     * Generate a valid Postgres query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    protected Optional<String> getSnapshotSelect(
                                                 RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YBPartition, YugabyteDBOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {
        return snapshotter.buildSnapshotQuery(tableId, columns);
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class YugabyteDBSnapshotContext extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YBPartition, YugabyteDBOffsetContext> {

        public YugabyteDBSnapshotContext(YBPartition partition, String catalogName) throws SQLException {
            super(partition, catalogName);
        }
    }
}
