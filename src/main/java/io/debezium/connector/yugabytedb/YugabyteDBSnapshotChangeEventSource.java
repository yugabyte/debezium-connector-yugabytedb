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
    private final YugabyteDBConnection connection;

    private final AsyncYBClient asyncClient;
    private final YBClient syncClient;

    private OpId lastCompletelyProcessedLsn;

    private YugabyteDBTypeRegistry yugabyteDbTypeRegistry;

    private final Metronome retryMetronome;

    public YugabyteDBSnapshotChangeEventSource(YugabyteDBConnectorConfig connectorConfig,
                                               YugabyteDBTaskContext taskContext,
                                               Snapshotter snapshotter, YugabyteDBConnection connection,
                                               YugabyteDBSchema schema, YugabyteDBEventDispatcher<TableId> dispatcher, Clock clock,
                                               SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.taskContext = taskContext;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotter = snapshotter;
        this.connection = connection;
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

        this.retryMetronome = Metronome.parker(
            Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);

        this.yugabyteDbTypeRegistry = taskContext.schema().getTypeRegistry();
    }

    @Override
    public SnapshotResult<YugabyteDBOffsetContext> execute(ChangeEventSourceContext context, YBPartition partition, YugabyteDBOffsetContext previousOffset)
            throws InterruptedException {
        SnapshottingTask snapshottingTask = getSnapshottingTask(partition, previousOffset);
        LOGGER.info("Dispatcher in snapshot: " + dispatcher.toString());
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
            Set<YBPartition> partitions = new YugabyteDBPartition.Provider(connectorConfig).getPartitions();

            LOGGER.info("Setting offsetContext/previousOffset for snapshot...");
            previousOffset = YugabyteDBOffsetContext.initialContextForSnapshot(this.connectorConfig, connection, clock, partitions);

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

    private void determineCapturedTables(RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YBPartition, YugabyteDBOffsetContext> ctx)
            throws Exception {
        // Get the TableIds - io.debezium.relation.TableId
        // Set<TableId> allTableIds = determineDataCollectionsToBeSnapshotted(getAllTableIds(ctx)).collect(Collectors.toSet());

        // Set<TableId> capturedTables = new HashSet<>();
        // Set<TableId> capturedSchemaTables = new HashSet<>();

        // for (TableId tableId : allTableIds) {
        //     if (connectorConfig.getTableFilters().eligibleDataCollectionFilter().isIncluded(tableId)) {
        //         LOGGER.trace("Adding table {} to the list of capture schema tables", tableId);
        //         capturedSchemaTables.add(tableId);
        //     }
        //     if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)) {
        //         LOGGER.trace("Adding table {} to the list of captured tables", tableId);
        //         capturedTables.add(tableId);
        //     }
        //     else {
        //         LOGGER.trace("Ignoring table {} as it's not included in the filter configuration", tableId);
        //     }
        // }

        // ctx.capturedTables = sort(capturedTables);
        // ctx.capturedSchemaTables = capturedSchemaTables
        //         .stream()
        //         .sorted()
        //         .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    @Override
    protected SnapshotResult<YugabyteDBOffsetContext> doExecute(ChangeEventSourceContext context, YugabyteDBOffsetContext previousOffset,
                                                                SnapshotContext<YBPartition, YugabyteDBOffsetContext> snapshotContext,
                                                                SnapshottingTask snapshottingTask)
            throws Exception {
      return SnapshotResult.skipped(previousOffset);
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
      }

      Map<String, YBTable> tableIdToTable = new HashMap<>();
      Set<String> tableUUIDs = tableToTabletIds.stream()
                                  .map(pair -> pair.getLeft())
                                  .collect(Collectors.toSet());
      for (String tableUUID : tableUUIDs) {
        tableIdToTable.put(tableUUID, this.syncClient.openTableByUUID(tableUUID));
      }

      Map<TableId, String> tableIdToUuid = new HashMap<>();
      Set<TableId> dbzTableIds = new HashSet<>();
      // TODO: Filter tablets based on the snapshot include list, and set checkpoint with bootstrap
      // for streaming for the rest of them
      for (Entry<String, YBTable> entry : tableIdToTable.entrySet()) {
        tableIdToUuid.put(YBClientUtils.getTableIdFromYbTable(entry.getValue()), entry.getKey());
      }
      dbzTableIds = tableIdToUuid.entrySet().stream()
                                  .map(entry -> entry.getKey())
                                  .collect(Collectors.toSet());
      LOGGER.info("Debezium table IDs:");
      for (TableId t : dbzTableIds) {
        LOGGER.info("====================== {}", t.toString());
      }

      //todo Vaibhav: This is more of a hack to get the filter to work, need to do it in a cleaner way
      Set<Pattern> dataCollectionsToBeSnapshotted = this.connectorConfig.getDataCollectionsToBeSnapshotted();
      Set<String> patterns = new HashSet<>();
      for (Pattern p : dataCollectionsToBeSnapshotted) {
        patterns.add(p.pattern());
      }

      Set<TableId> filteredTables = dbzTableIds.stream()
          .filter(dataCollectionId -> patterns.contains(dataCollectionId.schema()+"."+dataCollectionId.table()))
          .collect(Collectors.toSet());

      for (TableId t : filteredTables) {
        LOGGER.info("======================= " + t.toString());
      }
      // tableIdToUuid will only contain the tables ready for streaming
      tableIdToUuid.keySet().removeIf(obj -> !filteredTables.contains(obj));

      Map<String, Boolean> schemaNeeded = new HashMap<>();
      Set<String> snapshotCompletedTablets = new HashSet<>();
      Set<String> snapshotCompletedPreviously = new HashSet<>();

      for (Pair<String, String> entry : tableToTabletIds) {
        schemaNeeded.put(entry.getValue(), Boolean.TRUE);

        previousOffset.initSourceInfo(entry.getValue(), this.connectorConfig);
      }

      List<Pair<String, String>> tableToTabletForSnapshot = new ArrayList<>();
      List<Pair<String, String>> tableToTabletNoSnapshot = new ArrayList<>();

      for (Pair<String, String> entry : tableToTabletIds) {
        String tableUuid = entry.getKey();
        String tabletUuid = entry.getValue();

        // todo: convert this to ternary operator maybe
        if (tableIdToUuid.containsValue(tableUuid)) {
          // this means we need to add this table/tablet to snapshot
          tableToTabletForSnapshot.add(entry);
        } else {
          tableToTabletNoSnapshot.add(entry);
          // bootstrap the tablets if snapshot is not required for them
          LOGGER.info("Skipping the tablet {} since it is not a part of the snapshot collection include list", tabletUuid);
          YBClientUtils.setCheckpoint(this.syncClient, this.connectorConfig.streamId(), 
                                      tableUuid, tabletUuid, -1, -1, true, true);
        }
      }

      // Set checkpoint with bootstrap and initialCheckpoint as false.
      // A call to set the checkpoint is required first otherwise we will get an error 
      // from the server side saying:
      // INTERNAL_ERROR[code 21]: Stream ID {} is expired for Tablet ID {}
      for (Pair<String, String> entry : tableToTabletForSnapshot) {
        try {
          if (hasSnapshotCompletedPreviously(entry.getKey(), entry.getValue())) {
            LOGGER.info("Skipping snapshot for tablet {} since tablet has streamed some data before", 
                        entry.getValue());
            snapshotCompletedTablets.add(entry.getValue());
            snapshotCompletedPreviously.add(entry.getValue());
          } else {
            YBClientUtils.setCheckpoint(this.syncClient, 
                                        this.connectorConfig.streamId(), 
                                        entry.getKey() /* tableId */, 
                                        entry.getValue() /* tabletId */, 
                                        -1 /* term */, -1 /* index */, 
                                        false /* initialCheckpoint */, false /* bootstrap */);
          }
        } catch (Exception e) {
          throw new DebeziumException(e);
        }
      }

      short retryCount = 0;
      while (context.isRunning() && retryCount <= this.connectorConfig.maxConnectorRetries()) {
        try {
            while (context.isRunning() && (previousOffset.getStreamingStoppingLsn() == null)) {
              for (Pair<String, String> tableIdToTabletId : tableToTabletForSnapshot) {
                String tableId = tableIdToTabletId.getKey();
                YBTable table = tableIdToTable.get(tableId);

                String tabletId = tableIdToTabletId.getValue();
                YBPartition part = new YBPartition(tabletId);
                
                 // Check if snapshot is completed here, if it is, then break out of the loop
                if (snapshotCompletedTablets.size() == tableToTabletForSnapshot.size()) {
                    LOGGER.info("Snapshot completed for all the tablets");
                    // Commit checkpoints for all tablets to make them ready for streaming changes
                    for (Pair<String, String> entry : tableToTabletIds) {
                      try {
                        // Only checkpoint in case this is the first time snapshot has been 
                        // completed, otherwise we will end up setting the checkpoint to 0,0 even
                        // for tablets for which snapshot has been completed in a previous run
                        // of the connector
                        if (!snapshotCompletedPreviously.contains(entry.getValue())) {
                          YBClientUtils.setCheckpoint(this.syncClient, 
                                                      this.connectorConfig.streamId(),
                                                      entry.getKey() /* tableId */,
                                                      entry.getValue() /* tabletId */,
                                                      0 /* term */, 0 /* index */,
                                                      true /* initialCheckpoint */, 
                                                      true /* bootstrap */);
                        }
                      } catch (Exception e) {
                        throw new DebeziumException(e);
                      }
                    }

                    return SnapshotResult.completed(previousOffset);
                }

                // Skip the tablet if snapshot has already been taken for this tablet
                if (snapshotCompletedTablets.contains(tabletId)) {
                  continue;
                }

                OpId cp = previousOffset.snapshotLSN(tabletId);

                LOGGER.debug("Going to fetch from checkpoint {} for tablet {} for table {}", 
                            cp, tabletId, table.getName());

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
                      Table t = schema.tableFor(tId);
                      LOGGER.debug("The schema is already registered {}", t);
                      if (t == null) {
                        // If we fail to achieve the table, that means we have not specified 
                        // correct schema information. Now try to refresh the schema.
                        schema.refreshWithSchema(tId, message.getSchema(), pgSchemaName);
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
                                                                connection, tId, message, 
                                                                pgSchemaName));

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
                
                if (finalOpId.equals(new OpId(-1, -1, "".getBytes(), 0, 0))) {
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

          // TODO Vaibhav: handle failure scenarios here
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
      // The default behaviour is configured to allow the connector to take a snapshot again
      // and returning false at this stage would indicate that the snapshot has not been taken
      // so the connector would try to take a snapshot considering this as the first time
      // it is going for the snapshot
      if (this.connectorConfig.snapshotAgain()) {
        return false;
      }

      GetCheckpointResponse resp = this.syncClient.getCheckpoint(
                                       this.syncClient.openTableByUUID(tableId), 
                                       this.connectorConfig.streamId(), tabletId);

      if (resp.getTerm() == -1 && resp.getIndex() == -1) {
        // This condition indicates that some data has already streamed from the tablet.
        return false;
      } else {
        return true;
      }
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

        // Close the YbClient instances
        try {
          this.syncClient.close();
          this.asyncClient.close();
        } catch (Exception e) {
          throw new DebeziumException("Cannot shutdown the yb-client instances", e);
        }
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
