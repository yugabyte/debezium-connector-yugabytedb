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
import org.yb.client.*;

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

    // Test only member variables, DO NOT try to modify in the source code.
    public static boolean FAIL_AFTER_BOOTSTRAP_GET_CHANGES;
    public static boolean FAIL_AFTER_SETTING_INITIAL_CHECKPOINT;
    public static boolean TRACK_EXPLICIT_CHECKPOINTS = false;
    public static CdcSdkCheckpoint LAST_EXPLICIT_CHECKPOINT;

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
    private Map<String, CdcSdkCheckpoint> tabletToExplicitCheckpoint;
    protected Map<String, Long> tabletSafeTime;
    protected Map<String, YBTable> tableIdToTable;
    protected Set<String> shouldWaitForCallback;

    // This set will contain the tablets for which the server has sent the snapshot
    // end marker, but we have not received the callback from Kafka - this will ensure
    // that we do not end up sending redundant GetChanges calls.
    protected Set<String> tabletsWaitingForCallback;

    private boolean snapshotComplete = false;

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
            .maxRpcAttempts(connectorConfig.maxRPCRetryAttempts())
            .sleepTime(connectorConfig.rpcRetrySleepTime())
            .build();
        
        this.syncClient = new YBClient(this.asyncClient);

        this.yugabyteDbTypeRegistry = taskContext.schema().getTypeRegistry();
        this.tabletToExplicitCheckpoint = new HashMap<>();
        this.tabletSafeTime = new HashMap<>();
        this.tableIdToTable = new HashMap<>();
        this.shouldWaitForCallback = new HashSet<>();
        this.tabletsWaitingForCallback = new HashSet<>();
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

            // For snapshot, set all partitions to use tableID as identifier.
            partitions.forEach(YBPartition::markTableAsColocated);

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

    protected boolean isSnapshotComplete() {
        return this.snapshotComplete;
    }

    @Override
    protected SnapshotResult<YugabyteDBOffsetContext> doExecute(ChangeEventSourceContext context, YugabyteDBOffsetContext previousOffset,
                                                                SnapshotContext<YBPartition, YugabyteDBOffsetContext> snapshotContext,
                                                                SnapshottingTask snapshottingTask)
            throws Exception {
      return SnapshotResult.skipped(previousOffset);
    }

    /**
     * To set the checkpoint on the service indicating that this is the initial checkpoint.
     * Technically, it means that we are trying to set the active time for the configured
     * stream ID and the passed tablet ID in the method.
     * @param tableId
     * @param tabletId
     * @throws Exception when the SetCheckpoint RPC fails on the service
     */
    protected void makeStreamActive(String tableId, String tabletId, boolean bootstrap) throws Exception {
      short retryCount = 0;

      // The SetCDCCheckPoint RPC, relies on a cache to obtain a list of all the tservers.
      // In case of multi host port connection url, if one of the DB node goes down,
      // it takes some time for the cache to refresh and return correct tserver list.
      // This refresh time may be longer and hence we need additional number of retries here.
      int totalRetries = this.connectorConfig.maxConnectorRetries() * 5;

      while (retryCount <= totalRetries) {
        try {
          long term = 0;
          long index = 0;
          if (bootstrap) {
            term = -1;
            index = -1;
          }
          
          LOGGER.info("Setting checkpoint on tablet {} with {}.{},"
            + " will be taking snapshot now", tabletId, term, index);
          YBClientUtils.setCheckpoint(this.syncClient, this.connectorConfig.streamId(), tableId, tabletId, term, index,
            true /*initialCheckpoint */, bootstrap);

          // Reaching this point would mean that the process went through without failure
          return;
        } catch (Exception e) {
          ++retryCount;

          if (retryCount > totalRetries) {
            LOGGER.error("Too many errors while trying to set checkpoint, "
                + "all {} retries failed.", totalRetries);

            throw e;
          }
          LOGGER.warn("Error while trying to set the checkpoint; will attempt "
              + "retry {} of {} after {} milli-seconds. Exception message: {}", retryCount,
              totalRetries,
              this.connectorConfig.connectorRetryDelayMs(), e.getMessage());
          LOGGER.debug("Stacktrace: ", e);

          try {
            final Metronome retryMetronome = Metronome
                .parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
            retryMetronome.pause();
          } catch (InterruptedException ie) {
            LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
            Thread.currentThread().interrupt();
          }
        }
      }
    }

    /**
     * Get the checkpoint for given {@link YBTable} and tabletID
     * @param ybTable
     * @param tabletId
     * @throws Exception if the RPC hits any error
     */
    protected GetCheckpointResponse getCheckpointWithRetry(YBTable ybTable, String tabletId)
      throws Exception {
      short retryCount = 0;
      while (retryCount <= connectorConfig.maxConnectorRetries()) {
        try {
          GetCheckpointResponse resp =
              this.syncClient.getCheckpoint(ybTable, connectorConfig.streamId(), tabletId);

          return resp;
        } catch (Exception e) {
          ++retryCount;

          if (retryCount > this.connectorConfig.maxConnectorRetries()) {
            LOGGER.error("Too many errors while trying to get checkpoint for tablet {}, "
                          + "all {} retries failed.", tabletId, this.connectorConfig.maxConnectorRetries());

            throw e;
          }

          LOGGER.warn("Error while trying to get the checkpoint for tablet {}; will attempt " 
                      + "retry {} of {} after {} milli-seconds. Exception message: {}",
                      tabletId, retryCount, 
                      connectorConfig.maxConnectorRetries(), 
                      connectorConfig.connectorRetryDelayMs(), e.getMessage());
          LOGGER.debug("Stacktrace: ", e);

          try {
            final Metronome retryMetronome =
                Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
            retryMetronome.pause();
          } catch (InterruptedException ie) {
            LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
            Thread.currentThread().interrupt();
          }
        }
      }

      // In ideal scenarios, this code will never be hit.
      return null;
    }

    /**
     * Decide if we need to take snapshot or do nothing if snapshot has completed previously
     */
    protected boolean isSnapshotRequired(GetCheckpointResponse getCheckpointResponse,
                                         String tableId, String tabletId,
                                         Set<String> snapshotCompletedTablets,
                                         Set<String> snapshotCompletedPreviously) 
                                           throws Exception {
      if (hasSnapshotCompletedPreviously(getCheckpointResponse)) {
        LOGGER.info("Skipping snapshot for table {} tablet {} since tablet has streamed some data before",
                  tableId, tabletId);
        snapshotCompletedTablets.add(tabletId);
        snapshotCompletedPreviously.add(tabletId);

        return false;
      } else {
        // Mark the stream as active by setting a checkpoint.
        if (!OpId.isValid(getCheckpointResponse.getTerm(), getCheckpointResponse.getIndex())) {
          // Set checkpoint with bootstrap as false and initialCheckpoint as true.
          // A call to set the checkpoint is required first otherwise we will get an error 
          // from the server side saying:
          // INTERNAL_ERROR[code 21]: Stream ID {} is expired for Tablet ID {}
          makeStreamActive(tableId, tabletId, false);
        }

        return true;
      }
    }

    /**
     * This method will ONLY be called when we are sure that we have to take the snapshot.
     * @param getCheckpointResponse
     * @return the from_op_id to start snapshot with
     */
    protected OpId getSnapshotStartLsn(GetCheckpointResponse getCheckpointResponse) {
      Objects.requireNonNull(getCheckpointResponse);

      if (getCheckpointResponse.getTerm() > 0 && getCheckpointResponse.getIndex() > 0) {
        // This is when we are already in the middle of the snapshot and we need to directly
        // resume snapshot from the checkpoint received from the service.
        return OpId.from(getCheckpointResponse);
      } else {
        // This is the case when we have either just bootstrapped snapshot or starting snapshot
        // for the first time.
        return YugabyteDBOffsetContext.snapshotStartLsn();
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

      Set<String> tableUUIDs = tableToTabletIds.stream()
                                  .map(pair -> pair.getLeft())
                                  .collect(Collectors.toSet());
      for (String tableUUID : tableUUIDs) {
        tableIdToTable.put(tableUUID, this.syncClient.openTableByUUID(tableUUID));
      }

      Map<TableId, String> filteredTableIdToUuid = determineTablesForSnapshot(tableIdToTable);
      List<Pair<String, String>> tableToTabletForSnapshot = new ArrayList<>();

      Map<String, Boolean> schemaNeeded = new HashMap<>();
      Set<String> snapshotCompletedTablets = new HashSet<>();
      Set<String> snapshotCompletedPreviously = new HashSet<>();

      for (Pair<String, String> entry : tableToTabletIds) {
        // We can use tableIdToTable.get(entry.getKey()).isColocated() to get actual status.
        String tableId = entry.getKey();
        String tabletId = entry.getValue();
        YBPartition p = new YBPartition(tableId, tabletId, true /* colocated */);

        GetCheckpointResponse resp = getCheckpointWithRetry(tableIdToTable.get(tableId), tabletId);
        LOGGER.info("Checkpoint before snapshotting tablet {}: Term {} Index {} SnapshotKey: {}",
                    tabletId, resp.getTerm(), resp.getIndex(), Arrays.toString(resp.getSnapshotKey()));

        OpId startLsn = OpId.from(resp);
        if (filteredTableIdToUuid.containsValue(tableId)) {
          // We need to take the snapshot for this table.
          tableToTabletForSnapshot.add(entry);

          if (isSnapshotRequired(resp, tableId, tabletId, snapshotCompletedTablets, snapshotCompletedPreviously)) {
            startLsn = getSnapshotStartLsn(resp);
          }
        } else {
          // At this stage we know that the particular table is not a part of the
          // snapshot.include.collection.list so we simply need to bootstrap the tablet
          // for streaming.
          LOGGER.info("Skipping the table {} tablet {} since it is not a part of the"
                      + " snapshot.include.collection.list", entry.getKey(), entry.getValue());
          makeStreamActive(tableId, tabletId, true);
        }

        previousOffset.initSourceInfo(p, this.connectorConfig, startLsn);
        schemaNeeded.put(p.getId(), Boolean.TRUE);
        shouldWaitForCallback.add(p.getId());
        LOGGER.debug("Previous offset for table {} tablet {} is {}", p.getTableId(),
                     p.getTabletId(), previousOffset.toString());
      }

      if (FAIL_AFTER_SETTING_INITIAL_CHECKPOINT) {
        throw new RuntimeException("[TEST ONLY] Throwing error explicitly after setting initial checkpoint");
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

                String tableUUID = tableIdToTabletId.getKey();
                YBTable table = tableIdToTable.get(tableUUID);

                String tabletId = tableIdToTabletId.getValue();
                YBPartition part = new YBPartition(tableUUID, tabletId, true /* colocated */);

                 // Check if snapshot is completed here, if it is, then break out of the loop
                if (snapshotCompletedTablets.size() == tableToTabletForSnapshot.size()) {
                    LOGGER.info("Snapshot completed for all the tablets");
                    this.snapshotComplete = true;
                    return SnapshotResult.completed(previousOffset);
                }

                // Skip the tablet if snapshot has already been taken for this tablet
                if (snapshotCompletedTablets.contains(part.getId())
                      || tabletsWaitingForCallback.contains(part.getId())) {
                  // Before continuing, check if the tablets waiting for callback have been updated in case of explicit checkpointing.
                  if (!snapshotCompletedTablets.contains(part.getId()) && taskContext.shouldEnableExplicitCheckpointing()) {
                    doSnapshotCompletionCheck(part, snapshotCompletedTablets, tabletsWaitingForCallback, previousOffset);
                  }
                  continue;
                }

                CdcSdkCheckpoint explicitCdcSdkCheckpoint = null;
                if (taskContext.shouldEnableExplicitCheckpointing()) {
                  explicitCdcSdkCheckpoint = tabletToExplicitCheckpoint.get(part.getId());
                }

                OpId cp = previousOffset.snapshotLSN(part);

                if (LOGGER.isDebugEnabled()
                    || (connectorConfig.logGetChanges() && System.currentTimeMillis() >= (lastLoggedTimeForGetChanges + connectorConfig.logGetChangesIntervalMs()))) {
                  LOGGER.info("Requesting changes for tablet {} from OpId {} for table {} with explicit checkpoint {}",
                              tabletId, cp, table.getName(), explicitCdcSdkCheckpoint);
                  lastLoggedTimeForGetChanges = System.currentTimeMillis();
                }

                if (!context.isRunning()) {
                  LOGGER.info("Connector has been stopped");
                  break;
                }

                GetChangesResponse resp = this.syncClient.getChangesCDCSDK(table,
                    connectorConfig.streamId(), tabletId, cp.getTerm(), cp.getIndex(), cp.getKey(),
                    cp.getWrite_id(), cp.getTime(), schemaNeeded.get(part.getId()),
                    explicitCdcSdkCheckpoint,
                    tabletSafeTime.getOrDefault(part.getId(), -1L));

                if (TRACK_EXPLICIT_CHECKPOINTS) {
                  LAST_EXPLICIT_CHECKPOINT = explicitCdcSdkCheckpoint;
                }

                tabletSafeTime.put(part.getId(), resp.getResp().getSafeHybridTime());

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

                      schemaNeeded.put(part.getId(), Boolean.FALSE);

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

                      // In case of snapshots, we do not want to ignore tableUUID while updating
                      // OpId value for a table-tablet pair.
                      previousOffset.updateRecordPosition(part, lsn, lastCompletelyProcessedLsn,
                                                          message.getRawCommitTime(),
                                                          String.valueOf(message.getTransactionId()),
                                                          tId, message.getRecordTime());

                      boolean dispatched = (message.getOperation() != Operation.NOOP) &&
                          dispatcher.dispatchDataChangeEvent(part, tId,
                              new YugabyteDBChangeRecordEmitter(part, previousOffset, clock,
                                                                this.connectorConfig, schema,
                                                                connection, tId, message,
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
                LOGGER.debug("Final OpId for tablet {} is {}", part.getId(), finalOpId);

                // If the response doesn't have any record, it is safe to assume that we should not wait
                // for the callback to come and that we can proceed further in processing this particular tablet.
                if (previousOffset.getSourceInfo(part).noRecordSeen()) {
                  LOGGER.info("Should not wait for callback on tablet {}", part.getId());
                  shouldWaitForCallback.remove(part.getId());
                }

                /*
                   This block checks and validates for two scenarios:
                   1. Explicit checkpointing:
                      a. In case of explicit, check whether the checkpoint received in the callback
                         is the checkpoint complete marker so that the snapshot can be marked as
                         completed.
                      b. If the explicit checkpoint is not the snapshot end marker, there is
                         a possibility that the finalOpId received above in the response is the
                         snapshot complete marker - add the tablet to the set so that we do not end
                         up calling redundant GetChanges
                   2. Implicit checkpointing:
                      a. In this case, only checking the response final_op_id is enough to mark
                         the snapshot as completed.
                 */
                if (taskContext.shouldEnableExplicitCheckpointing()) {
                  // snapshotCompletedTablets contain the tablets for which the following two
                  // conditions are met:
                  // 1. The server has sent the snapshot end marker.
                  // 2. In case of EXPLICIT checkpointing - Kafka has sent the callback so we are
                  //    sure we have received the data.
                  //
                  // Now over here, the additional set i.e. tabletsWaitingForCallback is for cases
                  // of EXPLICIT checkpointing only where the above point 2 is not satisfied,
                  // so that we know that server has sent the data (1 is satisfied) but
                  // Kafka hasn't acknowledged the message's presence. If we always add the
                  // tabletId to snapshotCompletedTablets - there is a chance that when the
                  // connector crashes, we may lose some data since we may not have published them
                  // to Kafka yet.
                  CdcSdkCheckpoint explicitCheckpoint = tabletToExplicitCheckpoint.get(part.getId());
                  if (explicitCheckpoint != null && isSnapshotCompleteMarker(OpId.from(explicitCheckpoint))) {
                    // This will mark the snapshot completed for the tablet
                    snapshotCompletedTablets.add(part.getId());

                    markSnapshotDoneOnServer(part, previousOffset);

                    // Remove the tablet from the set.
                    tabletsWaitingForCallback.removeIf(t -> t.equals(part.getId()));
                    LOGGER.info("E: Snapshot completed for tablet {} belonging to table {} ({})",
                      part.getTabletId(), table.getName(), part.getTableId());
                  } else if (isSnapshotCompleteMarker(finalOpId)) {
                    // Add it to tablets waiting for callback so that the connector doesn't end up
                    // calling GetChanges for the same again.
                    if (shouldWaitForCallback.contains(part.getId())) {
                      if (!tabletsWaitingForCallback.contains(part.getId())) {
                        LOGGER.info("Adding tablet {} of table {} ({}) to wait-list",
                                    part.getId(), table.getName(), part.getTableId());
                        tabletsWaitingForCallback.add(part.getId());
                      }
                    } else {
                      LOGGER.info("Adding {} to the list of snapshot completed tablets", part.getId());
                      snapshotCompletedTablets.add(part.getId());
                      markSnapshotDoneOnServer(part, previousOffset);
                    }
                  }
                } else if (!taskContext.shouldEnableExplicitCheckpointing() && isSnapshotCompleteMarker(finalOpId)) {
                  snapshotCompletedTablets.add(part.getId());
                  markSnapshotDoneOnServer(part, previousOffset);
                  LOGGER.info("I: Snapshot completed for tablet {} belonging to table {} ({})",
                    part.getTabletId(), table.getName(), part.getTableId());
                }

                previousOffset.updateWalPosition(part, finalOpId);
            }
            
            // Reset the retry count here indicating that if the flow has reached here then
            // everything succeeded without any exceptions
            retryCount = 0;

            // This flag is only meant for testing purposes only.
            if (FAIL_AFTER_BOOTSTRAP_GET_CHANGES) {
              throw new RuntimeException("[TEST ONLY] Throwing error explicitly after bootstrap snapshot GetChanges call");
            }
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
                      + "retry {} of {} after {} milli-seconds. Exception: {}", retryCount, 
                       this.connectorConfig.maxConnectorRetries(), 
                       this.connectorConfig.connectorRetryDelayMs(), e);

          try {
            final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
            retryMetronome.pause();
          } catch (InterruptedException ie) {
            LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
            Thread.currentThread().interrupt();
          }
        }
      }

      if (!context.isRunning()) {
        // Context can be set to not running in tests because of multiple reasons and hence will
        // lead to a snapshot result aborted. However, in tests, the validation of records will
        // ensure that records are received by the consumer properly.
        LOGGER.warn("Sending an aborted snapshot result because context is set to not running");
      }

      // If the flow comes at this stage then it either failed or was aborted by
      // some user interruption
      return SnapshotResult.aborted();
    }

  /**
   * Check if the tablet has received an explicit checkpoint - if yes, remove it from the waiting
   * list and add it to the list of completed tablets.
   * @param partition the YBPartition to obtain the Id from
   * @param snapshotCompletedTablets a set containing all the tablets for which snapshot has been completed
   * @param tabletsWaitingForCallback a set containing tablets which have completed snapshot from server but have not received the explicit checkpoint
   */
  public void doSnapshotCompletionCheck(YBPartition partition, Set<String> snapshotCompletedTablets,
                                        Set<String> tabletsWaitingForCallback, YugabyteDBOffsetContext offsetContext) throws Exception {
      if (this.tabletToExplicitCheckpoint.get(partition.getId()) == null) {
        // If we have no OpId stored in the explicit checkpoint map then that would indicate that
        // we haven't yet received any callback from Kafka even once and we should wait more.
        return;
      }

      OpId opId = OpId.from(this.tabletToExplicitCheckpoint.get(partition.getId()));
      if (isSnapshotCompleteMarker(opId)) {
        LOGGER.info("Adding tablet {} to snapshot completed list", partition.getId());
        snapshotCompletedTablets.add(partition.getId());
        markSnapshotDoneOnServer(partition, offsetContext);
        tabletsWaitingForCallback.removeIf(t -> t.equals(partition.getId()));
      }
    }

  public void markSnapshotDoneOnServer(YBPartition partition, YugabyteDBOffsetContext offsetContext) throws Exception {
    OpId snapshotDoneMarker = offsetContext.snapshotDoneKeyLsn(partition);

    short retryCount = 0;
    while (retryCount <= connectorConfig.maxConnectorRetries()) {
      try {
        LOGGER.info("Marking snapshot completed on service for table {} tablet {}", partition.getTableId(), partition.getTabletId());
        GetChangesResponse response =
            this.syncClient.getChangesCDCSDK(tableIdToTable.get(partition.getTableId()), connectorConfig.streamId(), 
                                             partition.getTabletId(), snapshotDoneMarker.getTerm(),
                                             snapshotDoneMarker.getIndex(), snapshotDoneMarker.getKey(), 
                                             snapshotDoneMarker.getWrite_id(), snapshotDoneMarker.getTime(),
                              false /* schema is not needed since this is a dummy call */,
                                             taskContext.shouldEnableExplicitCheckpointing() ? tabletToExplicitCheckpoint.get(partition.getId()) : null,
                                             tabletSafeTime.getOrDefault(partition.getId(), -1L));

        // Break upon successful request.
        break;
      } catch (Exception e) {
        ++retryCount;

        if (retryCount > connectorConfig.maxConnectorRetries()) {
          LOGGER.error("Too many errors while trying to mark snapshot completed on service for table {} tablet {} error: ",
                       partition.getTableId(), partition.getTabletId(), e);
          throw e;
        }
        
        LOGGER.warn("Error while marking snapshot completed on service for table {} tablet {}, will attempt retry {} of {} for error {}",
                    partition.getTableId(), partition.getTabletId(), retryCount, connectorConfig.maxConnectorRetries(), e);
      }
    }

  }

    /**
     * For EXPLICIT checkpointing, the following code flow is used:<br><br>
     * 1. Kafka Connect invokes the callback {@code commit()} which further invokes
     * {@code commitOffset(offsets)} in the Debezium API <br><br>
     * 2. Both the streaming and snapshot change event source classes maintain a map
     * {@code tabletToExplicitCheckpoint} which stores the offsets sent by Kafka Connect. <br><br>
     * 3. So when the connector gets the acknowledgement back by saying that Kafka has received
     * records till certain offset, we update the value in {@code tabletToExplicitCheckpoint} <br><br>
     * 4. While making the next `GetChanges` call, we pass the value from the map
     * {@code tabletToExplicitCheckpoint} for the relevant tablet and hence the server then takes
     * care of updating those checkpointed values in the {@code cdc_state} table <br><br>
     * @param offset a map containing the {@link OpId} information for all the tablets
     */
    public void commitOffset(Map<String, ?> offset) {
        if (!taskContext.shouldEnableExplicitCheckpointing()) {
            return;
        }

        try {
            LOGGER.info("{} | Committing offsets on server for snapshot", taskContext.getTaskId());

            for (Map.Entry<String, ?> entry : offset.entrySet()) {
                // TODO: The transaction_id field is getting populated somewhere and see if it can
                // be removed or blocked from getting added to this map.
                if (!entry.getKey().equals("transaction_id")) {
                    LOGGER.debug("Tablet: {} OpId: {}", entry.getKey(), entry.getValue());

                    // Parse the string to get the OpId object.
                    // Note that the entry.getKey() will be returning a key in the format tableId.tabletId
                    OpId tempOpId = OpId.valueOf((String) entry.getValue());
                    this.tabletToExplicitCheckpoint.put(entry.getKey(), tempOpId.toCdcSdkCheckpoint());

                    LOGGER.debug("Committed checkpoint on server for stream ID {} tablet {} with term {} index {}",
                            this.connectorConfig.streamId(), entry.getKey(), tempOpId.getTerm(), tempOpId.getIndex());
                }
            }
        } catch (Exception e) {
            LOGGER.warn("{} | Unable to update the explicit checkpoint map {}", taskContext.getTaskId(), e);
        }
    }

    /**
     * Check if the passed OpId matches the conditions which signify that the snapshot has
     * been complete.
     *
     * @param opId the {@link OpId} to check for
     * @return true if the passed {@link OpId} means snapshot is complete, false otherwise
     */
    private boolean isSnapshotCompleteMarker(OpId opId) {
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
     * @param getCheckpointResponse
     */
    protected boolean hasSnapshotCompletedPreviously(GetCheckpointResponse getCheckpointResponse) {
      if (getCheckpointResponse.getSnapshotKey() != null) {
        // This indicates that snapshot key is present and the connector is either in the middle
        // of the snapshot or snapshot has just been bootstrapped and we haven't called further
        // GetChanges on the tablet.
        //
        // In either of the cases, snapshot is not complete and need to be taken.
        return false;
      }

      // If no snapshot key is present then it could be either of the two cases:
      // 1. Snapshot hasn't been initiated (so snapshot incomplete) -> indicated by invalid OpId 
      // 2. Snapshot is complete and tablet is in streaming mode -> OpId is valid
      return OpId.isValid(getCheckpointResponse.getTerm(), getCheckpointResponse.getIndex());
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
