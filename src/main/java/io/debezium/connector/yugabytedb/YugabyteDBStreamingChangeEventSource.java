/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import io.debezium.DebeziumException;
import io.debezium.connector.yugabytedb.connection.*;
import io.debezium.connector.yugabytedb.connection.ReplicationMessage.Operation;
import io.debezium.connector.yugabytedb.connection.pgproto.YbProtoReplicationMessage;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.Metronome;
import org.apache.commons.lang3.tuple.Pair;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;
import org.yb.client.*;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;

/**
 *
 * @author Suranjan Kumar (skumar@yugabyte.com)
 */
public class YugabyteDBStreamingChangeEventSource implements
        StreamingChangeEventSource<YBPartition, YugabyteDBOffsetContext> {

    private static final String KEEP_ALIVE_THREAD_NAME = "keep-alive";

    /**
     * Number of received events without sending anything to Kafka which will
     * trigger a "WAL backlog growing" warning.
     */
    private static final int GROWING_WAL_WARNING_LOG_INTERVAL = 10_000;

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBStreamingChangeEventSource.class);

    // PGOUTPUT decoder sends the messages with larger time gaps than other decoders
    // We thus try to read the message multiple times before we make poll pause
    private static final int THROTTLE_NO_MESSAGE_BEFORE_PAUSE = 5;

    private final YugabyteDBConnection connection;
    private final YugabyteDBEventDispatcher<TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final YugabyteDBSchema schema;
    private final YugabyteDBConnectorConfig connectorConfig;
    private final YugabyteDBTaskContext taskContext;

    private final Snapshotter snapshotter;
    private final DelayStrategy pauseNoMessage;
    private final ElapsedTimeStrategy connectionProbeTimer;

    /**
     * The minimum of (number of event received since the last event sent to Kafka,
     * number of event received since last WAL growing warning issued).
     */
    private long numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
    private OpId lastCompletelyProcessedLsn;

    private final AsyncYBClient asyncYBClient;
    private final YBClient syncClient;
    private YugabyteDBTypeRegistry yugabyteDBTypeRegistry;
    private final Map<String, OpId> checkPointMap;
    private final ChangeEventQueue<DataChangeEvent> queue;

    public YugabyteDBStreamingChangeEventSource(YugabyteDBConnectorConfig connectorConfig, Snapshotter snapshotter,
                                                YugabyteDBConnection connection, YugabyteDBEventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock,
                                                YugabyteDBSchema schema, YugabyteDBTaskContext taskContext, ReplicationConnection replicationConnection,
                                                ChangeEventQueue<DataChangeEvent> queue) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        pauseNoMessage = DelayStrategy.constant(taskContext.getConfig().getPollInterval().toMillis());
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        checkPointMap = new ConcurrentHashMap<>();
        this.connectionProbeTimer = ElapsedTimeStrategy.constant(Clock.system(), connectorConfig.statusUpdateInterval());

        String masterAddress = connectorConfig.masterAddresses();
        asyncYBClient = new AsyncYBClient.AsyncYBClientBuilder(masterAddress)
                .defaultAdminOperationTimeoutMs(connectorConfig.adminOperationTimeoutMs())
                .defaultOperationTimeoutMs(connectorConfig.operationTimeoutMs())
                .defaultSocketReadTimeoutMs(connectorConfig.socketReadTimeoutMs())
                .numTablets(connectorConfig.maxNumTablets())
                .sslCertFile(connectorConfig.sslRootCert())
                .sslClientCertFiles(connectorConfig.sslClientCert(), connectorConfig.sslClientKey())
                .build();

        syncClient = new YBClient(asyncYBClient);
        yugabyteDBTypeRegistry = taskContext.schema().getTypeRegistry();
        this.queue = queue;
    }

    @Override
    public void execute(ChangeEventSourceContext context, YBPartition partition, YugabyteDBOffsetContext offsetContext) {
        Set<YBPartition> partitions = new YugabyteDBPartition.Provider(connectorConfig).getPartitions();
        boolean hasStartLsnStoredInContext = offsetContext != null && !offsetContext.getTabletSourceInfo().isEmpty();

        LOGGER.info("Coming to execute inside the streaming class now");

        if (!hasStartLsnStoredInContext) {
            LOGGER.info("No start opid found in the context.");
            // if (false /*snapshotter.shouldSnapshot()*/) {
            //     LOGGER.info("Going for snapshot!");
            //     offsetContext = YugabyteDBOffsetContext.initialContextForSnapshot(connectorConfig, connection, clock, partitions);
            // }
            // else {
                offsetContext = YugabyteDBOffsetContext.initialContext(connectorConfig, connection, clock, partitions);
            // }
        }

        try {
            // note to Vaibhav: removing code because it is not used anywhere

            getChanges2(context, partition, offsetContext, hasStartLsnStoredInContext);
        }
        catch (Throwable e) {
            errorHandler.setProducerThrowable(e);
        }
        finally {

            if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
                // Need to see in CDCSDK what can be done.
            }
            if (asyncYBClient != null) {
                try {
                    asyncYBClient.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (syncClient != null) {
                try {
                    syncClient.close();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // private GetChangesResponse getChangeResponse(YugabyteDBOffsetContext offsetContext) throws Exception {
    //     return null;
    // }

    // private void getSnapshotChanges(ChangeEventSourceContext context,
    //                                 YBPartition partitionn,
    //                                 YugabyteDBOffsetContext offsetContext,
    //                                 boolean previousOffsetPresent) {

    // }

    private void bootstrapTablet(YBTable table, String tabletId) throws Exception {
        GetCheckpointResponse getCheckpointResponse = this.syncClient.getCheckpoint(table, connectorConfig.streamId(), tabletId);

        long term = getCheckpointResponse.getTerm();
        long index = getCheckpointResponse.getIndex();
        LOGGER.info("Checkpoint for tablet {} before going to bootstrap: {}.{}", tabletId, term, index);
        if (term == -1 && index == -1) {
            LOGGER.info("Bootstrapping the tablet {}", tabletId);
            this.syncClient.bootstrapTablet(table, connectorConfig.streamId(), tabletId, 0, 0, true, true);
        }
        else {
            LOGGER.info("Skipping bootstrap for tablet {} as it has a checkpoint {}.{}", tabletId, term, index);
        }
    }

    private void bootstrapTabletWithRetry(List<Pair<String,String>> tabletPairList) throws Exception {
        short retryCountForBootstrapping = 0;
        final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
        for (Pair<String, String> entry : tabletPairList) {
            // entry is a Pair<tableId, tabletId>
            boolean shouldRetry = true;
            while (retryCountForBootstrapping <= connectorConfig.maxConnectorRetries() && shouldRetry) {
                try {
                    bootstrapTablet(this.syncClient.openTableByUUID(entry.getKey()), entry.getValue());

                    // Reset the retry flag if the bootstrap was successful
                    shouldRetry = false;
                } catch (Exception e) {
                    ++retryCountForBootstrapping;

                    // The connector should go for a retry if any exception is thrown
                    shouldRetry = true;

                    if (retryCountForBootstrapping > connectorConfig.maxConnectorRetries()) {
                        LOGGER.error("Failed to bootstrap the tablet {} after {} retries", entry.getValue(), connectorConfig.maxConnectorRetries());
                        throw e;
                    }

                    // If there are retries left, perform them after the specified delay.
                    LOGGER.warn("Error while trying to bootstrap tablet {}; will attempt retry {} of {} after {} milli-seconds. Exception message: {}",
                            entry.getValue(), retryCountForBootstrapping, connectorConfig.maxConnectorRetries(), connectorConfig.connectorRetryDelayMs(), e.getMessage());

                    try {
                        retryMetronome.pause();
                    } catch (InterruptedException ie) {
                        LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }


    private void getChanges2(ChangeEventSourceContext context,
                             YBPartition partitionn,
                             YugabyteDBOffsetContext offsetContext,
                             boolean previousOffsetPresent)
            throws Exception {
        LOGGER.debug("The offset is " + offsetContext.getOffset());

        LOGGER.info("Processing messages");
        // ListTablesResponse tablesResp = syncClient.getTablesList();

        String tabletList = this.connectorConfig.getConfig().getString(YugabyteDBConnectorConfig.TABLET_LIST);

        // This tabletPairList has Pair<String, String> objects wherein the key is the table UUID
        // and the value is tablet UUID
        List<Pair<String, String>> tabletPairList = null;
        try {
            tabletPairList = (List<Pair<String, String>>) ObjectUtil.deserializeObjectFromString(tabletList);
            LOGGER.debug("The tablet list is " + tabletPairList);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        Map<String, YBTable> tableIdToTable = new HashMap<>();
        String streamId = connectorConfig.streamId();

        LOGGER.info("Using DB stream ID: " + streamId);

        Set<String> tIds = tabletPairList.stream().map(pair -> pair.getLeft()).collect(Collectors.toSet());
        for (String tId : tIds) {
            LOGGER.debug("Table UUID: " + tIds);
            YBTable table = this.syncClient.openTableByUUID(tId);
            tableIdToTable.put(tId, table);
        }

        // int noMessageIterations = 0;

        LOGGER.debug("The init tabletSourceInfo before updating is " + offsetContext.getTabletSourceInfo());
        // todo: rename schemaStreamed to something else
        Map<String, Boolean> schemaStreamed = new HashMap<>();
        for (Pair<String, String> entry : tabletPairList) {
            schemaStreamed.put(entry.getValue(), Boolean.TRUE);
        }

        for (Pair<String, String> entry : tabletPairList) {
            final String tabletId = entry.getValue();
            offsetContext.initSourceInfo(tabletId, this.connectorConfig);
        }

        // This will contain the tablet ID mapped to the number of records it has seen in the transactional block.
        // Note that the entry will be created only when a BEGIN block is encountered.
        Map<String, Integer> recordsInTransactionalBlock = new HashMap<>();

        LOGGER.debug("The init tabletSourceInfo after updating is " + offsetContext.getTabletSourceInfo());

        final Metronome pollIntervalMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.cdcPollIntervalms()), Clock.SYSTEM);
        final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);

        // Set<String> snapshotCompleted = new HashSet<>();
        if (!snapshotter.shouldSnapshot()) { // only bootstrap if no snapshot has been enabled
            bootstrapTabletWithRetry(tabletPairList);
        } else {
            LOGGER.info("Skipping bootstrap because snapshot has been taken so streaming will resume there onwards");
        }

        LOGGER.info("Dispatcher in streaming: " + dispatcher.toString());

        short retryCount = 0;
        while (context.isRunning() && retryCount <= connectorConfig.maxConnectorRetries()) {
            try {
                while (context.isRunning() && (offsetContext.getStreamingStoppingLsn() == null ||
                        (lastCompletelyProcessedLsn.compareTo(offsetContext.getStreamingStoppingLsn()) < 0))) {
                    // Pause for the specified duration before asking for a new set of changes from the server
                    LOGGER.debug("Pausing for {} milliseconds before polling further", connectorConfig.cdcPollIntervalms());
                    pollIntervalMetronome.pause();

                    if (this.connectorConfig.cdcLimitPollPerIteration()
                            && queue.remainingCapacity() < queue.totalCapacity()) {
                        LOGGER.debug("Queue has {} items. Skipping", queue.totalCapacity() - queue.remainingCapacity());
                        continue;
                    }

                    for (Pair<String, String> entry : tabletPairList) {
                        final String tabletId = entry.getValue();
                        YBPartition part = new YBPartition(tabletId);
                    //   if (snapshotCompleted.size() == tabletPairList.size()) {
                    //     LOGGER.debug("Snapshot completed for all the tablets! Stopping.");
                    //     if (!snapshotter.shouldStream()) {
                    //         // This block will be executed in case of initial_only mode
                    //         LOGGER.info("Snapshot completed for initial_only mode, stopping the connector now");
                    //         return;
                    //     }
                    //   }


                      OpId cp = /*snapshotter.shouldSnapshot() ? offsetContext.snapshotLSN(tabletId) :*/ offsetContext.lsn(tabletId);
                    //   if (snapshotCompleted.contains(tabletId)) {
                    //     // If the snapshot is completed for a tablet then we should switch to streaming
                    //     if (snapshotter.shouldStream()) {
                    //         LOGGER.debug("Streaming changes for tablet {}", tabletId);
                    //         cp = offsetContext.lsn(tabletId);
                    //         if (cp.getTerm() == -1 && cp.getIndex() == -1) {
                    //             // When the first call will be made to find the lsn after switching
                    //             // from snapshot to streaming - it will return <-1,-1> - in that case
                    //             // we need to call GetChanges with 0,0  checkpoint
                    //             cp = new OpId(0, 0, null, 0, 0);
                    //         }
                    //     }
                    //   }

                      YBTable table = tableIdToTable.get(entry.getKey());

                      LOGGER.debug("Going to fetch for tablet " + tabletId + " from OpId " + cp + " " +
                        "table " + table.getName() + " Running:" + context.isRunning());

                      // Check again if the thread has been interrupted.
                      if (!context.isRunning()) {
                        LOGGER.info("Connector has been stopped");
                        break;
                      }
                        GetChangesResponse response = this.syncClient.getChangesCDCSDK(
                                table, streamId, tabletId,
                                cp.getTerm(), cp.getIndex(), cp.getKey(), cp.getWrite_id(), cp.getTime(), schemaStreamed.get(tabletId));

                        LOGGER.debug("Processing {} records from getChanges call",
                                response.getResp().getCdcSdkProtoRecordsList().size());
                        for (CdcService.CDCSDKProtoRecordPB record : response
                                .getResp()
                                .getCdcSdkProtoRecordsList()) {
                            CdcService.RowMessage m = record.getRowMessage();
                            YbProtoReplicationMessage message = new YbProtoReplicationMessage(
                                    m, this.yugabyteDBTypeRegistry);

                            String pgSchemaNameInRecord = m.getPgschemaName();

                            final OpId lsn = new OpId(record.getCdcSdkOpId().getTerm(),
                                    record.getCdcSdkOpId().getIndex(),
                                    record.getCdcSdkOpId().getWriteIdKey().toByteArray(),
                                    record.getCdcSdkOpId().getWriteId(),
                                    response.getSnapshotTime());

                            if (message.isLastEventForLsn()) {
                                lastCompletelyProcessedLsn = lsn;
                            }

                            try {
                                // Tx BEGIN/END event
                                if (message.isTransactionalMessage()) {
                                    if (!connectorConfig.shouldProvideTransactionMetadata()) {
                                        LOGGER.debug("Received transactional message {}", record);
                                        // Don't skip on BEGIN message as it would flush LSN for the whole transaction
                                        // too early
                                        if (message.getOperation() == Operation.BEGIN) {
                                            LOGGER.debug("LSN in case of BEGIN is " + lsn);

                                            recordsInTransactionalBlock.put(tabletId, 0);
                                        }
                                        if (message.getOperation() == Operation.COMMIT) {
                                            LOGGER.debug("LSN in case of COMMIT is " + lsn);
                                            offsetContext.updateWalPosition(tabletId, lsn, lastCompletelyProcessedLsn, message.getCommitTime(),
                                                    String.valueOf(message.getTransactionId()), null, null/* taskContext.getSlotXmin(connection) */);
                                            commitMessage(part, offsetContext, lsn);

                                            if (recordsInTransactionalBlock.containsKey(tabletId)) {
                                                if (recordsInTransactionalBlock.get(tabletId) == 0) {
                                                    LOGGER.warn("Records in the transactional block of transaction: {}, with LSN: {}, for tablet {} are 0",
                                                                message.getTransactionId(), lsn, tabletId);
                                                } else {
                                                    LOGGER.debug("Records in the transactional block transaction: {}, with LSN: {}, for tablet {}: {}",
                                                                 message.getTransactionId(), lsn, tabletId, recordsInTransactionalBlock.get(tabletId));
                                                }
                                            } else {
                                                throw new DebeziumException("COMMIT record encountered without a preceding BEGIN record");
                                            }

                                            recordsInTransactionalBlock.remove(tabletId);
                                        }
                                        continue;
                                    }

                                    if (message.getOperation() == Operation.BEGIN) {
                                        LOGGER.debug("LSN in case of BEGIN is " + lsn);
                                        dispatcher.dispatchTransactionStartedEvent(part,
                                                message.getTransactionId(), offsetContext);

                                        recordsInTransactionalBlock.put(tabletId, 0);
                                    }
                                    else if (message.getOperation() == Operation.COMMIT) {
                                        LOGGER.debug("LSN in case of COMMIT is " + lsn);
                                        offsetContext.updateWalPosition(tabletId, lsn, lastCompletelyProcessedLsn, message.getCommitTime(),
                                                String.valueOf(message.getTransactionId()), null, null/* taskContext.getSlotXmin(connection) */);
                                        commitMessage(part, offsetContext, lsn);
                                        dispatcher.dispatchTransactionCommittedEvent(part, offsetContext);

                                        if (recordsInTransactionalBlock.containsKey(tabletId)) {
                                            if (recordsInTransactionalBlock.get(tabletId) == 0) {
                                                LOGGER.warn("Records in the transactional block of transaction: {}, with LSN: {}, for tablet {} are 0",
                                                            message.getTransactionId(), lsn, tabletId);
                                            } else {
                                                LOGGER.debug("Records in the transactional block transaction: {}, with LSN: {}, for tablet {}: {}",
                                                             message.getTransactionId(), lsn, tabletId, recordsInTransactionalBlock.get(tabletId));
                                            }
                                        } else {
                                            throw new DebeziumException("COMMIT record encountered without a preceding BEGIN record");
                                        }

                                        recordsInTransactionalBlock.remove(tabletId);
                                    }
                                    maybeWarnAboutGrowingWalBacklog(true);
                                }
                                else if (message.isDDLMessage()) {
                                    LOGGER.debug("Received DDL message {}", message.getSchema().toString()
                                            + " the table is " + message.getTable());

                                    // If a DDL message is received for a tablet, we do not need its schema again
                                    schemaStreamed.put(tabletId, Boolean.FALSE);

                                    TableId tableId = null;
                                    if (message.getOperation() != Operation.NOOP) {
                                        tableId = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaNameInRecord);
                                        Objects.requireNonNull(tableId);
                                    }
                                    // Getting the table with the help of the schema.
                                    Table t = schema.tableFor(tableId);
                                    LOGGER.debug("The schema is already registered {}", t);
                                    if (t == null) {
                                        // If we fail to achieve the table, that means we have not specified correct schema information,
                                        // now try to refresh the schema.
                                        schema.refreshWithSchema(tableId, message.getSchema(), pgSchemaNameInRecord);
                                    }
                                }
                                // DML event
                                else {
                                    TableId tableId = null;
                                    if (message.getOperation() != Operation.NOOP) {
                                        tableId = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaNameInRecord);
                                        Objects.requireNonNull(tableId);
                                    }
                                    // If you need to print the received record, change debug level to info
                                    LOGGER.debug("Received DML record {}", record);

                                    offsetContext.updateWalPosition(tabletId, lsn, lastCompletelyProcessedLsn, message.getCommitTime(),
                                            String.valueOf(message.getTransactionId()), tableId, null/* taskContext.getSlotXmin(connection) */);

                                    boolean dispatched = message.getOperation() != Operation.NOOP
                                            && dispatcher.dispatchDataChangeEvent(part, tableId, new YugabyteDBChangeRecordEmitter(part, offsetContext, clock, connectorConfig,
                                                    schema, connection, tableId, message, pgSchemaNameInRecord));

                                    if (recordsInTransactionalBlock.containsKey(tabletId)) {
                                        recordsInTransactionalBlock.merge(tabletId, 1, Integer::sum);
                                    }

                                    maybeWarnAboutGrowingWalBacklog(dispatched);
                                }
                            }
                            catch (InterruptedException ie) {
                                ie.printStackTrace();
                            }
                            catch (SQLException se) {
                                se.printStackTrace();
                            }
                        }

                        probeConnectionIfNeeded();

                        if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
                            // During catch up streaming, the streaming phase needs to hold a transaction open so that
                            // the phase can stream event up to a specific lsn and the snapshot that occurs after the catch up
                            // streaming will not lose the current view of data. Since we need to hold the transaction open
                            // for the snapshot, this block must not commit during catch up streaming.
                            // CDCSDK Find out why this fails : connection.commit();
                        }
                        OpId finalOpid = new OpId(
                                response.getTerm(),
                                response.getIndex(),
                                response.getKey(),
                                response.getWriteId(),
                                response.getSnapshotTime());
                        offsetContext.getSourceInfo(tabletId)
                                .updateLastCommit(finalOpid);

                        LOGGER.debug("The final opid is " + finalOpid);
                        // if (snapshotter.shouldSnapshot() && finalOpid.equals(new OpId(-1, -1, "".getBytes(), 0 ,0))) {
                        //   snapshotCompleted.add(tabletId);
                        //   LOGGER.info("Stopping the snapshot for the tablet " + tabletId);
                        // }
                    }
                    // Reset the retry count, because if flow reached at this point, it means that the connection
                    // has succeeded
                    retryCount = 0;
                }
            }
            catch (Exception e) {
                ++retryCount;
                // If the retry limit is exceeded, log an error with a description and throw the exception.
                if (retryCount > connectorConfig.maxConnectorRetries()) {
                    LOGGER.error("Too many errors while trying to get the changes from server. All {} retries failed.", connectorConfig.maxConnectorRetries());
                    throw e;
                }

                // If there are retries left, perform them after the specified delay.
                LOGGER.warn("Error while trying to get the changes from the server; will attempt retry {} of {} after {} milli-seconds. Exception message: {}",
                        retryCount, connectorConfig.maxConnectorRetries(), connectorConfig.connectorRetryDelayMs(), e.getMessage());
                LOGGER.debug("Stacktrace", e);

                try {
                    retryMetronome.pause();
                }
                catch (InterruptedException ie) {
                    LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void searchWalPosition(ChangeEventSourceContext context, final ReplicationStream stream, final WalPositionLocator walPosition)
            throws SQLException, InterruptedException {
        AtomicReference<OpId> resumeLsn = new AtomicReference<>();
        int noMessageIterations = 0;

        LOGGER.info("Searching for WAL resume position");
        while (context.isRunning() && resumeLsn.get() == null) {

            boolean receivedMessage = stream.readPending(message -> {
                final OpId lsn = stream.lastReceivedLsn();
                resumeLsn.set(walPosition.resumeFromLsn(lsn, message).orElse(null));
            });

            if (receivedMessage) {
                noMessageIterations = 0;
            }
            else {
                noMessageIterations++;
                if (noMessageIterations >= THROTTLE_NO_MESSAGE_BEFORE_PAUSE) {
                    noMessageIterations = 0;
                    pauseNoMessage.sleepWhen(true);
                }
            }

            probeConnectionIfNeeded();
        }
        LOGGER.info("WAL resume position '{}' discovered", resumeLsn.get());
    }

    private void probeConnectionIfNeeded() throws SQLException {
        // CDCSDK Find out why it fails.
        // if (connectionProbeTimer.hasElapsed()) {
        // connection.prepareQuery("SELECT 1");
        // connection.commit();
        // }
    }

    private void commitMessage(YBPartition partition, YugabyteDBOffsetContext offsetContext,
                               final OpId lsn)
            throws SQLException, InterruptedException {
        lastCompletelyProcessedLsn = lsn;
        offsetContext.updateCommitPosition(lsn, lastCompletelyProcessedLsn);
        maybeWarnAboutGrowingWalBacklog(false);
        dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
    }

    /**
     * If we receive change events but all of them get filtered out, we cannot
     * commit any new offset with Apache Kafka. This in turn means no LSN is ever
     * acknowledged with the replication slot, causing any ever growing WAL backlog.
     * <p>
     * This situation typically occurs if there are changes on the database server,
     * (e.g. in an excluded database), but none of them is in table.include.list.
     * To prevent this, heartbeats can be used, as they will allow us to commit
     * offsets also when not propagating any "real" change event.
     * <p>
     * The purpose of this method is to detect this situation and log a warning
     * every {@link #GROWING_WAL_WARNING_LOG_INTERVAL} filtered events.
     *
     * @param dispatched
     *            Whether an event was sent to the broker or not
     */
    private void maybeWarnAboutGrowingWalBacklog(boolean dispatched) {
        if (dispatched) {
            numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
        }
        else {
            numberOfEventsSinceLastEventSentOrWalGrowingWarning++;
        }

        if (numberOfEventsSinceLastEventSentOrWalGrowingWarning > GROWING_WAL_WARNING_LOG_INTERVAL && !dispatcher.heartbeatsEnabled()) {
            LOGGER.warn("Received {} events which were all filtered out, so no offset could be committed. "
                    + "This prevents the replication slot from acknowledging the processed WAL offsets, "
                    + "causing a growing backlog of non-removeable WAL segments on the database server. "
                    + "Consider to either adjust your filter configuration or enable heartbeat events "
                    + "(via the {} option) to avoid this situation.",
                    numberOfEventsSinceLastEventSentOrWalGrowingWarning, Heartbeat.HEARTBEAT_INTERVAL_PROPERTY_NAME);

            numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
        }
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        // try {
        LOGGER.debug("Commit offset: " + offset);
        ReplicationStream replicationStream = null; // this.replicationStream.get();
        final OpId commitLsn = null; // OpId.valueOf((String) offset.get(PostgresOffsetContext.LAST_COMMIT_LSN_KEY));
        final OpId changeLsn = OpId.valueOf((String) offset.get(YugabyteDBOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY));
        final OpId lsn = (commitLsn != null) ? commitLsn : changeLsn;

        if (replicationStream != null && lsn != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Flushing LSN to server: {}", lsn);
            }
            // tell the server the point up to which we've processed data, so it can be free to recycle WAL segments
            // CDCSDK yugabyte does it automatically.
            // but we may need an API
            // replicationStream.flushLsn(lsn);
        }
        else {
            LOGGER.debug("Streaming has already stopped, ignoring commit callback...");
        }
    }

    /**
     * Returns whether the current streaming phase is running a catch up streaming
     * phase that runs before a snapshot. This is useful for transaction
     * management.
     *
     * During pre-snapshot catch up streaming, we open the snapshot transaction
     * early and hold the transaction open throughout the pre snapshot catch up
     * streaming phase so that we know where to stop streaming and can start the
     * snapshot phase at a consistent location. This is opposed the regular streaming,
     * where we do not a lingering open transaction.
     *
     * @return true if the current streaming phase is performing catch up streaming
     */
    private boolean isInPreSnapshotCatchUpStreaming(YugabyteDBOffsetContext offsetContext) {
        return offsetContext.getStreamingStoppingLsn() != null;
    }

    @FunctionalInterface
    public static interface PgConnectionSupplier {
        BaseConnection get() throws SQLException;
    }
}
