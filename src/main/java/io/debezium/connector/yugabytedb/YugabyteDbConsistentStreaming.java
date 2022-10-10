package io.debezium.connector.yugabytedb;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.connector.yugabytedb.connection.ReplicationConnection;
import io.debezium.connector.yugabytedb.connection.ReplicationMessage;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.connection.pgproto.YbProtoReplicationMessage;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;
import org.yb.client.GetChangesResponse;
import org.yb.client.ListTablesResponse;
import org.yb.client.YBTable;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class YugabyteDbConsistentStreaming extends YugabyteDBStreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDbConsistentStreaming.class);

    public YugabyteDbConsistentStreaming(YugabyteDBConnectorConfig connectorConfig, Snapshotter snapshotter, YugabyteDBConnection connection, YugabyteDBEventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock, YugabyteDBSchema schema, YugabyteDBTaskContext taskContext, ReplicationConnection replicationConnection, ChangeEventQueue<DataChangeEvent> queue) {
        super(connectorConfig, snapshotter, connection, dispatcher, errorHandler, clock, schema, taskContext, replicationConnection, queue);
    }

    @Override
    protected void getChanges2(ChangeEventSourceContext context,
                             YBPartition partitionn,
                             YugabyteDBOffsetContext offsetContext,
                             boolean previousOffsetPresent)
            throws Exception {
        LOGGER.debug("The offset is " + offsetContext.getOffset());

        LOGGER.info("Processing messages");
        ListTablesResponse tablesResp = syncClient.getTablesList();

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

        int noMessageIterations = 0;

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

        Set<String> snapshotCompleted = new HashSet<>();
        bootstrapTabletWithRetry(tabletPairList);

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
                        if (snapshotCompleted.size() == tabletPairList.size()) {
                            LOGGER.debug("Snapshot completed for all the tablets! Stopping.");
                            if (!snapshotter.shouldStream()) {
                                // This block will be executed in case of initial_only mode
                                LOGGER.info("Snapshot completed for initial_only mode, stopping the connector now");
                                return;
                            }
                        }


                        OpId cp = snapshotter.shouldSnapshot() ? offsetContext.snapshotLSN(tabletId) : offsetContext.lsn(tabletId);
                        if (snapshotCompleted.contains(tabletId)) {
                            // If the snapshot is completed for a tablet then we should switch to streaming
                            if (snapshotter.shouldStream()) {
                                LOGGER.debug("Streaming changes for tablet {}", tabletId);
                                cp = offsetContext.lsn(tabletId);
                                if (cp.getTerm() == -1 && cp.getIndex() == -1) {
                                    // When the first call will be made to find the lsn after switching
                                    // from snapshot to streaming - it will return <-1,-1> - in that case
                                    // we need to call GetChanges with 0,0  checkpoint
                                    cp = new OpId(0, 0, null, 0, 0);
                                }
                            }
                        }

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

                            dispatchMessage(offsetContext, schemaStreamed, recordsInTransactionalBlock, tabletId, part, response, record, m, message);
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
                        if (snapshotter.shouldSnapshot() && finalOpid.equals(new OpId(-1, -1, "".getBytes(), 0 ,0))) {
                            snapshotCompleted.add(tabletId);
                            LOGGER.info("Stopping the snapshot for the tablet " + tabletId);
                        }
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

    private boolean dispatchMessage(YugabyteDBOffsetContext offsetContext,
                                    Map<String, Boolean> schemaStreamed,
                                    Map<String, Integer> recordsInTransactionalBlock,
                                    String tabletId,
                                    YBPartition part,
                                    GetChangesResponse response,
                                    CdcService.CDCSDKProtoRecordPB record,
                                    CdcService.RowMessage m,
                                    YbProtoReplicationMessage message) {
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
                    if (message.getOperation() == ReplicationMessage.Operation.BEGIN) {
                        LOGGER.debug("LSN in case of BEGIN is " + lsn);

                        recordsInTransactionalBlock.put(tabletId, 0);
                    }
                    if (message.getOperation() == ReplicationMessage.Operation.COMMIT) {
                        LOGGER.debug("LSN in case of COMMIT is " + lsn);
                        offsetContext.updateWalPosition(tabletId, lsn, lastCompletelyProcessedLsn, message.getRawCommitTime(),
                                String.valueOf(message.getTransactionId()), null, null,/* taskContext.getSlotXmin(connection) */
                                message.getRecordTime());
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
                    return true;
                }

                if (message.getOperation() == ReplicationMessage.Operation.BEGIN) {
                    LOGGER.debug("LSN in case of BEGIN is " + lsn);
                    dispatcher.dispatchTransactionStartedEvent(part,
                            message.getTransactionId(), offsetContext);

                    recordsInTransactionalBlock.put(tabletId, 0);
                }
                else if (message.getOperation() == ReplicationMessage.Operation.COMMIT) {
                    LOGGER.debug("LSN in case of COMMIT is " + lsn);
                    offsetContext.updateWalPosition(tabletId, lsn, lastCompletelyProcessedLsn, message.getRawCommitTime(),
                            String.valueOf(message.getTransactionId()), null, null,/* taskContext.getSlotXmin(connection) */
                            message.getRecordTime());
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
                if (message.getOperation() != ReplicationMessage.Operation.NOOP) {
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
                if (message.getOperation() != ReplicationMessage.Operation.NOOP) {
                    tableId = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaNameInRecord);
                    Objects.requireNonNull(tableId);
                }
                // If you need to print the received record, change debug level to info
                LOGGER.debug("Received DML record {}", record);

                offsetContext.updateWalPosition(tabletId, lsn, lastCompletelyProcessedLsn, message.getRawCommitTime(),
                        String.valueOf(message.getTransactionId()), tableId, null/* taskContext.getSlotXmin(connection) */,
                        message.getRecordTime());

                boolean dispatched = message.getOperation() != ReplicationMessage.Operation.NOOP
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
        return false;
    }

}
