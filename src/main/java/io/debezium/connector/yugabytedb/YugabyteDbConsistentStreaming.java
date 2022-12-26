package io.debezium.connector.yugabytedb;

import io.debezium.DebeziumException;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.connector.yugabytedb.connection.ReplicationConnection;
import io.debezium.connector.yugabytedb.connection.ReplicationMessage;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.connection.pgproto.YbProtoReplicationMessage;
import io.debezium.connector.yugabytedb.consistent.Merger;
import io.debezium.connector.yugabytedb.consistent.Message;
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
import org.yb.client.*;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

public class YugabyteDbConsistentStreaming extends YugabyteDBStreamingChangeEventSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDbConsistentStreaming.class);

    private long totalMessagesSent = 0;

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

        String tabletList = this.connectorConfig.getConfig().getString(YugabyteDBConnectorConfig.TABLET_LIST);

        // This tabletPairList has Pair<String, String> objects wherein the key is the table UUID
        // and the value is tablet UUID
        List<Pair<String, String>> tabletPairList = null;
        try {
            tabletPairList = (List<Pair<String, String>>) ObjectUtil.deserializeObjectFromString(tabletList);
            LOGGER.debug("The tablet list is " + tabletPairList);
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.error("Exception while deserializing tablet pair list", e);
            throw new RuntimeException(e);
        }

        Map<String, YBTable> tableIdToTable = new HashMap<>();
        Map<String, GetTabletListToPollForCDCResponse> tabletListResponse = new HashMap<>();
        String streamId = connectorConfig.streamId();

        LOGGER.info("Using DB stream ID: " + streamId);

        Set<String> tIds = tabletPairList.stream().map(pair -> pair.getLeft()).collect(Collectors.toSet());
        for (String tId : tIds) {
            LOGGER.debug("Table UUID: " + tIds);
            YBTable table = this.syncClient.openTableByUUID(tId);
            tableIdToTable.put(tId, table);

            GetTabletListToPollForCDCResponse resp =
                    this.syncClient.getTabletListToPollForCdc(table, streamId, tId);
            tabletListResponse.put(tId, resp);
        }

        LOGGER.debug("The init tabletSourceInfo before updating is " + offsetContext.getTabletSourceInfo());

        // Initialize the offsetContext and other supporting flags
        Map<String, Boolean> schemaNeeded = new HashMap<>();
        for (Pair<String, String> entry : tabletPairList) {
            // entry.getValue() will give the tabletId
            OpId opId = YBClientUtils.getOpIdFromGetTabletListResponse(
                    tabletListResponse.get(entry.getKey()), entry.getValue());
            offsetContext.initSourceInfo(entry.getValue(), this.connectorConfig, opId);
            schemaNeeded.put(entry.getValue(), Boolean.TRUE);
        }

        for (Pair<String, String> entry : tabletPairList) {
            final String tabletId = entry.getValue();
            offsetContext.initSourceInfo(tabletId, this.connectorConfig);
        }

        Merger merger = new Merger(tabletPairList.stream().map(Pair::getRight).collect(Collectors.toList()));

        // This will contain the tablet ID mapped to the number of records it has seen
        // in the transactional block. Note that the entry will be created only when
        // a BEGIN block is encountered.
        Map<String, Integer> recordsInTransactionalBlock = new HashMap<>();

        // This will contain the tablet ID mapped to the number of begin records observed for
        // a tablet. Consider the scenario for a colocated tablet with two tables, it is possible
        // that we can encounter BEGIN-BEGIN-COMMIT-COMMIT. To handle this scenario, we need the
        // count for the BEGIN records so that we can verify that we have equal COMMIT records
        // in the stream as well.
        Map<String, Integer> beginCountForTablet = new HashMap<>();

        LOGGER.debug("The init tabletSourceInfo after updating is " + offsetContext.getTabletSourceInfo());

        // Only bootstrap if no snapshot has been enabled - if snapshot is enabled then
        // the assumption is that there will already be some checkpoints for the tablet in
        // the cdc_state table. Avoiding additional bootstrap call in that case will also help
        // us avoid unnecessary network calls.
        if (snapshotter.shouldSnapshot()) {
            LOGGER.info("Skipping bootstrap because snapshot has been taken so streaming will resume there onwards");
        } else {
            bootstrapTabletWithRetry(tabletPairList);
        }

        // This log while indicate that the connector has either bootstrapped the tablets or skipped
        // it so that streaming can begin now. This is added to indicate the tests or pipelines
        // waiting for the bootstrapping to finish so that they can start inserting data now.
        LOGGER.info("Beginning to poll the changes from the server");

        short retryCount = 0;

        // Helper internal variable to log GetChanges request at regular intervals.
        long lastLoggedTimeForGetChanges = System.currentTimeMillis();

        String curTabletId = "";
        while (context.isRunning() && retryCount <= connectorConfig.maxConnectorRetries()) {
            try {
                while (context.isRunning() && (offsetContext.getStreamingStoppingLsn() == null ||
                        (lastCompletelyProcessedLsn.compareTo(offsetContext.getStreamingStoppingLsn()) < 0))) {
                    // Pause for the specified duration before asking for a new set of changes from the server
                    LOGGER.debug("Pausing for {} milliseconds before polling further", connectorConfig.cdcPollIntervalms());
                    final Metronome pollIntervalMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.cdcPollIntervalms()), Clock.SYSTEM);
                    pollIntervalMetronome.pause();

                    if (this.connectorConfig.cdcLimitPollPerIteration()
                            && queue.remainingCapacity() < queue.totalCapacity()) {
                        LOGGER.debug("Queue has {} items. Skipping", queue.totalCapacity() - queue.remainingCapacity());
                        continue;
                    }

                    for (Pair<String, String> entry : tabletPairList) {
                        final String tabletId = entry.getValue();
                        curTabletId = entry.getValue();
                        YBPartition part = new YBPartition(tabletId);

                        OpId cp = offsetContext.lsn(tabletId);

                        YBTable table = tableIdToTable.get(entry.getKey());

                        if (LOGGER.isDebugEnabled()
                                || (connectorConfig.logGetChanges() && System.currentTimeMillis() >= (lastLoggedTimeForGetChanges + connectorConfig.logGetChangesIntervalMs()))) {
                            LOGGER.info("Requesting changes for tablet {} from OpId {} for table {}",
                                    tabletId, cp, table.getName());
                            lastLoggedTimeForGetChanges = System.currentTimeMillis();
                        }

                        // Check again if the thread has been interrupted.
                        if (!context.isRunning()) {
                            LOGGER.info("Connector has been stopped");
                            break;
                        }

                        GetChangesResponse response = null;

                        if (schemaNeeded.get(tabletId)) {
                            LOGGER.info("Requesting schema for tablet: {}", tabletId);
                        }

                        if (merger.isSlotEmpty(tabletId)) {
                            try {
                                response = this.syncClient.getChangesCDCSDK(
                                        table, streamId, tabletId, cp.getTerm(), cp.getIndex(), cp.getKey(),
                                        cp.getWrite_id(), cp.getTime(), schemaNeeded.get(tabletId));
                            } catch (CDCErrorException cdcException) {
                                // Check if exception indicates a tablet split.
                                if (cdcException.getCDCError().getCode() == CdcService.CDCErrorPB.Code.TABLET_SPLIT) {
                                    LOGGER.info("Encountered a tablet split, handling it gracefully");
                                    if (LOGGER.isDebugEnabled()) {
                                        cdcException.printStackTrace();
                                    }

                                    handleTabletSplit(cdcException, tabletPairList, offsetContext, streamId, schemaNeeded);

                                    // Break out of the loop so that the iteration can start afresh on the modified list.
                                    break;
                                } else {
                                    throw cdcException;
                                }
                            }

                            LOGGER.debug("Processing {} records from getChanges call",
                                    response.getResp().getCdcSdkProtoRecordsList().size());
                            for (CdcService.CDCSDKProtoRecordPB record : response
                                    .getResp()
                                    .getCdcSdkProtoRecordsList()) {
                                CdcService.RowMessage.Op op = record.getRowMessage().getOp();
                                if (op != CdcService.RowMessage.Op.INSERT) {
                                    ++totalMessagesSent;
                                }
                                merger.addMessage(new Message.Builder()
                                        .setRecord(record)
                                        .setTabletId(tabletId)
                                        .setSnapshotTime(response.getSnapshotTime())
                                        .build());
                                OpId finalOpid = new OpId(
                                        response.getTerm(),
                                        response.getIndex(),
                                        response.getKey(),
                                        response.getWriteId(),
                                        response.getSnapshotTime());
                                offsetContext.getSourceInfo(tabletId)
                                        .updateLastCommit(finalOpid);
                                LOGGER.debug("The final opid is " + finalOpid);
                            }
                        } else {
                            LOGGER.info("Remaining records available in merger: {}", merger.totalQueueSize());
                            LOGGER.info("Records for tablet {} in merger: {}", tabletId, merger.pendingMessagesInTablet(tabletId));
                            LOGGER.info("Safetime for tablet {}: {} with stream safe-time: {}", tabletId, merger.safeTimeForTablet(tabletId), merger.streamSafeTime());
                            LOGGER.info("=========================================================");
                        }

                        probeConnectionIfNeeded();

                        if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
                            // During catch up streaming, the streaming phase needs to hold a transaction open so that
                            // the phase can stream event up to a specific lsn and the snapshot that occurs after the catch up
                            // streaming will not lose the current view of data. Since we need to hold the transaction open
                            // for the snapshot, this block must not commit during catch up streaming.
                            // CDCSDK Find out why this fails : connection.commit();
                        }
                    }

                    while (merger.peek().isPresent()) {
                        LOGGER.info("Merger has records");
                        Message message = merger.poll();
                        CdcService.RowMessage m = message.record.getRowMessage();
                        YbProtoReplicationMessage ybMessage = new YbProtoReplicationMessage(
                                m, this.yugabyteDBTypeRegistry);
                        dispatchMessage(offsetContext, schemaNeeded, recordsInTransactionalBlock,
                                beginCountForTablet, message.tablet, new YBPartition(message.tablet),
                                message.snapShotTime, message.record, m, ybMessage);
                    }

                    // Reset the retry count, because if flow reached at this point, it means that the connection
                    // has succeeded
                    retryCount = 0;
                }
            } catch (Exception e) {
                ++retryCount;
                // If the retry limit is exceeded, log an error with a description and throw the exception.
                if (retryCount > connectorConfig.maxConnectorRetries()) {
                    LOGGER.error("Too many errors while trying to get the changes from server for tablet: {}. All {} retries failed.", curTabletId, connectorConfig.maxConnectorRetries());
                    throw e;
                }

                // If there are retries left, perform them after the specified delay.
                LOGGER.warn("Error while trying to get the changes from the server; will attempt retry {} of {} after {} milli-seconds. Exception message: {}",
                        retryCount, connectorConfig.maxConnectorRetries(), connectorConfig.connectorRetryDelayMs(), e.getMessage());
                LOGGER.warn("Stacktrace", e);

                try {
                    final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
                    retryMetronome.pause();
                }
                catch (InterruptedException ie) {
                    LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void dispatchMessage(YugabyteDBOffsetContext offsetContext, Map<String, Boolean> schemaNeeded,
                                 Map<String, Integer> recordsInTransactionalBlock,
                                 Map<String, Integer> beginCountForTablet,
                                 String tabletId, YBPartition part, long snapshotTime,
                                 CdcService.CDCSDKProtoRecordPB record, CdcService.RowMessage m,
                                 YbProtoReplicationMessage message) throws SQLException {
        String pgSchemaNameInRecord = m.getPgschemaName();

        // This is a hack to skip tables in case of colocated tables
        TableId tempTid = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaNameInRecord);
        if (!new Filters(connectorConfig).tableFilter().isIncluded(tempTid)) {
            return;
        }

        final OpId lsn = new OpId(record.getCdcSdkOpId().getTerm(),
                record.getCdcSdkOpId().getIndex(),
                record.getCdcSdkOpId().getWriteIdKey().toByteArray(),
                record.getCdcSdkOpId().getWriteId(),
                snapshotTime);

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
                        beginCountForTablet.merge(tabletId, 1, Integer::sum);
                    }
                    if (message.getOperation() == ReplicationMessage.Operation.COMMIT) {
                        LOGGER.debug("LSN in case of COMMIT is " + lsn);
                        offsetContext.updateWalPosition(tabletId, lsn, lastCompletelyProcessedLsn, message.getRawCommitTime(),
                                String.valueOf(message.getTransactionId()), null, null,/* taskContext.getSlotXmin(connection) */
                                message.getRecordTime());
                        commitMessage(part, offsetContext, lsn);

                        if (recordsInTransactionalBlock.containsKey(tabletId)) {
                            if (recordsInTransactionalBlock.get(tabletId) == 0) {
                                LOGGER.debug("Records in the transactional block of transaction: {}, with LSN: {}, for tablet {} are 0",
                                        message.getTransactionId(), lsn, tabletId);
                            } else {
                                LOGGER.debug("Records in the transactional block transaction: {}, with LSN: {}, for tablet {}: {}",
                                        message.getTransactionId(), lsn, tabletId, recordsInTransactionalBlock.get(tabletId));
                            }
                        } else if (beginCountForTablet.get(tabletId).intValue() == 0) {
                            throw new DebeziumException("COMMIT record encountered without a preceding BEGIN record");
                        }

                        recordsInTransactionalBlock.remove(tabletId);
                        beginCountForTablet.merge(tabletId, -1, Integer::sum);
                    }
                    return;
                }

                if (message.getOperation() == ReplicationMessage.Operation.BEGIN) {
                    LOGGER.debug("LSN in case of BEGIN is " + lsn);
                    dispatcher.dispatchTransactionStartedEvent(part,
                            message.getTransactionId(), offsetContext);

                    recordsInTransactionalBlock.put(tabletId, 0);
                    beginCountForTablet.merge(tabletId, 1, Integer::sum);
                }
                else if (message.getOperation() == ReplicationMessage.Operation.COMMIT) {
                    LOGGER.debug("LSN in case of COMMIT is " + lsn);
                    offsetContext.updateWalPosition(tabletId, lsn, lastCompletelyProcessedLsn, message.getRawCommitTime(),
                            String.valueOf(message.getTransactionId()), null, null/* taskContext.getSlotXmin(connection) */,
                            message.getRecordTime());
                    commitMessage(part, offsetContext, lsn);
                    dispatcher.dispatchTransactionCommittedEvent(part, offsetContext);

                    if (recordsInTransactionalBlock.containsKey(tabletId)) {
                        if (recordsInTransactionalBlock.get(tabletId) == 0) {
                            LOGGER.debug("Records in the transactional block of transaction: {}, with LSN: {}, for tablet {} are 0",
                                    message.getTransactionId(), lsn, tabletId);
                        } else {
                            LOGGER.debug("Records in the transactional block transaction: {}, with LSN: {}, for tablet {}: {}",
                                    message.getTransactionId(), lsn, tabletId, recordsInTransactionalBlock.get(tabletId));
                        }
                    } else if (beginCountForTablet.get(tabletId).intValue() == 0) {
                        throw new DebeziumException("COMMIT record encountered without a preceding BEGIN record");
                    }

                    recordsInTransactionalBlock.remove(tabletId);
                    beginCountForTablet.merge(tabletId, -1, Integer::sum);
                }
                maybeWarnAboutGrowingWalBacklog(true);
            }
            else if (message.isDDLMessage()) {
                LOGGER.debug("Received DDL message {}", message.getSchema().toString()
                        + " the table is " + message.getTable());

                // If a DDL message is received for a tablet, we do not need its schema again
                schemaNeeded.put(tabletId, Boolean.FALSE);

                TableId tableId = null;
                if (message.getOperation() != ReplicationMessage.Operation.NOOP) {
                    tableId = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaNameInRecord);
                    Objects.requireNonNull(tableId);
                }
                // Getting the table with the help of the schema.
                Table t = schema.tableForTablet(tableId, tabletId);
                if (YugabyteDBSchema.shouldRefreshSchema(t, message.getSchema())) {
                    // If we fail to achieve the table, that means we have not specified correct schema information,
                    // now try to refresh the schema.
                    if (t == null) {
                        LOGGER.info("Registering the schema for tablet {} since it was not registered already", tabletId);
                    } else {
                        LOGGER.info("Refreshing the schema for tablet {} because of mismatch in cached schema and received schema", tabletId);
                    }
                    schema.refreshSchemaWithTabletId(tableId, message.getSchema(), pgSchemaNameInRecord, tabletId);
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
                        schema, connection, tableId, message, pgSchemaNameInRecord, tabletId, taskContext.isBeforeImageEnabled()));

                if (recordsInTransactionalBlock.containsKey(tabletId)) {
                    recordsInTransactionalBlock.merge(tabletId, 1, Integer::sum);
                }

                maybeWarnAboutGrowingWalBacklog(dispatched);
            }
        } catch (InterruptedException ie) {
            LOGGER.error("Interrupted exception while processing change records", ie);
            Thread.currentThread().interrupt();
        }
        return;
    }
}
