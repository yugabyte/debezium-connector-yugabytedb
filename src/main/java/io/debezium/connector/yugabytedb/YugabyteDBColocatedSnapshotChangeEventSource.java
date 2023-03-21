package io.debezium.connector.yugabytedb;

import io.debezium.DebeziumException;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.connector.yugabytedb.connection.ReplicationMessage;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.connection.pgproto.YbProtoReplicationMessage;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;
import org.yb.client.*;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class to facilitate the processing of snapshot for only the colocated tables in YugabyteDB. We
 * have made one assumption here that all the tables which will become a part of the task executing
 * this class will be colocated tables.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBColocatedSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource<YBPartition, YugabyteDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBColocatedSnapshotChangeEventSource.class);
    private final YugabyteDBConnectorConfig connectorConfig;
    private final YugabyteDBSchema schema;
    private final SnapshotProgressListener<YBPartition> snapshotProgressListener;
    private final YugabyteDBTaskContext taskContext;
    private final EventDispatcher<YBPartition, TableId> dispatcher;
    protected final Clock clock;
    private final Snapshotter snapshotter;
    private final YugabyteDBConnection connection;
    private final YBClient syncClient;

    private OpId lastCompletelyProcessedLsn;

    private final YugabyteDBTypeRegistry yugabyteDbTypeRegistry;

    private String colocatedTabletId;

    private boolean snapshotComplete = false;

    private Map<String, CdcSdkCheckpoint> tabletToExplicitCheckpoint;

    public YugabyteDBColocatedSnapshotChangeEventSource(
            YugabyteDBConnectorConfig connectorConfig, YugabyteDBTaskContext taskContext,
            Snapshotter snapshotter, YugabyteDBConnection connection, YugabyteDBSchema schema,
            YugabyteDBEventDispatcher<TableId> dispatcher, Clock clock,
            SnapshotProgressListener<YBPartition> snapshotProgressListener) {
        super(connectorConfig, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.taskContext = taskContext;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotter = snapshotter;
        this.connection = connection;
        this.snapshotProgressListener = snapshotProgressListener;

        AsyncYBClient asyncClient = new AsyncYBClient.AsyncYBClientBuilder(connectorConfig.masterAddresses())
                .defaultAdminOperationTimeoutMs(connectorConfig.adminOperationTimeoutMs())
                .defaultOperationTimeoutMs(connectorConfig.operationTimeoutMs())
                .defaultSocketReadTimeoutMs(connectorConfig.socketReadTimeoutMs())
                .numTablets(connectorConfig.maxNumTablets())
                .sslCertFile(connectorConfig.sslRootCert())
                .sslClientCertFiles(connectorConfig.sslClientCert(), connectorConfig.sslClientKey())
                .build();

        this.syncClient = new YBClient(asyncClient);

        this.yugabyteDbTypeRegistry = taskContext.schema().getTypeRegistry();
        this.tabletToExplicitCheckpoint = new HashMap<>();
    }

    @Override
    public SnapshotResult<YugabyteDBOffsetContext> execute(
            ChangeEventSourceContext context, YBPartition partition,
            YugabyteDBOffsetContext previousOffset) throws InterruptedException {
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
        } catch (Exception e) {
            LOGGER.error("Failed to initialize snapshot context.", e);
            throw new RuntimeException(e);
        }

        boolean completedSuccessfully = true;

        try {
            snapshotProgressListener.snapshotStarted(partition);
            Set<YBPartition> partitions = new YBPartition.Provider(connectorConfig).getPartitions();

            LOGGER.info("Setting offsetContext/previousOffset for snapshot...");
            previousOffset =
                    YugabyteDBOffsetContext.initialContextForSnapshot(this.connectorConfig,
                                                                      connection, clock,
                                                                      partitions);

            return doExecute(context, partition, previousOffset, ctx, snapshottingTask);
        } catch (InterruptedException e) {
            completedSuccessfully = false;
            LOGGER.warn("Snapshot was interrupted before completion");
            throw e;
        } catch (Exception t) {
            completedSuccessfully = false;
            throw new DebeziumException(t);
        } finally {
            LOGGER.info("Snapshot - Final stage");
            complete(ctx);

            if (completedSuccessfully) {
                snapshotProgressListener.snapshotCompleted(partition);
            } else {
                snapshotProgressListener.snapshotAborted(partition);
            }
        }
    }

    protected SnapshotResult<YugabyteDBOffsetContext> doExecute(
            ChangeEventSourceContext context, YBPartition partition,
            YugabyteDBOffsetContext previousOffset,
            SnapshotContext<YBPartition, YugabyteDBOffsetContext> snapshotContext,
            SnapshottingTask snapshottingTask) throws Exception {
        LOGGER.info("Starting the snapshot process now");

        // Get the list of tablets, we are assuming that all the tables are colocated
        // i.e. all the tables share the same common tablet
        List<Pair<String, String>> tableToTabletIds = null;
        try {
            String tabletList =
                this.connectorConfig.getConfig().getString(YugabyteDBConnectorConfig.TABLET_LIST);
            tableToTabletIds = (List<Pair<String, String>>) ObjectUtil.deserializeObjectFromString(tabletList);

            LOGGER.info("VKVK tableToTabletIds size is {}", tableToTabletIds.size());
            // Since all the tables share the same tablet, get one pair and assign its tabletId to
            // the global member attribute. Assert that the tabletId is common across all the pairs.
            this.colocatedTabletId = tableToTabletIds.get(0).getValue();

            // TODO: Put this assertion loop inside some flag controlled block maybe?
            for (Pair<String, String> p : tableToTabletIds) {
                assert Objects.equals(this.colocatedTabletId, p.getValue());
            }
        } catch (Exception e) {
            LOGGER.error("The tablet list cannot be deserialized");
            throw new DebeziumException(e);
        }

        Map<String, YBTable> tableIdToTable = new HashMap<>();
        Set<String> tableUUIDs = tableToTabletIds.stream()
                .map(Pair::getLeft)
                .collect(Collectors.toSet());
        for (String tableUUID : tableUUIDs) {
            tableIdToTable.put(tableUUID, this.syncClient.openTableByUUID(tableUUID));
        }

        Map<TableId, String> filteredTableIdToUuid = determineTablesForSnapshot(tableIdToTable);

        // Everything will be keyed by the "tableId.colocatedTabletId" combination
        Map<String, Boolean> schemaNeeded = new HashMap<>();
        Set<String> snapshotCompletedTables = new HashSet<>();
        Set<String> snapshotCompletedPreviously = new HashSet<>();

        for (Pair<String, String> entry : tableToTabletIds) {
            schemaNeeded.put(getLookupKey(entry.getKey()), Boolean.TRUE);

            previousOffset.initSourceInfo(getLookupKey(entry.getKey()), this.connectorConfig, new OpId(-1, -1, "".getBytes(), -1, 0));
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
                // Vaibhav: No bootstrap will be required in case any table doesn't need to be snapshotted
                // it can simply be streamed later on.
                // TODO Vaibhav: Confirm this behavior with Adithya and Suranjan
                LOGGER.info(
                        "Skipping the table {} since it is not a part of the snapshot collection include list",
                        tableUuid);
//                YBClientUtils.setCheckpoint(this.syncClient, this.connectorConfig.streamId(),
//                        tableUuid, tabletUuid, -1, -1, true, true);
            }
        }

        // Set checkpoint with bootstrap and initialCheckpoint as false.
        // A call to set the checkpoint is required first otherwise we will get an error
        // from the server side saying:
        // INTERNAL_ERROR[code 21]: Stream ID {} is expired for Tablet ID {}

        // TODO Vaibhav: See if this works without any change on the server side
        for (Pair<String, String> entry : tableToTabletForSnapshot) {
            setCheckpointWithRetryBeforeSnapshot(entry.getKey() /*tableId*/,
                    snapshotCompletedTables,
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

//                        String tabletId = tableIdToTabletId.getValue();
                        // We are keying partitions by the lookup key as well in case of colocated snapshot
                        YBPartition part = new YBPartition(getLookupKey(tableId));

                        // Check if snapshot is completed here, if it is, then break out of the loop
                        if (snapshotCompletedTables.size() == tableToTabletForSnapshot.size()) {
                            LOGGER.info("Snapshot completed for all the colocated tables");
                            this.snapshotComplete = true;
                            return SnapshotResult.completed(previousOffset);
                        }

                        // Skip the tablet if snapshot has already been taken for this tablet
                        if (snapshotCompletedTables.contains(tableId)) {
                            continue;
                        }

                        OpId cp = previousOffset.snapshotLSN(getLookupKey(tableId));

                        if (LOGGER.isDebugEnabled()
                                || (connectorConfig.logGetChanges() && System.currentTimeMillis() >= (lastLoggedTimeForGetChanges + connectorConfig.logGetChangesIntervalMs()))) {
                            LOGGER.info("Requesting changes for tablet {} from OpId {} for table {}",
                                    this.colocatedTabletId, cp, table.getName());
                            lastLoggedTimeForGetChanges = System.currentTimeMillis();
                        }

                        if (!context.isRunning()) {
                            LOGGER.info("Connector has been stopped");
                            break;
                        }

                        GetChangesResponse resp = this.syncClient.getChangesCDCSDK(table,
                                connectorConfig.streamId(), this.colocatedTabletId, cp.getTerm(), cp.getIndex(), cp.getKey(),
                                cp.getWrite_id(), cp.getTime(), schemaNeeded.get(getLookupKey(tableId)));

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

                                    schemaNeeded.put(getLookupKey(tableId), Boolean.FALSE);

                                    TableId tId = null;
                                    if (message.getOperation() != ReplicationMessage.Operation.NOOP) {
                                        tId = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaName);
                                        Objects.requireNonNull(tId);
                                    }
                                    // Getting the table with the help of the schema.
                                    Table t = schema.tableForTablet(tId, getLookupKey(tableId));
                                    if (YugabyteDBSchema.shouldRefreshSchema(t, message.getSchema())) {
                                        // If we fail to achieve the table, that means we have not specified
                                        // correct schema information. Now try to refresh the schema.
                                        // TODO Vaibhav: Over here, even passing this.colocatedTabletId should work as the schema is already
                                        // stored with table.tablet level
                                        schema.refreshSchemaWithTabletId(tId, message.getSchema(), pgSchemaName, getLookupKey(tableId));
                                    }
                                } else {
                                    // DML event
                                    LOGGER.debug("For table {}, received a DML record {}",
                                            message.getTable(), record);

                                    TableId tId = null;
                                    if (message.getOperation() != ReplicationMessage.Operation.NOOP) {
                                        tId = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaName);
                                        Objects.requireNonNull(tId);
                                    }

                                    previousOffset.updateWalPosition(getLookupKey(tableId), lsn, lastCompletelyProcessedLsn,
                                            message.getCommitTime(),
                                            String.valueOf(message.getTransactionId()),
                                            tId, null /* xmin */);

                                    boolean dispatched = (message.getOperation() != ReplicationMessage.Operation.NOOP) &&
                                            dispatcher.dispatchDataChangeEvent(part, tId,
                                                    new YugabyteDBChangeRecordEmitter(part, previousOffset, clock,
                                                            this.connectorConfig, schema,
                                                            connection, tId, message,
                                                            pgSchemaName, getLookupKey(tableId),
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

                        previousOffset.getSourceInfo(getLookupKey(tableId)).updateLastCommit(finalOpId);

                        if (isSnapshotCompleteMarker(finalOpId)) {
                            // This will mark the snapshot completed for the tablet
                            snapshotCompletedTables.add(tableId);
                            LOGGER.info("Snapshot completed for table {} ({})", table.getTableId(), table.getName());
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

                    LOGGER.info("Tables in the failed task:");
                    for (Pair<String, String> entry : tableToTabletIds) {
                        LOGGER.info("Colocated tablet: {} table: {}({})",
                                this.colocatedTabletId /* tablet UUID */,
                                tableIdToTable.get(entry.getKey()).getName() /* table name */,
                                entry.getKey() /* table UUID */);
                    }

                    throw e;
                }

                LOGGER.warn("Error while trying to get the snapshot for colocated tables from the server; will attempt "
                                + "retry {} of {} after {} milli-seconds. Exception message: {}", retryCount,
                        this.connectorConfig.maxConnectorRetries(),
                        this.connectorConfig.connectorRetryDelayMs(), e.getMessage());
                LOGGER.warn("Stacktrace: ", e);

                try {
                    final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
                    retryMetronome.pause();
                } catch (InterruptedException ie) {
                    LOGGER.warn("Connector retry sleep interrupted by exception", ie);
                    Thread.currentThread().interrupt();
                }
            }
        }

        // If the flow comes at this stage then it either failed or was aborted by
        // some user interruption
        return SnapshotResult.aborted();
    }

    /**
     * This function returns a lookup key to get the values in different map based structures.
     * @param tableId the table UUID to use while forming lookup key
     * @return a string value for the lookup key of the format {@code tableId.colocatedTabletId}
     * @throws NullPointerException if {@code colocatedTabletId} has not been populated yet
     */
    private String getLookupKey(String tableId) throws NullPointerException {
        if (this.colocatedTabletId == null || this.colocatedTabletId.isEmpty()) {
            throw new NullPointerException("Variable colocatedTabletId has not been initialized");
        }

        return tableId + "." + this.colocatedTabletId;
    }

    private boolean isSnapshotCompleteMarker(OpId opId) {
        return Arrays.equals(opId.getKey(), "".getBytes()) && opId.getWrite_id() == 0
                && opId.getTime() == 0;
    }

    protected boolean isSnapshotComplete() {
        return this.snapshotComplete;
    }

    protected void setCheckpointWithRetryBeforeSnapshot(
            String tableId, Set<String> snapshotCompletedTables,
            Set<String> snapshotCompletedPreviously) throws Exception {
        short retryCount = 0;
        try {
            if (hasSnapshotCompletedPreviously(tableId)) {
                LOGGER.info("Skipping snapshot for colocated table {} since the corresponding tablet {} has streamed some data before", tableId, this.colocatedTabletId);
                snapshotCompletedTables.add(tableId);
                snapshotCompletedPreviously.add(tableId);
            } else {
                YBClientUtils.setCheckpoint(this.syncClient,
                        this.connectorConfig.streamId(),
                        tableId /* tableId */,
                        this.colocatedTabletId /* tabletId of the colocated tablet */,
                        0 /* term */, 0 /* index */,
                        true /* initialCheckpoint */, false /* bootstrap */,
                        0 /* invalid cdcsdkSafeTime */);
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
                LOGGER.warn("Connector retry sleep interrupted by exception", ie);
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Check on the server side if the table already has some checkpoint, if it does then do
     * not take a snapshot for it again since some data has already been streamed out of it
     * @param tableId the UUID of the colocated table
     * @return true if snapshot has been taken or some data has streamed already
     * @throws Exception if checkpoint cannot be retrieved from server side
     */
    protected boolean hasSnapshotCompletedPreviously(String tableId)
            throws Exception {
        GetCheckpointResponse resp = this.syncClient.getCheckpoint(
                this.syncClient.openTableByUUID(tableId),
                this.connectorConfig.streamId(), this.colocatedTabletId);

        return !(resp.getTerm() == -1 && resp.getIndex() == -1);
    }

    @Override
    protected SnapshotResult<YugabyteDBOffsetContext> doExecute(
            ChangeEventSourceContext context, YugabyteDBOffsetContext previousOffset,
            SnapshotContext<YBPartition, YugabyteDBOffsetContext> snapshotContext,
            SnapshottingTask snapshottingTask) throws Exception {
        return null;
    }

    private Map<TableId, String> determineTablesForSnapshot(Map<String, YBTable> tableIdToTable)
            throws Exception {
        Map<TableId, String> res = new HashMap<>();

        Set<TableId> dbzTableIds;

        for (Map.Entry<String, YBTable> entry : tableIdToTable.entrySet()) {
            res.put(YBClientUtils.getTableIdFromYbTable(this.syncClient, entry.getValue()),
                                                        entry.getKey());
        }

        dbzTableIds = res.entrySet().stream().map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        // Get a list of filtered tables which are to be snapshotted
        Set<TableId> filteredTables = getDataCollectionsToBeSnapshotted(dbzTableIds)
                .collect(Collectors.toSet());

        res.keySet().removeIf(tableId -> !filteredTables.contains(tableId));

        return res;
    }

    public void commitOffset(Map<String, ?> offsets) {
        // DO nothing for now.
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
                            .anyMatch(s -> s.matcher(dataCollectionId.schema() + "." + dataCollectionId.table())
                                    .matches()));
        }
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(YBPartition partition, YugabyteDBOffsetContext previousOffset) {
        if (snapshotter.shouldSnapshot()) {
            LOGGER.info("According to the connector configuration data will be snapshotted");
        } else {
            LOGGER.info("According to the connector configuration no snapshot will be executed");
        }

        // For YugabyteDB, snapshot for schema and data are not two different tasks, so pass on the
        // same value for both the parameters.
        return new SnapshottingTask(snapshotter.shouldSnapshot() /* snapshotSchema */,
                                    snapshotter.shouldSnapshot() /* snapshotData */);
    }

    @Override
    protected SnapshotContext<YBPartition, YugabyteDBOffsetContext> prepare(YBPartition partition) throws Exception {
        return new YugabyteDBColocatedSnapshotContext(partition, connectorConfig.databaseName());
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class YugabyteDBColocatedSnapshotContext extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YBPartition, YugabyteDBOffsetContext> {

        public YugabyteDBColocatedSnapshotContext(YBPartition partition, String catalogName) throws SQLException {
            super(partition, catalogName);
        }
    }
}
