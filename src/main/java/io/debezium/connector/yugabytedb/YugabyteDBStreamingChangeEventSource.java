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
import io.debezium.connector.yugabytedb.util.YugabyteDBConnectorUtils;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.snapshot.Snapshotter;
import io.debezium.util.Clock;
import io.debezium.util.DelayStrategy;
import io.debezium.util.ElapsedTimeStrategy;
import io.debezium.util.Metronome;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;
import org.yb.cdc.CdcService.TabletCheckpointPair;
import org.yb.cdc.CdcService.CDCErrorPB.Code;
import org.yb.cdc.CdcService.RowMessage.Op;
import org.yb.client.*;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;

/**
 *
 * @author Suranjan Kumar (skumar@yugabyte.com)
 */
public class YugabyteDBStreamingChangeEventSource implements
        StreamingChangeEventSource<YBPartition, YugabyteDBOffsetContext> {
    // Test only flags, DO NOT modify in the source code.
    public static boolean TEST_WAIT_BEFORE_GETTING_CHILDREN = false;
    public static boolean TEST_TRACK_EXPLICIT_CHECKPOINTS = false;
    public static boolean TEST_UPDATE_EXPLICIT_CHECKPOINT = true;
    public static boolean TEST_PAUSE_GET_CHANGES_CALLS = false;
    public static boolean TEST_STOP_ADVANCING_CHECKPOINTS = false;
    public static boolean TEST_FAIL_WHILE_PROCESSING_BATCH = false;
    public static Map<String, CdcSdkCheckpoint> TEST_explicitCheckpoints;

    protected static final String KEEP_ALIVE_THREAD_NAME = "keep-alive";

    /**
     * Number of received events without sending anything to Kafka which will
     * trigger a "WAL backlog growing" warning.
     */
    protected static final int GROWING_WAL_WARNING_LOG_INTERVAL = 10_000;

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBStreamingChangeEventSource.class);

    // PGOUTPUT decoder sends the messages with larger time gaps than other decoders
    // We thus try to read the message multiple times before we make poll pause
    protected static final int THROTTLE_NO_MESSAGE_BEFORE_PAUSE = 5;

    protected final YugabyteDBConnection connection;
    protected final YugabyteDBEventDispatcher<TableId> dispatcher;
    protected final ErrorHandler errorHandler;
    protected final Clock clock;
    protected final YugabyteDBSchema schema;
    protected final YugabyteDBConnectorConfig connectorConfig;
    protected final YugabyteDBTaskContext taskContext;

    protected final Snapshotter snapshotter;
    
    protected final DelayStrategy pauseNoMessage;
    protected final ElapsedTimeStrategy connectionProbeTimer;

    /**
     * The minimum of (number of event received since the last event sent to Kafka,
     * number of event received since last WAL growing warning issued).
     */
    protected long numberOfEventsSinceLastEventSentOrWalGrowingWarning = 0;
    protected OpId lastCompletelyProcessedLsn;
    protected YugabyteDBTypeRegistry yugabyteDBTypeRegistry;
    protected final Map<String, OpId> checkPointMap;
    protected final ChangeEventQueue<DataChangeEvent> queue;

    protected Map<String, CdcSdkCheckpoint> tabletToExplicitCheckpoint;

    protected final Filters filters;

    // This set will contain the list of partition IDs for the tablets which have been split
    // and waiting for the callback from Kafka.
    protected Set<String> splitTabletsWaitingForCallback;
    protected List<HashPartition> partitionRanges;

    // This tabletPairList has Pair<String, String> objects wherein the key is the table UUID
    // and the value is tablet UUID
    protected List<Pair<String, String>> tabletPairList;

    public YugabyteDBStreamingChangeEventSource(YugabyteDBConnectorConfig connectorConfig, SnapshotterService snapshotterService,
                                                YugabyteDBConnection connection, YugabyteDBEventDispatcher<TableId> dispatcher, ErrorHandler errorHandler, Clock clock,
                                                YugabyteDBSchema schema, YugabyteDBTaskContext taskContext, ReplicationConnection replicationConnection,
                                                ChangeEventQueue<DataChangeEvent> queue) {
        this.connectorConfig = connectorConfig;
        this.connection = connection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        pauseNoMessage = DelayStrategy.constant(taskContext.getConfig().getPollInterval());
        this.taskContext = taskContext;
        this.snapshotter = snapshotterService.getSnapshotter();
        checkPointMap = new ConcurrentHashMap<>();
        this.connectionProbeTimer = ElapsedTimeStrategy.constant(Clock.system(), connectorConfig.statusUpdateInterval());

        String masterAddress = connectorConfig.masterAddresses();
        yugabyteDBTypeRegistry = taskContext.schema().getTypeRegistry();
        this.queue = queue;
        this.tabletToExplicitCheckpoint = new ConcurrentHashMap<>();
        this.splitTabletsWaitingForCallback = new HashSet<>();
        this.filters = new Filters(connectorConfig);
        this.partitionRanges = new ArrayList<>();
        this.tabletPairList = new CopyOnWriteArrayList<>();

        if (TEST_TRACK_EXPLICIT_CHECKPOINTS) {
            TEST_explicitCheckpoints = new ConcurrentHashMap<>();
        }
    }

    @Override
    public void execute(ChangeEventSourceContext context, YBPartition partition, YugabyteDBOffsetContext offsetContext) {
        if (!snapshotter.shouldStream()) {
            LOGGER.info("Skipping streaming since it's not enabled in the configuration");
            return;
        }

        Set<YBPartition> partitions = new YBPartition.Provider(connectorConfig).getPartitions();
        boolean hasStartLsnStoredInContext = offsetContext != null && !offsetContext.getTabletSourceInfo().isEmpty();

        LOGGER.info("Starting the change streaming process now");

        if (!hasStartLsnStoredInContext) {
            LOGGER.info("No start opid found in the context.");
                offsetContext = YugabyteDBOffsetContext.initialContext(connectorConfig, connection, clock, partitions);
        }
        try {
            // Populate partition ranges.
            this.partitionRanges = YugabyteDBConnectorUtils.populatePartitionRanges(connectorConfig.getConfig().getString(YugabyteDBConnectorConfig.HASH_RANGES_LIST));
            getChanges2(context, partition, offsetContext, hasStartLsnStoredInContext);
        } catch (Throwable e) {
            Objects.requireNonNull(e);
            errorHandler.setProducerThrowable(e);
        }
        finally {
            if (!isInPreSnapshotCatchUpStreaming(offsetContext)) {
                // Need to see in CDCSDK what can be done.
            }
        }
    }

    private void bootstrapTablet(YBClient syncClient, YBTable table, String tabletId) throws Exception {
        LOGGER.info("Bootstrapping the tablet {}", tabletId);
        syncClient.bootstrapTablet(table, connectorConfig.streamId(), tabletId, 0, 0, true, true);
        markNoSnapshotNeeded(syncClient, table, tabletId);

        GetCheckpointResponse getCheckpointResponse =
          YBClientUtils.getCheckpointWithRetry(connectorConfig, syncClient, table, tabletId);
        Objects.requireNonNull(getCheckpointResponse);
        LOGGER.info("Checkpoint after bootstrapping tablet {}: {}.{}", tabletId, getCheckpointResponse.getTerm(), getCheckpointResponse.getIndex());
    }

    protected void bootstrapTabletWithRetry(YBClient syncClient, List<Pair<String,String>> tabletPairList,
                                            Map<String, YBTable> tableIdToTable) throws Exception {
        Set<String> tabletsWithoutBootstrap = new HashSet<>();
        for (Pair<String, String> entry : tabletPairList) {
            boolean shouldRetry = true;
            short retryCountForGetCheckpoint = 0;
            while (retryCountForGetCheckpoint <= connectorConfig.maxConnectorRetries() && shouldRetry) {
                try {
                    GetCheckpointResponse resp = syncClient.getCheckpoint(tableIdToTable.get(entry.getKey()), connectorConfig.streamId(), entry.getValue());
                    if (resp.getTerm() == -1 && resp.getIndex() == -1) {
                        LOGGER.info("Bootstrap required for table {} tablet {} as it has checkpoint -1.-1", entry.getKey(), entry.getValue());
                    } else {
                        LOGGER.info("No bootstrap needed for tablet {} with checkpoint {}.{}", entry.getValue(), resp.getTerm(), resp.getIndex());
                        tabletsWithoutBootstrap.add(entry.getValue() /* tabletId */ );
                    }

                    // Reset the flag to retry.
                    shouldRetry = false;
                } catch (Exception e) {
                    ++retryCountForGetCheckpoint;

                    shouldRetry = true;

                    if (retryCountForGetCheckpoint > connectorConfig.maxConnectorRetries()) {
                        LOGGER.error("Failed to get checkpoint for tablet {} after {} retries", entry.getValue(), connectorConfig.maxConnectorRetries());
                        throw e;
                    }

                    // If there are retries left, perform them after the specified delay.
                    LOGGER.warn("Error while trying to get the checkpoint for tablet {}; will attempt retry {} of {} after {} milli-seconds. Exception message: {}",
                            entry.getValue(), retryCountForGetCheckpoint, connectorConfig.maxConnectorRetries(), connectorConfig.connectorRetryDelayMs(), e.getMessage());

                    try {
                        final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
                        retryMetronome.pause();
                    } catch (InterruptedException ie) {
                        LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }

        // The bootstrap method calls the SetCDCCheckPoint RPC, which relies on a cache to obtain a
        // list of all the tservers. In case of multi host port connection url, if one of the DB node
        // goes down, it takes some time for the cache to refresh and return correct tserver list.
        // This refresh time may be longer and hence we need additional number of retries here.
        int maxBootstrapRetries = connectorConfig.maxConnectorRetries() * 5;
        for (Pair<String, String> entry : tabletPairList) {
            // entry is a Pair<tableId, tabletId>
            boolean shouldRetry = true;
            short retryCountForBootstrapping = 0;
            while (retryCountForBootstrapping <= maxBootstrapRetries && shouldRetry) {
                try {
                    if (!tabletsWithoutBootstrap.contains(entry.getValue())) {
                        YBTable table = syncClient.openTableByUUID(entry.getKey());
                        bootstrapTablet(syncClient, table, entry.getValue());
                    } else {
                        LOGGER.info("Skipping bootstrap for table {} tablet {} as it has a checkpoint", entry.getKey(), entry.getValue());
                    }

                    // Reset the retry flag if the bootstrap was successful
                    shouldRetry = false;
                } catch (Exception e) {
                    ++retryCountForBootstrapping;

                    // The connector should go for a retry if any exception is thrown
                    shouldRetry = true;

                    if (retryCountForBootstrapping > maxBootstrapRetries) {
                        LOGGER.error("Failed to bootstrap the tablet {} after {} retries", entry.getValue(), maxBootstrapRetries);
                        throw e;
                    }

                    // If there are retries left, perform them after the specified delay.
                    LOGGER.warn("Error while trying to bootstrap tablet {}; will attempt retry {} of {} after {} milli-seconds. Exception message: {}",
                            entry.getValue(), retryCountForBootstrapping, maxBootstrapRetries, connectorConfig.connectorRetryDelayMs(), e.getMessage());

                    try {
                        final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
                        retryMetronome.pause();
                    } catch (InterruptedException ie) {
                        LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    protected void markNoSnapshotNeeded(YBClient syncClient, YBTable ybTable, String tabletId) throws Exception {
        short retryCount = 0;
        while (retryCount <= connectorConfig.maxConnectorRetries()) {
            try {
                LOGGER.info("Marking no snapshot on service for table {} tablet {}", ybTable.getTableId(), tabletId);
                GetChangesResponse response =
                    syncClient.getChangesCDCSDK(ybTable, connectorConfig.streamId(),
                                                    tabletId, -1, -1, YugabyteDBOffsetContext.SNAPSHOT_DONE_KEY.getBytes(),
                                                    0, 0, false /* schema is not needed since this is a dummy call */);

                // Break upon successful request.
                break;
            } catch (Exception e) {
                ++retryCount;

                if (retryCount > connectorConfig.maxConnectorRetries()) {
                LOGGER.error("Too many errors while trying to mark no snapshot on service for table {} tablet {} error: ",
                            ybTable.getTableId(), tabletId, e);
                throw e;
                }

                LOGGER.warn("Error while marking no snapshot on service for table {} tablet {}, will attempt retry {} of {} for error {}",
                            ybTable.getTableId(), tabletId, retryCount, connectorConfig.maxConnectorRetries(), e);

                try {
                    final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
                    retryMetronome.pause();
                } catch (InterruptedException ie) {
                    LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Use the {@link GetTabletListToPollForCDCResponse} and the populated {@code partitionRanges}
     * to verify if the tablets in the response should be a part of this task. The logic only
     * adds the tablets in the response which are contained in the parent partition ranges. Note
     * that this method also assumes that the object {@code partitionRanges} is already populated.
     *
     * @param tableId the tableUUID for which the {@link GetTabletListToPollForCDCResponse} is passed
     * @param response of type {@link GetTabletListToPollForCDCResponse}
     * @param tabletPairList a list of {@link Pair} to be populated where each pair is {@code <tableId, tabletId>}
     */
    protected void populateTableToTabletPairsForTask(String tableId, GetTabletListToPollForCDCResponse response) {
        // Verify that the partitionRanges are already populated.
        Objects.requireNonNull(partitionRanges);
        assert !partitionRanges.isEmpty();

        // Iterate over the stored partitions and add valid tablets for streaming.
        for (TabletCheckpointPair pair : response.getTabletCheckpointPairList()) {
            HashPartition hp = HashPartition.from(pair, tableId);

            for (HashPartition parent : partitionRanges) {
                if (parent.containsPartition(hp)) {
                    this.tabletPairList.add(new ImmutablePair<>(hp.getTableId(), hp.getTabletId()));
                }
            }
        }
    }

    protected void getChanges2(ChangeEventSourceContext context,
                             YBPartition partitionn,
                             YugabyteDBOffsetContext offsetContext,
                             boolean previousOffsetPresent)
            throws Exception {
        LOGGER.info("Processing messages");
        try (YBClient syncClient = YBClientUtils.getYbClient(this.connectorConfig)) {
            Map<String, YBTable> tableIdToTable = new HashMap<>();
            Map<String, GetTabletListToPollForCDCResponse> tabletListResponse = new HashMap<>();
            String streamId = connectorConfig.streamId();

            LOGGER.info("Using DB stream ID: " + streamId);

            Set<String> tIds = partitionRanges.stream().map(HashPartition::getTableId).collect(Collectors.toSet());
            for (String tId : tIds) {
                YBTable table = syncClient.openTableByUUID(tId);
                tableIdToTable.put(tId, table);

                GetTabletListToPollForCDCResponse resp =
                        YBClientUtils.getTabletListToPollForCDCWithRetry(table, tId, connectorConfig);

                // Validate that we receive the complete range of tablets in case of non-colocated tables.
                if (!table.isColocated()) {
                    HashPartition.validateCompleteRanges(HashPartition.getListFrom(resp, tId));
                }

                // TODO: One optimisation where we initialise the offset context here itself
                //  without storing the GetTabletListToPollForCDCResponse
                populateTableToTabletPairsForTask(tId, resp);
                LOGGER.info("Table: {} with number of tablets {}", tId, resp.getTabletCheckpointPairListSize());
                tabletListResponse.put(tId, resp);
            }

            LOGGER.debug("The init tabletSourceInfo before updating is " + offsetContext.getTabletSourceInfo());

            // Initialize the offsetContext and other supporting flags.
            // This schemaNeeded map here would have the elements as <tableId.tabletId>:<boolean-value>
            Map<String, Boolean> schemaNeeded = new HashMap<>();
            Map<String, Long> tabletSafeTime = new HashMap<>();
            for (Pair<String, String> entry : tabletPairList) {
                // entry.getValue() will give the tabletId
                OpId opId = YBClientUtils.getOpIdFromGetTabletListResponse(
                        tabletListResponse.get(entry.getKey()), entry.getValue());

                if (opId == null) {
                    Set<String> tabletsForTable =
                            tabletListResponse.get(entry.getKey()).getTabletCheckpointPairList().stream()
                                    .map(pair -> pair.getTabletLocations().getTabletId().toStringUtf8())
                                    .collect(Collectors.toSet());
                    LOGGER.error("No entry for tablet {} was found in the response for table {} from service, current entries {}",
                            entry.getValue(), entry.getKey(), tabletsForTable);
                    throw new RuntimeException(String.format("OpId for the given tablet %s was not found for table %s"
                                    + " in the response, restart the connector to try again",
                            entry.getValue(), entry.getKey()));
                }

                // If we are getting a term and index as -1 and -1 from the server side it means
                // that the streaming has not yet started on that tablet ID. In that case, assign a
                // starting OpId so that the connector can poll using proper checkpoints.
                LOGGER.info("Checkpoint from GetTabletListToPollForCDC for tablet {} as {}", entry.getValue(), opId);
                if (opId.getTerm() == -1 && opId.getIndex() == -1) {
                    opId = YugabyteDBOffsetContext.streamingStartLsn();
                }

                // For streaming, we do not want any colocated information and want to process the tables
                // based on just their tablet IDs - pass false as the 'colocated' flag to enforce the same.
                YBPartition p = new YBPartition(entry.getKey(), entry.getValue(), false /* colocated */);
                offsetContext.initSourceInfo(p, this.connectorConfig, opId);

                if (taskContext.shouldEnableExplicitCheckpointing()) {
                    // We can initialise the explicit checkpoint for this tablet to the value returned by
                    // the cdc_service through the 'GetTabletListToPollForCDC' API
                    tabletToExplicitCheckpoint.put(p.getId(), opId.toCdcSdkCheckpoint());
                }

                schemaNeeded.put(p.getId(), Boolean.TRUE);
            }

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
            if (snapshotter.shouldStreamEventsStartingFromSnapshot()) {
                LOGGER.info("Skipping bootstrap because snapshot has been taken so streaming will resume there onwards");
            } else {
                bootstrapTabletWithRetry(syncClient, tabletPairList, tableIdToTable);
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
                            YBPartition part = new YBPartition(entry.getKey() /* tableId */, tabletId, false /* colocated */);

                            OpId cp = offsetContext.lsn(part);

                            if (taskContext.shouldEnableExplicitCheckpointing()
                                    && splitTabletsWaitingForCallback.contains(part.getId())) {
                                // We do not want to process anything related to the tablets which have
                                // sent the tablet split message but we have not received an explicit
                                // callback for the tablet. At this stage, check if the explicit
                                // checkpoint is the same as from_op_id, if yes then handle the tablet
                                // for split.
                                CdcSdkCheckpoint explicitCheckpoint = tabletToExplicitCheckpoint.get(part.getId());
                                OpId lastRecordCheckpoint = offsetContext.getSourceInfo(part).lastRecordCheckpoint();

                                if (explicitCheckpoint != null && (lastRecordCheckpoint == null || lastRecordCheckpoint.isLesserThanOrEqualTo(explicitCheckpoint))) {
                                    // At this position, we know we have received a callback for split tablet
                                    // handle tablet split and delete the tablet from the waiting list.

                                    // Call getChanges to make sure checkpoint is set on the cdc_state table.
                                    LOGGER.info("Setting explicit checkpoint for tablet {} to {}.{}", part.getTabletId(),
                                                explicitCheckpoint.getTerm(), explicitCheckpoint.getIndex());
                                    setCheckpointWithGetChanges(syncClient, tableIdToTable.get(part.getTableId()), part,
                                            cp, explicitCheckpoint, schemaNeeded.get(part.getId()),
                                            tabletSafeTime.get(part.getId()), offsetContext.getWalSegmentIndex(part));

                                    LOGGER.info("Handling tablet split for enqueued tablet {} as we have now received the commit callback",
                                            part.getTabletId());
                                    handleTabletSplit(syncClient, part.getTabletId(), tabletPairList, offsetContext, streamId, schemaNeeded);
                                    splitTabletsWaitingForCallback.remove(part.getId());
                                    // Break out of the loop so that processing can happen on the modified list.
                                    break;
                                } else {
                                    continue;
                                }
                            }

                            // If enabled, this will cause the connector to skip GetChanges calls for all the tablets.
                            if (TEST_PAUSE_GET_CHANGES_CALLS) {
                                LOGGER.info("[Test only] Skipping over the GetChanges call for tablet {}", tabletId);
                                continue;
                            }

                            YBTable table = tableIdToTable.get(entry.getKey());

                            CdcSdkCheckpoint explicitCheckpoint = getExplicitCheckpoint(part, cp);

                            if (connectorConfig.logGetChanges() || LOGGER.isDebugEnabled()
                                  || (System.currentTimeMillis() >= (lastLoggedTimeForGetChanges + connectorConfig.logGetChangesIntervalMs()))) {
                                LOGGER.info("Requesting changes for table {} tablet {}, explicit_checkpoint: {} from_op_id: {}",
                                  table.getName(), part.getId(), explicitCheckpoint, cp.toSerString());

                                lastLoggedTimeForGetChanges = System.currentTimeMillis();
                            }

                            // Check again if the thread has been interrupted.
                            if (!context.isRunning()) {
                                LOGGER.info("Connector has been stopped");
                                break;
                            }

                            GetChangesResponse response = null;

                            if (schemaNeeded.get(part.getId())) {
                                LOGGER.debug("Requesting schema for tablet: {}", tabletId);
                            }

                            try {
                                response = syncClient.getChangesCDCSDK(
                                        table, streamId, tabletId, cp.getTerm(), cp.getIndex(), cp.getKey(),
                                        cp.getWrite_id(), cp.getTime(), schemaNeeded.get(part.getId()),
                                        explicitCheckpoint,
                                        tabletSafeTime.getOrDefault(part.getId(), cp.getTime()), offsetContext.getWalSegmentIndex(part));

                                // Test only.
                                if (TEST_TRACK_EXPLICIT_CHECKPOINTS) {
                                    TEST_explicitCheckpoints.put(tabletId, explicitCheckpoint);
                                }
                            } catch (CDCErrorException cdcException) {
                                // Check if exception indicates a tablet split.
                                LOGGER.info("Code received in CDCErrorException: {}", cdcException.getCDCError().getCode());
                                if (cdcException.getCDCError().getCode() == Code.TABLET_SPLIT || cdcException.getCDCError().getCode() == Code.INVALID_REQUEST) {
                                    LOGGER.info("Encountered a tablet split on tablet {}, handling it gracefully", tabletId);
                                    if (LOGGER.isDebugEnabled()) {
                                        cdcException.printStackTrace();
                                    }

                                    if (taskContext.shouldEnableExplicitCheckpointing()) {
                                        OpId lastRecordCheckpoint = offsetContext.getSourceInfo(part).lastRecordCheckpoint();

                                        // If explicit checkpointing is enabled then we should check if we have the explicit checkpoint
                                        // the same as from_op_id, if yes then handle tablet split directly, if not, add the partition ID
                                        // (table.tablet) to be processed later.
                                        if (explicitCheckpoint != null && (lastRecordCheckpoint == null || lastRecordCheckpoint.isLesserThanOrEqualTo(explicitCheckpoint))) {
                                            LOGGER.info("Explicit checkpoint same as last seen record's checkpoint, handling tablet split immediately for partition {}, explicit checkpoint {}:{}:{} lastRecordCheckpoint: {}.{}.{}",
                                                    part.getId(), explicitCheckpoint.getTerm(), explicitCheckpoint.getIndex(), explicitCheckpoint.getTime(), lastRecordCheckpoint.getTerm(), lastRecordCheckpoint.getIndex(), lastRecordCheckpoint.getTime());

                                            handleTabletSplit(syncClient, part.getTabletId(), tabletPairList, offsetContext, streamId, schemaNeeded);
                                        } else {
                                            // Add the tablet for being processed later, this will mark the tablet as locked. There is a chance that explicit checkpoint may
                                            // be null, in that case, just to avoid NullPointerException in the log, simply log a null value.
                                            final String explicitString = (explicitCheckpoint == null) ? null : (explicitCheckpoint.getTerm() + "." + explicitCheckpoint.getIndex() + ":" + explicitCheckpoint.getTime());
                                            LOGGER.info("Adding partition {} to wait-list since the explicit checkpoint ({}) and last seen record's checkpoint ({}.{}.{}) are not the same",
                                                    part.getId(), explicitString, lastRecordCheckpoint.getTerm(), lastRecordCheckpoint.getIndex(), lastRecordCheckpoint.getTime());
                                            splitTabletsWaitingForCallback.add(part.getId());
                                        }
                                    } else {
                                        handleTabletSplit(syncClient, part.getTabletId(), tabletPairList, offsetContext, streamId, schemaNeeded);
                                    }

                                    // Break out of the loop so that the iteration can start afresh on the modified list.
                                    break;
                                } else {
                                    LOGGER.warn("Throwing error with code: {}", cdcException.getCDCError().getCode());
                                    throw cdcException;
                                }
                            }

                            LOGGER.debug("Processing {} records from getChanges call",
                                    response.getResp().getCdcSdkProtoRecordsList().size());
                            for (CdcService.CDCSDKProtoRecordPB record : response
                                    .getResp()
                                    .getCdcSdkProtoRecordsList()) {
                                CdcService.RowMessage m = record.getRowMessage();
                                YbProtoReplicationMessage message = new YbProtoReplicationMessage(
                                        m, this.yugabyteDBTypeRegistry);

                                // Ignore safepoint record as they are not meant to be processed here.
                                if (m.getOp() == Op.SAFEPOINT) {
                                    continue;
                                }

                                String pgSchemaNameInRecord = m.getPgschemaName();
                                if (!message.isTransactionalMessage()) {
                                    TableId tempTid;
                                    if (connectorConfig.isYSQLDbType()) {
                                        // This is a hack to skip tables in case of colocated tables
                                        tempTid = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaNameInRecord);
                                    } else {
                                        tempTid = YugabyteDBSchema.parseWithKeyspace(message.getTable(),
                                                connectorConfig.databaseName());
                                    }

                                    if (!filters.tableFilter().isIncluded(tempTid) && !connectorConfig.cqlTableFilter().isIncluded(tempTid)) {
                                        LOGGER.info("Skipping a record for table {} because it was not included", tempTid.table());
                                        continue;
                                    }
                                }

                                // TODO: Rename to Checkpoint, since OpId is misleading.
                                // This is the checkpoint which will be stored in Kafka and will be used for explicit checkpointing.
                                final OpId lsn = new OpId(record.getFromOpId().getTerm(),
                                        record.getFromOpId().getIndex(),
                                        record.getFromOpId().getWriteIdKey().toByteArray(),
                                        record.getFromOpId().getWriteId(),
                                        record.getRowMessage().getCommitTime() - 1);

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

                                                recordsInTransactionalBlock.put(part.getId(), 0);
                                                beginCountForTablet.merge(part.getId(), 1, Integer::sum);
                                            }
                                            if (message.getOperation() == Operation.COMMIT) {
                                                LOGGER.debug("LSN in case of COMMIT is " + lsn);
                                                offsetContext.updateRecordPosition(part, lsn, lastCompletelyProcessedLsn, message.getRawCommitTime(),
                                                        String.valueOf(message.getTransactionId()), null, message.getRecordTime());

                                                if (recordsInTransactionalBlock.containsKey(part.getId())) {
                                                    if (recordsInTransactionalBlock.get(part.getId()) == 0) {
                                                        LOGGER.debug("Records in the transactional block of transaction: {}, with LSN: {}, for tablet {} are 0",
                                                                message.getTransactionId(), lsn, part.getId());
                                                    } else {
                                                        LOGGER.debug("Records in the transactional block transaction: {}, with LSN: {}, for tablet {}: {}",
                                                                message.getTransactionId(), lsn, part.getId(), recordsInTransactionalBlock.get(part.getId()));
                                                    }
                                                } else if (beginCountForTablet.get(part.getId()).intValue() == 0) {
                                                    throw new DebeziumException("COMMIT record encountered without a preceding BEGIN record");
                                                }

                                                recordsInTransactionalBlock.remove(part.getId());
                                                beginCountForTablet.merge(part.getId(), -1, Integer::sum);
                                            }
                                            continue;
                                        }

                                        if (message.getOperation() == Operation.BEGIN) {
                                            LOGGER.debug("LSN in case of BEGIN is " + lsn);
                                            dispatcher.dispatchTransactionStartedEvent(part, message.getTransactionId(), offsetContext, message.getCommitTime());

                                            recordsInTransactionalBlock.put(part.getId(), 0);
                                            beginCountForTablet.merge(part.getId(), 1, Integer::sum);
                                        } else if (message.getOperation() == Operation.COMMIT) {
                                            LOGGER.debug("LSN in case of COMMIT is " + lsn);
                                            offsetContext.updateRecordPosition(part, lsn, lastCompletelyProcessedLsn, message.getRawCommitTime(),
                                                    String.valueOf(message.getTransactionId()), null, message.getRecordTime());
                                            dispatcher.dispatchTransactionCommittedEvent(part, offsetContext, message.getCommitTime());

                                            if (recordsInTransactionalBlock.containsKey(part.getId())) {
                                                if (recordsInTransactionalBlock.get(part.getId()) == 0) {
                                                    LOGGER.debug("Records in the transactional block of transaction: {}, with LSN: {}, for tablet {} are 0",
                                                            message.getTransactionId(), lsn, part.getId());
                                                } else {
                                                    LOGGER.debug("Records in the transactional block transaction: {}, with LSN: {}, for tablet {}: {}",
                                                            message.getTransactionId(), lsn, part.getId(), recordsInTransactionalBlock.get(part.getId()));
                                                }
                                            } else if (beginCountForTablet.get(part.getId()).intValue() == 0) {
                                                throw new DebeziumException("COMMIT record encountered without a preceding BEGIN record");
                                            }

                                            recordsInTransactionalBlock.remove(part.getId());
                                            beginCountForTablet.merge(part.getId(), -1, Integer::sum);
                                        }
                                        maybeWarnAboutGrowingWalBacklog(true);
                                    } else if (message.isDDLMessage()) {
                                        LOGGER.debug("Received DDL message {}", message.getSchema().toString()
                                                + " the table is " + message.getTable());

                                        // If a DDL message is received for a tablet, we do not need its schema again
                                        schemaNeeded.put(part.getId(), Boolean.FALSE);

                                        TableId tableId = null;
                                        if (message.getOperation() != Operation.NOOP) {
                                            if (connectorConfig.isYSQLDbType()) {
                                                tableId = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaNameInRecord);
                                            } else {
                                                tableId = YugabyteDBSchema.parseWithKeyspace(message.getTable(),connectorConfig.databaseName());
                                            }
                                            Objects.requireNonNull(tableId);
                                        }
                                        // Getting the table with the help of the schema.
                                        Table t = schema.tableForTablet(tableId, tabletId);
                                        if (YugabyteDBSchema.shouldRefreshSchema(t, message.getSchema())) {
                                            // If we fail to achieve the table, that means we have not specified correct schema information,
                                            // now try to refresh the schema.
                                            if (t == null) {
                                                LOGGER.info("Registering the schema for table {} tablet {} since it was not registered already", entry.getKey(), tabletId);
                                            } else {
                                                LOGGER.info("Refreshing the schema for table {} tablet {} because of mismatch in cached schema and received schema", entry.getKey(), tabletId);
                                            }
                                            if (connectorConfig.isYSQLDbType()) {
                                                schema.refreshSchemaWithTabletId(tableId, message.getSchema(), pgSchemaNameInRecord, tabletId);
                                            } else {
                                                schema.refreshSchemaWithTabletId(tableId, message.getSchema(), tableId.catalog(), tabletId);
                                            }
                                        }
                                    }
                                    // DML event
                                    else {
                                        TableId tableId = null;
                                        if (message.getOperation() != Operation.NOOP) {
                                            if (connectorConfig.isYSQLDbType()) {
                                                tableId = YugabyteDBSchema.parseWithSchema(message.getTable(), pgSchemaNameInRecord);
                                            } else {
                                                tableId = YugabyteDBSchema.parseWithKeyspace(message.getTable(), connectorConfig.databaseName());
                                            }
                                            Objects.requireNonNull(tableId);
                                        }
                                        // If you need to print the received record, change debug level to info
                                        LOGGER.debug("Received DML record {}", record);

                                        offsetContext.updateRecordPosition(part, lsn, lastCompletelyProcessedLsn, message.getRawCommitTime(),
                                                String.valueOf(message.getTransactionId()), tableId, message.getRecordTime());

                                        // Do not change the error message.
                                        if (TEST_FAIL_WHILE_PROCESSING_BATCH) {
                                            throw new RuntimeException("[TEST ONLY] Failing while processing the batch of records");
                                        }

                                        boolean dispatched = message.getOperation() != Operation.NOOP
                                            && dispatcher.dispatchDataChangeEvent(part, tableId,
                                                    new YugabyteDBChangeRecordEmitter(part, offsetContext, clock,
                                                            connectorConfig,
                                                            schema, connection, tableId, message, tabletId,
                                                            taskContext.isBeforeImageEnabled()));

                                        if (!dispatched) {
                                            LOGGER.warn("Failed to dispatch record for table {} tablet {}", tableId, tabletId);
                                        }

                                        if (recordsInTransactionalBlock.containsKey(part.getId())) {
                                            recordsInTransactionalBlock.merge(part.getId(), 1, Integer::sum);
                                        }

                                        maybeWarnAboutGrowingWalBacklog(dispatched);
                                    }
                                } catch (InterruptedException ie) {
                                    LOGGER.error("Interrupted exception while processing change records", ie);
                                    Thread.currentThread().interrupt();
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
                                    response.getResp().getSafeHybridTime());
                            offsetContext.updateWalPosition(part, finalOpid);
                            offsetContext.updateWalSegmentIndex(part, response.getResp().getWalSegmentIndex());

                            tabletSafeTime.put(part.getId(), response.getResp().getSafeHybridTime());

                            // In cases where there is no transactions on the server, the response checkpoint can still move ahead and we should
                            // also move the explicit checkpoint forward, given that it was already greater than the lsn of the last seen valid record.
                            // Otherwise the explicit checkpoint can get stuck at older values, and upon connector restart
                            // we will resume from an older point than necessary.
                            if (taskContext.shouldEnableExplicitCheckpointing() && !TEST_STOP_ADVANCING_CHECKPOINTS) {
                                OpId lastRecordCheckpoint = offsetContext.getSourceInfo(part).lastRecordCheckpoint();
                                if (lastRecordCheckpoint == null || lastRecordCheckpoint.isLesserThanOrEqualTo(explicitCheckpoint)) {
                                    tabletToExplicitCheckpoint.put(part.getId(), finalOpid.toCdcSdkCheckpoint());
                                }
                            }

                            LOGGER.debug("The final opid for tablet {} is {}", part.getId(), finalOpid);
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
                    LOGGER.warn("Error while trying to get the changes from the server for tablet {}; will attempt retry {} of {} after {} milli-seconds. Exception: {}",
                            curTabletId, retryCount, connectorConfig.maxConnectorRetries(), connectorConfig.connectorRetryDelayMs(), e);

                    try {
                        final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
                        retryMetronome.pause();
                    } catch (InterruptedException ie) {
                        LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private void probeConnectionIfNeeded() throws SQLException {
        // CDCSDK Find out why it fails.
        // if (connectionProbeTimer.hasElapsed()) {
        // connection.prepareQuery("SELECT 1");
        // connection.commit();
        // }
    }

    /**
     * @param partition the {@link YBPartition} to get the checkpoint for
     * @param fromOpId the request {@link OpId} (from_op_id)
     * @return the checkpoint which should be marked as explicit checkpoint on service
     */
    protected CdcSdkCheckpoint getExplicitCheckpoint(YBPartition partition, OpId fromOpId) {
        CdcSdkCheckpoint explicitCheckpoint = tabletToExplicitCheckpoint.get(partition.getId());

        if (fromOpId.isLesserThanOrEqualTo(explicitCheckpoint)) {
            LOGGER.debug("Request OpId for partition {} ({}) is less than or equal to explicit checkpoint ({})",
                         partition.getId(), fromOpId.toSerString(), explicitCheckpoint);
            return fromOpId.toCdcSdkCheckpoint();
        }

        return explicitCheckpoint;
    }

    /**
     * Mark the checkpoint on the service. This method is only supposed to be used to set the
     * explicit checkpoint for the tablets who are in the wait-list.
     * @param ybTable {@link YBTable} object
     * @param partition {@link YBPartition} object
     * @param fromOpId {@link OpId} being used as from_op_id
     * @param explicitCheckpoint {@link CdcSdkCheckpoint} to be set to cdc_state table
     * @param schemaNeeded whether we need schema in the response
     * @param tabletSafeTime
     * @param walSegmentIndex
     * @throws Exception if we receive any other error than the one for tablet split upon calling
     * GetChanges
     */
    protected void setCheckpointWithGetChanges(YBClient syncClient, YBTable ybTable, YBPartition partition,
                                               OpId fromOpId, CdcSdkCheckpoint explicitCheckpoint,
                                               boolean schemaNeeded, long tabletSafeTime,
                                               int walSegmentIndex) throws Exception {
        try {
            // This will throw an error saying tablet split detected as we are calling GetChanges again on the
            // same checkpoint - handle the error and move ahead.
            GetChangesResponse resp = syncClient.getChangesCDCSDK(
              ybTable, connectorConfig.streamId(), partition.getTabletId(), fromOpId.getTerm(),
              fromOpId.getIndex(), fromOpId.getKey(), fromOpId.getWrite_id(), fromOpId.getTime(),
              schemaNeeded, explicitCheckpoint, tabletSafeTime, walSegmentIndex);

            // We do not update the tablet safetime we get from the response at this
            // point because the previous GetChanges call is supposed to throw
            // an exception which will be handled.
        } catch (CDCErrorException cdcErrorException) {
            if (cdcErrorException.getCDCError().getCode() == Code.TABLET_SPLIT) {
                LOGGER.info("Handling tablet split error gracefully for enqueued tablet {}", partition.getTabletId());
            } else {
                throw cdcErrorException;
            }
        }
    }

    public Set<YBPartition> getActivePartitionsBeingPolled() {
        Set<YBPartition> partitions = new HashSet<>();

        for (Pair<String, String> pair : this.tabletPairList) {
            partitions.add(new YBPartition(pair.getKey(), pair.getValue(), false));
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Returning an active partition set with size: {}", partitions.size());
        }

        return partitions;
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
    protected void maybeWarnAboutGrowingWalBacklog(boolean dispatched) {
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
    @Override
    public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
        if (!taskContext.shouldEnableExplicitCheckpointing()) {
            return;
        }

        // Test only.
        if (!TEST_UPDATE_EXPLICIT_CHECKPOINT) {
            LOGGER.info("[Test Only] Not updating explicit checkpoint");
            return;
        }

        try {
            LOGGER.info("{} | Committing offsets on server", taskContext.getTaskId());

            for (Map.Entry<String, ?> entry : offset.entrySet()) {
                // TODO: The transaction_id field is getting populated somewhere and see if it can
                // be removed or blocked from getting added to this map.
                if (!entry.getKey().equals("transaction_id")) {
                    LOGGER.debug("Tablet: {} OpId: {}", entry.getKey(), entry.getValue());

                    // Parse the string to get the OpId object.
                    OpId tempOpId = OpId.valueOf((String) entry.getValue());
                    // We should check if the received OpId is less than the checkpoint already present
                    // in the map. If this is so, then we don't update the checkpoint. Updating to a lesser value
                    // than one already present would throw the error: CDCSDK: Trying to fetch already GCed intents
                    if (this.tabletToExplicitCheckpoint.get(entry.getKey()) != null &&
                            tempOpId.getIndex() < this.tabletToExplicitCheckpoint.get(entry.getKey()).getIndex()) {
                        LOGGER.debug("The received OpId {} is less than the older checkpoint {} for tablet {}",
                                    tempOpId.getIndex(), this.tabletToExplicitCheckpoint.get(entry.getKey()).getIndex(), entry.getKey());
                        continue;
                    }
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
    protected boolean isInPreSnapshotCatchUpStreaming(YugabyteDBOffsetContext offsetContext) {
        return offsetContext.getStreamingStoppingLsn() != null;
    }

    @FunctionalInterface
    public static interface PgConnectionSupplier {
        BaseConnection get() throws SQLException;
    }

    /**
     * Get the entry from the tablet pair list corresponding to the given tablet ID. This function
     * is helpful at the time of tablet split where we know the tablet ID of the tablet which has
     * been split and now we want to remove the corresponding pair from the polling list of
     * table-tablet pairs.
     * @param tabletPairList list of table-tablet pair to poll from
     * @param tabletId the tablet ID to match with
     * @return a pair of table-tablet IDs which matches the provided tablet ID
     */
    private Pair<String, String> getEntryToDelete(List<Pair<String,String>> tabletPairList, String tabletId) {
        for (Pair<String, String> entry : tabletPairList) {
            if (entry.getValue().equals(tabletId)) {
                return entry;
            }
        }

        return null;
    }

    /**
     * Verify that the passed tablet is present in the original ranges assigned to this task.
     * This method is generally supposed to be called in the tablet split flow to check whether
     * the child tablet we have received has a parent partition already.
     * @param tabletCheckpointPair the {@link TabletCheckpointPair} for the tablet to be verified
     * @param tableId the table UUID
     */
    private void assertTabletPresentInOriginalRanges(TabletCheckpointPair tabletCheckpointPair, String tableId) {
        boolean tabletFound = false;

        HashPartition tabletToVerify = HashPartition.from(tabletCheckpointPair, tableId);
        for (HashPartition parent : partitionRanges) {
            if (parent.containsPartition(tabletToVerify)) {
                tabletFound = true;
                break;
            }
        }

        if (!tabletFound) {
            throw new IllegalStateException("None of the specified partition ranges contain the tablet " + tabletToVerify.getTabletId());
        }
    }

    /**
     * Add the tablet from the provided tablet checkpoint pair to the list of tablets to poll from
     * if it is not present there
     * @param tabletPairList the list of tablets to poll from - list having Pair<tableId, tabletId>
     * @param pair the tablet checkpoint pair
     * @param tableId table UUID of the table to which the tablet belongs
     * @param offsetContext the offset context having the lsn info
     * @param schemaNeeded map of flags indicating whether we need the schema for a tablet or not
     */
    private void addTabletIfNotPresent(List<Pair<String,String>> tabletPairList,
                                       TabletCheckpointPair pair,
                                       String tableId,
                                       YugabyteDBOffsetContext offsetContext,
                                       Map<String, Boolean> schemaNeeded) {
        assertTabletPresentInOriginalRanges(pair, tableId);

        String tabletId = pair.getTabletLocations().getTabletId().toStringUtf8();
        ImmutablePair<String, String> tableTabletPair =
          new ImmutablePair<String, String>(tableId, tabletId);

        if (!tabletPairList.contains(tableTabletPair)) {
            tabletPairList.add(tableTabletPair);

            // This flow will be executed in case of tablet split only and since tablet split
            // is not possible on colocated tables, it is safe to assume that the tablets here
            // would be all non-colocated.
            YBPartition p = new YBPartition(tableId, tabletId, false /* colocated */);

            // Get the checkpoint for child tablet and unset its time.
            OpId checkpoint = OpId.from(pair.getCdcSdkCheckpoint());
            checkpoint.unsetTime();

            offsetContext.initSourceInfo(p, this.connectorConfig, checkpoint);

            tabletToExplicitCheckpoint.put(p.getId(), checkpoint.toCdcSdkCheckpoint());

            LOGGER.info("Initialized offset context for tablet {} with OpId {}", tabletId, checkpoint);

            // Add the flag to indicate that we need the schema for the new tablets so that the schema can be registered.
            schemaNeeded.put(p.getId(), Boolean.TRUE);
        }
    }

    protected void handleTabletSplit(YBClient syncClient, String splitTabletId,
                                     List<Pair<String,String>> tabletPairList,
                                     YugabyteDBOffsetContext offsetContext,
                                     String streamId,
                                     Map<String, Boolean> schemaNeeded) throws Exception {
        LOGGER.info("Removing the tablet {} from the list to get the changes since it has been split", splitTabletId);

        Pair<String, String> entryToBeDeleted = getEntryToDelete(tabletPairList, splitTabletId);
        Objects.requireNonNull(entryToBeDeleted);

        String tableId = entryToBeDeleted.getKey();
        YBTable ybTable = syncClient.openTableByUUID(tableId);

        if (TEST_WAIT_BEFORE_GETTING_CHILDREN) {
            final int secondsToWait = 60;
            LOGGER.info("[TEST ONLY] Waiting for {} seconds before getting children for tablet {}",
                        secondsToWait, splitTabletId);
            final Metronome waitMetronome = Metronome.parker(Duration.ofSeconds(secondsToWait), Clock.SYSTEM);
            waitMetronome.pause();
        }

        GetTabletListToPollForCDCResponse getTabletListResponse =
          getTabletListResponseWithRetry(syncClient,
              ybTable,
              streamId,
              tableId,
              splitTabletId);
        Objects.requireNonNull(getTabletListResponse);

        // Remove the entry with the tablet which has been split.
        boolean removeSuccessful = tabletPairList.remove(entryToBeDeleted);

        // Remove the corresponding entry to indicate that we don't need the schema now.
        schemaNeeded.remove(entryToBeDeleted.getValue());

        // Remove the entry for the tablet which has been split from: 'tabletToExplicitCheckpoint'.
        tabletToExplicitCheckpoint.remove(splitTabletId);

        // Log a warning if the element cannot be removed from the list.
        if (!removeSuccessful) {
            LOGGER.warn("Failed to remove the entry table {} - tablet {} from tablet pair list after split, will try once again", entryToBeDeleted.getKey(), entryToBeDeleted.getValue());

            if (!tabletPairList.remove(entryToBeDeleted)) {
                String exceptionMessageFormat = "Failed to remove the entry table {} - tablet {} from the tablet pair list after split";
                throw new RuntimeException(String.format(exceptionMessageFormat, entryToBeDeleted.getKey(), entryToBeDeleted.getValue()));
            }
        }

        if (getTabletListResponse.getTabletCheckpointPairListSize() != 2) {
            LOGGER.warn("Received response with unexpected children count: {}", getTabletListResponse.getTabletCheckpointPairListSize());
            for (TabletCheckpointPair p : getTabletListResponse.getTabletCheckpointPairList()) {
                LOGGER.warn("Tablet {}", p.getTabletLocations().getTabletId().toStringUtf8());
            }
        }

        for (TabletCheckpointPair pair : getTabletListResponse.getTabletCheckpointPairList()) {
            addTabletIfNotPresent(tabletPairList, pair, tableId, offsetContext, schemaNeeded);
        }
    }

    /**
     * Get the children tablets of the specified tablet which has been split. This API will retry
     * until the retry limit has been hit.
     * @param ybTable the {@link YBTable} object to which the split tablet belongs
     * @param streamId the DB stream ID being used for polling
     * @param tableId table UUID of the table to which the split tablet belongs
     * @param splitTabletId tablet UUID of the split tablet
     * @return {@link GetTabletListToPollForCDCResponse} containing the list of child tablets
     * @throws Exception when the retry limit has been hit or the metronome pause is interrupted
     */
    private GetTabletListToPollForCDCResponse getTabletListResponseWithRetry(YBClient syncClient,
        YBTable ybTable, String streamId, String tableId, String splitTabletId) throws Exception {
        short retryCount = 0;
        while (retryCount <= connectorConfig.maxConnectorRetries()) {
            try {
                // Note that YBClient also retries internally if it encounters an error which can
                // be retried.
                GetTabletListToPollForCDCResponse response =
                    syncClient.getTabletListToPollForCdc(
                        ybTable,
                        streamId,
                        tableId,
                        splitTabletId);

                if (response.getTabletCheckpointPairListSize() != 2) {
                    LOGGER.warn("Found {} tablets for the parent tablet {}",
                            response.getTabletCheckpointPairListSize(), splitTabletId);
                    throw new Exception("Found unexpected ("
                            + response.getTabletCheckpointPairListSize()
                            + ") number of tablets while trying to fetch the children of parent "
                            + splitTabletId);
                }

                retryCount = 0;
                return response;
            } catch (Exception e) {
                ++retryCount;
                if (retryCount > connectorConfig.maxConnectorRetries()) {
                    LOGGER.error("Too many errors while trying to get children for split tablet {}", splitTabletId);
                    LOGGER.error("Error", e);
                    throw e;
                }

                LOGGER.warn("Error while trying to get the children for the split tablet {}, will retry attempt {} of {} after {} milliseconds",
                            splitTabletId, retryCount, connectorConfig.maxConnectorRetries(), connectorConfig.connectorRetryDelayMs());
                LOGGER.warn("Stacktrace", e);

                try {
                    final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(connectorConfig.connectorRetryDelayMs()), Clock.SYSTEM);
                    retryMetronome.pause();
                }
                catch (InterruptedException ie) {
                    LOGGER.warn("Connector retry sleep while pausing to get the children tablets for parent {} interrupted", splitTabletId);
                    LOGGER.warn("Exception for interruption", ie);
                    Thread.currentThread().interrupt();
                }
            }
        }

        // In ideal scenarios, this should NEVER be returned from this function.
        return null;
    }
}
