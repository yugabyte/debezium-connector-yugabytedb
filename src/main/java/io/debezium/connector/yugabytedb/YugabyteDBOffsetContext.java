/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;

/**
 * Class to manage the offsets of multiple partitions per task.
 *
 * @author Suranjan Kumar, Rajat Venkatesh, Vaibhav Kushwaha
 */
public class YugabyteDBOffsetContext implements OffsetContext {
    public static final String LAST_COMPLETELY_PROCESSED_LSN_KEY = "lsn_proc";
    public static final String SNAPSHOT_DONE_KEY = "snapshot_done_key";

    private static final Logger LOGGER =
      LoggerFactory.getLogger(YugabyteDBSnapshotChangeEventSource.class);

    // The two maps tabletSourceInfo and fromLsn are used to store offsets. However, there are
    // differences between the offsets they store:
    // tabletSourceInfo - this has the offset for each processed record and thus these offsets are
    // used in the commitOffset callback to set checkpoints
    // fromLsn - stores the OpId we should use to call next GetChangesRequest
    private final Map<String, SourceInfo> tabletSourceInfo;
    private final Map<String, OpId> fromLsn;
    private YugabyteDBTransactionContext transactionContext;
    private IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
    private YugabyteDBConnectorConfig connectorConfig;
    private final Map<String, Integer> tabletWalSegmentIndex;
    private final Map<String, Long> tabletSafeTime;

    private YugabyteDBOffsetContext(YugabyteDBConnectorConfig connectorConfig,
                                    YugabyteDBTransactionContext transactionContext,
                                    IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        this.tabletSourceInfo = new ConcurrentHashMap<>();
        this.fromLsn = new ConcurrentHashMap<>();
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
        this.connectorConfig = connectorConfig;
        this.tabletWalSegmentIndex = new ConcurrentHashMap<>();
        this.tabletSafeTime = new ConcurrentHashMap<>();
    }

    public YugabyteDBOffsetContext(Offsets<YBPartition, YugabyteDBOffsetContext> previousOffsets,
                                   YugabyteDBConnectorConfig config) {
        this.tabletSourceInfo = new ConcurrentHashMap<>();
        this.fromLsn = new ConcurrentHashMap<>();

        for (Map.Entry<YBPartition, YugabyteDBOffsetContext> context :
                previousOffsets.getOffsets().entrySet()) {
            YugabyteDBOffsetContext c = context.getValue();
            if (c != null) {
               initSourceInfo(context.getKey() /* YBPartition */, config, streamingStartLsn());
               this.updateRecordPosition(context.getKey(), streamingStartLsn(), streamingStartLsn(), 0L, null, null, 0L, false);
            }
        }
        LOGGER.debug("Populating the tabletsourceinfo with " + this.getTabletSourceInfo());
        this.transactionContext = new YugabyteDBTransactionContext();
        this.incrementalSnapshotContext = new SignalBasedIncrementalSnapshotContext<>();
        this.connectorConfig = config;
        this.tabletWalSegmentIndex = new ConcurrentHashMap<>();
        this.tabletSafeTime = new ConcurrentHashMap<>();
    }

    public static YugabyteDBOffsetContext initialContextForSnapshot(YugabyteDBConnectorConfig connectorConfig,
                                                                    YugabyteDBConnection jdbcConnection,
                                                                    Clock clock,
                                                                    Set<YBPartition> partitions) {
        return initialContext(connectorConfig, jdbcConnection, clock, snapshotStartLsn(),
                              snapshotStartLsn(), partitions, false);
    }

    public static YugabyteDBOffsetContext initialContext(YugabyteDBConnectorConfig connectorConfig,
                                                         YugabyteDBConnection jdbcConnection,
                                                         Clock clock,
                                                         Set<YBPartition> partitions) {
        LOGGER.info("Initializing streaming context");
        return initialContext(connectorConfig, jdbcConnection, clock, streamingStartLsn(),
                              streamingStartLsn(), partitions, false);
    }

    public static YugabyteDBOffsetContext initialContext(YugabyteDBConnectorConfig connectorConfig,
                                                         YugabyteDBConnection jdbcConnection,
                                                         Clock clock,
                                                         OpId lastCommitLsn,
                                                         OpId lastCompletelyProcessedLsn,
                                                         Set<YBPartition> partitions,
                                                         boolean colocated) {
        LOGGER.info("Creating initial offset context");

        final long txId = 0L;// new OpId(0,0,"".getBytes(), 0);

        YugabyteDBOffsetContext context = new YugabyteDBOffsetContext(
                connectorConfig,
                new YugabyteDBTransactionContext(),
                new SignalBasedIncrementalSnapshotContext<>());
        for (YBPartition p : partitions) {
            if (context.getTabletSourceInfo().get(p.getId()) == null) {
                context.initSourceInfo(p, connectorConfig, lastCompletelyProcessedLsn);

                // Since this loop is called on every partition, it is safe to pass `false` as
                // colocated as we will anyway end up updating all the partitions.
                context.updateRecordPosition(p, lastCommitLsn, lastCompletelyProcessedLsn, clock.currentTimeInMillis(), String.valueOf(txId), null, 0L, false);
            }
        }
        return context;
    }

    /**
     * @return the starting {@link OpId} to begin the snapshot with
     */
    public static OpId snapshotStartLsn() {
        return new OpId(-1, -1, "".getBytes(), -1, 0);
    }

    /**
     * @return the starting {@link OpId} to begin the streaming with
     */
    public static OpId streamingStartLsn() {
        return new OpId(0, 0, "".getBytes(), 0, 0);
    }

    /**
     * @return the {@link OpId} which tells the server that the connector has marked the snapshot
     * as completed, and it is now transitioning towards streaming
     */
    public OpId snapshotDoneKeyLsn(YBPartition partition) {
        OpId snapshotLsn = snapshotLSN(partition);
        return new OpId(snapshotLsn.getTerm(), snapshotLsn.getIndex(), SNAPSHOT_DONE_KEY.getBytes(), 0, 0);
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> result = new HashMap<>();

        for (Map.Entry<String, SourceInfo> entry : this.tabletSourceInfo.entrySet()) {
            // The entry.getKey() here would be tableId.tabletId or just tabletId
            result.put(entry.getKey(), entry.getValue().lsn().toSerString());
        }

        return result;
    }

    public Struct getSourceInfoForTablet(YBPartition partition) {
        return this.tabletSourceInfo.get(partition.getId()).struct();
    }

    public void updateWalSegmentIndex(YBPartition partition, int index, boolean colocated) {
        this.tabletWalSegmentIndex.put(partition.getId(), index);

        if (colocated) {
            // Iterate over the complete fromLsn map and update every entry which contains the
            // same tablet.
            for (Map.Entry<String, Integer> entry : this.tabletWalSegmentIndex.entrySet()) {
                if (entry.getKey().contains(partition.getTabletId())) {
                    this.tabletWalSegmentIndex.put(entry.getKey(), index);
                }
            }
        }
    }

    public Integer getWalSegmentIndex(YBPartition partition) {
        return this.tabletWalSegmentIndex.getOrDefault(partition.getId(), 0);
    }

    public void updateTabletSafeTime(YBPartition partition, Long tabletSafeTime, boolean colocated) {
        this.tabletSafeTime.put(partition.getId(), tabletSafeTime);

        if (colocated) {
            // Iterate over the complete fromLsn map and update every entry which contains the
            // same tablet.
            for (Map.Entry<String, Long> entry : this.tabletSafeTime.entrySet()) {
                if (entry.getKey().contains(partition.getTabletId())) {
                    this.tabletSafeTime.put(entry.getKey(), tabletSafeTime);
                }
            }
        }
    }

    public long getTabletSafeTime(YBPartition partition) {
        return this.tabletSafeTime.getOrDefault(partition.getId(), this.fromLsn.get(partition.getId()).getTime());
    }

    /**
     * [NOT MEANT FOR USAGES] This method simply returns a dummy schema since we have to override
     * this method while implementing the class, otherwise we are not using this method anywhere.
     * @throws {@link UnsupportedOperationException}
     * @return nothing
     */
    @Override
    public Schema getSourceInfoSchema() {
        throw new UnsupportedOperationException("This method is not in use and thus not implemented");
    }

    /**
     * [NOT MEANT FOR USAGES] This method simply returns a dummy Struct since we have to override
     * this method while implementing the class, otherwise we are not using this method anywhere.
     * @throws {@link UnsupportedOperationException}
     * @return nothing
     */
    @Override
    public Struct getSourceInfo() {
        throw new UnsupportedOperationException("This method is not in use and thus not implemented");
    }

    public SourceInfo getSourceInfo(YBPartition partition) {
        SourceInfo info = tabletSourceInfo.get(partition.getId());
        if (info == null) {
            tabletSourceInfo.put(partition.getId(), new SourceInfo(connectorConfig, YugabyteDBOffsetContext.streamingStartLsn()));
        }

        return tabletSourceInfo.get(partition.getId());
    }

    @Override
    public boolean isSnapshotRunning() {
        // TODO: think about making this work
        return true;
        // return sourceInfo.isSnapshot();
    }

    @Override
    public void preSnapshotStart() {
        // Do nothing.
    }

    @Override
    public void preSnapshotCompletion() {
        // Do nothing.
    }

    @Override
    public void postSnapshotCompletion() {
        // Do nothing.
    }

    /**
     * Update the offset position which we should use to call the next {@code GetChangesRequest}
     * @param partition the {@link YBPartition} to update the lsn/offset/OpId for
     * @param lsn the {@link OpId} to update the offset with
     * @param colocated whether the table is a colocated table
     */
    public void updateWalPosition(YBPartition partition, OpId lsn, boolean colocated) {
        this.fromLsn.put(partition.getId(), lsn);

        if (colocated) {
            // Iterate over the complete fromLsn map and update every entry which contains the
            // same tablet.
            for (Map.Entry<String, OpId> entry : this.fromLsn.entrySet()) {
                if (entry.getKey().contains(partition.getTabletId())) {
                    this.fromLsn.put(entry.getKey(), lsn);
                }
            }
        }
    }

    /**
     * Update the offsets for the records which are processed by the connector.
     * @param partition {@link YBPartition} to update the offset for
     * @param lsn the {@link OpId} value to update with
     * @param lastCompletelyProcessedLsn {@link OpId}
     * @param commitTime commit time of the record
     * @param txId transaction ID to which the record belongs
     * @param tableId {@link TableId} object to identify the table
     * @param recordTime record time for the record
     * @param colocated whether the table is colocated
     */
    public void updateRecordPosition(YBPartition partition, OpId lsn,
                                     OpId lastCompletelyProcessedLsn, long commitTime,
                                     String txId, TableId tableId, Long recordTime,
                                     boolean colocated) {
        updateRecordPositionImpl(partition, lsn, lastCompletelyProcessedLsn, commitTime, txId, tableId, recordTime);

        if (colocated) {
            for (Map.Entry<String, SourceInfo> entry : this.tabletSourceInfo.entrySet()) {
                if (entry.getKey().contains(partition.getTabletId())) {
                    updateRecordPositionImpl(YBPartition.fromFullPartitionId(entry.getKey()), lsn,
                                                lastCompletelyProcessedLsn, commitTime, txId, tableId, recordTime);
                }
            }
        }
    }

    private void updateRecordPositionImpl(YBPartition partition, OpId lsn,
                                          OpId lastCompletelyProcessedLsn, long commitTime,
                                          String txId, TableId tableId, Long recordTime) {
        SourceInfo info = this.tabletSourceInfo.get(partition.getId());

        // There is a possibility upon the transition from snapshot to streaming mode that we try
        // to retrieve a SourceInfo which may not be available in the map as we will just be looking
        // up using the tabletId. Store the SourceInfo in that case.
        if (info == null) {
            info = new SourceInfo(connectorConfig, lsn);
        }

        info.update(partition, lsn, commitTime, txId, tableId, recordTime);
        info.updateLastCommit(lsn);
        this.tabletSourceInfo.put(partition.getId(), info);
    }

    public void initSourceInfo(YBPartition partition, YugabyteDBConnectorConfig connectorConfig, OpId opId) {
        this.tabletSourceInfo.put(partition.getId(), new SourceInfo(connectorConfig, opId));
        this.fromLsn.put(partition.getId(), opId);
    }

    public Map<String, SourceInfo> getTabletSourceInfo() {
        return tabletSourceInfo;
    }

    /**
     * Get the LSN from which the connector should call the GetChangesRequest
     * @param partition the partition to get the LSN for
     * @return an {@link OpId} value to be used in {@link org.yb.client.GetChangesRequest} as
     * from_op_id
     */
    OpId lsn(YBPartition partition) {
        return this.fromLsn.getOrDefault(partition.getId(), streamingStartLsn());
    }


    /**
     * If a previous OpId is null then we want the server to send the snapshot from the
     * beginning. Requesting from the term -1, index -1 and empty key would indicate
     * the server that a snapshot needs to be taken and the write ID as -1 tells that we are
     * in the snapshot mode and snapshot time 0 signifies that we are bootstrapping
     * the snapshot flow.
     * <p>
     * In short, we are telling the server to decide an appropriate checkpoint till which the
     * snapshot needs to be taken and send it as a response back to the connector.
     *
     * @param partition the partition to get the LSN for
     * @return {@link OpId} from which we need to read the snapshot from the server
     */
    OpId snapshotLSN(YBPartition partition) {
      return this.fromLsn.getOrDefault(partition.getId(), snapshotStartLsn());
    }

    /**
     * @deprecated NOT in use at the moment
     * Returns the LSN that the streaming phase should stream events up to or null if
     * a stopping point is not set. If set during the streaming phase, any event with
     * an LSN less than the stopping LSN will be processed and once the stopping LSN
     * is reached, the streaming phase will end. Useful for a pre-snapshot catch up
     * streaming phase.
     */
    OpId getStreamingStoppingLsn() {
        return null;
    }

    @Override
    public String toString() {
        return "YugabyteDBOffsetContext [transactionContext=" + transactionContext
                + ", incrementalSnapshotContext=" + incrementalSnapshotContext
                + ", fromLsn=" + fromLsn
                + ", tabletSourceInfo=" + tabletSourceInfo + "]";
    }

    @Override
    public void markLastSnapshotRecord() {
        // Do nothing.
    }

    @Override
    public void event(DataCollectionId tableId, Instant instant) {
        // Do nothing.
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public void incrementalSnapshotEvents() {
        // Do nothing.
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    public static class Loader implements OffsetContext.Loader<YugabyteDBOffsetContext> {

        private final YugabyteDBConnectorConfig connectorConfig;

        public Loader(YugabyteDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        private Long readOptionalLong(Map<String, ?> offset, String key) {
            final Object obj = offset.get(key);
            return (obj == null) ? null : ((Number) obj).longValue();
        }

        private String readOptionalString(Map<String, ?> offset, String key) {
            final Object obj = offset.get(key);
            return (obj == null) ? null : ((String) obj);
        }

        @SuppressWarnings("unchecked")
        @Override
        public YugabyteDBOffsetContext load(Map<String, ?> offset) {

            LOGGER.debug("The offset being loaded in YugabyteDBOffsetContext.. " + offset);
            OpId lastCompletelyProcessedLsn;
            if (offset != null) {
                lastCompletelyProcessedLsn = OpId.valueOf((String) offset.get(YugabyteDBOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY));
            }
            else {
                lastCompletelyProcessedLsn = new OpId(0, 0, "".getBytes(), 0, 0);
            }
            /*
             * final OpId lsn = OpId.valueOf(readOptionalString(offset, SourceInfo.LSN_KEY));
             * final OpId lastCompletelyProcessedLsn = OpId.valueOf(readOptionalString(offset,
             * LAST_COMPLETELY_PROCESSED_LSN_KEY));
             * final OpId lastCommitLsn = OpId.valueOf(readOptionalString(offset,
             * LAST_COMPLETELY_PROCESSED_LSN_KEY));
             * final String txId = readOptionalString(offset, SourceInfo.TXID_KEY);
             *
             * final Instant useconds = Conversions.toInstantFromMicros((Long) offset
             * .get(SourceInfo.TIMESTAMP_USEC_KEY));
             * final boolean snapshot = (boolean) ((Map<String, Object>) offset)
             * .getOrDefault(SourceInfo.SNAPSHOT_KEY, Boolean.FALSE);
             * final boolean lastSnapshotRecord = (boolean) ((Map<String, Object>) offset)
             * .getOrDefault(SourceInfo.LAST_SNAPSHOT_RECORD_KEY, Boolean.FALSE);
             * return new YugabyteDBOffsetContext(connectorConfig, lsn, lastCompletelyProcessedLsn,
             * lastCommitLsn, txId, useconds, snapshot, lastSnapshotRecord,
             * TransactionContext.load(offset), SignalBasedIncrementalSnapshotContext
             * .load(offset));
             */

            return new YugabyteDBOffsetContext(connectorConfig,
                    YugabyteDBTransactionContext.load(offset),
                    SignalBasedIncrementalSnapshotContext.load(offset));

        }
    }
}
