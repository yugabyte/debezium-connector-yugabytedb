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
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.spi.OffsetState;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;

public class YugabyteDBOffsetContext implements OffsetContext {
    public static final String LAST_COMPLETELY_PROCESSED_LSN_KEY = "lsn_proc";
    public static final String SNAPSHOT_DONE_KEY = "snapshot_done_key";

    private static final Logger LOGGER = LoggerFactory
            .getLogger(YugabyteDBSnapshotChangeEventSource.class);
//    private final Schema sourceInfoSchema;
    private final Map<String, SourceInfo> tabletSourceInfo;
    private final Map<String, OpId> fromLsn;
//    private final SourceInfo sourceInfo;
//    private boolean lastSnapshotRecord;
//    private OpId lastCompletelyProcessedLsn;
//    private OpId lastCommitLsn;
//    private OpId streamingStoppingLsn = null;
    private YugabyteDBTransactionContext transactionContext;
    private IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
    private YugabyteDBConnectorConfig connectorConfig;
    private final Map<String, Integer> tabletWalSegmentIndex;

    private YugabyteDBOffsetContext(YugabyteDBConnectorConfig connectorConfig,
                                    OpId lsn, OpId lastCompletelyProcessedLsn,
                                    OpId lastCommitLsn,
                                    String txId,
                                    Instant time,
                                    boolean snapshot,
                                    boolean lastSnapshotRecord,
                                    YugabyteDBTransactionContext transactionContext,
                                    IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
//        sourceInfo = new SourceInfo(connectorConfig);
        this.tabletSourceInfo = new ConcurrentHashMap<>();
        this.fromLsn = new ConcurrentHashMap<>();
//        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
//        this.lastCommitLsn = lastCommitLsn;
        // sourceInfo.update(lsn, time, txId, null, sourceInfo.xmin());
//        sourceInfo.updateLastCommit(lastCommitLsn);
//        sourceInfoSchema = sourceInfo.schema();

//        this.lastSnapshotRecord = lastSnapshotRecord;
//        if (this.lastSnapshotRecord) {
//            postSnapshotCompletion();
//        }
//        else {
//            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
//        }
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
        this.connectorConfig = connectorConfig;
        this.tabletWalSegmentIndex = new ConcurrentHashMap<>();
    }

    public YugabyteDBOffsetContext(Offsets<YBPartition, YugabyteDBOffsetContext> previousOffsets,
                                   YugabyteDBConnectorConfig config) {
        this.tabletSourceInfo = new ConcurrentHashMap();
        this.fromLsn = new ConcurrentHashMap<>();
//        this.sourceInfo = new SourceInfo(config);
//        this.sourceInfoSchema = sourceInfo.schema();

        for (Map.Entry<YBPartition, YugabyteDBOffsetContext> context :
                previousOffsets.getOffsets().entrySet()) {
            YugabyteDBOffsetContext c = context.getValue();
            if (c != null) {
//                this.lastCompletelyProcessedLsn = c.lastCompletelyProcessedLsn;
//                this.lastCommitLsn = c.lastCommitLsn;
//                initSourceInfo(context.getKey() /* YBPartition */, config, c.lastCompletelyProcessedLsn);
//                this.updateWalPosition(context.getKey(), this.lastCommitLsn, lastCompletelyProcessedLsn, 0L, null, null, null, 0L);
            }
        }
        LOGGER.debug("Populating the tabletsourceinfo with " + this.getTabletSourceInfo());
        this.transactionContext = new YugabyteDBTransactionContext();
        this.incrementalSnapshotContext = new SignalBasedIncrementalSnapshotContext<>();
        this.connectorConfig = config;
        this.tabletWalSegmentIndex = new ConcurrentHashMap<>();
    }

    public static YugabyteDBOffsetContext initialContextForSnapshot(YugabyteDBConnectorConfig connectorConfig,
                                                                    YugabyteDBConnection jdbcConnection,
                                                                    Clock clock,
                                                                    Set<YBPartition> partitions) {
        return initialContext(connectorConfig, jdbcConnection, clock, snapshotStartLsn(),
                              snapshotStartLsn(), partitions);
    }

    public static YugabyteDBOffsetContext initialContext(YugabyteDBConnectorConfig connectorConfig,
                                                         YugabyteDBConnection jdbcConnection,
                                                         Clock clock,
                                                         Set<YBPartition> partitions) {
        LOGGER.info("Initializing streaming context");
        return initialContext(connectorConfig, jdbcConnection, clock, streamingStartLsn(),
                              streamingStartLsn(), partitions);
    }

    public static YugabyteDBOffsetContext initialContext(YugabyteDBConnectorConfig connectorConfig,
                                                         YugabyteDBConnection jdbcConnection,
                                                         Clock clock,
                                                         OpId lastCommitLsn,
                                                         OpId lastCompletelyProcessedLsn,
                                                         Set<YBPartition> partitions) {
        LOGGER.info("Creating initial offset context");

        final long txId = 0L;// new OpId(0,0,"".getBytes(), 0);

        YugabyteDBOffsetContext context = new YugabyteDBOffsetContext(
                connectorConfig,
                null, /* passing null since this value is not being used anywhere in constructor */
                lastCompletelyProcessedLsn,
                lastCommitLsn,
                String.valueOf(txId),
                clock.currentTimeAsInstant(),
                false,
                false,
                new YugabyteDBTransactionContext(),
                new SignalBasedIncrementalSnapshotContext<>());
        // todo: Q: Wouldn't this code cause all the partitions to be initialised with lastCompletelyProcessedLsn
        for (YBPartition p : partitions) {
            if (context.getTabletSourceInfo().get(p.getId()) == null) {
                context.initSourceInfo(p, connectorConfig, lastCompletelyProcessedLsn);
                context.updateRecordPosition(p, lastCommitLsn, lastCompletelyProcessedLsn, clock.currentTimeInMillis(), String.valueOf(txId), null, 0L);
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
    public static OpId snapshotDoneKeyLsn() {
        return new OpId(0, 0, SNAPSHOT_DONE_KEY.getBytes(), 0, 0);
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> result = new HashMap<>();

        for (Map.Entry<String, SourceInfo> entry : this.tabletSourceInfo.entrySet()) {
            // The entry.getKey() here would be tableId.tabletId or just tabletId
            result.put(entry.getKey(), entry.getValue().lsn().toSerString());
        }

        return result;
//        return sourceInfo.isSnapshot() ? result
//                : incrementalSnapshotContext
//                        .store(transactionContext.store(result));
    }

    public Struct getSourceInfoForTablet(YBPartition partition) {
        return this.tabletSourceInfo.get(partition.getId()).struct();
    }

    public void updateWalSegmentIndex(YBPartition partition, int index) {
        this.tabletWalSegmentIndex.put(partition.getId(), index);
    }

    public Integer getWalSegmentIndex(YBPartition partition) {
        return this.tabletWalSegmentIndex.getOrDefault(partition.getId(), 0);
    }

    @Override
    public Schema getSourceInfoSchema() {
        return SchemaBuilder.struct().build();
//        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        // return sourceInfo
        return (Struct) SchemaBuilder.struct().build();
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
        return true;
//        return sourceInfo.isSnapshot();
    }

    @Override
    public void preSnapshotStart() {
//        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
//        lastSnapshotRecord = false;
    }

    @Override
    public void preSnapshotCompletion() {
//        lastSnapshotRecord = true;
    }

    @Override
    public void postSnapshotCompletion() {
//        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

//    public void updateSnapshotPosition(Instant timestamp, TableId tableId) {
//        sourceInfo.update(timestamp, tableId);
//    }

    /**
     * Update the offset position which we should use to call the next {@code GetChangesRequest}
     * @param partition the {@link YBPartition} to update the lsn/offset/OpId for
     * @param lsn the {@link OpId} to update the offset with
     */
    public void updateWalPosition(YBPartition partition, OpId lsn) {
        OpId opId = this.fromLsn.get(partition.getId());

        if (opId == null) {
            opId = lsn;
        }

        this.fromLsn.put(partition.getId(), opId);
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
     */
    public void updateRecordPosition(YBPartition partition, OpId lsn,
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
        this.tabletSourceInfo.put(partition.getId(), info);
    }

    public void initSourceInfo(YBPartition partition, YugabyteDBConnectorConfig connectorConfig, OpId opId) {
        this.tabletSourceInfo.put(partition.getId(), new SourceInfo(connectorConfig, opId));
    }

    public Map<String, SourceInfo> getTabletSourceInfo() {
        return tabletSourceInfo;
    }

    public void updateCommitPosition(OpId lsn, OpId lastCompletelyProcessedLsn) {
//        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
//        this.lastCommitLsn = lsn;
//        sourceInfo.updateLastCommit(lsn);
    }

//    boolean hasLastKnownPosition() {
//        return sourceInfo.lsn() != null;
//    }
//
//    boolean hasCompletelyProcessedPosition() {
//        return this.lastCompletelyProcessedLsn != null;
//    }

//    OpId lsn() {
//        return sourceInfo.lsn() == null ? streamingStartLsn()
//                : sourceInfo.lsn();
//    }

    OpId lsn(YBPartition partition) {
        // get the sourceInfo of the tablet
        SourceInfo sourceInfo = getSourceInfo(partition);
        return sourceInfo.lsn() == null ? streamingStartLsn()
                : sourceInfo.lsn();
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
      // get the sourceInfo of the tablet
      SourceInfo sourceInfo = getSourceInfo(partition);
      return sourceInfo.lsn() == null ? snapshotStartLsn()
        : sourceInfo.lsn();
    }

//    OpId lastCompletelyProcessedLsn() {
//        return lastCompletelyProcessedLsn;
//    }
//
//    OpId lastCompletelyProcessedLsn(String tabletId) {
//        return lastCompletelyProcessedLsn;
//    }
//
//    OpId lastCommitLsn() {
//        return lastCommitLsn;
//    }

    /**
     * todo: see if the javadoc is correct
     * Returns the LSN that the streaming phase should stream events up to or null if
     * a stopping point is not set. If set during the streaming phase, any event with
     * an LSN less than the stopping LSN will be processed and once the stopping LSN
     * is reached, the streaming phase will end. Useful for a pre-snapshot catch up
     * streaming phase.
     */
    OpId getStreamingStoppingLsn() {
        return YugabyteDBOffsetContext.snapshotDoneKeyLsn();
    }

    public void setStreamingStoppingLsn(OpId streamingStoppingLsn) {
//        this.streamingStoppingLsn = streamingStoppingLsn;
    }

//    Long xmin() {
//        return sourceInfo.xmin();
//    }

    @Override
    public String toString() {
        return "YugabyteDBOffsetContext [" //sourceInfoSchema=" + sourceInfoSchema
////                ", sourceInfo=" + sourceInfo
//                + ", lastSnapshotRecord=" + lastSnapshotRecord
//                + ", lastCompletelyProcessedLsn=" + lastCompletelyProcessedLsn
//                + ", lastCommitLsn=" + lastCommitLsn
//                + ", streamingStoppingLsn=" + streamingStoppingLsn
                + ", transactionContext=" + transactionContext
                + ", incrementalSnapshotContext=" + incrementalSnapshotContext
                + ", tabletSourceInfo=" + tabletSourceInfo + "]";
    }

//    public OffsetState asOffsetState() {
//        return new OffsetState(
//                sourceInfo.lsn(),
//                sourceInfo.txId(),
//                sourceInfo.xmin(),
//                sourceInfo.timestamp(),
//                sourceInfo.isSnapshot());
//    }

    @Override
    public void markLastSnapshotRecord() {
//        sourceInfo.setSnapshot(SnapshotRecord.LAST);
    }

    @Override
    public void event(DataCollectionId tableId, Instant instant) {
//        sourceInfo.update(instant, (TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public void incrementalSnapshotEvents() {
//        sourceInfo.setSnapshot(SnapshotRecord.INCREMENTAL);
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
                    lastCompletelyProcessedLsn,
                    lastCompletelyProcessedLsn,
                    lastCompletelyProcessedLsn,
                    "txId", Instant.MIN, false, false,
                    YugabyteDBTransactionContext.load(offset),
                    SignalBasedIncrementalSnapshotContext.load(offset));

        }
    }
}
