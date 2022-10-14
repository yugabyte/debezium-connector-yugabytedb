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
import io.debezium.time.Conversions;
import io.debezium.util.Clock;

public class YugabyteDBOffsetContext implements OffsetContext {
    public static final String LAST_COMPLETELY_PROCESSED_LSN_KEY = "lsn_proc";
    public static final String LAST_COMMIT_LSN_KEY = "lsn_commit";
    private static final Logger LOGGER = LoggerFactory
            .getLogger(YugabyteDBSnapshotChangeEventSource.class);
    private final Schema sourceInfoSchema;
    private final Map<String, SourceInfo> tabletSourceInfo;
    private final SourceInfo sourceInfo;
    private boolean lastSnapshotRecord;
    private OpId lastCompletelyProcessedLsn;
    private OpId lastCommitLsn;
    private OpId streamingStoppingLsn = null;
    private TransactionContext transactionContext;
    private IncrementalSnapshotContext<TableId> incrementalSnapshotContext;

    private YugabyteDBOffsetContext(YugabyteDBConnectorConfig connectorConfig,
                                    OpId lsn, OpId lastCompletelyProcessedLsn,
                                    OpId lastCommitLsn,
                                    String txId,
                                    Instant time,
                                    boolean snapshot,
                                    boolean lastSnapshotRecord,
                                    TransactionContext transactionContext,
                                    IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        sourceInfo = new SourceInfo(connectorConfig);
        this.tabletSourceInfo = new ConcurrentHashMap();
        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
        this.lastCommitLsn = lastCommitLsn;
        // sourceInfo.update(lsn, time, txId, null, sourceInfo.xmin());
        sourceInfo.updateLastCommit(lastCommitLsn);
        sourceInfoSchema = sourceInfo.schema();

        this.lastSnapshotRecord = lastSnapshotRecord;
        if (this.lastSnapshotRecord) {
            postSnapshotCompletion();
        }
        else {
            sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
        }
        this.transactionContext = transactionContext;
        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    public YugabyteDBOffsetContext(Offsets<YBPartition, YugabyteDBOffsetContext> previousOffsets,
                                   YugabyteDBConnectorConfig config) {
        this.tabletSourceInfo = new ConcurrentHashMap();
        this.sourceInfo = new SourceInfo(config);
        this.sourceInfoSchema = sourceInfo.schema();

        for (Map.Entry<YBPartition, YugabyteDBOffsetContext> context :
                previousOffsets.getOffsets().entrySet()) {
            YugabyteDBOffsetContext c = context.getValue();
            if (c != null) {
                this.lastCompletelyProcessedLsn = c.lastCompletelyProcessedLsn;
                this.lastCommitLsn = c.lastCommitLsn;
                String tabletId = context.getKey().getSourcePartition().values().stream().findAny().get();
                initSourceInfo(tabletId, config);
                this.updateWalPosition(tabletId,
                        this.lastCommitLsn, lastCompletelyProcessedLsn, null, null, null, null);
            }
        }
        LOGGER.debug("Populating the tabletsourceinfo with " + this.getTabletSourceInfo());
        this.transactionContext = new TransactionContext();
        this.incrementalSnapshotContext = new SignalBasedIncrementalSnapshotContext<>();
    }

    public static YugabyteDBOffsetContext initialContextForSnapshot(YugabyteDBConnectorConfig connectorConfig,
                                                                    YugabyteDBConnection jdbcConnection,
                                                                    Clock clock,
                                                                    Set<YBPartition> partitions) {
        return initialContext(connectorConfig, jdbcConnection, clock, new OpId(-1, -1, "".getBytes(), -1, 0),
                new OpId(-1, -1, "".getBytes(), -1, 0), partitions);
    }

    public static YugabyteDBOffsetContext initialContext(YugabyteDBConnectorConfig connectorConfig,
                                                         YugabyteDBConnection jdbcConnection,
                                                         Clock clock,
                                                         Set<YBPartition> partitions) {
        return initialContext(connectorConfig, jdbcConnection, clock, new OpId(0, 0, "".getBytes(), 0, 0),
                new OpId(0, 0, "".getBytes(), 0, 0), partitions);
    }

    public static YugabyteDBOffsetContext initialContext(YugabyteDBConnectorConfig connectorConfig,
                                                         YugabyteDBConnection jdbcConnection,
                                                         Clock clock,
                                                         OpId lastCommitLsn,
                                                         OpId lastCompletelyProcessedLsn,
                                                         Set<YBPartition> partitions) {
        LOGGER.info("Creating initial offset context");
        final OpId lsn = null; // OpId.valueOf(jdbcConnection.currentXLogLocation());
        // TODO:Suranjan read the offset for each of the tablet
        final long txId = 0L;// new OpId(0,0,"".getBytes(), 0);
        LOGGER.info("Read checkpoint at '{}' ", lsn, txId);
        YugabyteDBOffsetContext context = new YugabyteDBOffsetContext(
                connectorConfig,
                lsn,
                lastCompletelyProcessedLsn,
                lastCommitLsn,
                String.valueOf(txId),
                clock.currentTimeAsInstant(),
                false,
                false,
                new TransactionContext(),
                new SignalBasedIncrementalSnapshotContext<>());
        for (YBPartition p : partitions) {
            if (context.getTabletSourceInfo().get(p.getTabletId()) == null) {
                context.initSourceInfo(p.getTabletId(), connectorConfig);
                context.updateWalPosition(p.getTabletId(), lastCommitLsn, lastCompletelyProcessedLsn, clock.currentTimeAsInstant(), String.valueOf(txId), null, null);
            }
        }
        return context;
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> result = new HashMap<>();
        if (sourceInfo.timestamp() != null) {
            result.put(SourceInfo.TIMESTAMP_USEC_KEY, Conversions
                    .toEpochMicros(sourceInfo.timestamp()));
        }
        if (sourceInfo.txId() != null) {
            result.put(SourceInfo.TXID_KEY, sourceInfo.txId());
        }
        if (sourceInfo.lsn() != null) {
            result.put(SourceInfo.LSN_KEY, sourceInfo.lsn().toSerString());
        }
        if (sourceInfo.xmin() != null) {
            result.put(SourceInfo.XMIN_KEY, sourceInfo.xmin());
        }
        if (sourceInfo.isSnapshot()) {
            result.put(SourceInfo.SNAPSHOT_KEY, true);
            result.put(SourceInfo.LAST_SNAPSHOT_RECORD_KEY, lastSnapshotRecord);
        }
        if (lastCompletelyProcessedLsn != null) {
            result.put(LAST_COMPLETELY_PROCESSED_LSN_KEY, lastCompletelyProcessedLsn.toSerString());
        }
        if (lastCommitLsn != null) {
            result.put(LAST_COMMIT_LSN_KEY, lastCommitLsn.toSerString());
        }
        return sourceInfo.isSnapshot() ? result
                : incrementalSnapshotContext
                        .store(transactionContext.store(result));
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    public SourceInfo getSourceInfo(String tabletId) {
        return tabletSourceInfo.get(tabletId);
    }

    @Override
    public boolean isSnapshotRunning() {
        return sourceInfo.isSnapshot();
    }

    @Override
    public void preSnapshotStart() {
        sourceInfo.setSnapshot(SnapshotRecord.TRUE);
        lastSnapshotRecord = false;
    }

    @Override
    public void preSnapshotCompletion() {
        lastSnapshotRecord = true;
    }

    @Override
    public void postSnapshotCompletion() {
        sourceInfo.setSnapshot(SnapshotRecord.FALSE);
    }

    public void updateSnapshotPosition(Instant timestamp, TableId tableId) {
        sourceInfo.update(timestamp, tableId);
    }

    public void updateWalPosition(String tabletId, OpId lsn, OpId lastCompletelyProcessedLsn,
                                  Instant commitTime,
                                  String txId, TableId tableId, Long xmin) {

        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;

        sourceInfo.update(tabletId, lsn, commitTime, txId, tableId, xmin);
        SourceInfo info = this.tabletSourceInfo.get(tabletId);
        info.update(tabletId, lsn, commitTime, txId, tableId, xmin);
        this.tabletSourceInfo.put(tabletId, info);
    }

    // todo Vaibhav: you may need to change this to get the new offset values from the TabletCheckpointPair
    public void initSourceInfo(String tabletId, YugabyteDBConnectorConfig connectorConfig) {
        this.tabletSourceInfo.put(tabletId, new SourceInfo(connectorConfig));
    }

    public void initSourceInfo(String tabletId, YugabyteDBConnectorConfig connectorConfig, OpId opId) {
        this.tabletSourceInfo.put(tabletId, new SourceInfo(connectorConfig, opId));
    }

    public Map<String, SourceInfo> getTabletSourceInfo() {
        return tabletSourceInfo;
    }

    public void updateCommitPosition(OpId lsn, OpId lastCompletelyProcessedLsn) {
        this.lastCompletelyProcessedLsn = lastCompletelyProcessedLsn;
        this.lastCommitLsn = lsn;
        sourceInfo.updateLastCommit(lsn);
    }

    boolean hasLastKnownPosition() {
        return sourceInfo.lsn() != null;
    }

    boolean hasCompletelyProcessedPosition() {
        return this.lastCompletelyProcessedLsn != null;
    }

    OpId lsn() {
        return sourceInfo.lsn() == null ? new OpId(0, 0, null, 0, 0)
                : sourceInfo.lsn();
    }

    OpId lsn(String tabletId) {
        // get the sourceInfo of the tablet
        SourceInfo sourceInfo = getSourceInfo(tabletId);
        return sourceInfo.lsn() == null ? new OpId(0, 0, null, 0, 0)
                : sourceInfo.lsn();
    }

    OpId snapshotLSN(String tabletId) {
      // get the sourceInfo of the tablet
      SourceInfo sourceInfo = getSourceInfo(tabletId);
      return sourceInfo.lsn() == null ? new OpId(-1, -1, "".getBytes(), -1, -1)
        : sourceInfo.lsn();
    }

    OpId lastCompletelyProcessedLsn() {
        return lastCompletelyProcessedLsn;
    }
    
    OpId lastCompletelyProcessedLsn(String tabletId) {
        return lastCompletelyProcessedLsn;
    }

    OpId lastCommitLsn() {
        return lastCommitLsn;
    }

    /**
     * Returns the LSN that the streaming phase should stream events up to or null if
     * a stopping point is not set. If set during the streaming phase, any event with
     * an LSN less than the stopping LSN will be processed and once the stopping LSN
     * is reached, the streaming phase will end. Useful for a pre-snapshot catch up
     * streaming phase.
     */
    OpId getStreamingStoppingLsn() {
        return streamingStoppingLsn;
    }

    public void setStreamingStoppingLsn(OpId streamingStoppingLsn) {
        this.streamingStoppingLsn = streamingStoppingLsn;
    }

    Long xmin() {
        return sourceInfo.xmin();
    }

    @Override
    public String toString() {
        return "YugabyteDBOffsetContext [sourceInfoSchema=" + sourceInfoSchema +
                ", sourceInfo=" + sourceInfo
                + ", lastSnapshotRecord=" + lastSnapshotRecord
                + ", lastCompletelyProcessedLsn=" + lastCompletelyProcessedLsn
                + ", lastCommitLsn=" + lastCommitLsn
                + ", streamingStoppingLsn=" + streamingStoppingLsn
                + ", transactionContext=" + transactionContext
                + ", incrementalSnapshotContext=" + incrementalSnapshotContext
                + ", tabletSourceInfo=" + tabletSourceInfo + "]";
    }

    public OffsetState asOffsetState() {
        return new OffsetState(
                sourceInfo.lsn(),
                sourceInfo.txId(),
                sourceInfo.xmin(),
                sourceInfo.timestamp(),
                sourceInfo.isSnapshot());
    }

    @Override
    public void markLastSnapshotRecord() {
        sourceInfo.setSnapshot(SnapshotRecord.LAST);
    }

    @Override
    public void event(DataCollectionId tableId, Instant instant) {
        sourceInfo.update(instant, (TableId) tableId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public void incrementalSnapshotEvents() {
        sourceInfo.setSnapshot(SnapshotRecord.INCREMENTAL);
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
                    TransactionContext.load(offset),
                    SignalBasedIncrementalSnapshotContext.load(offset));

        }
    }
}
