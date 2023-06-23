/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.common.BaseSourceInfo;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.relational.TableId;
import io.debezium.time.Conversions;

/**
 * Information about the source of information for a particular record.
 *
 * @author Suranjan Kumar, Rajat Venkatesh, Vaibhav Kushwaha
 */
@NotThreadSafe
public final class SourceInfo extends BaseSourceInfo {

    public static final String TIMESTAMP_USEC_KEY = "ts_usec";
    public static final String TXID_KEY = "txId";
    public static final String LSN_KEY = "lsn";
<<<<<<< HEAD

    public static final String COMMIT_TIME = "commit_time";

    public static final String RECORD_TIME = "record_time";

    public static final String TABLE_ID = "table_id";
    public static final String TABLET_ID = "tablet_id";
    public static final String PARTITION_ID_KEY = "partition_id";
=======
>>>>>>> main

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String dbName;

    private OpId lsn;
    private OpId lastCommitLsn;
    private String txId;
    private Instant timestamp;
    private String schemaName;
    private String tableName;
    private String tableUUID;
    private String tabletId;
    private Long commitTime;
    private Long recordTime;

    protected SourceInfo(YugabyteDBConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.dbName = connectorConfig.databaseName();
    }

    protected SourceInfo(YugabyteDBConnectorConfig connectorConfig, OpId lastCommitLsn) {
        super(connectorConfig);
        this.dbName = connectorConfig.databaseName();
        this.lastCommitLsn = lastCommitLsn;
        this.lsn = lastCommitLsn;
    }

    /**
     * Updates the source with information about a particular received or read event.
     *
     * @param tabletId Tablet ID of the partition
     * @param lsn the position in the server WAL for a particular event; may be null indicating that this information is not
     * available
     * @param commitTime the commit time of the transaction that generated the event;
     * may be null indicating that this information is not available
     * @param txId the ID of the transaction that generated the transaction; may be null if this information is not available
     * @param tableId the table that should be included in the source info; may be null
<<<<<<< HEAD
     * @param recordTime Hybrid Time Stamp Time of the statement within the transaction.
     * @return this instance
     */
    protected SourceInfo update(YBPartition partition, OpId lsn, long commitTime, String txId,
                                TableId tableId, Long recordTime) {
=======
     * @return this instance
     */
    protected SourceInfo update(YBPartition partition, OpId lsn, Instant commitTime, String txId,
                                TableId tableId) {
>>>>>>> main
        this.lsn = lsn;
        this.commitTime = commitTime;
        this.txId = txId;
<<<<<<< HEAD
        this.recordTime = recordTime;
        this.tableUUID = partition.getTableId();
        this.tabletId = partition.getTabletId();

        // The commit time of the record is technically the timestamp of the record.
        this.timestamp = Conversions.toInstantFromMicros(commitTime);

=======
>>>>>>> main
        if (tableId != null && tableId.schema() != null) {
            this.schemaName = tableId.schema();
        }
        if (tableId != null && tableId.table() != null) {
            this.tableName = tableId.table();
        }
        return this;
    }

    /**
     * Updates the source with the LSN of the last committed transaction.
     */
    protected SourceInfo updateLastCommit(OpId lsn) {
        this.lastCommitLsn = lsn;
        this.lsn = lsn;
        return this;
    }

    protected SourceInfo update(Instant timestamp, TableId tableId) {
        this.timestamp = timestamp;
        if (tableId != null && tableId.schema() != null) {
            this.schemaName = tableId.schema();
        }
        if (tableId != null && tableId.table() != null) {
            this.tableName = tableId.table();
        }
        return this;
    }

    public OpId lsn() {
        return this.lsn;
    }

    public String sequence() {
        List<String> sequence = new ArrayList<String>(2);
        String lastCommitLsn = (this.lastCommitLsn != null)
                ? this.lastCommitLsn.toSerString()
                : null;
        String lsn = (this.lsn != null)
                ? this.lsn.toSerString()
                : null;
        sequence.add(lastCommitLsn);
        sequence.add(lsn);
        try {
            return MAPPER.writeValueAsString(sequence);
        }
        catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected String database() {
        return dbName;
    }

    String schemaName() {
        return schemaName;
    }

    String tableName() {
        return tableName;
    }

    @Override
    protected Instant timestamp() {
        return timestamp;
    }

    protected String txId() {
        return txId;
    }

    protected Long commitTime() {
        return this.commitTime;
    }

    protected Long recordTime() {
        return this.recordTime;
    }

    protected String tabletId() {
        return this.tabletId;
    }

    protected String tableUUID() {
        return this.tableUUID;
    }

    @Override
    public SnapshotRecord snapshot() {
        return super.snapshot();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("source_info[");
        sb.append("server='").append(serverName()).append('\'');
        sb.append("db='").append(dbName).append('\'');
        if (lsn != null) {
            sb.append(", lsn=").append(lsn);
        }
        if (txId != null) {
            sb.append(", txId=").append(txId);
        }
        if (lastCommitLsn != null) {
            sb.append(", lastCommitLsn=").append(lastCommitLsn);
        }
        if (timestamp != null) {
            sb.append(", timestamp=").append(timestamp);
        }
        sb.append(", snapshot=").append(snapshot());
        if (schemaName != null) {
            sb.append(", schema=").append(schemaName);
        }
        if (tableName != null) {
            sb.append(", table=").append(tableName);
        }
        sb.append(']');
        return sb.toString();
    }
}
