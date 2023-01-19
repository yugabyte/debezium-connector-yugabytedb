/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.connection.pgoutput;

import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;

import io.debezium.connector.yugabytedb.YugabyteDBStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.yugabytedb.YugabyteDBType;
import io.debezium.connector.yugabytedb.YugabyteDBTypeRegistry;
import io.debezium.connector.yugabytedb.connection.ReplicationMessage;
import io.debezium.connector.yugabytedb.connection.ReplicationMessageColumnValueResolver;

/**
 * @author Gunnar Morling
 * @author Chris Cranford
 */
public class YbOutputReplicationMessage implements ReplicationMessage {

    private Operation op;
    private Instant commitTimestamp;
    private Long transactionId;
    private String table;
    private List<Column> oldColumns;
    private List<Column> newColumns;

    public YbOutputReplicationMessage(Operation op, String table, Instant commitTimestamp, Long transactionId, List<Column> oldColumns, List<Column> newColumns) {
        this.op = op;
        this.commitTimestamp = commitTimestamp;
        this.transactionId = transactionId;
        this.table = table;
        this.oldColumns = oldColumns;
        this.newColumns = newColumns;
    }

    @Override
    public Operation getOperation() {
        return op;
    }

    @Override
    public Instant getCommitTime() {
        return commitTimestamp;
    }

    @Override
    public String getTransactionId() {
        return transactionId == null ? null : String.valueOf(transactionId);
    }

    @Override
    public String getTable() {
        return table;
    }

    @Override
    public List<Column> getOldTupleList() {
        return oldColumns;
    }

    @Override
    public List<Column> getNewTupleList() {
        return newColumns;
    }

    @Override
    public boolean hasTypeMetadata() {
        return true;
    }

    @Override
    public boolean isLastEventForLsn() {
        return true;
    }

    @Override
    public boolean shouldSchemaBeSynchronized() {
        return false;
    }

    /**
     * Converts the value (string representation) coming from PgOutput plugin to
     * a Java value based on the type of the column from the message.  This value will be converted later on if necessary by the
     * connector's value converter to match whatever the Connect schema type expects.
     *
     * Note that the logic here is tightly coupled on the pgoutput plugin logic which writes the actual value.
     *
     * @return the value; may be null
     */
    public static Object getValue(String columnName, YugabyteDBType type, String fullType, String rawValue,
                                  boolean includeUnknownDataTypes, YugabyteDBTypeRegistry yugabyteDBTypeRegistry) {
        final YbOutputColumnValue columnValue = new YbOutputColumnValue(rawValue);
        return ReplicationMessageColumnValueResolver.resolveValue(columnName, type, fullType, columnValue, includeUnknownDataTypes, yugabyteDBTypeRegistry);
    }
}
