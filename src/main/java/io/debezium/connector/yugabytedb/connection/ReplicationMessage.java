/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb.connection;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.List;

import org.postgresql.geometric.PGbox;
import org.postgresql.geometric.PGcircle;
import org.postgresql.geometric.PGline;
import org.postgresql.geometric.PGpath;
import org.postgresql.geometric.PGpoint;
import org.postgresql.geometric.PGpolygon;
import org.postgresql.util.PGmoney;

import io.debezium.connector.yugabytedb.YugabyteDBStreamingChangeEventSource;
import io.debezium.connector.yugabytedb.YugabyteDBStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.yugabytedb.YugabyteDBType;
import io.debezium.connector.yugabytedb.YugabyteDBTypeRegistry;

/**
 * An abstract representation of a replication message that is sent by a PostgreSQL logical decoding plugin and
 * is processed by the Debezium PostgreSQL connector.
 *
 * @author Jiri Pechanec
 *
 */
public interface ReplicationMessage {

    /**
     *
     * Data modification operation executed
     *
     */
    public enum Operation {
        INSERT,
        UPDATE,
        DELETE,
        TRUNCATE,
        MESSAGE,
        BEGIN,
        COMMIT,
        DDL,
        READ,
        NOOP
    }

    /**
     * A representation of column value delivered as a part of replication message
     */
    public interface Column {
        String getName();

        YugabyteDBType getType();

        /**
         * Returns additional metadata about this column's type.
         */
        ColumnTypeMetadata getTypeMetadata();

        Object getValue(final PgConnectionSupplier connection, boolean includeUnknownDatatypes);

        boolean isOptional();

        default boolean isToastedColumn() {
            return false;
        }
    }

    public interface ColumnTypeMetadata {
        int getLength();

        int getScale();
    }

    public interface ColumnValue<T> {
        T getRawValue();

        boolean isNull();

        String asString();

        Boolean asBoolean();

        Integer asInteger();

        Long asLong();

        Float asFloat();

        Double asDouble();

        Object asDecimal();

        LocalDate asLocalDate();

        OffsetDateTime asOffsetDateTimeAtUtc();

        Instant asInstant();

        Object asTime();

        Object asLocalTime();

        OffsetTime asOffsetTimeUtc();

        byte[] asByteArray();

        PGbox asBox();

        PGcircle asCircle();

        Object asInterval();

        PGline asLine();

        Object asLseg();

        PGmoney asMoney();

        PGpath asPath();

        PGpoint asPoint();

        PGpolygon asPolygon();

        boolean isArray(YugabyteDBType type);

        Object asArray(String columnName, YugabyteDBType type, String fullType, PgConnectionSupplier connection);

        Object asDefault(YugabyteDBTypeRegistry typeRegistry, int columnType, String columnName, String fullType, boolean includeUnknownDatatypes, PgConnectionSupplier connection);
    }

    /**
     * @return A data operation executed
     */
    public Operation getOperation();

    /**
     * @return Transaction commit time for this change
     */
    public Instant getCommitTime();

    /**
     * @return An id of transaction to which this change belongs; will not be
     *         present for non-transactional logical decoding messages for instance
     */
    public String getTransactionId();

    /**
     * @return Table changed
     */
    public String getTable();

    /**
     * @return Set of original values of table columns, null for INSERT
     */
    public List<Column> getOldTupleList();

    /**
     * @return Set of new values of table columns, null for DELETE
     */
    public List<Column> getNewTupleList();

    /**
     * @return true if type metadata are passed as a part of message
     */
    boolean hasTypeMetadata();

    /**
     * @return true if this is the last message in the batch of messages with same LSN
     */
    boolean isLastEventForLsn();

    /**
     * @return true if the stream producer should synchronize the schema when processing messages, false otherwise
     */
    default boolean shouldSchemaBeSynchronized() {
        return true;
    }

    /**
     * Whether this message represents the begin or end of a transaction.
     */
    default boolean isTransactionalMessage() {
        return getOperation() == Operation.BEGIN || getOperation() == Operation.COMMIT;
    }

<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 7747711... Return transationId as a string
    default boolean isDDLMessage() {
        return getOperation() == Operation.DDL;
    }

<<<<<<< HEAD
=======
>>>>>>> 9ffbf7f... Fix compilation errors in ConnectorTask
=======
>>>>>>> 7747711... Return transationId as a string
    /**
     * A special message type that is used to replace event filtered already at {@link MessageDecoder}.
     * Enables {@link YugabyteDBStreamingChangeEventSource} to advance LSN forward even in case of such messages.
     */
    public class NoopMessage implements ReplicationMessage {

        private final Long transactionId;
        private final Instant commitTime;
        private final Operation operation;

        public NoopMessage(Long transactionId, Instant commitTime) {
            this.operation = Operation.NOOP;
            this.transactionId = transactionId;
            this.commitTime = commitTime;
        }

        @Override
        public boolean isLastEventForLsn() {
            return true;
        }

        @Override
        public boolean hasTypeMetadata() {
            throw new UnsupportedOperationException();
        }

        @Override
<<<<<<< HEAD
<<<<<<< HEAD
        public String getTransactionId() {
            return transactionId == null ? null : String.valueOf(transactionId);
=======
        public OptionalLong getTransactionId() {
            return transactionId == null ? OptionalLong.empty() : OptionalLong.of(transactionId);
>>>>>>> 9ffbf7f... Fix compilation errors in ConnectorTask
=======
        public String getTransactionId() {
            return transactionId == null ? null : String.valueOf(transactionId);
>>>>>>> 7747711... Return transationId as a string
        }

        @Override
        public String getTable() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Operation getOperation() {
            return operation;
        }

        @Override
        public List<Column> getOldTupleList() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Column> getNewTupleList() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant getCommitTime() {
            return commitTime;
        }
    }
}
