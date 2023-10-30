/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb.connection.pgproto;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.Common;
import org.yb.QLType;
import org.yb.Value;
import org.yb.cdc.CdcService;

import io.debezium.connector.yugabytedb.YugabyteDBStreamingChangeEventSource.PgConnectionSupplier;
import io.debezium.connector.yugabytedb.YugabyteDBType;
import io.debezium.connector.yugabytedb.YugabyteDBTypeRegistry;
import io.debezium.connector.yugabytedb.connection.AbstractReplicationMessageColumn;
import io.debezium.connector.yugabytedb.connection.ReplicationMessage;
import io.debezium.connector.yugabytedb.connection.ReplicationMessageColumnValueResolver;
import io.debezium.util.Strings;

/**
 * Replication message representing message sent by <a href="https://github.com/debezium/postgres-decoderbufs">Postgres Decoderbufs</>
 *
 * @author Suranjan Kumar
 */
public class YbProtoReplicationMessage implements ReplicationMessage {

    private static final Logger LOGGER = LoggerFactory.getLogger(YbProtoReplicationMessage.class);

    private final CdcService.RowMessage rawMessage;
    private final YugabyteDBTypeRegistry yugabyteDBTypeRegistry;

    public YbProtoReplicationMessage(CdcService.RowMessage rawMessage,
                                     YugabyteDBTypeRegistry yugabyteDBTypeRegistry) {
        this.rawMessage = rawMessage;
        this.yugabyteDBTypeRegistry = yugabyteDBTypeRegistry;
    }

    @Override
    public Operation getOperation() {
        switch (rawMessage.getOp()) {
            case INSERT:
                return Operation.INSERT;
            case UPDATE:
                return Operation.UPDATE;
            case DELETE:
                return Operation.DELETE;
            case READ:
                return Operation.READ;
            case BEGIN:
                return Operation.BEGIN;
            case COMMIT:
                return Operation.COMMIT;
            case DDL:
                return Operation.DDL;
        }
        throw new IllegalArgumentException(
                "Unknown operation '" + rawMessage.getOp() + "' in replication stream message " + rawMessage);
    }

    @Override
    public Instant getCommitTime() {
        // value is microseconds
        return Instant.ofEpochSecond(0, rawMessage.getCommitTime() * 1_000);
    }

    public long getRawCommitTime() {
        return rawMessage.getCommitTime();
    }

    public long getRecordTime() {
        return rawMessage.getRecordTime();
    }
    @Override
    public String getTransactionId() {
        return rawMessage.getTransactionId() == null ? null : rawMessage.getTransactionId().toStringUtf8();
    }

    @Override
    public String getTable() {
        return rawMessage.getTable();
    }

    @Override
    public List<ReplicationMessage.Column> getOldTupleList() {
        return transform(rawMessage.getOldTupleList(), null);
    }

    @Override
    public List<ReplicationMessage.Column> getNewTupleList() {
        return transform(rawMessage.getNewTupleList(), rawMessage.getNewTypeinfoList());
    }

    @Override
    public boolean hasTypeMetadata() {
        return !(rawMessage.getNewTypeinfoList() == null || rawMessage.getNewTypeinfoList().isEmpty());
    }

    private List<ReplicationMessage.Column> transform(List<Common.DatumMessagePB> messageList,
                                                      List<CdcService.TypeInfo> typeInfoList) {
        return IntStream.range(0, messageList.size())
                .mapToObj(index -> {
                    final Common.DatumMessagePB datum = messageList.get(index);
                    final Optional<CdcService.TypeInfo> typeInfo = Optional.ofNullable(hasTypeMetadata() && typeInfoList != null ? typeInfoList.get(index) : null);
                    final String columnName = Strings.unquoteIdentifierPart(datum.getColumnName());

                    if (!datum.hasCqlType()) {
                        final YugabyteDBType type = yugabyteDBTypeRegistry.get((int) datum.getColumnType());
                        final String fullType = typeInfo.map(CdcService.TypeInfo::getModifier).orElse(null);
                        return new AbstractReplicationMessageColumn(columnName, type, fullType,
                                typeInfo.map(CdcService.TypeInfo::getValueOptional).orElse(Boolean.FALSE), hasTypeMetadata()) {
                            @Override
                            public Object getValue(PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
                                return YbProtoReplicationMessage.this.getValue(columnName, type,
                                        fullType, datum, connection, includeUnknownDatatypes);
                            }
                            @Override
                            public String toString() {
                                return datum.toString();
                            }
                        };
                    }
                    else {
                        final Common.QLTypePB type = datum.getCqlType();
                        final String fullType = typeInfo.map(CdcService.TypeInfo::getModifier).orElse(null);
                        return new AbstractReplicationMessageColumn(columnName, type, fullType,
                                typeInfo.map(CdcService.TypeInfo::getValueOptional).orElse(Boolean.FALSE), hasTypeMetadata()) {
                            @Override
                            public Object getValue(PgConnectionSupplier connection, boolean includeUnknownDatatypes) {
                                return YbProtoReplicationMessage.this.getValue(columnName, type
                                        , datum);
                            }
                            @Override
                            public String toString() {
                                return datum.toString();
                            }
                        };
                    }
                })
                .collect(Collectors.toList());
    }

    // todo vaibhav: why does this return true and why not some value based on some condition
    @Override
    public boolean isLastEventForLsn() {
        return true;
    }

    public Object getValue(String columnName, YugabyteDBType type, String fullType,
                           Common.DatumMessagePB datumMessage,
                           final PgConnectionSupplier connection,
                           boolean includeUnknownDatatypes) {
        final YbProtoColumnValue columnValue = new YbProtoColumnValue(datumMessage);
        return ReplicationMessageColumnValueResolver.resolveValue(columnName, type, fullType,
                columnValue, connection, includeUnknownDatatypes, yugabyteDBTypeRegistry);
    }

    public Object getValue(String columnName, Common.QLTypePB type,
                           Common.DatumMessagePB datumMessage) {
        final YbProtoCqlColumnValue columnValue = new YbProtoCqlColumnValue(datumMessage.getCqlValue());
        return ReplicationMessageColumnValueResolver.resolveValue(columnName, QLType.createFromQLTypePB(type),
                columnValue);
    }

    public CdcService.CDCSDKSchemaPB getSchema() {
        return this.rawMessage.getSchema();
    }
}
