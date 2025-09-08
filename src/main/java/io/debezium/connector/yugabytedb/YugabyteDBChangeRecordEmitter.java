/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.yugabytedb.connection.ReplicationMessage;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.data.Envelope.Operation;
import io.debezium.function.Predicates;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.*;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

/**
 * Emits change data based on a logical decoding event coming as protobuf or JSON message.
 *
 * @author Suranjan Kumar, Vaibhav Kushwaha
 */
public class YugabyteDBChangeRecordEmitter extends RelationalChangeRecordEmitter<YBPartition> {
    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBChangeRecordEmitter.class);

    private final ReplicationMessage message;
    private final YugabyteDBSchema schema;
    private final YugabyteDBConnectorConfig connectorConfig;
    private final YugabyteDBConnection connection;
    private final TableId tableId;

    private boolean shouldSendBeforeImage = false;

    private final String tabletId;
    private final YugabyteDBOffsetContext offsetContext;

    public YugabyteDBChangeRecordEmitter(YBPartition partition, YugabyteDBOffsetContext offset, Clock clock,
                                         YugabyteDBConnectorConfig connectorConfig,
                                         YugabyteDBSchema schema, YugabyteDBConnection connection,
                                         TableId tableId, ReplicationMessage message,
                                         String tabletId, boolean shouldSendBeforeImage) {
        super(partition, offset, clock, connectorConfig);

        this.schema = schema;
        this.message = message;
        this.connectorConfig = connectorConfig;
        this.connection = connection;

        this.tableId = tableId;
        Objects.requireNonNull(this.tableId);

        this.tabletId = tabletId;

        this.shouldSendBeforeImage = shouldSendBeforeImage;

        this.offsetContext = offset;
    }

    @Override
    public Operation getOperation() {
        switch (message.getOperation()) {
            case INSERT:
                return Operation.CREATE;
            case UPDATE:
                return Operation.UPDATE;
            case DELETE:
                return Operation.DELETE;
            case READ:
                return Operation.READ;
            case TRUNCATE:
                return Operation.TRUNCATE;
            default:
                throw new IllegalArgumentException("Received event of unexpected command type: " + message.getOperation());
        }
    }

    @Override
    public void emitChangeRecords(DataCollectionSchema schema, Receiver receiver) throws InterruptedException {
        schema = synchronizeTableSchema(schema);
        super.emitChangeRecords(schema, receiver);
    }

    @Override
    protected void emitTruncateRecord(Receiver receiver, TableSchema tableSchema) throws InterruptedException {
        Struct envelope = tableSchema.getEnvelopeSchema().truncate(offsetContext.getSourceInfoForTablet(getPartition()), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), tableSchema, Operation.TRUNCATE, null, envelope, getOffset(), null);
    }

    @Override
    protected Object[] getOldColumnValues() {

        try {
            switch (getOperation()) {
                case CREATE:
                    return null;
                case READ:
                    return null;
                // return columnValues(message.getOldTupleList(), tableId, true,
                // message.hasTypeMetadata(), true, true);
                case UPDATE:
                    if (!shouldSendBeforeImage) {
                        return null;
                    }
                default:
                    return columnValues(message.getOldTupleList(), tableId, true,
                            message.hasTypeMetadata(), false, true);
            }
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    protected Object[] getNewColumnValues() {
        try {
            switch (getOperation()) {
                case CREATE:
                    return columnValues(message.getNewTupleList(), tableId, true, message.hasTypeMetadata(), false, false);
                case UPDATE:
                    // todo vaibhav: add scenario for the case of multiple columns being updated
                    // return columnValues(message.getNewTupleList(), tableId, true, message.hasTypeMetadata(), false, false);
                    return updatedColumnValues(message.getNewTupleList(), tableId, true, message.hasTypeMetadata(), false, false);
                case READ:
                    return columnValues(message.getNewTupleList(), tableId, true, message.hasTypeMetadata(), false, false);
                default:
                    return null;
            }
        }
        catch (SQLException e) {
            throw new ConnectException(e);
        }
    }

    private DataCollectionSchema synchronizeTableSchema(DataCollectionSchema tableSchema) {
        if (getOperation() == Operation.DELETE || !message.shouldSchemaBeSynchronized()) {
            return tableSchema;
        }

        final TableId tableId = (TableId) tableSchema.id();

        return schema.schemaForTablet(tableId, tabletId);
    }

    private Object[] columnValues(List<ReplicationMessage.Column> columns, TableId tableId,
                                  boolean refreshSchemaIfChanged, boolean metadataInMessage,
                                  boolean sourceOfToasted, boolean oldValues)
            throws SQLException {
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        final Table table = schema.tableForTablet(tableId, tabletId);
        if (table == null) {
            schema.dumpTableId();
        }

        LOGGER.debug("Column count in schema for tablet {}: {}", tabletId, table.columns().size());
        
        Objects.requireNonNull(table);

        // based on the schema columns, create the values on the same position as the columns
        List<Column> schemaColumns = table.columns();
        // based on the replication message without toasted columns for now
        List<ReplicationMessage.Column> columnsWithoutToasted = columns.stream().filter(Predicates.not(ReplicationMessage.Column::isToastedColumn))
                .collect(Collectors.toList());
        // JSON does not deliver a list of all columns for REPLICA IDENTITY DEFAULT
        Object[] values = new Object[columnsWithoutToasted.size() < schemaColumns.size()
                ? schemaColumns.size()
                : columnsWithoutToasted.size()];

        final Set<String> undeliveredToastableColumns = new HashSet<>(schema
                .getToastableColumnsForTableId(table.id()));
        for (ReplicationMessage.Column column : columns) {
            // DBZ-298 Quoted column names will be sent like that in messages,
            // but stored unquoted in the column names
            final String columnName = Strings.unquoteIdentifierPart(column.getName());
            undeliveredToastableColumns.remove(columnName);
            int position = getPosition(columnName, table, values);
            if (position != -1) {
                Object value = column.getValue(() -> (BaseConnection) connection.connection(),
                        connectorConfig.includeUnknownDatatypes());
                // values[position] = value;
                values[position] = new Object[]{ value, Boolean.TRUE };
            }
        }
        return values;
    }

    private Object[] updatedColumnValues(List<ReplicationMessage.Column> columns, TableId tableId,
                                         boolean refreshSchemaIfChanged, boolean metadataInMessage,
                                         boolean sourceOfToasted, boolean oldValues)
            throws SQLException {
        if (columns == null || columns.isEmpty()) {
            return null;
        }
        final Table table = schema.tableForTablet(tableId, tabletId);
        if (table == null) {
            schema.dumpTableId();
        }
        Objects.requireNonNull(table);

        // based on the schema columns, create the values on the same position as the columns
        List<Column> schemaColumns = table.columns();
        // based on the replication message without toasted columns for now
        List<ReplicationMessage.Column> columnsWithoutToasted = columns.stream().filter(Predicates.not(ReplicationMessage.Column::isToastedColumn))
                .collect(Collectors.toList());
        // JSON does not deliver a list of all columns for REPLICA IDENTITY DEFAULT
        Object[] values = new Object[columnsWithoutToasted.size() < schemaColumns.size()
                ? schemaColumns.size()
                : columnsWithoutToasted.size()];

        // initialize to unset

        final Set<String> undeliveredToastableColumns = new HashSet<>(schema
                .getToastableColumnsForTableId(table.id()));
        for (ReplicationMessage.Column column : columns) {
            // DBZ-298 Quoted column names will be sent like that in messages,
            // but stored unquoted in the column names
            final String columnName = Strings.unquoteIdentifierPart(column.getName());
            undeliveredToastableColumns.remove(columnName);

            int position = getPosition(columnName, table, values);
            if (position != -1) {
                Object value = column.getValue(() -> (BaseConnection) connection.connection(),
                        connectorConfig.includeUnknownDatatypes());

                values[position] = new Object[]{ value, Boolean.TRUE };
            }
        }
        return values;
    }

    private int getPosition(String columnName, Table table, Object[] values) {
        final Column tableColumn = table.columnWithName(columnName);

        if (tableColumn == null) {
            LOGGER.warn(
                    "Internal schema is out-of-sync with incoming decoder events; column {} will be omitted from the change event.",
                    columnName);
            return -1;
        }
        int position = tableColumn.position() - 1;
        if (position < 0 || position >= values.length) {
            LOGGER.warn(
                    "Internal schema is out-of-sync with incoming decoder events; column {} will be omitted from the change event.",
                    columnName);
            return -1;
        }
        return position;
    }

    private Optional<DataCollectionSchema> newTable(TableId tableId) {
        LOGGER.debug("Creating a new schema entry for table: {} and tablet {}", tableId, tabletId);
        refreshTableFromDatabase(tableId);
        final TableSchema tableSchema = schema.schemaForTablet(tableId, tabletId);
        if (tableSchema == null) {
            LOGGER.warn("cannot load schema for table '{}'", tableId);
            return Optional.empty();
        }
        else {
            LOGGER.debug("refreshed DB schema to include table '{}'", tableId);
            return Optional.of(tableSchema);
        }
    }

    private void refreshTableFromDatabase(TableId tableId) {
        try {
            // Using another implementation of refresh() to take into picture the schema information too.
            LOGGER.debug("Refreshing schema for the table {}", tableId);
            if (connectorConfig.isYSQLDbType()) {
                schema.refresh(connection, tableId,
                        connectorConfig.skipRefreshSchemaOnMissingToastableData(),
                        schema.getSchemaPBForTablet(tableId, tabletId), tabletId);

            } else {
                // This implementation of refresh is for cql tables where we do not have a JDBC connection
                schema.refresh(tableId, connectorConfig.skipRefreshSchemaOnMissingToastableData(),
                        schema.getSchemaPBForTablet(tableId, tabletId), tabletId);
            }

        }
        catch (SQLException e) {
            throw new ConnectException("Database error while refresing table schema", e);
        }
    }

    static Optional<DataCollectionSchema> updateSchema(YBPartition partition, TableId tableId,
                                                       ChangeRecordEmitter changeRecordEmitter) {
        return ((YugabyteDBChangeRecordEmitter) changeRecordEmitter).newTable(tableId);
    }

    @Override
    protected boolean skipEmptyMessages() {
        return true;
    }

    @Override
    protected void emitCreateRecord(Receiver<YBPartition> receiver, TableSchema tableSchema) throws InterruptedException {
        Object[] newColumnValues = getNewColumnValues();
        Struct newKey = tableSchema.keyFromColumnData(newColumnValues);
        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct envelope = tableSchema.getEnvelopeSchema().create(newValue, offsetContext.getSourceInfoForTablet(getPartition()), getClock().currentTimeAsInstant());

        if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
            // This case can be hit on UPDATE / DELETE when there's no primary key defined while using certain decoders
            LOGGER.warn("no new values found for table '{}' from create message at '{}'; skipping record", tableSchema, offsetContext.getSourceInfoForTablet(getPartition()));
            return;
        }
        receiver.changeRecord(getPartition(), tableSchema, Operation.CREATE, newKey, envelope, getOffset(), null);
    }

    @Override
    protected void emitReadRecord(Receiver<YBPartition> receiver, TableSchema tableSchema) throws InterruptedException {
        Object[] newColumnValues = getNewColumnValues();
        Struct newKey = tableSchema.keyFromColumnData(newColumnValues);
        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct envelope = tableSchema.getEnvelopeSchema().read(newValue, offsetContext.getSourceInfoForTablet(getPartition()), getClock().currentTimeAsInstant());

        receiver.changeRecord(getPartition(), tableSchema, Operation.READ, newKey, envelope, getOffset(), null);
    }

    // In case of YB, the update schema is different
    @Override
    protected void emitUpdateRecord(Receiver receiver, TableSchema tableSchema)
            throws InterruptedException {

        Object[] oldColumnValues = getOldColumnValues();
        Object[] newColumnValues = getNewColumnValues();

        Struct oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Struct newKey = tableSchema.keyFromColumnData(newColumnValues);

        Struct newValue = tableSchema.valueFromColumnData(newColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        if (skipEmptyMessages() && (newColumnValues == null || newColumnValues.length == 0)) {
            LOGGER.warn("no new values found for table '{}' from update message at '{}'; skipping record", tableSchema, tabletId);
            return;
        }
        // some configurations does not provide old values in case of updates
        // in this case we handle all updates as regular ones
        if (oldKey == null || Objects.equals(oldKey, newKey)) {
            Struct envelope = tableSchema.getEnvelopeSchema().update(oldValue, newValue, offsetContext.getSourceInfoForTablet(getPartition()), getClock().currentTimeAsInstant());
            receiver.changeRecord(getPartition(), tableSchema, Operation.UPDATE, newKey, envelope, getOffset(), null);
        }
        // PK update -> emit as delete and re-insert with new key
        else {
            ConnectHeaders headers = new ConnectHeaders();
            headers.add(PK_UPDATE_NEWKEY_FIELD, newKey, tableSchema.keySchema());

            Struct envelope = tableSchema.getEnvelopeSchema().delete(oldValue, offsetContext.getSourceInfoForTablet(getPartition()), getClock().currentTimeAsInstant());
            receiver.changeRecord(getPartition(), tableSchema, Operation.DELETE, oldKey, envelope, getOffset(), headers);

            headers = new ConnectHeaders();
            headers.add(PK_UPDATE_OLDKEY_FIELD, oldKey, tableSchema.keySchema());

            envelope = tableSchema.getEnvelopeSchema().create(newValue, offsetContext.getSourceInfoForTablet(getPartition()), getClock().currentTimeAsInstant());
            receiver.changeRecord(getPartition(), tableSchema, Operation.CREATE, newKey, envelope, getOffset(), headers);
        }
    }

    @Override
    protected void emitDeleteRecord(Receiver receiver, TableSchema tableSchema) throws InterruptedException {
        Object[] oldColumnValues = getOldColumnValues();
        Struct oldKey = tableSchema.keyFromColumnData(oldColumnValues);
        Struct oldValue = tableSchema.valueFromColumnData(oldColumnValues);

        if (skipEmptyMessages() && (oldColumnValues == null || oldColumnValues.length == 0) && shouldSendBeforeImage) {
            LOGGER.warn("no old values found for table '{}' from delete message at '{}'; skipping record", tableSchema, offsetContext.getSourceInfoForTablet(getPartition()));
            return;
        }

        Struct envelope = tableSchema.getEnvelopeSchema().delete(oldValue, offsetContext.getSourceInfoForTablet(getPartition()), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), tableSchema, Operation.DELETE, oldKey, envelope, getOffset(), null);
    }
}
