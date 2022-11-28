/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.*;

import io.debezium.data.Envelope;
import io.debezium.relational.mapping.ColumnMappers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.yugabytedb.connection.ServerInfo;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.*;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Collect;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Component that records the schema information for the {@link YugabyteDBConnector}. The schema information contains
 * the {@link Tables table definitions} and the Kafka Connect {@link #schemaFor(TableId) Schema}s for each table, where the
 * {@link Schema} excludes any columns that have been {@link YugabyteDBConnectorConfig#COLUMN_EXCLUDE_LIST specified} in the
 * configuration.
 *
 * @author Suranjan Kumar (skumar@yugabyte.com)
 */
@NotThreadSafe
public class YugabyteDBSchema extends RelationalDatabaseSchema {

    protected final static String PUBLIC_SCHEMA_NAME = "public";
    private final static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBSchema.class);

    private final YugabyteDBTypeRegistry yugabyteDBTypeRegistry;

    private final Map<TableId, List<String>> tableIdToToastableColumns;
    private final Map<Integer, TableId> relationIdToTableId;
    private final boolean readToastableColumns;
    private Map<TableId, List<String>> cachedTableSchema;

    private CdcService.CDCSDKSchemaPB cdcsdkSchemaPB;

    private Map<String, CdcService.CDCSDKSchemaPB> tabletToCdcsdkSchemaPB = new HashMap<>();
    private Map<String, Table> tabletIdToTable = new HashMap<>();
    private Map<String, TableSchema> tabletIdToTableSchema = new HashMap<>();

    private YugabyteDBConnectorConfig config;
    private YugabyteDBValueConverter valueConverter;
    private TopicSelector<TableId> topicSelector;

    /**
     * Create a schema component given the supplied {@link YugabyteDBConnectorConfig Postgres connector configuration}.
     *
     * @param config the connector configuration, which is presumed to be valid
     */
    protected YugabyteDBSchema(YugabyteDBConnectorConfig config, YugabyteDBTypeRegistry yugabyteDBTypeRegistry,
                               TopicSelector<TableId> topicSelector, YugabyteDBValueConverter valueConverter) {
        super(config, topicSelector, new Filters(config).tableFilter(),
                config.getColumnFilter(), getTableSchemaBuilder(config, valueConverter), false,
                config.getKeyMapper());

        this.yugabyteDBTypeRegistry = yugabyteDBTypeRegistry;
        this.tableIdToToastableColumns = new HashMap<>();
        this.relationIdToTableId = new HashMap<>();
        this.readToastableColumns = config.skipRefreshSchemaOnMissingToastableData();

        this.config = config;
        this.valueConverter = valueConverter;
        this.topicSelector = topicSelector;
    }

    private static TableSchemaBuilder getTableSchemaBuilder(YugabyteDBConnectorConfig config,
                                                            YugabyteDBValueConverter valueConverter) {
        return new YBTableSchemaBuilder(valueConverter, SchemaNameAdjuster.create(),
                config.customConverterRegistry(), config.getSourceInfoStructMaker().schema(),
                config.getSanitizeFieldNames(), false);
    }

    /**
     * Initializes the content for this schema by reading all the database information from the supplied connection.
     *
     * @param connection               a {@link JdbcConnection} instance, never {@code null}
     * @param printReplicaIdentityInfo whether or not to look and print out replica identity information about the tables
     * @return this object so methods can be chained together; never null
     * @throws SQLException if there is a problem obtaining the schema from the database server
     */
    protected YugabyteDBSchema refresh(YugabyteDBConnection connection,
                                       boolean printReplicaIdentityInfo)
            throws SQLException {
        // read all the information from the DB
        // get from the cached Table schema

        // tables()
        // modify and put the schema information here
        //
        connection.readSchema(tables(), null, null,
                getTableFilter(), null, true);
        if (printReplicaIdentityInfo) {
            // print out all the replica identity info
            tableIds().forEach(tableId -> printReplicaIdentityInfo(connection, tableId));
        }
        // and then refresh the schemas
        refreshSchemas();
        if (readToastableColumns) {
            tableIds().forEach(tableId -> refreshToastableColumnsMap(connection, tableId));
        }
        return this;
    }

    protected YugabyteDBSchema refresh(TableId tableId, CdcService.CDCSDKSchemaPB schemaPB) {
        // and then refresh the schemas
        // refreshSchemas();
        if (cdcsdkSchemaPB == null) {
            cdcsdkSchemaPB = schemaPB;
        }

        readSchema(tables(), null, null,
                getTableFilter(), null, true, schemaPB, tableId);
        refreshSchemas(tableId);
        return this;
    }

    protected YugabyteDBSchema refreshWithSchema(TableId tableId,
                                                 CdcService.CDCSDKSchemaPB schemaPB,
                                                 String schemaName) {
        // and then refresh the schemas
        // refreshSchemas();
        if (cdcsdkSchemaPB == null) {
            cdcsdkSchemaPB = schemaPB;
        }

        readSchema(tables(), null, schemaName,
                getTableFilter(), null, true, schemaPB, tableId);
        refreshSchemas(tableId);
        return this;
    }

    protected YugabyteDBSchema refreshSchemaWithTabletId(TableId tableId,
                                                         CdcService.CDCSDKSchemaPB schemaPB,
                                                         String schemaName,
                                                         String tabletId) {
        // and then refresh the schemas
        // refreshSchemas();
        if (cdcsdkSchemaPB == null) {
            tabletToCdcsdkSchemaPB.put(tabletId, schemaPB);
//             cdcsdkSchemaPB = schemaPB;
        }

        readSchemaWithTablet(tables(), null, schemaName,
                getTableFilter(), null, true, schemaPB, tableId, tabletId);
        refreshSchemasWithTabletId(tableId, tabletId);
        return this;
    }

    protected CdcService.CDCSDKSchemaPB getSchemaPB() {
        return this.cdcsdkSchemaPB;
    }

    private void printReplicaIdentityInfo(YugabyteDBConnection connection, TableId tableId) {
        try {
            ServerInfo.ReplicaIdentity replicaIdentity = connection.readReplicaIdentityInfo(tableId);
            LOGGER.info("REPLICA IDENTITY for '{}' is '{}'; {}", tableId, replicaIdentity, replicaIdentity.description());
        }
        catch (SQLException e) {
            LOGGER.warn("Cannot determine REPLICA IDENTITY info for '{}'", tableId);
        }
    }

    // schemaNamePattern is the name of the schema here
    public void readSchema(Tables tables, String databaseCatalog, String schemaNamePattern,
                           Tables.TableFilter tableFilter, Tables.ColumnNameFilter columnFilter,
                           boolean removeTablesNotFoundInJdbc,
                           CdcService.CDCSDKSchemaPB schemaPB,
                           TableId tableId) {
        // Before we make any changes, get the copy of the set of table IDs ...
        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());
        // final String catalogName = "yugabyte";
        // final String schemaName = "public";
        // final String tableName = "t1";
        // TableId tableId = new TableId(null, schemaName, tableName);

        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        // Find regular and materialized views as they cannot be snapshotted
        final Set<TableId> tableIds = new HashSet<>();
        if (tableFilter == null || tableFilter.isIncluded(tableId)) {
            tableIds.add(tableId);
        }
        int totalTables = 0;

        for (TableId includeTable : tableIds) {
            Map<TableId, List<Column>> cols = getColumnsDetailsWithSchema(databaseCatalog, schemaNamePattern,
                    includeTable.table(), tableFilter,
                    columnFilter, schemaPB, schemaNamePattern);
            columnsByTable.putAll(cols);
        }

        // Read the metadata for the primary keys ...
        for (Map.Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
            // First get the primary key information, which must be done for *each* table ...
            List<String> pkColumnNames = readPrimaryKeyOrUniqueIndexNames(schemaPB, tableEntry.getKey());

            // Then define the table ...
            List<Column> columns = tableEntry.getValue();
            Collections.sort(columns);
            String defaultCharsetName = null; // JDBC does not expose character sets
            tables.overwriteTable(tableEntry.getKey(), columns, pkColumnNames, defaultCharsetName);
        }

        if (removeTablesNotFoundInJdbc) {
            // Remove any definitions for tables that were not found in the database metadata ...
            // tableIdsBefore.removeAll(columnsByTable.keySet());
            // tableIdsBefore.forEach(tables::removeTable);
        }
    }

    public void readSchemaWithTablet(Tables tables, String databaseCatalog, String schemaNamePattern,
                           Tables.TableFilter tableFilter, Tables.ColumnNameFilter columnFilter,
                           boolean removeTablesNotFoundInJdbc,
                           CdcService.CDCSDKSchemaPB schemaPB,
                           TableId tableId, String tabletId) {
        // Before we make any changes, get the copy of the set of table IDs ...
        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());

        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        // Find regular and materialized views as they cannot be snapshotted
        final Set<TableId> tableIds = new HashSet<>();
        if (tableFilter == null || tableFilter.isIncluded(tableId)) {
            tableIds.add(tableId);
        }
        int totalTables = 0;

        for (TableId includeTable : tableIds) {
            Map<TableId, List<Column>> cols = getColumnsDetailsWithSchema(databaseCatalog, schemaNamePattern,
                    includeTable.table(), tableFilter,
                    columnFilter, schemaPB, schemaNamePattern);
            columnsByTable.putAll(cols);
        }

        // Read the metadata for the primary keys ...
        for (Map.Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
            // First get the primary key information, which must be done for *each* table ...
            List<String> pkColumnNames = readPrimaryKeyOrUniqueIndexNames(schemaPB, tableEntry.getKey());

            // Then define the table ...
            List<Column> columns = tableEntry.getValue();
            Collections.sort(columns);
            String defaultCharsetName = null; // JDBC does not expose character sets
            Table updatedTable = Table.editor()
              .tableId(tableId)
              .addColumns(columns)
              .setPrimaryKeyNames(pkColumnNames)
              .setDefaultCharsetName(null)
              .create();
            tabletIdToTable.put(tabletId, updatedTable);
//            tables.overwriteTable(tableEntry.getKey(), columns, pkColumnNames, defaultCharsetName);
        }

        if (removeTablesNotFoundInJdbc) {
            // Remove any definitions for tables that were not found in the database metadata ...
            // tableIdsBefore.removeAll(columnsByTable.keySet());
            // tableIdsBefore.forEach(tables::removeTable);
        }
    }

    protected Table getTableForTablet(String tabletId) {
        return tabletIdToTable.get(tabletId);
    }

    protected List<String> readPrimaryKeyOrUniqueIndexNames(CdcService.CDCSDKSchemaPB schemaPB,
                                                            TableId id) {
        final List<String> pkColumnNames = readPrimaryKeyNames(schemaPB, id);
        return pkColumnNames;
    }

    public List<String> readPrimaryKeyNames(CdcService.CDCSDKSchemaPB schemaPB, TableId id) {
        final List<String> pkColumnNames = new ArrayList<>();
        int columnIndex = 1;
        for (CdcService.CDCSDKColumnInfoPB c : schemaPB.getColumnInfoList()) {
            String columnName = c.getName();
            if (c.getIsKey()) {
                Collect.set(pkColumnNames, columnIndex - 1, columnName, null);
            }
            columnIndex++;
        }

        return pkColumnNames;
    }

    private Map<TableId, List<Column>> getColumnsDetails(String databaseCatalog,
                                                         String schemaNamePattern,
                                                         String tableName,
                                                         Tables.TableFilter tableFilter,
                                                         Tables.ColumnNameFilter columnFilter,
                                                         CdcService.CDCSDKSchemaPB schemaPB) {
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        int position = 1;
        for (CdcService.CDCSDKColumnInfoPB columnMetadata : schemaPB.getColumnInfoList()) {
            // final String catalogName = "yugabyte";
            final String schemaName = "public";
            // final String tableName = "test1";
            TableId tableId = new TableId(null, schemaName, tableName);

            // exclude views and non-captured tables
            if ((tableFilter != null && !tableFilter.isIncluded(tableId))) {
                continue;
            }

            // add all included columns
            readTableColumn(columnMetadata, tableId, columnFilter, position).ifPresent(column -> {
                columnsByTable.computeIfAbsent(tableId, t -> new ArrayList<>())
                        .add(column.create());
            });
            position++;
        }
        return columnsByTable;
    }

    private Map<TableId, List<Column>> getColumnsDetailsWithSchema(String databaseCatalog,
                                                                   String schemaNamePattern,
                                                                   String tableName,
                                                                   Tables.TableFilter tableFilter,
                                                                   Tables.ColumnNameFilter columnFilter,
                                                                   CdcService.CDCSDKSchemaPB schemaPB,
                                                                   String schemaNameFromYb) {
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        int position = 1;
        for (CdcService.CDCSDKColumnInfoPB columnMetadata : schemaPB.getColumnInfoList()) {
            // final String catalogName = "yugabyte";
            final String schemaName = schemaNameFromYb;
            // final String tableName = "test1";
            TableId tableId = new TableId(databaseCatalog, schemaName, tableName);

            // exclude views and non-captured tables
            if ((tableFilter != null && !tableFilter.isIncluded(tableId))) {
                continue;
            }

            // add all included columns
            readTableColumn(columnMetadata, tableId, columnFilter, position).ifPresent(column -> {
                columnsByTable.computeIfAbsent(tableId, t -> new ArrayList<>())
                        .add(column.create());
            });
            position++;
        }
        return columnsByTable;
    }

    protected Optional<ColumnEditor> readTableColumn(CdcService.CDCSDKColumnInfoPB columnMetadata,
                                                     TableId tableId,
                                                     Tables.ColumnNameFilter columnFilter,
                                                     int position) {
        final String columnName = columnMetadata.getName();
        final int oid = columnMetadata.getOid();
        if (columnFilter == null || columnFilter.matches(tableId.catalog(), tableId.schema(),
                tableId.table(), columnName)) {
            final ColumnEditor column = Column.editor().name(columnName);
            column.type(columnMetadata.getType().toString());
            // TODO: Handle the non default length/scale later.
            column.length(getLength(oid));
            column.scale(getScale(oid));
            column.optional(columnMetadata.getIsNullable());
            column.position(position);
            // TODO: Handle it later, Mark it as false for now
            column.autoIncremented(false);
            column.generated(false);
            column.nativeType(resolveNativeType(oid));
            column.jdbcType(resolveJdbcType(column.nativeType()));
            return Optional.of(column);
        }

        return Optional.empty();
    }

    private int getLength(String typeName) {
        return getTypeRegistry().get(typeName).getDefaultLength();
    }

    private int getScale(String typeName) {
        return getTypeRegistry().get(typeName).getDefaultScale();
    }

    private int resolveNativeType(String typeName) {
        return getTypeRegistry().get(typeName).getRootType().getOid();
    }

    private int getLength(int oid) {
        return getTypeRegistry().get(oid).getDefaultLength();
    }

    private int getScale(int oid) {
        return getTypeRegistry().get(oid).getDefaultScale();
    }

    private int resolveNativeType(int oid) {
        return getTypeRegistry().get(oid).getRootType().getOid();
    }

    private int resolveJdbcType(int nativeType) {
        // Special care needs to be taken for columns that use user-defined domain type data types
        // where resolution of the column's JDBC type needs to be that of the root type instead of
        // the actual column to properly influence schema building and value conversion.
        return getTypeRegistry().get(nativeType).getRootType().getJdbcId();
    }

    /**
     * Refreshes this schema's content for a particular table
     *
     * @param connection              a {@link JdbcConnection} instance, never {@code null}
     * @param tableId                 the table identifier; may not be null
     * @param refreshToastableColumns refreshes the cache of toastable columns for `tableId`, if {@code true}
     * @throws SQLException if there is a problem refreshing the schema from the database server
     */
    protected void refresh(YugabyteDBConnection connection, TableId tableId,
                           boolean refreshToastableColumns)
            throws SQLException {
        Tables temp = new Tables();
        readSchema(temp, null, tableId.schema(), tableId::equals,
                null, true, null, tableId);

        // the table could be deleted before the event was processed
        if (temp.size() == 0) {
            LOGGER.warn("Refresh of {} was requested but the table no longer exists", tableId);
            return;
        }
        // overwrite (add or update) or views of the tables
        tables().overwriteTable(temp.forTable(tableId));
        // refresh the schema
        refreshSchema(tableId);

        if (refreshToastableColumns) {
            // and refresh toastable columns info
            refreshToastableColumnsMap(connection, tableId);
        }
    }

    protected void refresh(YugabyteDBConnection connection, TableId tableId,
                           boolean refreshToastableColumns, CdcService.CDCSDKSchemaPB schemaPB, String tabletId)
            throws SQLException {
        Tables temp = new Tables();
        readSchemaWithTablet(temp, null, tableId.schema(), tableId::equals,
                null, true, schemaPB, tableId, tabletId);

        // the table could be deleted before the event was processed
        if (temp.size() == 0) {
            LOGGER.warn("Refresh of {} was requested but the table no longer exists", tableId);
            return;
        }
        // overwrite (add or update) or views of the tables
        tables().overwriteTable(temp.forTable(tableId));
//        tabletIdToTableSchema.put(tabletId, temp.forTable(tableId).)
        // refresh the schema
        refreshSchema(tableId);

        if (refreshToastableColumns) {
            // and refresh toastable columns info
            refreshToastableColumnsMap(connection, tableId);
        }
    }

    protected boolean isFilteredOut(TableId id) {
        return !getTableFilter().isIncluded(id);
    }

    /**
     * Discard any currently-cached schemas and rebuild them using the filters.
     */
    protected void refreshSchemas(TableId id) {
        // clearSchemas();
        removeSchema(id);
        // Create TableSchema instances for any existing table ...
        tableIds().forEach(this::refreshSchema);
    }

    protected void refreshSchemasWithTabletId(TableId tableId, String tabletId) {
        tabletIdToTableSchema.remove(tabletId);

        refreshSchemaWithTablet(tableId, tabletId);
    }

    protected void refreshSchemaWithTablet(TableId id, String tabletId) {
        // if (LOG.isDebugEnabled()) {
        //     LOG.debug("refreshing DB schema for table '{}'", id);
        // }
        Table table = null; //tableFor(id);

        buildAndRegisterSchemaForTablet(id, tabletId);
    }

//    protected void refreshSchema(TableId id) {
////        if (LOG.isDebugEnabled()) {
////            LOG.debug("refreshing DB schema for table '{}'", id);
////        }
//        Table table = tableFor(id);
//
//        buildAndRegisterSchema(table);
//    }

    protected void buildAndRegisterSchemaForTablet(TableId id, String tabletId) {
        LOGGER.info("Refreshing DB schema for table {} and tablet {}", id, tabletId);

        Table table = tabletIdToTable.get(tabletId);
        TableSchemaBuilder schemaBuilder = getTableSchemaBuilder(config, valueConverter);
        TableSchema schema = schemaBuilder.create(getSchemaPrefix(config.getLogicalName()), getEnvelopeSchemaName(table), table, config.getColumnFilter(), ColumnMappers.create(config), config.getKeyMapper());

        if (new Filters(config).tableFilter().isIncluded(table.id())) {
            tabletIdToTableSchema.put(tabletId, schema);
        }

    }

//    @Override
//    public TableSchema schemaFor(TableId id) {
//        return super.schemaFor(id);
//    }

    public TableSchema schemaForTablet(String tabletId) {
        return tabletIdToTableSchema.get(tabletId);
    }

    private String getEnvelopeSchemaName(Table table) {
        return Envelope.schemaName(topicSelector.topicNameFor(table.id()));
    }

    private static String getSchemaPrefix(String serverName) {
        if (serverName == null) {
            return "";
        }
        else {
            serverName = serverName.trim();
            return serverName.endsWith(".") || serverName.isEmpty() ? serverName : serverName + ".";
        }
    }

    protected void refreshSchemas() {
        clearSchemas();
        // removeSchema(id);
        // Create TableSchema instances for any existing table ...
        tableIds().forEach(this::refreshSchema);
    }

    private void refreshToastableColumnsMap(YugabyteDBConnection connection, TableId tableId) {
        // This method populates the list of 'toastable' columns for `tableId`.
        // A toastable column is one that has storage strategy 'x' (inline-compressible + secondary storage enabled),
        // 'e' (secondary storage enabled), or 'm' (inline-compressible).
        //
        // Note that, rather confusingly, the 'm' storage strategy does in fact permit secondary storage, but only as a
        // last resort.
        //
        // Also, we cannot account for the possibility that future versions of PostgreSQL introduce new storage strategies
        // that include secondary storage. We should move to native decoding in PG 10 and get rid of this hacky code
        // before that possibility is realized.

        // Collect the non-system (attnum > 0), present (not attisdropped) column names that are toastable.
        //
        // NOTE (Ian Axelrod):
        // I Would prefer to use data provided by PgDatabaseMetaData, but the PG JDBC driver does not expose storage type
        // information. Thus, we need to make a separate query. If we are refreshing schemas rarely, this is not a big
        // deal.
        List<String> toastableColumns = new ArrayList<>();
        String relName = tableId.table();
        String schema = tableId.schema() != null && tableId.schema().length() > 0 ? tableId.schema() : "public";
        String statement = "select att.attname" +
                " from pg_attribute att " +
                " join pg_class tbl on tbl.oid = att.attrelid" +
                " join pg_namespace ns on tbl.relnamespace = ns.oid" +
                " where tbl.relname = ?" +
                " and ns.nspname = ?" +
                " and att.attnum > 0" +
                " and att.attstorage in ('x', 'e', 'm')" +
                " and not att.attisdropped;";

        try {
            connection.prepareQuery(statement, stmt -> {
                stmt.setString(1, relName);
                stmt.setString(2, schema);
            }, rs -> {
                while (rs.next()) {
                    toastableColumns.add(rs.getString(1));
                }
            });
            if (!connection.connection().getAutoCommit()) {
                connection.connection().commit();
            }
        }
        catch (SQLException e) {
            throw new ConnectException("Unable to refresh toastable columns mapping", e);
        }

        tableIdToToastableColumns.put(tableId, Collections.unmodifiableList(toastableColumns));
    }

    protected static TableId parse(String table) {
        TableId tableId = TableId.parse(table, false);
        if (tableId == null) {
            return null;
        }
        return tableId.schema() == null ? new TableId(tableId.catalog(), PUBLIC_SCHEMA_NAME, tableId.table()) : tableId;
    }

    protected static TableId parseWithSchema(String table, String pgSchemaName) {
        TableId tableId = TableId.parse(table, false);
        if (tableId == null) {
            return null;
        }

        return tableId.schema() == null ? new TableId(tableId.catalog(), pgSchemaName, tableId.table()) : tableId;
    }

    public YugabyteDBTypeRegistry getTypeRegistry() {
        return yugabyteDBTypeRegistry;
    }

    public List<String> getToastableColumnsForTableId(TableId tableId) {
        return tableIdToToastableColumns.getOrDefault(tableId, Collections.emptyList());
    }

    /**
     * Applies schema changes for the specified table.
     *
     * @param relationId the postgres relation unique identifier for the table
     * @param table      externally constructed table, typically from the decoder; must not be null
     */
    public void applySchemaChangesForTable(int relationId, Table table) {
        assert table != null;

        if (isFilteredOut(table.id())) {
            LOGGER.trace("Skipping schema refresh for table '{}' with relation '{}' as table is filtered", table.id(), relationId);
            return;
        }

        relationIdToTableId.put(relationId, table.id());
        refresh(table);
    }

    /**
     * Resolve a {@link Table} based on a supplied table relation unique identifier.
     * <p>
     * This implementation relies on a prior call to {@link #applySchemaChangesForTable(int, Table)} to have
     * applied schema changes from a replication stream with the {@code relationId} for the relationship to exist
     * and be capable of lookup.
     *
     * @param relationId the unique table relation identifier
     * @return the resolved table or null
     */
    public Table tableFor(int relationId) {
        TableId tableId = relationIdToTableId.get(relationId);
        if (tableId == null) {
            LOGGER.debug("Relation '{}' is unknown, cannot resolve to table", relationId);
            return null;
        }
        LOGGER.debug("Relation '{}' resolved to table '{}'", relationId, tableId);
        return tableFor(tableId);
    }

    public Table tableForTablet(String tabletId) {
        Table table = tabletIdToTable.get(tabletId);
        return new Filters(config).tableFilter().isIncluded(table.id()) ? table : null;
    }

    @Override
    public boolean tableInformationComplete() {
        // PostgreSQL does not support HistorizedDatabaseSchema - so no tables are recovered
        return false;
    }

    public void dumpTableId() {
        LOGGER.info("The relationid to tableid in YugabyteDBSchema is " + this.relationIdToTableId);
    }
}
