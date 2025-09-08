/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.stream.Collectors;

import io.debezium.data.Envelope;
import io.debezium.relational.mapping.ColumnMappers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.Common.QLTypePB;
import org.yb.cdc.CdcService;
import org.yb.cdc.CdcService.CDCSDKColumnInfoPB;
import org.yb.cdc.CdcService.CDCSDKSchemaPB;
import org.yb.QLType;


import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.relational.*;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.TopicSelector;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Collect;
import io.debezium.schema.SchemaNameAdjuster;

/**
 * Component that records the schema information for the {@link YugabyteDBgRPCConnector}. The schema information contains
 * the {@link Tables table definitions} and the Kafka Connect {@link #schemaFor(TableId) Schema}s for each table, where the
 * {@link Schema} excludes any columns that have been {@link YugabyteDBConnectorConfig#COLUMN_EXCLUDE_LIST specified} in the
 * configuration.
 *
 * @author Suranjan Kumar (skumar@yugabyte.com), Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
@NotThreadSafe
public class YugabyteDBSchema extends RelationalDatabaseSchema {

    protected final static String PUBLIC_SCHEMA_NAME = "public";
    private final static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBSchema.class);

    private final YugabyteDBTypeRegistry yugabyteDBTypeRegistry;

    private final Map<TableId, List<String>> tableIdToToastableColumns;
    private final Map<Integer, TableId> relationIdToTableId;

    private CdcService.CDCSDKSchemaPB cdcsdkSchemaPB;

    private Map<String, CdcService.CDCSDKSchemaPB> tabletIdToCdcsdkSchemaPB = new HashMap<>();
    private Map<String, Table> tabletIdToTable = new HashMap<>();
    private Map<String, TableSchema> tabletIdToTableSchema = new HashMap<>();

    private YugabyteDBConnectorConfig config;
    private YugabyteDBValueConverter valueConverter;
    private YugabyteDBCQLValueConverter cqlValueConverter;
    private TopicNamingStrategy<TableId> topicNamingStrategy;
    private TableFilter tableFilter;

    /**
     * Create a schema component given the supplied {@link YugabyteDBConnectorConfig Postgres connector configuration}.
     *
     * @param config the connector configuration, which is presumed to be valid
     */
    // TODO Vaibhav: need to implement default value converter and use it here.
    protected YugabyteDBSchema(YugabyteDBConnectorConfig config, YugabyteDBValueConverter defaultValueConverter,
                               TopicNamingStrategy<TableId> topicNamingStrategy, YugabyteDBValueConverter valueConverter,
                               YugabyteDBTypeRegistry yugabyteDBTypeRegistry) {
        super(config, topicNamingStrategy, new Filters(config).tableFilter(),
                config.getColumnFilter(), getTableSchemaBuilder(config, valueConverter), false,
                config.getKeyMapper());

        this.yugabyteDBTypeRegistry = yugabyteDBTypeRegistry;
        this.tableIdToToastableColumns = new HashMap<>();
        this.relationIdToTableId = new HashMap<>();

        this.config = config;
        this.valueConverter = valueConverter;
        this.cqlValueConverter = null;
        this.topicNamingStrategy = topicNamingStrategy;
        this.tableFilter = new Filters(config).tableFilter();
    }

    protected YugabyteDBSchema(YugabyteDBConnectorConfig config,TopicNamingStrategy<TableId> topicNamingStrategy, YugabyteDBCQLValueConverter cqlValueConverter) {
        super(config, topicNamingStrategy, new Filters(config).tableFilter(),
                config.getColumnFilter(), getTableSchemaBuilder(config, cqlValueConverter), false,
                config.getKeyMapper());
        this.yugabyteDBTypeRegistry = null;
        this.tableIdToToastableColumns = new HashMap<>();
        this.relationIdToTableId = new HashMap<>();

        this.config = config;
        this.valueConverter = null;
        this.cqlValueConverter = cqlValueConverter;
        this.topicNamingStrategy = topicNamingStrategy;
        this.tableFilter = config.cqlTableFilter();
    }

    private static YBTableSchemaBuilder getTableSchemaBuilder(YugabyteDBConnectorConfig config,
            ValueConverterProvider valueConverter) {
        return new YBTableSchemaBuilder(valueConverter, SchemaNameAdjuster.create(),
                config.customConverterRegistry(), config.getSourceInfoStructMaker().schema(),
                config.getFieldNamer(), false);
    }

    /**
     * Refresh the cached schema. This API was used when the schema was cached at the table level
     * @param tableId the Debezium style TableId
     * @param schemaPB the schema object received from a DDL change record
     * @param schemaName name of the schema of the table
     * @return schema object
     */
    @Deprecated
    protected YugabyteDBSchema refreshWithSchema(TableId tableId,
                                                 CdcService.CDCSDKSchemaPB schemaPB,
                                                 String schemaName) {
        if (cdcsdkSchemaPB == null) {
            cdcsdkSchemaPB = schemaPB;
        }

        readSchema(tables(), null, schemaName,
                getTableFilter(), null, true, schemaPB, tableId);
        refreshSchemas(tableId);
        return this;
    }

    /**
     * Refresh the cached schema for the given tablet ID
     *
     * @param tableId    the Debezium style TableId
     * @param schemaPB   the schema object received from a DDL change record
     * @param schemaName name of the schema of the table
     * @param tabletId   the tablet UUID to refresh the schema for
     */
    protected void refreshSchemaWithTabletId(TableId tableId,
                                             CDCSDKSchemaPB schemaPB,
                                             String schemaName,
                                             String tabletId) {
        String lookupKey = getLookupKey(tableId, tabletId);
        if (!tabletIdToCdcsdkSchemaPB.containsKey(lookupKey) || cdcsdkSchemaPB == null) {
            tabletIdToCdcsdkSchemaPB.put(lookupKey, schemaPB);
        }

        if (config.isYSQLDbType()) {
            readSchemaWithTablet(tables(), null, schemaName,
                getTableFilter(), null, true, schemaPB, tableId, tabletId);
        } else {
            readSchemaWithTablet(tables(), tableId.catalog(), schemaName,
                    config.cqlTableFilter(), null, true, schemaPB, tableId, tabletId);
        }
        
        refreshSchemasWithTabletId(tableId, tabletId);
    }

    /**
     * Get the cached schemaPB object for the given tablet
     * @param tableId the {@link TableId} of the table
     * @param tabletId the tablet UUID
     * @return a cached schemaPB object
     */
    protected CdcService.CDCSDKSchemaPB getSchemaPBForTablet(TableId tableId, String tabletId) {
        return tabletIdToCdcsdkSchemaPB.get(getLookupKey(tableId, tabletId));
    }

    // schemaNamePattern is the name of the schema here
    public void readSchema(Tables tables, String databaseCatalog, String schemaNamePattern,
                           Tables.TableFilter tableFilter, Tables.ColumnNameFilter columnFilter,
                           boolean removeTablesNotFoundInJdbc,
                           CdcService.CDCSDKSchemaPB schemaPB,
                           TableId tableId) {

        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        // Find regular and materialized views as they cannot be snapshotted
        final Set<TableId> tableIds = new HashSet<>();
        if (tableFilter == null || tableFilter.isIncluded(tableId)) {
            tableIds.add(tableId);
        }

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
            tables.overwriteTable(tableEntry.getKey(), columns, pkColumnNames, defaultCharsetName, null);
        }
    }

    /**
     * Check whether we should refresh the cached schema in the connector. The idea is to not
     * refresh if the same schema already exists in the connector. This function will be called
     * whenever we receive a DDL record.
     * @param table the table object cached in the connector
     * @param schemaPB the {@link CDCSDKSchemaPB} schema object received in DDL message
     * @return true if a schema refresh is needed, false otherwise
     */
    public static boolean shouldRefreshSchema(Table table, CDCSDKSchemaPB schemaPB) {
        if (table == null) {
            // Pre-check to indicate that we need to refresh the schema if table is null indicating
            // that is has not been registered in cache.
            return true;
        }

        List<String> columnNamesInTable = new ArrayList<>();
        List<String> columnNamesInSchema = new ArrayList<>();

        for (Column column : table.columns()) {
            columnNamesInTable.add(column.name());
        }

        for (CDCSDKColumnInfoPB columnInfoPB : schemaPB.getColumnInfoList()) {
            columnNamesInSchema.add(columnInfoPB.getName());
        }

        columnNamesInTable = columnNamesInTable.stream().sorted().collect(Collectors.toList());
        columnNamesInSchema = columnNamesInSchema.stream().sorted().collect(Collectors.toList());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Columns in existing table: {}", columnNamesInTable);
            LOGGER.debug("Columns in schemaPB received: {}", columnNamesInSchema);
        }

        // We should refresh schema if either the column count doesn't match or the names
        // of the columns have changed
        return !((columnNamesInTable.size() == columnNamesInSchema.size())
            && (columnNamesInTable.equals(columnNamesInSchema)));
    }

    public void readSchemaWithTablet(Tables tables, String databaseCatalog, String schemaNamePattern,
                           Tables.TableFilter tableFilter, Tables.ColumnNameFilter columnFilter,
                           boolean removeTablesNotFoundInJdbc,
                           CdcService.CDCSDKSchemaPB schemaPB,
                           TableId tableId, String tabletId) {
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        // Find regular and materialized views as they cannot be snapshotted
        final Set<TableId> tableIds = new HashSet<>();

        if (tableFilter == null || tableFilter.isIncluded(tableId)) {
            tableIds.add(tableId);
        }

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
            Table updatedTable = Table.editor()
              .tableId(tableId)
              .addColumns(columns)
              .setPrimaryKeyNames(pkColumnNames)
              .setDefaultCharsetName(null /* default character set name */) // JDBC does not expose character sets
              .create();

            String lookupKey = getLookupKey(tableId, tabletId);
            LOGGER.debug("Updating table with lookup key {} and columns {}", lookupKey, updatedTable.columns());
            tabletIdToTable.put(lookupKey, updatedTable);
        }

        // Set dummy flag to tables object so as to make sure it is not accessed anywhere else.
        tables = null;
    }

    protected Table getTableForTablet(TableId tableId, String tabletId) {
        return tabletIdToTable.get(getLookupKey(tableId, tabletId));
    }

    protected List<String> readPrimaryKeyOrUniqueIndexNames(CdcService.CDCSDKSchemaPB schemaPB,
                                                            TableId id) {
        return readPrimaryKeyNames(schemaPB, id);
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
            TableId tableId = new TableId(databaseCatalog, schemaNameFromYb, tableName);

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
            column.optional(columnMetadata.getIsNullable());
            column.position(position);
            // TODO: Handle it later, Mark it as false for now
            column.autoIncremented(false);
            column.generated(false);

            if (config.isYSQLDbType()) {
                // TODO: Handle the non default length/scale later.
                column.length(getLength(oid));
                column.scale(getScale(oid));
                column.nativeType(resolveNativeType(oid));
                column.jdbcType(resolveJdbcType(column.nativeType()));
                return Optional.of(column);
            } else {
                column.length(0); // TODO: Set length and scale properly for Decimal and Varint
                column.scale(0);
                column.nativeType(resolveQLType(QLType.createFromQLTypePB(columnMetadata.getType()))); 
                return Optional.of(column);
            }
        }

        return Optional.empty();
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

    private int resolveQLType(QLType type)
    {
        switch (type.getMain()) {
            case INT8: return Types.INTEGER;
            case INT16: return Types.INTEGER;
            case INT32: return Types.INTEGER;
            case INT64: return Types.BIGINT;
            case STRING: return Types.VARCHAR;
            case BOOL: return Types.BOOLEAN;
            case FLOAT: return Types.FLOAT;
            case DOUBLE: return Types.DOUBLE;
            case DECIMAL: return Types.DECIMAL;
            case BINARY: return Types.BINARY;
            case DATE: return Types.DATE;
            case INET: return PgOid.INET;
            case TIME: return PgOid.TIME;
            case TIMESTAMP: return PgOid.TIMESTAMP;  
            case VARINT: return PgOid.VARINT;
            case TIMEUUID: 
            case UUID: return PgOid.UUID;
            default:
                LOGGER.error("Invalid column type for CQL column");
                return -1;

        }
    }

    private int resolveJdbcType(int nativeType) {
        // Special care needs to be taken for columns that use user-defined domain type data types
        // where resolution of the column's JDBC type needs to be that of the root type instead of
        // the actual column to properly influence schema building and value conversion.
        return getTypeRegistry().get(nativeType).getRootType().getJdbcId();
    }

    // TODO: Further refactoring can be done to remove the dependency of these functions on YugabyteDBConnection
    /**
     * Refresh the cached schema.
     * @param connection the {@link YugabyteDBConnection} object for JDBC connections. Although one
     *                   thing to note here would be that we do not use JDBC connections to retrieve
     *                   any data.
     * @param tableId the Debezium style TableId for the table
     * @param refreshToastableColumns whether to refresh toastable columns, not used in YugabyteDB
     * @param schemaPB the schemaPB object
     * @param tabletId the tablet UUId to refresh the schema for
     * @throws SQLException if JDBC connection fails
     */
    protected void refresh(YugabyteDBConnection connection, TableId tableId,
                           boolean refreshToastableColumns, CdcService.CDCSDKSchemaPB schemaPB,
                           String tabletId) throws SQLException {
        readSchemaWithTablet(null /* dummy object */, null, tableId.schema(), tableId::equals,
                             null, true, schemaPB, tableId, tabletId);
        
        if (refreshToastableColumns) {
            // and refresh toastable columns info
            refreshToastableColumnsMap(connection, tableId);
        }
    }

    protected void refresh(TableId tableId,
            boolean refreshToastableColumns, CdcService.CDCSDKSchemaPB schemaPB,
            String tabletId) {
        readSchemaWithTablet(null /* dummy object */, tableId.catalog(), tableId.schema(), tableId::equals,
                null, true, schemaPB, tableId, tabletId);
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
        String lookupKey = getLookupKey(tableId, tabletId);;
        tabletIdToTableSchema.remove(lookupKey);

        refreshSchemaWithTablet(tableId, tabletId);
    }

    protected void refreshSchemaWithTablet(TableId id, String tabletId) {
        LOGGER.debug("Building and registering schema for tablet {}", tabletId);
        buildAndRegisterSchemaForTablet(id, tabletId);
    }

    /**
     * Build the schema for the tablet and register it in the local cache.
     */
    protected void buildAndRegisterSchemaForTablet(TableId id, String tabletId) {
        String lookupKey = getLookupKey(id, tabletId);
        TableSchemaBuilder schemaBuilder;
        if (config.isYSQLDbType()) {
            schemaBuilder = getTableSchemaBuilder(config, valueConverter);
        } else {
            schemaBuilder = getTableSchemaBuilder(config, cqlValueConverter);
        }

        Table table = tabletIdToTable.get(lookupKey);

        TableSchema schema = schemaBuilder.create(this.topicNamingStrategy, table, config.getColumnFilter(), ColumnMappers.create(config), config.getKeyMapper());
      
        if (tableFilter.isIncluded(table.id())) {
            LOGGER.debug("Updating table schema with lookup key {}", lookupKey);
            tabletIdToTableSchema.put(lookupKey, schema);
        } else {
            LOGGER.warn("{} not included in the table filter list",table.id());
        }
    }

    @Override
    public TableSchema schemaFor(TableId id) {
        return super.schemaFor(id);
    }

    public TableSchema schemaForTablet(TableId tableId, String tabletId) {
        return tabletIdToTableSchema.get(getLookupKey(tableId, tabletId));
    }

    private String getEnvelopeSchemaName(Table table) {
        return Envelope.schemaName(topicNamingStrategy.dataChangeTopic(table.id()));
    }

    private static String getSchemaPrefix(String serverName) {
        if (serverName == null) {
            return "";
        } else {
            serverName = serverName.trim();
            return serverName.endsWith(".") || serverName.isEmpty() ? serverName : serverName + ".";
        }
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

    protected static TableId parseWithSchema(String table, String pgSchemaName) {
        TableId tableId = TableId.parse(table, false);
        if (tableId == null) {
            return null;
        }

        return tableId.schema() == null ? new TableId(tableId.catalog(), pgSchemaName, tableId.table()) : tableId;
    }

    protected static TableId parseWithKeyspace(String table, String keyspace) {
        TableId tableId = TableId.parse(table, true);
        if (tableId == null) {
            return null;
        }
        // Since there is no schema in CQL we will use a static String as schema name for topic creation
        return tableId.catalog() == null ? new TableId(keyspace, keyspace, tableId.table())
                                         : new TableId(tableId.catalog(),
                                                       tableId.catalog(), tableId.table());
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

    /**
     * Resolve a {@link Table} based on the supplied table ID and the tablet UUID.
     * @param tableId the Debezium TableId for the table
     * @param tabletId tablet UUID
     * @return the resolved table or null
     */
    public Table tableForTablet(TableId tableId, String tabletId) {
        String lookupKey = getLookupKey(tableId, tabletId);
        if (!tabletIdToTable.containsKey(lookupKey)) {
            return null;
        }

        Table table = tabletIdToTable.get(lookupKey);
        
        LOGGER.debug("Getting a table with lookup key {}", lookupKey);
        return tableFilter.isIncluded(table.id()) ? table : null;
    }

    @Override
    public boolean tableInformationComplete() {
        // YugabyteDB does not support HistorizedDatabaseSchema - so no tables are recovered
        return false;
    }

    public void dumpTableId() {
        LOGGER.info("The relationid to tableid in YugabyteDBSchema is " + this.relationIdToTableId);
    }

    /**
     * Get a lookup key by which the tablets are mapped to their respective tables as well as
     * schemas in the cached map.
     * @param tableId the Debezium style TableID
     * @param tabletId tablet UUID
     * @return lookup key in the form databaseName.schemaName.tableName.tabletId
     */
    public String getLookupKey(TableId tableId, String tabletId) {
        return config.databaseName() + "." + tableId.schema() + "." + tableId.table()
                + "." + tabletId;
    }

}
