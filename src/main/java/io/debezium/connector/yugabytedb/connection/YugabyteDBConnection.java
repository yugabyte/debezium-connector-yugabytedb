/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb.connection;

import java.nio.charset.Charset;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.TimestampUtils;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.util.PGmoney;
import org.postgresql.util.PSQLState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.PgOid;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig;
import io.debezium.connector.yugabytedb.YugabyteDBSchema;
import io.debezium.connector.yugabytedb.YugabyteDBType;
import io.debezium.connector.yugabytedb.YugabyteDBTypeRegistry;
import io.debezium.connector.yugabytedb.YugabyteDBValueConverter;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * {@link JdbcConnection} connection extension used for connecting to YugabyteBD instances.
 *
 * @author Suranjan Kumar (skumar@yugabyte.com)
 */
public class YugabyteDBConnection extends JdbcConnection {
    public static final String CONNECTION_STREAMING = "Debezium Streaming";
    public static final String CONNECTION_SLOT_INFO = "Debezium Slot Info";
    public static final String CONNECTION_DROP_SLOT = "Debezium Drop Slot";
    public static final String CONNECTION_VALIDATE_CONNECTION = "Debezium Validate Connection";
    public static final String CONNECTION_HEARTBEAT = "Debezium Heartbeat";
    public static final String CONNECTION_GENERAL = "Debezium General";

    private static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBConnection.class);

    public static final String MULTI_HOST_URL_PATTERN = "jdbc:postgresql://${" + JdbcConfiguration.HOSTNAME + "}/${" + JdbcConfiguration.DATABASE + "}";
    public static final String SINGLE_HOST_URL_PATTERN = "jdbc:postgresql://${" + JdbcConfiguration.HOSTNAME + "}:${"
            + JdbcConfiguration.PORT + "}/${" + JdbcConfiguration.DATABASE + "}";

    protected static ConnectionFactory FACTORY; 

    private final YugabyteDBTypeRegistry yugabyteDBTypeRegistry;
    private final YugabyteDBDefaultValueConverter defaultValueConverter;

    private final JdbcConfiguration config;

    /**
     * Creates a Postgres connection using the supplied configuration.
     * If necessary this connection is able to resolve data type mappings.
     * Such a connection requires a {@link YugabyteDBValueConverter}, and will provide its own {@link YugabyteDBTypeRegistry}.
     * Usually only one such connection per connector is needed.
     *
     * @param config {@link Configuration} instance, may not be null.
     * @param valueConverterBuilder supplies a configured {@link YugabyteDBValueConverter} for a given {@link YugabyteDBTypeRegistry}
     */
    public YugabyteDBConnection(JdbcConfiguration config, YugabyteDBValueConverterBuilder valueConverterBuilder, String connectionUsage, ConnectionFactory factory) {
        super(addDefaultSettings(config, connectionUsage), factory, YugabyteDBConnection::validateServerVersion, "\"", "\"");
        YugabyteDBConnection.FACTORY = factory;
        this.config = config;
        if (Objects.isNull(valueConverterBuilder)) {
            this.yugabyteDBTypeRegistry = null;
            this.defaultValueConverter = null;
        }
        else {
            this.yugabyteDBTypeRegistry = new YugabyteDBTypeRegistry(this);

            final YugabyteDBValueConverter valueConverter = valueConverterBuilder.build(this.yugabyteDBTypeRegistry);
            this.defaultValueConverter = new YugabyteDBDefaultValueConverter(valueConverter, this.getTimestampUtils());
        }


    }
    public YugabyteDBConnection(JdbcConfiguration config, YugabyteDBValueConverterBuilder valueConverterBuilder, String connectionUsage) {
        this(config, valueConverterBuilder, connectionUsage, config.getHostname().contains(":")
                        ? JdbcConnection.patternBasedFactory(MULTI_HOST_URL_PATTERN,
                            org.postgresql.Driver.class.getName(),
                            YugabyteDBConnection.class.getClassLoader(),
                            JdbcConfiguration.PORT.withDefault(YugabyteDBConnectorConfig.PORT.defaultValueAsString()))
                        : JdbcConnection.patternBasedFactory(SINGLE_HOST_URL_PATTERN,
                                org.postgresql.Driver.class.getName(),
                                YugabyteDBConnection.class.getClassLoader(), JdbcConfiguration.PORT
                                        .withDefault(YugabyteDBConnectorConfig.PORT.defaultValueAsString())));
    }

    /**
     * Create a Postgres connection using the supplied configuration and {@link YugabyteDBTypeRegistry}
     * @param config {@link Configuration} instance, may not be null.
     * @param yugabyteDBTypeRegistry an existing/already-primed {@link YugabyteDBTypeRegistry} instance
     */
    public YugabyteDBConnection(YugabyteDBConnectorConfig config,
            YugabyteDBTypeRegistry yugabyteDBTypeRegistry, String connectionUsage, ConnectionFactory factory) {
        super(addDefaultSettings(config.getJdbcConfig(), connectionUsage), factory,
                YugabyteDBConnection::validateServerVersion, "\"", "\"");
        YugabyteDBConnection.FACTORY = factory;
        this.config = config.getJdbcConfig();
        if (Objects.isNull(yugabyteDBTypeRegistry)) {
            this.yugabyteDBTypeRegistry = null;
            this.defaultValueConverter = null;
        }
        else {
            this.yugabyteDBTypeRegistry = yugabyteDBTypeRegistry;
            final YugabyteDBValueConverter valueConverter = YugabyteDBValueConverter.of(config, this.getDatabaseCharset(),
                    yugabyteDBTypeRegistry);
            this.defaultValueConverter = new YugabyteDBDefaultValueConverter(valueConverter, this.getTimestampUtils());
        }
    }

    public YugabyteDBConnection(YugabyteDBConnectorConfig config,
                                YugabyteDBTypeRegistry yugabyteDBTypeRegistry, String connectionUsage) {
        this(config,yugabyteDBTypeRegistry, connectionUsage, config.getConnectionFactory());
    }

    /**
     * Creates a Postgres connection using the supplied configuration.
     * The connector is the regular one without datatype resolution capabilities.
     *
     * @param config {@link Configuration} instance, may not be null.
     */
    public YugabyteDBConnection(JdbcConfiguration config, String connectionUsage) {
        this(config, null, connectionUsage);
    }

    static JdbcConfiguration addDefaultSettings(JdbcConfiguration configuration, String connectionUsage) {
        // we require Postgres 9.4 as the minimum server version since that's where logical replication was first introduced
        return JdbcConfiguration.adapt(configuration.edit()
                .with("assumeMinServerVersion", "9.4")
                .with("ApplicationName", connectionUsage)
                .build());
    }

    /**
     * Returns a JDBC connection string for the current configuration.
     *
     * @return a {@code String} where the variables in {@code urlPattern} are replaced with values from the configuration
     */
    public String connectionString() {
        String hostName = config.getHostname();
        if (hostName.contains(":")) {
            return connectionString(MULTI_HOST_URL_PATTERN);
        } else {
            return connectionString(SINGLE_HOST_URL_PATTERN);
        }
    }

    /**
     * Prints out information about the REPLICA IDENTITY status of a table.
     * This in turn determines how much information is available for UPDATE and DELETE operations for logical replication.
     *
     * @param tableId the identifier of the table
     * @return the replica identity information; never null
     * @throws SQLException if there is a problem obtaining the replica identity information for the given table
     */
    public ServerInfo.ReplicaIdentity readReplicaIdentityInfo(TableId tableId) throws SQLException {
        String statement = "SELECT relreplident FROM pg_catalog.pg_class c " +
                "LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace=n.oid " +
                "WHERE n.nspname=? and c.relname=?";
        String schema = tableId.schema() != null && tableId.schema().length() > 0 ? tableId.schema() : "public";
        StringBuilder replIdentity = new StringBuilder();
        prepareQuery(statement, stmt -> {
            stmt.setString(1, schema);
            stmt.setString(2, tableId.table());
        }, rs -> {
            if (rs.next()) {
                replIdentity.append(rs.getString(1));
            }
            else {
                LOGGER.warn("Cannot determine REPLICA IDENTITY information for table '{}'", tableId);
            }
        });
        return ServerInfo.ReplicaIdentity.parseFromDB(replIdentity.toString());
    }

    protected ServerInfo.ReplicationSlot queryForSlot(String slotName, String database, String pluginName,
                                                      ResultSetMapper<ServerInfo.ReplicationSlot> map)
            throws SQLException {
        return prepareQueryAndMap("select * from pg_replication_slots where slot_name = ? and database = ? and plugin = ?", statement -> {
            statement.setString(1, slotName);
            statement.setString(2, database);
            statement.setString(3, pluginName);
        }, map);
    }

    /**
     * Obtains the LSN to resume streaming from. On PG 9.5 there is no confirmed_flushed_lsn yet, so restart_lsn will be
     * read instead. This may result in more records to be re-read after a restart.
     */
    private Lsn parseConfirmedFlushLsn(String slotName, String pluginName, String database, ResultSet rs) {
        Lsn confirmedFlushedLsn = null;

        try {
            confirmedFlushedLsn = tryParseLsn(slotName, pluginName, database, rs, "confirmed_flush_lsn");
        }
        catch (SQLException e) {
            LOGGER.info("unable to find confirmed_flushed_lsn, falling back to restart_lsn");
            try {
                confirmedFlushedLsn = tryParseLsn(slotName, pluginName, database, rs, "restart_lsn");
            }
            catch (SQLException e2) {
                throw new ConnectException("Neither confirmed_flush_lsn nor restart_lsn could be found");
            }
        }

        return confirmedFlushedLsn;
    }

    private Lsn parseRestartLsn(String slotName, String pluginName, String database, ResultSet rs) {
        Lsn restartLsn = null;
        try {
            restartLsn = tryParseLsn(slotName, pluginName, database, rs, "restart_lsn");
        }
        catch (SQLException e) {
            throw new ConnectException("restart_lsn could be found");
        }

        return restartLsn;
    }

    private Lsn tryParseLsn(String slotName, String pluginName, String database, ResultSet rs, String column) throws ConnectException, SQLException {
        Lsn lsn = null;

        String lsnStr = rs.getString(column);
        if (lsnStr == null) {
            return null;
        }
        try {
            lsn = Lsn.valueOf(lsnStr);
        }
        catch (Exception e) {
            throw new ConnectException("Value " + column + " in the pg_replication_slots table for slot = '"
                    + slotName + "', plugin = '"
                    + pluginName + "', database = '"
                    + database + "' is not valid. This is an abnormal situation and the database status should be checked.");
        }
        if (!lsn.isValid()) {
            throw new ConnectException("Invalid LSN returned from database");
        }
        return lsn;
    }

    /**
     * Drops a replication slot that was created on the DB
     *
     * @param slotName the name of the replication slot, may not be null
     * @return {@code true} if the slot was dropped, {@code false} otherwise
     */
    public boolean dropReplicationSlot(String slotName) {
        final int ATTEMPTS = 3;
        for (int i = 0; i < ATTEMPTS; i++) {
            try {
                execute("select pg_drop_replication_slot('" + slotName + "')");
                return true;
            }
            catch (SQLException e) {
                // slot is active
                if (PSQLState.OBJECT_IN_USE.getState().equals(e.getSQLState())) {
                    if (i < ATTEMPTS - 1) {
                        LOGGER.debug("Cannot drop replication slot '{}' because it's still in use", slotName);
                    }
                    else {
                        LOGGER.warn("Cannot drop replication slot '{}' because it's still in use", slotName);
                        return false;
                    }
                }
                else if (PSQLState.UNDEFINED_OBJECT.getState().equals(e.getSQLState())) {
                    LOGGER.debug("Replication slot {} has already been dropped", slotName);
                    return false;
                }
                else {
                    LOGGER.error("Unexpected error while attempting to drop replication slot", e);
                    return false;
                }
            }
            try {
                Metronome.parker(Duration.ofSeconds(1), Clock.system()).pause();
            }
            catch (InterruptedException e) {
            }
        }
        return false;
    }

    /**
     * Drops the debezium publication that was created.
     *
     * @param publicationName the publication name, may not be null
     * @return {@code true} if the publication was dropped, {@code false} otherwise
     */
    public boolean dropPublication(String publicationName) {
        try {
            LOGGER.debug("Dropping publication '{}'", publicationName);
            execute("DROP PUBLICATION " + publicationName);
            return true;
        }
        catch (SQLException e) {
            if (PSQLState.UNDEFINED_OBJECT.getState().equals(e.getSQLState())) {
                LOGGER.debug("Publication {} has already been dropped", publicationName);
            }
            else {
                LOGGER.error("Unexpected error while attempting to drop publication", e);
            }
            return false;
        }
    }

    @Override
    public synchronized void close() {
        try {
            super.close();
        }
        catch (SQLException e) {
            LOGGER.error("Unexpected error while closing Postgres connection", e);
        }
    }

    /**
     * Returns the PG id of the current active transaction
     *
     * @return a PG transaction identifier, or null if no tx is active
     * @throws SQLException if anything fails.
     */
    public Long currentTransactionId() throws SQLException {
        AtomicLong txId = new AtomicLong(0);
        query("select * from txid_current()", rs -> {
            if (rs.next()) {
                txId.compareAndSet(0, rs.getLong(1));
            }
        });
        long value = txId.get();
        return value > 0 ? value : null;
    }

    /**
     * Returns the current position in the server tx log.
     *
     * @return a long value, never negative
     * @throws SQLException if anything unexpected fails.
     */
    public long currentXLogLocation() throws SQLException {
        AtomicLong result = new AtomicLong(0);
        int majorVersion = connection().getMetaData().getDatabaseMajorVersion();
        query(majorVersion >= 10 ? "select * from pg_current_wal_lsn()" : "select * from pg_current_xlog_location()", rs -> {
            if (!rs.next()) {
                throw new IllegalStateException("there should always be a valid xlog position");
            }
            result.compareAndSet(0, LogSequenceNumber.valueOf(rs.getString(1)).asLong());
        });
        return result.get();
    }

    /**
     * Returns information about the PG server to which this instance is connected.
     *
     * @return a {@link ServerInfo} instance, never {@code null}
     * @throws SQLException if anything fails
     */
    public ServerInfo serverInfo() throws SQLException {
        ServerInfo serverInfo = new ServerInfo();
        query("SELECT version(), current_user, current_database()", rs -> {
            if (rs.next()) {
                serverInfo.withServer(rs.getString(1)).withUsername(rs.getString(2)).withDatabase(rs.getString(3));
            }
        });
        String username = serverInfo.username();
        if (username != null) {
            query("SELECT oid, rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, rolreplication FROM pg_roles " +
                    "WHERE pg_has_role('" + username + "', oid, 'member')",
                    rs -> {
                        while (rs.next()) {
                            String roleInfo = "superuser: " + rs.getBoolean(3) + ", replication: " + rs.getBoolean(8) +
                                    ", inherit: " + rs.getBoolean(4) + ", create role: " + rs.getBoolean(5) +
                                    ", create db: " + rs.getBoolean(6) + ", can log in: " + rs.getBoolean(7);
                            String roleName = rs.getString(2);
                            serverInfo.addRole(roleName, roleInfo);
                        }
                    });
        }
        return serverInfo;
    }

    public Charset getDatabaseCharset() {
        // get the name from from co-ordinator
        return Charset.forName("UTF-8");
        // try {
        // return Charset.forName(((BaseConnection) connection()).getEncoding().name());
        // }
        // catch (SQLException e) {
        // throw new DebeziumException("Couldn't obtain encoding for database " + database(), e);
        // }
    }

    public TimestampUtils getTimestampUtils() {
        try {
            return ((PgConnection) this.connection()).getTimestampUtils();
        }
        catch (SQLException e) {
            throw new DebeziumException("Couldn't get timestamp utils from underlying connection", e);
        }
    }

    protected static void defaultSettings(Configuration.Builder builder) {
        // we require Postgres 9.4 as the minimum server version since that's where logical replication was first introduced
        builder.with("assumeMinServerVersion", "9.4");
    }

    private static void validateServerVersion(Statement statement) throws SQLException {
        DatabaseMetaData metaData = statement.getConnection().getMetaData();
        int majorVersion = metaData.getDatabaseMajorVersion();
        int minorVersion = metaData.getDatabaseMinorVersion();
        if (majorVersion < 9 || (majorVersion == 9 && minorVersion < 4)) {
            throw new SQLException("Cannot connect to a version of Postgres lower than 9.4");
        }
    }

    @Override
    public String quotedColumnIdString(String columnName) {
        if (columnName.contains("\"")) {
            columnName = columnName.replaceAll("\"", "\"\"");
        }

        return super.quotedColumnIdString(columnName);
    }

    @Override
    protected int resolveNativeType(String typeName) {
        return getTypeRegistry().get(typeName).getRootType().getOid();
    }

    @Override
    protected int resolveJdbcType(int metadataJdbcType, int nativeType) {
        // Special care needs to be taken for columns that use user-defined domain type data types
        // where resolution of the column's JDBC type needs to be that of the root type instead of
        // the actual column to properly influence schema building and value conversion.
        return getTypeRegistry().get(nativeType).getRootType().getJdbcId();
    }

    @Override
    protected Optional<ColumnEditor> readTableColumn(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter) throws SQLException {
        return doReadTableColumn(columnMetadata, tableId, columnFilter);
    }

    public Optional<Column> readColumnForDecoder(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnNameFilter)
            throws SQLException {
        return doReadTableColumn(columnMetadata, tableId, columnNameFilter).map(ColumnEditor::create);
    }

    private Optional<ColumnEditor> doReadTableColumn(ResultSet columnMetadata, TableId tableId, Tables.ColumnNameFilter columnFilter)
            throws SQLException {
        final String columnName = columnMetadata.getString(4);
        if (columnFilter == null || columnFilter.matches(tableId.catalog(), tableId.schema(), tableId.table(), columnName)) {
            final ColumnEditor column = Column.editor().name(columnName);
            column.type(columnMetadata.getString(6));

            // first source the length/scale from the column metadata provided by the driver
            // this may be overridden below if the column type is a user-defined domain type
            column.length(columnMetadata.getInt(7));
            if (columnMetadata.getObject(9) != null) {
                column.scale(columnMetadata.getInt(9));
            }

            column.optional(isNullable(columnMetadata.getInt(11)));
            column.position(columnMetadata.getInt(17));
            column.autoIncremented("YES".equalsIgnoreCase(columnMetadata.getString(23)));

            String autogenerated = null;
            try {
                autogenerated = columnMetadata.getString(24);
            }
            catch (SQLException e) {
                // ignore, some drivers don't have this index - e.g. Postgres
            }
            column.generated("YES".equalsIgnoreCase(autogenerated));

            // Lookup the column type from the TypeRegistry
            // For all types, we need to set the Native and Jdbc types by using the root-type
            final YugabyteDBType nativeType = getTypeRegistry().get(column.typeName());
            column.nativeType(nativeType.getRootType().getOid());
            column.jdbcType(nativeType.getRootType().getJdbcId());

            // For domain types, the postgres driver is unable to traverse a nested unbounded
            // hierarchy of types and report the right length/scale of a given type. We use
            // the TypeRegistry to accomplish this since it is capable of traversing the type
            // hierarchy upward to resolve length/scale regardless of hierarchy depth.
            if (YugabyteDBTypeRegistry.DOMAIN_TYPE == nativeType.getJdbcId()) {
                column.length(nativeType.getDefaultLength());
                column.scale(nativeType.getDefaultScale());
            }

            final String defaultValueExpression = columnMetadata.getString(13);
            if (defaultValueExpression != null && getDefaultValueConverter().supportConversion(column.typeName())) {
                column.defaultValueExpression(defaultValueExpression);
            }

            return Optional.of(column);
        }

        return Optional.empty();
    }

    public YugabyteDBDefaultValueConverter getDefaultValueConverter() {
        Objects.requireNonNull(defaultValueConverter, "Connection does not provide default value converter");
        return defaultValueConverter;
    }

    public YugabyteDBTypeRegistry getTypeRegistry() {
        Objects.requireNonNull(yugabyteDBTypeRegistry, "Connection does not provide type registry");
        return yugabyteDBTypeRegistry;
    }

    // This method is not being used anywhere.
    public <T extends DatabaseSchema<TableId>> Object getColumnValue(ResultSet rs, int columnIndex,
                                                                     Column column,
                                                                     Table table, T schema)
            throws SQLException {
        try {
            final ResultSetMetaData metaData = rs.getMetaData();
            final String columnTypeName = metaData.getColumnTypeName(columnIndex);
            final YugabyteDBType type = ((YugabyteDBSchema) schema).getTypeRegistry().get(columnTypeName);

            LOGGER.trace("Type of incoming data is: {}", type.getOid());
            LOGGER.trace("ColumnTypeName is: {}", columnTypeName);
            LOGGER.trace("Type is: {}", type);

            if (type.isArrayType()) {
                return rs.getArray(columnIndex);
            }

            switch (type.getOid()) {
                case PgOid.MONEY:
                    // TODO author=Horia Chiorean date=14/11/2016
                    // description=workaround for https://github.com/pgjdbc/pgjdbc/issues/100
                    final String sMoney = rs.getString(columnIndex);
                    if (sMoney == null) {
                        return sMoney;
                    }
                    if (sMoney.startsWith("-")) {
                        // PGmoney expects negative values to be provided in the format of "($XXXXX.YY)"
                        final String negativeMoney = "(" + sMoney.substring(1) + ")";
                        return new PGmoney(negativeMoney).val;
                    }
                    return new PGmoney(sMoney).val;
                case PgOid.BIT:
                    return rs.getString(columnIndex);
                case PgOid.NUMERIC:
                    final String s = rs.getString(columnIndex);
                    if (s == null) {
                        return s;
                    }

                    Optional<SpecialValueDecimal> value = YugabyteDBValueConverter.toSpecialValue(s);
                    return value.isPresent() ? value.get() : new SpecialValueDecimal(rs.getBigDecimal(columnIndex));
                case PgOid.TIME:
                    // To handle time 24:00:00 supported by TIME columns, read the column as a string.
                case PgOid.TIMETZ:
                    // In order to guarantee that we resolve TIMETZ columns with proper microsecond precision,
                    // read the column as a string instead and then re-parse inside the converter.
                    return rs.getString(columnIndex);
                default:
                    Object x = rs.getObject(columnIndex);
                    if (x != null) {
                        LOGGER.trace("rs getobject returns class: {}; rs getObject value is: {}", x.getClass(), x);
                    }
                    return x;
            }
        }
        catch (SQLException e) {
            // not a known type
            LOGGER.error("Received unknown type for column: {}", column.name());
            throw e;
        }
    }

    @FunctionalInterface
    public interface YugabyteDBValueConverterBuilder {
        YugabyteDBValueConverter build(YugabyteDBTypeRegistry registry);
    }
}
