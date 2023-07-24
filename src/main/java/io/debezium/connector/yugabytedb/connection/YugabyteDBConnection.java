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
    public final YugabyteDBConnectorConfig connectorConfig;
    public JdbcConfiguration jdbcConfig;

    private static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBConnection.class);

    private static final String URL_PATTERN = "jdbc:yugabytedb://%s/${" + JdbcConfiguration.DATABASE + "}";
    protected static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
            org.postgresql.Driver.class.getName(),
            YugabyteDBConnection.class.getClassLoader(), JdbcConfiguration.PORT.withDefault(YugabyteDBConnectorConfig.PORT.defaultValueAsString()));

    private final YugabyteDBTypeRegistry yugabyteDBTypeRegistry;
    private final YugabyteDBDefaultValueConverter defaultValueConverter;

    public YugabyteDBConnection(YugabyteDBConnectorConfig config, YugabyteDBValueConverterBuilder valueConverterBuilder, String connectionUsage) {
        super(addDefaultSettings(config.getJdbcConfig(), connectionUsage) , FACTORY, YugabyteDBConnection::validateServerVersion, null, "\"", "\"");

        if (Objects.isNull(valueConverterBuilder)) {
            this.yugabyteDBTypeRegistry = null;
            this.defaultValueConverter = null;
        }
        else {
            this.yugabyteDBTypeRegistry = new YugabyteDBTypeRegistry(this);

            final YugabyteDBValueConverter valueConverter = valueConverterBuilder.build(this.yugabyteDBTypeRegistry);
            this.defaultValueConverter = new YugabyteDBDefaultValueConverter(valueConverter, this.getTimestampUtils());
        }

        this.connectorConfig = config;
    }

    public YugabyteDBConnection(JdbcConfiguration jdbcConfig, String connectionUsage) {
        super(addDefaultSettings(jdbcConfig, connectionUsage), FACTORY, YugabyteDBConnection::validateServerVersion, null, "\"", "\"");

        // Initialize with null objects.
        this.yugabyteDBTypeRegistry = null;
        this.defaultValueConverter = null;

        this.connectorConfig = null;
    }

//    /**
//     * Create a Postgres connection using the supplied configuration and {@link YugabyteDBTypeRegistry}
//     * @param config {@link Configuration} instance, may not be null.
//     * @param yugabyteDBTypeRegistry an existing/already-primed {@link YugabyteDBTypeRegistry} instance
//     */
    public YugabyteDBConnection(YugabyteDBConnectorConfig config,
                                YugabyteDBTypeRegistry yugabyteDBTypeRegistry, String connectionUsage) {
        super(addDefaultSettings(config.getJdbcConfig(), connectionUsage), FACTORY, YugabyteDBConnection::validateServerVersion, null, "\"", "\"");
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

        this.connectorConfig = config;
    }

    /**
     * Creates a Postgres connection using the supplied configuration.
     * The connector is the regular one without datatype resolution capabilities.
     *
     * @param config {@link Configuration} instance, may not be null.
     */
    public YugabyteDBConnection(YugabyteDBConnectorConfig config, String connectionUsage) {
        this(config, (YugabyteDBValueConverterBuilder) null, connectionUsage);
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
        return connectionString(String.format(URL_PATTERN, getHostPortString()));
    }

    private String getHostPortString() {
        // This change is to accommodate tests.
        if (connectorConfig == null) {
            assert jdbcConfig != null;
            return jdbcConfig.getHostname() + ":" + jdbcConfig.getPortAsString();
        } else {
            return connectorConfig.getModifiedHostPortString();
        }
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

    @Override
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
            return super.getColumnValue(rs, columnIndex, column, table, schema);
        }
    }

    @FunctionalInterface
    public interface YugabyteDBValueConverterBuilder {
        YugabyteDBValueConverter build(YugabyteDBTypeRegistry registry);
    }
}
