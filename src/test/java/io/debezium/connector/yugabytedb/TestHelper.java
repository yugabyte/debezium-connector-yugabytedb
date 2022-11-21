/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.YugabyteYSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.yb.client.AsyncYBClient;
import org.yb.client.ListTablesResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;
import org.yb.master.MasterDdlOuterClass.ListTablesResponsePB.TableInfo;

import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig.SecureConnectionMode;
import io.debezium.connector.yugabytedb.connection.ReplicationConnection;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection.YugabyteDBValueConverterBuilder;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

/**
 * A utility for integration test cases to connect the YugabyteDB instance running in the Docker 
 * container created by this module's build.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public final class TestHelper {

    protected static final String TEST_SERVER = "test_server";
    protected static final String TEST_DATABASE = "postgres";
    protected static final String PK_FIELD = "pk";
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";
    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    // If this variable is changed, do not forget to change the name in postgres_create_tables.ddl
    private static final String SECONDARY_DATABASE = "secondary_database";

    // Set the localhost value as the defaults for now
    private static String CONTAINER_YSQL_HOST = "127.0.0.1";
    private static int CONTAINER_YSQL_PORT = 5433;
    private static String CONTAINER_MASTER_PORT = "7100";
    private static String MASTER_ADDRESS = "";
    private static String DEFAULT_DATABASE_NAME = "yugabyte";

    /**
     * Key for schema parameter used to store DECIMAL/NUMERIC columns' precision.
     */
    static final String PRECISION_PARAMETER_KEY = "connect.decimal.precision";

    /**
     * Key for schema parameter used to store a source column's type name.
     */
    static final String TYPE_NAME_PARAMETER_KEY = "__debezium.source.column.type";

    /**
     * Key for schema parameter used to store a source column's type length.
     */
    static final String TYPE_LENGTH_PARAMETER_KEY = "__debezium.source.column.length";

    /**
     * Key for schema parameter used to store a source column's type scale.
     */
    static final String TYPE_SCALE_PARAMETER_KEY = "__debezium.source.column.scale";

    private TestHelper() {
    }

    protected static ResultSet performQuery(JdbcDatabaseContainer<?> container, String sql) throws SQLException {
        DataSource ds = getDataSource(container);
        Statement statement = ds.getConnection().createStatement();
        statement.execute(sql);
        ResultSet resultSet = statement.getResultSet();

        resultSet.next();
        return resultSet;
    }

    protected static DataSource getDataSource(JdbcDatabaseContainer<?> container) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(container.getJdbcUrl());
        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setDriverClassName(container.getDriverClassName());
        return new HikariDataSource(hikariConfig);
    }

    /**
     * Obtain a replication connection instance for the given slot name.
     *
     * @param slotName the name of the logical decoding slot
     * @param dropOnClose true if the slot should be dropped upon close
     * @return the PostgresConnection instance; never null
     * @throws SQLException if there is a problem obtaining a replication connection
     */
    public static ReplicationConnection createForReplication(String slotName, boolean dropOnClose) throws SQLException {
        final YugabyteDBConnectorConfig.LogicalDecoder plugin = decoderPlugin();
        final YugabyteDBConnectorConfig config = new YugabyteDBConnectorConfig(defaultConfig().build());
        return ReplicationConnection.builder(config)
                .withPlugin(plugin)
                .withSlot(slotName)
                .withTypeRegistry(getTypeRegistry())
                .dropSlotOnClose(dropOnClose)
                .statusUpdateInterval(Duration.ofSeconds(10))
                .withSchema(getSchema(config))
                .build();
    }

    /**
     * @return the decoder plugin used for testing and configured by system property
     */
    public static YugabyteDBConnectorConfig.LogicalDecoder decoderPlugin() {
        final String s = System.getProperty(YugabyteDBConnectorConfig.PLUGIN_NAME.name());
        return (s == null || s.length() == 0) ? YugabyteDBConnectorConfig.LogicalDecoder.DECODERBUFS : YugabyteDBConnectorConfig.LogicalDecoder.parse(s);
    }

    /**
     * Obtain a default DB connection.
     *
     * @return the YugabyteDBConnection instance; never null
     */
    public static YugabyteDBConnection create() {

        return new YugabyteDBConnection(defaultJdbcConfig(CONTAINER_YSQL_HOST, CONTAINER_YSQL_PORT), YugabyteDBConnection.CONNECTION_GENERAL);
    }

    public static YugabyteDBConnection createConnectionTo(String databaseName) {
        return new YugabyteDBConnection(defaultJdbcConfig(CONTAINER_YSQL_HOST, CONTAINER_YSQL_PORT, databaseName), YugabyteDBConnection.CONNECTION_GENERAL);
    }

    /**
     * Obtain a DB connection providing type registry.
     *
     * @return the PostgresConnection instance; never null
     */
    public static YugabyteDBConnection createWithTypeRegistry() {
        final YugabyteDBConnectorConfig config = new YugabyteDBConnectorConfig(defaultConfig().build());

        return new YugabyteDBConnection(
                config.getJdbcConfig(),
                getPostgresValueConverterBuilder(config), YugabyteDBConnection.CONNECTION_GENERAL);
    }

    /**
     * Obtain a DB connection with a custom application name.
     *
     * @param appName the name of the application used for PostgreSQL diagnostics
     *
     * @return the PostgresConnection instance; never null
     */
    public static YugabyteDBConnection create(String appName) {
        return new YugabyteDBConnection(Objects.requireNonNull(defaultJdbcConfigBuilder()).with("ApplicationName", appName).build(), YugabyteDBConnection.CONNECTION_GENERAL);
    }

    /**
     * Executes a JDBC statement using the default jdbc config
     *
     * @param statement A SQL statement
     * @param furtherStatements Further SQL statement(s)
     */
    public static void execute(String statement, String... furtherStatements) {
        if (furtherStatements != null) {
            for (String further : furtherStatements) {
                statement = statement + further;
            }
        }

        try (YugabyteDBConnection connection = create()) {
            connection.execute(statement);
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Execute a JDBC statement on a database
     * @param statement A SQL statement
     * @param databaseName The database to execute the statement onto
     */
    public static void executeInDatabase(String statement, String databaseName) {
        try (YugabyteDBConnection connection = createConnectionTo(databaseName)) {
            connection.execute(statement);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void executeBulk(String statement, int numRecords) {
        executeBulk(statement, numRecords, "yugabyte");
    }

    public static void executeBulk(String statement, int numRecords, String databaseName) {
        try (YugabyteDBConnection connection = createConnectionTo(databaseName)) {
            connection.setAutoCommit(false); // setting auto-commit to true
            for (int i = 0; i < numRecords; i++) {
                connection.executeWithoutCommitting(String.format(statement, i));
            }
            connection.commit();
        }
        catch (RuntimeException e) {
            throw e;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Executes the statement with the key range as [beginKey, endKey)
     * @param statement the format string of the statement to be executed
     * @param beginKey key to start inserting, included in the range
     * @param endKey key to end insertion at, excluded in the range
     * @param databaseName the database to create the connection onto
     */
    public static void executeBulkWithRange(String statement, int beginKey, int endKey,
                                            String databaseName) {
        try (YugabyteDBConnection connection = createConnectionTo(databaseName)) {
            connection.setAutoCommit(false); // setting auto-commit to true
            for (int i = beginKey; i < endKey; i++) {
                connection.executeWithoutCommitting(String.format(statement, i));
            }
            connection.commit();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void executeBulkWithRange(String statement, int beginKey, int endKey) {
        executeBulkWithRange(statement, beginKey, endKey, "yugabyte");
    }

    /**
     * Drops all the public non system schemas from the DB.
     *
     * @throws SQLException if anything fails.
     */
    public static void dropAllSchemas() throws SQLException {
        String lineSeparator = System.lineSeparator();
        Set<String> schemaNames = schemaNames();
        if (!schemaNames.contains(YugabyteDBSchema.PUBLIC_SCHEMA_NAME)) {
            schemaNames.add(YugabyteDBSchema.PUBLIC_SCHEMA_NAME);
        }
        String dropStmts = schemaNames.stream()
                .map(schema -> "\"" + schema.replaceAll("\"", "\"\"") + "\"")
                .map(schema -> "DROP SCHEMA IF EXISTS " + schema + " CASCADE;")
                .collect(Collectors.joining(lineSeparator));
        TestHelper.execute(dropStmts);
        try {
            TestHelper.executeDDL("init_database.ddl");
        }
        catch (Exception e) {
            throw new IllegalStateException("Failed to initialize database", e);
        }
    }

    public static void insertData() throws SQLException {
        String[] insertStmts = {
                "INSERT INTO t1 VALUES (1, 'Vaibhav', 'Kushwaha', 30);",
                "INSERT INTO t1 VALUES (2, 'V', 'K', 30.34);"
        };

        try {
            for (int i = 0; i < insertStmts.length; ++i) {
                LOGGER.info("Executing statement: " + insertStmts[i]);
                TestHelper.execute(insertStmts[i]);
            }
        }
        catch (Exception e) {
            throw new SQLException("Failed to write rows to database");
        }
    }

    public static void setMasterAddress(String address) {
        MASTER_ADDRESS = address;
    }

    public static YugabyteDBTypeRegistry getTypeRegistry() {
        final YugabyteDBConnectorConfig config = new YugabyteDBConnectorConfig(defaultConfig().build());
        try (final YugabyteDBConnection connection = new YugabyteDBConnection(config.getJdbcConfig(), getPostgresValueConverterBuilder(config), YugabyteDBConnection.CONNECTION_GENERAL)) {
            return connection.getTypeRegistry();
        }
    }

    public static YugabyteDBSchema getSchema(YugabyteDBConnectorConfig config) {
        return getSchema(config, TestHelper.getTypeRegistry());
    }

    public static YugabyteDBSchema getSchema(YugabyteDBConnectorConfig config, YugabyteDBTypeRegistry yugabyteDBTypeRegistry) {
        return new YugabyteDBSchema(
                config,
                yugabyteDBTypeRegistry,
                YugabyteDBTopicSelector.create(config),
                getPostgresValueConverter(yugabyteDBTypeRegistry, config));
    }

    protected static Set<String> schemaNames() throws SQLException {
        try (YugabyteDBConnection connection = create()) {
            return connection.readAllSchemaNames(Filters.IS_SYSTEM_SCHEMA.negate());
        }
    }

    protected static void createTableInSecondaryDatabase(String query) throws SQLException {
        // Create a connector to the new database and execute the query
        try (YugabyteDBConnection conn = createConnectionTo(SECONDARY_DATABASE)) {
            Statement st = conn.connection().createStatement();
            st.execute(query);
        }
    }

    protected static Configuration.Builder getConfigBuilder(String fullTableNameWithSchema, String dbStreamId) throws Exception {
        return getConfigBuilder("yugabyte", fullTableNameWithSchema, dbStreamId);
    }

    protected static Configuration.Builder getConfigBuilder(String namespaceName, String fullTableNameWithSchema, String dbStreamId) throws Exception {
        return TestHelper.defaultConfig()
                .with(YugabyteDBConnectorConfig.DATABASE_NAME, namespaceName)
                .with(YugabyteDBConnectorConfig.HOSTNAME, CONTAINER_YSQL_HOST) // this field is required as of now
                .with(YugabyteDBConnectorConfig.PORT, CONTAINER_YSQL_PORT)
                .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.NEVER.getValue())
                .with(YugabyteDBConnectorConfig.DELETE_STREAM_ON_STOP, Boolean.TRUE)
                .with(YugabyteDBConnectorConfig.MASTER_ADDRESSES, CONTAINER_YSQL_HOST + ":" + CONTAINER_MASTER_PORT)
                .with(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST, fullTableNameWithSchema)
                .with(YugabyteDBConnectorConfig.STREAM_ID, dbStreamId);
    }

    protected static void setContainerHostPort(String host, int port) {
        CONTAINER_YSQL_HOST = host;
        CONTAINER_YSQL_PORT = port;
    }

    protected static void setContainerMasterPort(int masterPort) {
        CONTAINER_MASTER_PORT = String.valueOf(masterPort);
    }

    protected static YugabyteYSQLContainer getYbContainer() {
        String dockerImageName = System.getenv("YB_DOCKER_IMAGE");
        
        if (dockerImageName == null || dockerImageName.isEmpty()) {
            dockerImageName = "yugabytedb/yugabyte:latest";
        }

        LOGGER.info("Using docker image in test: {}", dockerImageName);
        
        YugabyteYSQLContainer container = new YugabyteYSQLContainer(
            DockerImageName.parse(dockerImageName)
            .asCompatibleSubstituteFor("yugabytedb/yugabyte"));
        container.withPassword("yugabyte");
        container.withUsername("yugabyte");
        container.withDatabaseName("yugabyte");
        container.withExposedPorts(7100, 9100, 5433, 9042);
        container.withCreateContainerCmdModifier(cmd -> cmd.withHostName("127.0.0.1").getHostConfig().withPortBindings(new ArrayList<PortBinding>() {
            {
                add(new PortBinding(Ports.Binding.bindPort(7100), new ExposedPort(7100)));
                add(new PortBinding(Ports.Binding.bindPort(9100), new ExposedPort(9100)));
                add(new PortBinding(Ports.Binding.bindPort(5433), new ExposedPort(5433)));
                add(new PortBinding(Ports.Binding.bindPort(9042), new ExposedPort(9042)));
            }
        }));
        container.withCommand("bin/yugabyted start --listen=0.0.0.0 --master_flags=rpc_bind_addresses=0.0.0.0 --daemon=false");
        return container;
    }

    protected static YBClient getYbClient(String masterAddresses) throws Exception {
        AsyncYBClient asyncClient = new AsyncYBClient.AsyncYBClientBuilder(masterAddresses)
                .defaultAdminOperationTimeoutMs(YugabyteDBConnectorConfig.DEFAULT_ADMIN_OPERATION_TIMEOUT_MS)
                .defaultOperationTimeoutMs(YugabyteDBConnectorConfig.DEFAULT_OPERATION_TIMEOUT_MS)
                .defaultSocketReadTimeoutMs(YugabyteDBConnectorConfig.DEFAULT_SOCKET_READ_TIMEOUT_MS)
                .numTablets(YugabyteDBConnectorConfig.DEFAULT_MAX_NUM_TABLETS)
                .build();

        return new YBClient(asyncClient);
    }

    protected static YBTable getYbTable(YBClient syncClient, String tableName) throws Exception {
        ListTablesResponse resp = syncClient.getTablesList();

        for (TableInfo tableInfo : resp.getTableInfoList()) {
            if (Objects.equals(tableInfo.getName(), tableName)) {
                return syncClient.openTableByUUID(tableInfo.getId().toStringUtf8());
            }
        }

        // This will be returned in case no table match has been found for the given table name
        return null;
    }

    public static String getNewDbStreamId(String namespaceName, String tableName) throws Exception {
        YBClient syncClient = getYbClient(MASTER_ADDRESS);

        YBTable placeholderTable = getYbTable(syncClient, tableName);

        if (placeholderTable == null) {
            throw new NullPointerException("No table found with the name " + tableName);
        }

        String dbStreamId;
        try {
            dbStreamId = syncClient.createCDCStream(placeholderTable, namespaceName,
                                                           "PROTO", "IMPLICIT").getStreamId();
        } finally {
            syncClient.close();
        }

        return dbStreamId;
    }

    public static JdbcConfiguration.Builder defaultJdbcConfigBuilder() {
        try {
            return JdbcConfiguration.copy(Configuration.empty())
                    .withDefault(JdbcConfiguration.DATABASE, "yugabyte")
                    .withDefault(JdbcConfiguration.HOSTNAME, CONTAINER_YSQL_HOST)
                    .withDefault(JdbcConfiguration.PORT, 5433)
                    .withDefault(JdbcConfiguration.USER, "yugabyte")
                    .withDefault(JdbcConfiguration.PASSWORD, "yugabyte")
                    .with(YugabyteDBConnectorConfig.MAX_RETRIES, 2)
                    .with(YugabyteDBConnectorConfig.RETRY_DELAY_MS, 2000);
        }
        catch (Exception e) {
            LOGGER.error("Exception thrown while creating connection...", e);
            return null;
        }
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        try {
            return Objects.requireNonNull(defaultJdbcConfigBuilder()).build();
        }
        catch (Exception e) {
            LOGGER.error("Exception thrown while creating connection...", e);
            return null;
        }
    }

    public static JdbcConfiguration defaultJdbcConfig(String host, int ysqlPort) {
        return defaultJdbcConfig(host, ysqlPort, "yugabyte");
    }

    public static JdbcConfiguration defaultJdbcConfig(String host, int ysqlPort, String databaseName) {
        try {
            return JdbcConfiguration.copy(Configuration.empty())
                    .withDefault(JdbcConfiguration.DATABASE, databaseName)
                    .withDefault(JdbcConfiguration.HOSTNAME, host)
                    .withDefault(JdbcConfiguration.PORT, ysqlPort)
                    .withDefault(JdbcConfiguration.USER, "yugabyte")
                    .withDefault(JdbcConfiguration.PASSWORD, "yugabyte")
                    .with(YugabyteDBConnectorConfig.MAX_RETRIES, 2)
                    .with(YugabyteDBConnectorConfig.RETRY_DELAY_MS, 2000)
                    .build();
        }
        catch (Exception e) {
            LOGGER.error("Exception thrown while creating connection...", e);
            return null;
        }
    }

    protected static Configuration.Builder defaultConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();
        jdbcConfiguration.forEach((field, value) -> builder.with(YugabyteDBConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));
        builder.with(RelationalDatabaseConnectorConfig.SERVER_NAME, TEST_SERVER)
                .with(YugabyteDBConnectorConfig.DELETE_STREAM_ON_STOP, true)
                .with(YugabyteDBConnectorConfig.STATUS_UPDATE_INTERVAL_MS, 100)
                .with(YugabyteDBConnectorConfig.PLUGIN_NAME, decoderPlugin())
                .with(YugabyteDBConnectorConfig.SSL_MODE, SecureConnectionMode.DISABLED);
        final String testNetworkTimeout = System.getProperty(TEST_PROPERTY_PREFIX + "network.timeout");
        if (testNetworkTimeout != null && testNetworkTimeout.length() != 0) {
            builder.with(YugabyteDBConnectorConfig.STATUS_UPDATE_INTERVAL_MS, Integer.parseInt(testNetworkTimeout));
        }
        return builder;
    }

    protected static void executeDDL(String ddlFile) throws Exception {
        executeDDL(ddlFile, "yugabyte");
    }

    protected static void executeDDL(String ddlFile, String databaseName) throws Exception {
        URL ddlTestFile = TestHelper.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        String statements = Files.readAllLines(Paths.get(ddlTestFile.toURI()))
                .stream()
                .collect(Collectors.joining(System.lineSeparator()));
        try (YugabyteDBConnection connection = createConnectionTo(databaseName)) {
            connection.execute(statements);
        }
    }

    protected static void executeDDL(JdbcDatabaseContainer<?> container, String ddlFile) throws Exception {
        URL ddlTestFile = TestHelper.class.getClassLoader().getResource(ddlFile);
        assertNotNull("Cannot locate " + ddlFile, ddlTestFile);
        String statements = Files.readAllLines(Paths.get(ddlTestFile.toURI()))
                .stream()
                .collect(Collectors.joining(System.lineSeparator()));
        try (YugabyteDBConnection connection = create()) {
            TestHelper.performQuery(container, statements);
            // connection.executeWithoutCommitting(statements);
        }
    }

    protected static String topicName(String suffix) {
        return TestHelper.TEST_SERVER + "." + suffix;
    }

    protected static boolean shouldSSLConnectionFail() {
        return Boolean.parseBoolean(System.getProperty(TEST_PROPERTY_PREFIX + "ssl.failonconnect", "true"));
    }

    public static int waitTimeForRecords() {
        return Integer.parseInt(System.getProperty(TEST_PROPERTY_PREFIX + "records.waittime", "2"));
    }

    protected static SourceInfo sourceInfo() {
        return new SourceInfo(new YugabyteDBConnectorConfig(
                Configuration.create()
                        .with(YugabyteDBConnectorConfig.SERVER_NAME, TEST_SERVER)
                        .with(YugabyteDBConnectorConfig.DATABASE_NAME, TEST_DATABASE)
                        .build()));
    }

    protected static void assertNoOpenTransactions() throws SQLException {
        try (YugabyteDBConnection connection = TestHelper.create()) {
            connection.setAutoCommit(true);

            try {
                Awaitility.await()
                        .atMost(TestHelper.waitTimeForRecords() * 5, TimeUnit.SECONDS)
                        .until(() -> getOpenIdleTransactions(connection).size() == 0);
            }
            catch (ConditionTimeoutException e) {
                fail("Expected no open transactions but there was at least one.");
            }
        }
    }

    // Function to introduce dummy wait conditions in tests
    protected static void waitFor(Duration duration) {
        Awaitility.await()
            .pollDelay(duration)
            .atMost(duration.plusSeconds(1))
            .until(() -> {
                return true;
            });
    }

    private static List<String> getOpenIdleTransactions(YugabyteDBConnection connection) throws SQLException {
        int connectionPID = ((PgConnection) connection.connection()).getBackendPID();
        return connection.queryAndMap(
                "SELECT state FROM pg_stat_activity WHERE state like 'idle in transaction' AND pid <> " + connectionPID,
                rs -> {
                    final List<String> ret = new ArrayList<>();
                    while (rs.next()) {
                        ret.add(rs.getString(1));
                    }
                    return ret;
                });
    }

    private static YugabyteDBValueConverter getPostgresValueConverter(YugabyteDBTypeRegistry yugabyteDBTypeRegistry, YugabyteDBConnectorConfig config) {
        return getPostgresValueConverterBuilder(config).build(yugabyteDBTypeRegistry);
    }

    private static YugabyteDBValueConverterBuilder getPostgresValueConverterBuilder(YugabyteDBConnectorConfig config) {
        return typeRegistry -> new YugabyteDBValueConverter(
                Charset.forName("UTF-8"),
                config.getDecimalMode(),
                config.getTemporalPrecisionMode(),
                ZoneOffset.UTC,
                null,
                config.includeUnknownDatatypes(),
                typeRegistry,
                config.hStoreHandlingMode(),
                config.binaryHandlingMode(),
                config.intervalHandlingMode());
    }
}
