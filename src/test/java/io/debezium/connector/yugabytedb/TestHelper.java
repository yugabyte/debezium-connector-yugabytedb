/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

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
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.params.provider.Arguments;
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

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.HelperBeforeImageModes.BeforeImageMode;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig.SecureConnectionMode;
import io.debezium.connector.yugabytedb.connection.ReplicationConnection;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection.YugabyteDBValueConverterBuilder;
import io.debezium.connector.yugabytedb.container.CustomContainerWaitStrategy;
import io.debezium.connector.yugabytedb.container.YugabyteCustomContainer;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.spi.topic.TopicNamingStrategy;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A utility for integration test cases to connect the YugabyteDB instance running in the Docker 
 * container created by this module's build.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public final class TestHelper {

    public static final String TEST_SERVER = "test_server";
    public static final String TEST_DATABASE = "postgres";
    public static final String PK_FIELD = "pk";
    private static final String TEST_PROPERTY_PREFIX = "debezium.test.";
    private static final Logger LOGGER = LoggerFactory.getLogger(TestHelper.class);

    // If this variable is changed, do not forget to change the name in postgres_create_tables.ddl
    private static final String SECONDARY_DATABASE = "secondary_database";

    public static final String CONTAINER_HOSTNAME = "yugabyte-0";

    // Set the localhost value as the defaults for now
    private static String CONTAINER_YSQL_HOST = "127.0.0.1";
    private static String CONTAINER_YCQL_HOST = "127.0.0.1";
    private static int CONTAINER_YSQL_PORT = 5433;
    private static int CONTAINER_YCQL_PORT = 9042;
    private static String CONTAINER_MASTER_PORT = "7100";
    private static String MASTER_ADDRESS = "127.0.0.1:7100";
    private static String PLUGIN_NAME = "yboutput";
    private static String DEFAULT_DATABASE_NAME = "yugabyte";
    private static String DEFAULT_CASSANDRA_USER = "cassandra";

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

    public static String createPublicationForTableStatement = "CREATE PUBLICATION %s FOR TABLE %s ;";
    public static String createPublicationForALLTablesStatement = "CREATE PUBLICATION %s FOR ALL TABLES ;";
    public static String createReplicationSlotStatement = "SELECT pg_create_logical_replication_slot('test_replication_slot', 'yboutput');";
    public static String dropReplicationSlotStatement = "SELECT pg_drop_replication_slot('test_replication_slot');";
    public static String dropPublicationStatement = "DROP PUBLICATION IF EXISTS pub;";

    private TestHelper() {
    }

    public static ResultSet performQuery(JdbcDatabaseContainer<?> container, String sql) throws SQLException {
        DataSource ds = getDataSource(container);
        Statement statement = ds.getConnection().createStatement();
        statement.execute(sql);
        ResultSet resultSet = statement.getResultSet();

        resultSet.next();
        return resultSet;
    }

    public static DataSource getDataSource(JdbcDatabaseContainer<?> container) {
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

    public static void initDB(String initDdlFile) throws Exception {
        dropAllSchemas();
        executeDDL(initDdlFile);
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

    public static String getMasterAddress() {
        return MASTER_ADDRESS;
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
                getPostgresValueConverter(yugabyteDBTypeRegistry, config),
                config.getTopicNamingStrategy(CommonConnectorConfig.TOPIC_NAMING_STRATEGY),
                getPostgresValueConverter(yugabyteDBTypeRegistry, config),
                yugabyteDBTypeRegistry);
    }

    public static Set<String> schemaNames() throws SQLException {
        try (YugabyteDBConnection connection = create()) {
            return connection.readAllSchemaNames(Filters.IS_SYSTEM_SCHEMA.negate());
        }
    }

    public static void createTableInSecondaryDatabase(String query) throws SQLException {
        // Create a connector to the new database and execute the query
        try (YugabyteDBConnection conn = createConnectionTo(SECONDARY_DATABASE)) {
            Statement st = conn.connection().createStatement();
            st.execute(query);
        }
    }

    public static Configuration.Builder getConfigBuilder(String fullTableNameWithSchema, String dbStreamId) throws Exception {
        return getConfigBuilder("yugabyte", fullTableNameWithSchema, dbStreamId);
    }


    public static Configuration.Builder getConfigBuilderWithPublication(String namespaceName, String publicationName, String slotName) throws Exception {
        return TestHelper.defaultConfig()
                .with(YugabyteDBConnectorConfig.DATABASE_NAME, namespaceName)
                .with(YugabyteDBConnectorConfig.HOSTNAME, CONTAINER_YSQL_HOST)
                .with(YugabyteDBConnectorConfig.PORT, CONTAINER_YSQL_PORT)
                .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.NEVER.getValue())
                .with(YugabyteDBConnectorConfig.MASTER_ADDRESSES, MASTER_ADDRESS)
                .with(YugabyteDBConnectorConfig.PLUGIN_NAME , PLUGIN_NAME)
                .with(YugabyteDBConnectorConfig.PUBLICATION_NAME, publicationName)
                .with(YugabyteDBConnectorConfig.PUBLICATION_AUTOCREATE_MODE , "disabled")
                .with(YugabyteDBConnectorConfig.SLOT_NAME , slotName);
    }

    public static Configuration.Builder getConfigBuilder(String namespaceName, String fullTableNameWithSchema, String dbStreamId) throws Exception {
        return TestHelper.defaultConfig()
                .with(YugabyteDBConnectorConfig.DATABASE_NAME, namespaceName)
                .with(YugabyteDBConnectorConfig.HOSTNAME, CONTAINER_YSQL_HOST) // this field is required as of now
                .with(YugabyteDBConnectorConfig.PORT, CONTAINER_YSQL_PORT)
                .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.NEVER.getValue())
                .with(YugabyteDBConnectorConfig.DELETE_STREAM_ON_STOP, Boolean.TRUE)
                .with(YugabyteDBConnectorConfig.MASTER_ADDRESSES, MASTER_ADDRESS)
                .with(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST, fullTableNameWithSchema)
                .with(YugabyteDBConnectorConfig.STREAM_ID, dbStreamId)
                .with(YugabyteDBConnectorConfig.LOG_GET_CHANGES, true);
    }

    public static Configuration.Builder getConfigBuilderForCQL(String keyspaceName, String fullTableNameWithSchema, String dbStreamId) throws Exception {
        return TestHelper.defaultConfig()
                .with(YugabyteDBConnectorConfig.DATABASE_NAME, keyspaceName)
                .with(YugabyteDBConnectorConfig.HOSTNAME,CONTAINER_YCQL_HOST)
                .with(YugabyteDBConnectorConfig.PORT, CONTAINER_YCQL_PORT)
                .with(YugabyteDBConnectorConfig.USER, DEFAULT_CASSANDRA_USER)
                .with(YugabyteDBConnectorConfig.PASSWORD, "Yugabyte@123")
                .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.NEVER.getValue())
                .with(YugabyteDBConnectorConfig.DELETE_STREAM_ON_STOP, Boolean.TRUE)
                .with(YugabyteDBConnectorConfig.MASTER_ADDRESSES, CONTAINER_YCQL_HOST + ":" + CONTAINER_MASTER_PORT)
                .with(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST, fullTableNameWithSchema)
                .with(YugabyteDBConnectorConfig.STREAM_ID, dbStreamId);
    }

    public static void setContainerHostPort(String host, int sqlPort, int cqlPort) {
        CONTAINER_YSQL_HOST = host;
        CONTAINER_YSQL_PORT = sqlPort;
        CONTAINER_YCQL_PORT = cqlPort;
    }

    public static void setContainerMasterPort(int masterPort) {
        CONTAINER_MASTER_PORT = String.valueOf(masterPort);
    }

    /**
     * Return a TestContainer for YugabyteDB with the given master and tserver flags
     * @return a {@link YugabyteYSQLContainer}
     */
    public static YugabyteCustomContainer getYbContainer() {
        String dockerImageName = System.getenv("YB_DOCKER_IMAGE");
        
        if (dockerImageName == null || dockerImageName.isEmpty()) {
            LOGGER.info("Environment variable YB_DOCKER_IMAGE is empty, defaulting to image from Dockerhub.");
            dockerImageName = "yugabytedb/yugabyte:latest";
        }

        LOGGER.info("Using docker image in test: {}", dockerImageName);
        
        YugabyteCustomContainer container = new YugabyteCustomContainer(
            DockerImageName.parse(dockerImageName)
            .asCompatibleSubstituteFor("yugabytedb/yugabyte"));
        container.withPassword("yugabyte");
        container.withUsername("yugabyte");
        container.withDatabaseName("yugabyte");
        container.withExposedPorts(7100, 9100, 5433, 9042);
        container.withCreateContainerCmdModifier(cmd -> cmd.withHostName(CONTAINER_HOSTNAME).getHostConfig().withPortBindings(new ArrayList<PortBinding>() {
            {
                add(new PortBinding(Ports.Binding.bindPort(7100), new ExposedPort(7100)));
                add(new PortBinding(Ports.Binding.bindPort(9100), new ExposedPort(9100)));
                add(new PortBinding(Ports.Binding.bindPort(5433), new ExposedPort(5433)));
                add(new PortBinding(Ports.Binding.bindPort(9042), new ExposedPort(9042)));
            }
        }));

        String[] commandArray = {"/bin/bash", "-c", "while :; do sleep 1; done"};
        container.withCommand(commandArray);
        container.waitingFor(new CustomContainerWaitStrategy());
        return container;
    }

    public static YBClient getYbClient(String masterAddresses) throws Exception {
        AsyncYBClient asyncClient = new AsyncYBClient.AsyncYBClientBuilder(masterAddresses)
                .defaultAdminOperationTimeoutMs(YugabyteDBConnectorConfig.DEFAULT_ADMIN_OPERATION_TIMEOUT_MS)
                .defaultOperationTimeoutMs(YugabyteDBConnectorConfig.DEFAULT_OPERATION_TIMEOUT_MS)
                .defaultSocketReadTimeoutMs(YugabyteDBConnectorConfig.DEFAULT_SOCKET_READ_TIMEOUT_MS)
                .numTablets(YugabyteDBConnectorConfig.DEFAULT_MAX_NUM_TABLETS)
                .maxRpcAttempts(YugabyteDBConnectorConfig.DEFAULT_MAX_RPC_RETRY_ATTEMPTS)
                .sleepTime(YugabyteDBConnectorConfig.DEFAULT_RPC_RETRY_SLEEP_TIME_MS)
                .build();

        return new YBClient(asyncClient);
    }

    /**
     * Get the {@link YBTable} object for the given table name
     * @param syncClient the {@link YBClient} instance
     * @param tableName just the name of the table, without any schema name
     * @return a {@link YBTable} if a table is found with the given name, null otherwise
     * @throws Exception if there is any error in opening the table
     */
    public static YBTable getYbTable(YBClient syncClient, String tableName) throws Exception {
        ListTablesResponse resp = syncClient.getTablesList();

        for (TableInfo tableInfo : resp.getTableInfoList()) {
            if (Objects.equals(tableInfo.getName(), tableName)) {
                return syncClient.openTableByUUID(tableInfo.getId().toStringUtf8());
            }
        }

        // This will be returned in case no table match has been found for the given table name
        return null;
    }

    public static String getNewDbStreamId(String namespaceName, String tableName,
                                          boolean withBeforeImage, boolean explicitCheckpointing, BeforeImageMode mode, boolean withCQL,
                                          boolean consistentSnapshot, boolean useSnapshot)
            throws Exception {
        LOGGER.info("Before obtaining ybClient");
        YBClient syncClient = getYbClient(MASTER_ADDRESS);
        LOGGER.info("After obtaining ybClient");

        YBTable placeholderTable = getYbTable(syncClient, tableName);

        if (placeholderTable == null) {
            throw new NullPointerException("No table found with the name " + tableName);
        }

        String dbStreamId;
        try {
            dbStreamId = syncClient.createCDCStream(placeholderTable, namespaceName,
                                                    "PROTO", explicitCheckpointing ? "EXPLICIT" : "IMPLICIT",
                                                    withBeforeImage ? mode.toString() : BeforeImageMode.CHANGE.toString(), withCQL,
                                                    consistentSnapshot, useSnapshot).getStreamId();
        } finally {
            syncClient.close();
        }

        return dbStreamId;
    }

    public static String getNewDbStreamId(String namespaceName, String tableName,
                                          boolean withBeforeImage, boolean explicitCheckpointing, BeforeImageMode mode,
                                          boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        return getNewDbStreamId(namespaceName, tableName, withBeforeImage, explicitCheckpointing, mode, false, consistentSnapshot, useSnapshot);
    }

    public static String getNewDbStreamId(String namespaceName, String tableName,
                                          boolean withBeforeImage, boolean explicitCheckpointing,
                                          boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        return getNewDbStreamId(namespaceName, tableName, withBeforeImage, explicitCheckpointing, BeforeImageMode.CHANGE, consistentSnapshot, useSnapshot);
    }

    public static String getNewDbStreamId(String namespaceName, String tableName,
                                          boolean withBeforeImage) throws Exception {
        return getNewDbStreamId(namespaceName, tableName, withBeforeImage, true /* explicit */, false, false);
    }

    public static String getNewDbStreamId(String namespaceName, String tableName, boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        return getNewDbStreamId(namespaceName, tableName, false, true, consistentSnapshot, useSnapshot);
    }
    public static String getNewDbStreamId(String namespaceName, String tableName) throws Exception {
        return getNewDbStreamId(namespaceName, tableName, false, false);
    }

    public static String getStreamIdFromSlot(String slotName) throws Exception {
        try (YugabyteDBConnection ybConnection = TestHelper.create();
              Connection connection = ybConnection.connection();
              Statement statement = connection.createStatement()) {
            String query = "SELECT yb_stream_id FROM pg_replication_slots WHERE slot_name = '"+ slotName + "';";
            ResultSet rs = statement.executeQuery(query);
            if (rs.next()) {
                return rs.getString("yb_stream_id");
            }
        }
        throw new Exception("Replication slot " + slotName + " not found");
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

    public static Configuration.Builder defaultConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();
        jdbcConfiguration.forEach((field, value) -> builder.with(YugabyteDBConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));
        builder.with(RelationalDatabaseConnectorConfig.TOPIC_PREFIX, TEST_SERVER)
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

    public static void executeDDL(String ddlFile) throws Exception {
        executeDDL(ddlFile, "yugabyte");
    }

    public static void executeDDL(String ddlFile, String databaseName) throws Exception {
        URL ddlTestFile = TestHelper.class.getClassLoader().getResource(ddlFile);
        assertNotNull(ddlTestFile, "Cannot locate " + ddlFile);
        String content = Files.readString(Paths.get(ddlTestFile.toURI()));
        String[] statements = Arrays.stream(content.split("(?<=;)\\s*"))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .toArray(String[]::new);
        try (YugabyteDBConnection connection = createConnectionTo(databaseName)) {
            for (String statement : statements) {
                LOGGER.info("Executing: {}", statement);
                connection.execute(statement);
            }
        }
    }

    public static void executeDDL(JdbcDatabaseContainer<?> container, String ddlFile) throws Exception {
        URL ddlTestFile = TestHelper.class.getClassLoader().getResource(ddlFile);
        assertNotNull(ddlTestFile, "Cannot locate " + ddlFile);
        String statements = Files.readAllLines(Paths.get(ddlTestFile.toURI()))
                .stream()
                .collect(Collectors.joining(System.lineSeparator()));
        try (YugabyteDBConnection connection = create()) {
            TestHelper.performQuery(container, statements);
            // connection.executeWithoutCommitting(statements);
        }
    }

    public static String topicName(String suffix) {
        return TestHelper.TEST_SERVER + "." + suffix;
    }

    public static boolean shouldSSLConnectionFail() {
        return Boolean.parseBoolean(System.getProperty(TEST_PROPERTY_PREFIX + "ssl.failonconnect", "true"));
    }

    public static int waitTimeForRecords() {
        return Integer.parseInt(System.getProperty(TEST_PROPERTY_PREFIX + "records.waittime", "2"));
    }

    public static SourceInfo sourceInfo() {
        return new SourceInfo(new YugabyteDBConnectorConfig(
                Configuration.create()
                        .with(YugabyteDBConnectorConfig.TOPIC_PREFIX, TEST_SERVER)
                        .with(YugabyteDBConnectorConfig.DATABASE_NAME, TEST_DATABASE)
                        .build()));
    }

    public static void assertNoOpenTransactions() throws SQLException {
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
    public static void waitFor(Duration duration) {
        Awaitility.await()
            .pollDelay(duration)
            .atMost(duration.plusSeconds(1))
            .until(() -> {
                return true;
            });
    }

    /**
     * Wait until we see the given number of tablets for a table
     * @param ybClient
     * @param table {@link YBTable} object for the table in picture
     * @param tabletCount expected number of tablets
     */
    public static void waitForTablets(YBClient ybClient, YBTable table, int tabletCount) {
        Awaitility.await()
          .pollDelay(Duration.ofSeconds(2))
          .atMost(Duration.ofSeconds(20))
          .until(() -> {
            return ybClient.getTabletUUIDs(table).size() == tabletCount;
          });
    }

    /**
     * Get the value of the operation to which the given source records belongs.
     * @param record the {@link SourceRecord} to get the operation for
     * @return the string value of the operation - c, u, r or d
     */
    public static String getOpValue(SourceRecord record) {
        Struct s = (Struct) record.value();
        return s.getString("op");
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

    public static Stream<Arguments> streamTypeProviderForStreaming() {
        return Stream.of(
                // Arguments.of(false, false), // Older stream
                Arguments.of(true, false)); // NO_EXPORT stream
    }

    public static Stream<Arguments> streamTypeProviderForSnapshot() {
        return Stream.of(
                Arguments.of(false, false), // Older stream
                Arguments.of(true, true));  // USE_SNAPSHOT stream
    }
}
