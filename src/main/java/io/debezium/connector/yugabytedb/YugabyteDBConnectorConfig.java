/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.common.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.client.CDCStreamInfo;
import org.yb.client.GetDBStreamInfoResponse;
import org.yb.client.ListCDCStreamsResponse;
import org.yb.client.YBClient;

import io.debezium.DebeziumException;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.yugabytedb.connection.MessageDecoder;
import io.debezium.connector.yugabytedb.connection.MessageDecoderContext;
import io.debezium.connector.yugabytedb.connection.ReplicationConnection;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.connection.pgproto.YbProtoMessageDecoder;
import io.debezium.connector.yugabytedb.snapshot.AlwaysSnapshotter;
import io.debezium.connector.yugabytedb.snapshot.InitialOnlySnapshotter;
import io.debezium.connector.yugabytedb.snapshot.InitialSnapshotter;
import io.debezium.connector.yugabytedb.snapshot.NeverSnapshotter;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcConnection.ConnectionFactory;
import io.debezium.jdbc.JdbcConnectionException;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import io.debezium.util.Strings;

/**
 * The configuration properties for the {@link YugabyteDBgRPCConnector}
 *
 * @author Suranjan Kumar, Vaibhav Kushwaha
 */
public class YugabyteDBConnectorConfig extends RelationalDatabaseConnectorConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBConnectorConfig.class);

    /**
     * The set of predefined HStoreHandlingMode options or aliases
     */
    public enum HStoreHandlingMode implements EnumeratedValue {

        /**
         * Represents HStore value as json
         */
        JSON("json"),

        /**
         * Represents HStore value as map
         */
        MAP("map");

        private final String value;

        HStoreHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied values is one of the predefined options
         *
         * @param value the configuration property value ; may not be null
         * @return the matching option, or null if the match is not found
         */
        public static HStoreHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (HStoreHandlingMode option : HStoreHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied values is one of the predefined options
         *
         * @param value the configuration property value ; may not be null
         * @param defaultValue the default value ; may be null
         * @return the matching option or null if the match is not found and non-null default is invalid
         */
        public static HStoreHandlingMode parse(String value, String defaultValue) {
            HStoreHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * Defines modes of representation of {@code interval} datatype
     */
    public enum IntervalHandlingMode implements EnumeratedValue {

        /**
         * Represents interval as inexact microseconds count
         */
        NUMERIC("numeric"),

        /**
         * Represents interval as ISO 8601 time interval
         */
        STRING("string");

        private final String value;

        IntervalHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Convert mode name into the logical value
         *
         * @param value the configuration property value ; may not be null
         * @return the matching option, or null if the match is not found
         */
        public static IntervalHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (IntervalHandlingMode option : IntervalHandlingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Convert mode name into the logical value
         *
         * @param value the configuration property value ; may not be null
         * @param defaultValue the default value ; may be null
         * @return the matching option or null if the match is not found and non-null default is invalid
         */
        public static IntervalHandlingMode parse(String value, String defaultValue) {
            IntervalHandlingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * The set of predefined Snapshotter options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Always perform a snapshot when starting.
         */
        ALWAYS("always", (c) -> new AlwaysSnapshotter()),

        /**
         * Perform a snapshot only upon initial startup of a connector.
         */
        INITIAL("initial", (c) -> new InitialSnapshotter()),

        /**
         * Never perform a snapshot and only receive logical changes.
         */
        NEVER("never", (c) -> new NeverSnapshotter()),

        /**
         * Perform a snapshot and then stop before attempting to receive any logical changes.
         */
        INITIAL_ONLY("initial_only", (c) -> new InitialOnlySnapshotter()),

        /**
         * Perform an exported snapshot
         */
        @Deprecated
        EXPORTED("exported", (c) -> new InitialSnapshotter()),

        /**
         * Inject a custom snapshotter, which allows for more control over snapshots.
         */
        CUSTOM("custom", (c) -> {
            return c.getInstance(SNAPSHOT_MODE_CLASS, Snapshotter.class);
        });

        @FunctionalInterface
        public interface SnapshotterBuilder {
            Snapshotter buildSnapshotter(Configuration config);
        }

        private final String value;
        private final SnapshotterBuilder builderFunc;

        SnapshotMode(String value, SnapshotterBuilder buildSnapshotter) {
            this.value = value;
            this.builderFunc = buildSnapshotter;
        }

        public Snapshotter getSnapshotter(Configuration config) {
            return builderFunc.buildSnapshotter(config);
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    @Override
    public EnumeratedValue getSnapshotMode() {
        return this.snapshotMode;
    }

    /**
     * The set of predefined SecureConnectionMode options or aliases.
     */
    public enum SecureConnectionMode implements EnumeratedValue {

        /**
         * Establish an unencrypted connection
         *
         * see the {@code sslmode} Postgres JDBC driver option
         */
        DISABLED("disable"),

        /**
         * Establish a secure connection if the server supports secure connections.
         * The connection attempt fails if a secure connection cannot be established
         *
         * see the {@code sslmode} Postgres JDBC driver option
         */
        REQUIRED("require"),

        /**
         * Like REQUIRED, but additionally verify the server TLS certificate against the configured Certificate Authority
         * (CA) certificates. The connection attempt fails if no valid matching CA certificates are found.
         *
         * see the {@code sslmode} Postgres JDBC driver option
         */
        VERIFY_CA("verify-ca"),

        /**
         * Like VERIFY_CA, but additionally verify that the server certificate matches the host to which the connection is
         * attempted.
         *
         * see the {@code sslmode} Postgres JDBC driver option
         */
        VERIFY_FULL("verify-full");

        private final String value;

        SecureConnectionMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SecureConnectionMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SecureConnectionMode option : SecureConnectionMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SecureConnectionMode parse(String value, String defaultValue) {
            SecureConnectionMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public enum LogicalDecoder implements EnumeratedValue {
        YBOUTPUT("yboutput") {
            @Override
            public MessageDecoder messageDecoder(MessageDecoderContext config) {
                return new YbProtoMessageDecoder();
            }

            @Override
            public String getPostgresPluginName() {
                return getValue();
            }

            @Override
            public boolean supportsTruncate() {
                return false;
            }
        },
        DECODERBUFS("decoderbufs") {
            @Override
            public MessageDecoder messageDecoder(MessageDecoderContext config) {
                return new YbProtoMessageDecoder();
            }

            @Override
            public String getPostgresPluginName() {
                return getValue();
            }

            @Override
            public boolean supportsTruncate() {
                return false;
            }
        };

        private final String decoderName;

        LogicalDecoder(String decoderName) {
            this.decoderName = decoderName;
        }

        public abstract MessageDecoder messageDecoder(MessageDecoderContext config);

        public boolean forceRds() {
            return false;
        }

        public boolean hasUnchangedToastColumnMarker() {
            return true;
        }

        public boolean sendsNullToastedValuesInOld() {
            return true;
        }

        public static LogicalDecoder parse(String s) {
            return valueOf(s.trim().toUpperCase());
        }

        @Override
        public String getValue() {
            return decoderName;
        }

        public abstract String getPostgresPluginName();

        public abstract boolean supportsTruncate();
    }

    /**
     * The set of predefined TruncateHandlingMode options or aliases
     */
    public enum TruncateHandlingMode implements EnumeratedValue {

        /**
         * Skip TRUNCATE messages
         */
        SKIP("skip"),

        /**
         * Handle & Include TRUNCATE messages
         */
        INCLUDE("include");

        private final String value;

        TruncateHandlingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static TruncateHandlingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (TruncateHandlingMode truncateHandlingMode : TruncateHandlingMode.values()) {
                if (truncateHandlingMode.getValue().equalsIgnoreCase(value)) {
                    return truncateHandlingMode;
                }
            }
            return null;
        }
    }

    public enum SnapshotLockingMode implements EnumeratedValue {
        /**
         * This mode will lock in ACCESS SHARE MODE to avoid concurrent schema changes during the snapshot, and
         * this does not prevent writes to the table, but prevents changes to the table's schema.
         */
        SHARED("shared"),

        /**
         * This mode will avoid using ANY table locks during the snapshot process.
         * This mode should be used carefully only when no schema changes are to occur.
         */
        NONE("none"),

        CUSTOM("custom");

        private final String value;

        SnapshotLockingMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be {@code null}
         * @return the matching option, or null if no match is found
         */
        public static SnapshotLockingMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotLockingMode option : SnapshotLockingMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be {@code null}
         * @param defaultValue the default value; may be {@code null}
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotLockingMode parse(String value, String defaultValue) {
            SnapshotLockingMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * The set of predefined SchemaRefreshMode options or aliases.
     */
    public enum SchemaRefreshMode implements EnumeratedValue {
        /**
         * Refresh the in-memory schema cache whenever there is a discrepancy between it and the schema derived from the
         * incoming message.
         */
        COLUMNS_DIFF("columns_diff"),

        /**
         * Refresh the in-memory schema cache if there is a discrepancy between it and the schema derived from the
         * incoming message, unless TOASTable data can account for the discrepancy.
         *
         * This setting can improve connector performance significantly if there are frequently-updated tables that
         * have TOASTed data that are rarely part of these updates. However, it is possible for the in-memory schema to
         * become outdated if TOASTable columns are dropped from the table.
         */
        COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST("columns_diff_exclude_unchanged_toast");

        private final String value;

        SchemaRefreshMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SchemaRefreshMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SchemaRefreshMode option : SchemaRefreshMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    public enum ConsistencyMode implements EnumeratedValue {

        /**
         * Default. No consistency filters applied
         */
        DEFAULT("default"),

        /**
         * Key-level consistency
         */
        KEY("key"),

        /*
         * Global Consistency
         */
        GLOBAL("global");

        private final String value;

        ConsistencyMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static ConsistencyMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (ConsistencyMode consistencyMode : ConsistencyMode.values()) {
                if (consistencyMode.getValue().equalsIgnoreCase(value)) {
                    return consistencyMode;
                }
            }
            return null;
        }
    }

    protected static final String DATABASE_CONFIG_PREFIX = "database.";
    protected static final String TASK_CONFIG_PREFIX = "task.";
    protected static final String DEFAULT_QL_TYPE = "ysql";
    protected static final String DEFAULT_PLUGIN_NAME = "yboutput";
    protected static final boolean DEFAULT_DROP_SLOT_ON_STOP = false;
    protected static final String DEFAULT_PUBLICATION_AUTOCREATE_MODE = "all_tables";

    protected static final int DEFAULT_PORT = 5_433;
    protected static final int DEFAULT_SNAPSHOT_FETCH_SIZE = 10_240;
    protected static final int DEFAULT_MAX_RETRIES = 6;
    protected static final long DEFAULT_RETRY_DELAY_MS = 10000;
    protected static final int DEFAULT_MASTER_PORT = 7100;
    protected static final String DEFAULT_MASTER_ADDRESS = "127.0.0.1:7100";
    protected static final int DEFAULT_MAX_NUM_TABLETS = 300;
    protected static final long DEFAULT_ADMIN_OPERATION_TIMEOUT_MS = 900000; //15 minutes
    protected static final long DEFAULT_OPERATION_TIMEOUT_MS = 900000;  //15 minutes
    protected static final long DEFAULT_SOCKET_READ_TIMEOUT_MS = 60000;
    protected static final int DEFAULT_MAX_RPC_RETRY_ATTEMPTS = 1800; // Number of retries, large enough, to last till timeout
    protected static final int DEFAULT_RPC_RETRY_SLEEP_TIME_MS = 500;
    protected static final long DEFAULT_CDC_POLL_INTERVAL_MS = 500;
    protected static final int DEFAULT_MAX_CONNECTOR_RETRIES = 5;
    protected static final long DEFAULT_CONNECTOR_RETRY_DELAY_MS = 60000;
    protected static final boolean DEFAULT_LIMIT_ONE_POLL_PER_ITERATION = false;
    protected static final boolean DEFAULT_LOG_GET_CHANGES = false;
    protected static final long DEFAULT_NEW_TABLE_POLL_INTERVAL_MS = 5 * 60 * 1000L;
    protected static final long DEFAULT_LOG_GET_CHANGES_INTERVAL_MS = 5 * 60 * 1000L;
    public static final int DEFAULT_MBEAN_REGISTRATION_RETRIES = 12;
    public static final long DEFAULT_MBEAN_REGISTRATION_RETRY_DELAY_MS = 5_000;
    public static final long DEFAULT_LAST_CALLBACK_TIMEOUT_MS = 3 * 60 * 1000;
    public static final Pattern YB_HOSTNAME_PATTERN = Pattern.compile("^[a-zA-Z0-9-_.,:]+$");

    @Override
    public JdbcConfiguration getJdbcConfig() {
        return super.getJdbcConfig();
    }

    public static final Field HOSTNAME = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.HOSTNAME)
            .withDisplayName("Hostname")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 2))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .required()
            .withValidation(YugabyteDBConnectorConfig::validateYBHostname)
            .withDescription("Resolvable hostname or IP address of the database server.");

    public static final Field PORT = RelationalDatabaseConnectorConfig.PORT
            .withDefault(DEFAULT_PORT);

    public static final Field MASTER_ADDRESSES = Field.create(DATABASE_CONFIG_PREFIX + "master.addresses")
            .withDisplayName("Master Addresses")
            .withType(Type.STRING)
            .withImportance(Importance.HIGH)
            .withDefault(DEFAULT_MASTER_ADDRESS)
            .withDescription("Comma separated values of master addresses in the form host:port")
            .required();

    public static final Field STREAM_ID = Field.create(DATABASE_CONFIG_PREFIX + "streamid")
            .withDisplayName("YugabyteDB DB Stream ID")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION, 9))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.HIGH)
            .withDescription("ID of the Stream created in YugabyteDB");

    protected static final Field TASK_ID = Field.create("yugabytedb.task.id")
            .withDescription("Internal use only")
            .withValidation(Field::isInteger)
            .withInvisibleRecommender();

    protected static final Field SEND_BEFORE_IMAGE = Field.create("yugabytedb.send.before.image")
            .withDescription("Internal use only")
            .withValidation(Field::isBoolean)
            .withInvisibleRecommender();

    protected static final Field ENABLE_EXPLICIT_CHECKPOINTING = Field.create("yugabytedb.enable.explicit.checkpointing")
            .withDescription("Internal use only")
            .withValidation(Field::isBoolean)
            .withInvisibleRecommender();

    public static final Field HASH_RANGES_LIST = Field.create(TASK_CONFIG_PREFIX + ".hash.ranges.list")
            .withDisplayName("YugabyteDB tablet list with hash ranges")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withDescription("Internal task config: List of TabletIds to be fetched by this task");

    public static final Field MAX_NUM_TABLETS = Field.create("table.max.num.tablets")
            .withDisplayName("Maximum number of tablets that can be polled for in a table")
            .withType(Type.INT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_MAX_NUM_TABLETS)
            .withDescription("Specify the maximum number of tablets that the client can poll for");

    public static final Field ADMIN_OPERATION_TIMEOUT_MS = Field.create("admin.operation.timeout.ms")
            .withDisplayName("Admin operation timeout in milliseconds")
            .withType(Type.LONG)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_ADMIN_OPERATION_TIMEOUT_MS)
            .withDescription("Timeout after which the admin operations for the yb-client would fail");

    public static final Field OPERATION_TIMEOUT_MS = Field.create("operation.timeout.ms")
            .withDisplayName("Operation timeout in milliseconds")
            .withType(Type.LONG)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_OPERATION_TIMEOUT_MS);

    public static final Field MAX_RPC_RETRY_ATTEMPTS = Field.create("max.rpc.retry.attempts")
            .withDisplayName("Maximum number of RPC retry attempts")
            .withType(Type.INT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_MAX_RPC_RETRY_ATTEMPTS);

    public static final Field RPC_RETRY_SLEEP_TIME = Field.create("rpc.retry.sleep.time.ms")
            .withDisplayName("The base sleep time used for back-offs during rpc retries")
            .withType(Type.INT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_RPC_RETRY_SLEEP_TIME_MS);

    public static final Field SOCKET_READ_TIMEOUT_MS = Field.create("socket.read.timeout.ms")
            .withDisplayName("Socket read timeout in milliseconds")
            .withType(Type.LONG)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_SOCKET_READ_TIMEOUT_MS);

    public static final Field CDC_POLL_INTERVAL_MS = Field.create("cdc.poll.interval.ms")
            .withDisplayName("Poll interval in milliseconds to get changes from database")
            .withType(Type.LONG)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_CDC_POLL_INTERVAL_MS)
            .withDescription("The poll interval in milliseconds at which the client will request for changes from the database");

    public static final Field CDC_LIMIT_POLL_PER_ITERATION = Field.create("cdc.poll.limit")
            .withDisplayName("Limit number of polls per iteration to 1")
            .withType(Type.BOOLEAN)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_LIMIT_ONE_POLL_PER_ITERATION)
            .withDescription("Retrict polling of CDC RPC to once per iteration.");

    public static final Field MAX_CONNECTOR_RETRIES = Field.create("max.connector.retries")
            .withDisplayName("Maximum number of retries a connector can have")
            .withType(Type.INT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_MAX_CONNECTOR_RETRIES)
            .withDescription("The maximum number of times a connector can retry to get the changes from the server.");

    public static final Field CONNECTOR_RETRY_DELAY_MS = Field.create("connector.retry.delay.ms")
            .withDisplayName("Delay after which connector will attempt a retry")
            .withType(Type.LONG)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_CONNECTOR_RETRY_DELAY_MS)
            .withDescription("The amount of time after which the connector will attempt to retry to get the changes from the server.");

    public static final Field MBEAN_REGISTRATION_RETRIES = Field.create("mbean.registration.retries")
            .withDisplayName("Number of attempts for registering the metrics MBean")
            .withType(Type.INT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_MBEAN_REGISTRATION_RETRIES);

    public static final Field MBEAN_REGISTRATION_RETRY_DELAY_MS = Field.create("mbean.registration.retry.delay.ms")
            .withDisplayName("Retry interval between successive attempts to register metrics MBean")
            .withType(Type.LONG)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_MBEAN_REGISTRATION_RETRY_DELAY_MS);

    public static final Field IGNORE_EXCEPTIONS = Field.create("ignore.exceptions")
            .withDisplayName("Flag to ignore exceptions which do not cause an issue while execution")
            .withType(Type.BOOLEAN)
            .withImportance(Importance.LOW)
            .withDefault(false);

    public static final Field FORCE_USE_IMPLICIT_STREAM = Field.create("force.use.implicit.stream")
              .withDisplayName("Flag to forcefully use implicit streams in connector")
              .withType(Type.BOOLEAN)
              .withImportance(Importance.LOW)
              .withDefault(false);

    public static final Field CHAR_SET = Field.create(TASK_CONFIG_PREFIX + "charset")
            .withDisplayName("YugabyteDB charset")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withDescription("Internal task config: Charset for the YugabyteDB instance.");

    public static final Field NAME_TO_TYPE = Field.create(TASK_CONFIG_PREFIX + "nametotype")
            .withDisplayName("YugabyteDB Name to Type map")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withDescription("Internal task config: Name to type map used in decoding");

    public static final Field OID_TO_TYPE = Field.create(TASK_CONFIG_PREFIX + "oidtotype")
            .withDisplayName("YugabyteDB Oid to type map")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withDescription("Internal task config: Oid to type map used in decoding");

    public static final Field PLUGIN_NAME = Field.create("plugin.name")
            .withDisplayName("Plugin")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 0))
            .withEnum(LogicalDecoder.class, LogicalDecoder.YBOUTPUT)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDefault(DEFAULT_PLUGIN_NAME)
            .withDescription("The name of the Postgres logical decoding plugin installed on the server. " +
                    "Supported values are '" + LogicalDecoder.YBOUTPUT.getValue()
                    + "'. " +
                    "Defaults to '" + LogicalDecoder.YBOUTPUT.getValue() + "'.");

    public static final Field SLOT_NAME = Field.create("slot.name")
            .withDisplayName("Slot")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 1))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDefault(ReplicationConnection.Builder.DEFAULT_SLOT_NAME)
            .withValidation(YugabyteDBConnectorConfig::validateReplicationSlotName)
            .withDescription("The name of the YSQL logical decoding slot created for streaming changes from a plugin." +
                    "Defaults to 'debezium");
    
    public static final Field DROP_SLOT_ON_STOP = Field.create("slot.drop.on.stop")
            .withDisplayName("Drop slot on stop")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 3))
            .withDefault(DEFAULT_DROP_SLOT_ON_STOP)
            .withImportance(Importance.MEDIUM)
            .withDescription(
                    "Whether or not to drop the logical replication slot when the connector finishes orderly" +
                            "By default the replication is kept so that on restart progress can resume from the last recorded location");

    public static final Field PUBLICATION_NAME = Field.create("publication.name")
            .withDisplayName("Publication")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 8))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDefault(ReplicationConnection.Builder.DEFAULT_PUBLICATION_NAME)
            .withDescription("The name of the YSQL publication used for streaming changes from a plugin." +
                    "Defaults to '" + ReplicationConnection.Builder.DEFAULT_PUBLICATION_NAME + "'");

    public static final Field DELETE_STREAM_ON_STOP = Field.create("stream.delete.on.stop")
            .withDisplayName("Delete stream on stop")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 3))
            .withDefault(false)
            .withImportance(Importance.MEDIUM)
            .withDescription(
                    "Whether or not to delete the logical replication stream when the connector finishes orderly" +
                            "By default the replication is kept so that on restart progress can resume from the last recorded location");

    public enum AutoCreateMode implements EnumeratedValue {
        /**
         * No Publication will be created, it's expected the user
         * has already created the publication.
         */
        DISABLED("disabled"),
        /**
         * Enable publication for all tables.
         */
        ALL_TABLES("all_tables"),
        /**
         * Enable publication on a specific set of tables.
         */
        FILTERED("filtered");

        private final String value;

        AutoCreateMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static AutoCreateMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (AutoCreateMode option : AutoCreateMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value        the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static AutoCreateMode parse(String value, String defaultValue) {
            AutoCreateMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    public static final Field PUBLICATION_AUTOCREATE_MODE = Field.create("publication.autocreate.mode")
            .withDisplayName("Publication Auto Create Mode")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 9))
            .withEnum(AutoCreateMode.class, AutoCreateMode.ALL_TABLES)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDefault(DEFAULT_PUBLICATION_AUTOCREATE_MODE)
            .withDescription(
                    "Applies only when streaming changes using yboutput." +
                    "Determine how creation of a publication should work, the default is all_tables." +
                    "DISABLED - The connector will not attempt to create a publication at all. The expectation is " +
                    "that the user has created the publication up-front. If the publication isn't found to exist upon " +
                    "startup, the connector will throw an exception and stop." +
                    "ALL_TABLES - If no publication exists, the connector will create a new publication for all tables. " +
                    "Note this requires that the configured user has access. If the publication already exists, it will be used" +
                    ". i.e CREATE PUBLICATION <publication_name> FOR ALL TABLES;" +
                    "FILTERED - If no publication exists, the connector will create a new publication for all those tables matching" +
                    "the current filter configuration (see table/database include/exclude list properties). If the publication already" +
                    " exists, it will be used. i.e CREATE PUBLICATION <publication_name> FOR TABLE <tbl1, tbl2, etc>");

    public static final Field STREAM_PARAMS = Field.create("slot.stream.params")
            .withDisplayName("Optional parameters to pass to the logical decoder when the stream is started.")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 2))
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription(
                    "Any optional parameters used by logical decoding plugin. Semi-colon separated. E.g. 'add-tables=public.table,public.table2;include-lsn=true'");

    public static final Field MAX_RETRIES = Field.create("slot.max.retries")
            .withDisplayName("Retry count")
            .withType(Type.INT)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 4))
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_MAX_RETRIES)
            .withValidation(Field::isInteger)
            .withDescription("How many times to retry connecting to a replication slot when an attempt fails.");

    public static final Field RETRY_DELAY_MS = Field.create("slot.retry.delay.ms")
            .withDisplayName("Retry delay")
            .withType(Type.LONG)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 5))
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_RETRY_DELAY_MS)
            .withValidation(Field::isInteger)
            .withDescription(
                    "Time to wait between retry attempts when the connector fails to connect to a replication slot, given in milliseconds. Defaults to 10 seconds (10,000 ms).");

    public static final Field ON_CONNECT_STATEMENTS = Field.create(DATABASE_CONFIG_PREFIX + JdbcConfiguration.ON_CONNECT_STATEMENTS)
            .withDisplayName("Initial statements")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.LOW)
            .withDescription("A semicolon separated list of SQL statements to be executed when a JDBC connection to the database is established. "
                    + "Note that the connector may establish JDBC connections at its own discretion, so this should typically be used for configuration"
                    + "of session parameters only, but not for executing DML statements. Use doubled semicolon (';;') to use a semicolon as a character "
                    + "and not as a delimiter.");

    public static final Field SSL_MODE = Field.create(DATABASE_CONFIG_PREFIX + "sslmode")
            .withDisplayName("SSL mode")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 0))
            .withEnum(SecureConnectionMode.class, SecureConnectionMode.DISABLED)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Whether to use an encrypted connection to Postgres. Options include"
                    + "'disable' (the default) to use an unencrypted connection; "
                    + "'require' to use a secure (encrypted) connection, and fail if one cannot be established; "
                    + "'verify-ca' like 'required' but additionally verify the server TLS certificate against the configured Certificate Authority "
                    + "(CA) certificates, or fail if no valid matching CA certificates are found; or"
                    + "'verify-full' like 'verify-ca' but additionally verify that the server certificate matches the host to which the connection is attempted.");

    public static final Field SSL_CLIENT_CERT = Field.create(DATABASE_CONFIG_PREFIX + "sslcert")
            .withDisplayName("SSL Client Certificate")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 1))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("File containing the SSL Certificate for the client. See the Postgres SSL docs for further information");

    public static final Field SSL_CLIENT_KEY = Field.create(DATABASE_CONFIG_PREFIX + "sslkey")
            .withDisplayName("SSL Client Key")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 4))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("File containing the SSL private key for the client. See the Postgres SSL docs for further information");

    public static final Field SSL_CLIENT_KEY_PASSWORD = Field.create(DATABASE_CONFIG_PREFIX + "sslpassword")
            .withDisplayName("SSL Client Key Password")
            .withType(Type.PASSWORD)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 2))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("Password to access the client private key from the file specified by 'database.sslkey'. See the Postgres SSL docs for further information");

    public static final Field SSL_ROOT_CERT = Field.create(DATABASE_CONFIG_PREFIX + "sslrootcert")
            .withDisplayName("SSL Root Certificate")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 3))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("File containing the root certificate(s) against which the server is validated. See the Postgres JDBC SSL docs for further information");

    public static final Field SSL_SOCKET_FACTORY = Field.create(DATABASE_CONFIG_PREFIX + "sslfactory")
            .withDisplayName("SSL Root Certificate")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_SSL, 5))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription(
                    "A name of class to that creates SSL Sockets. Use org.postgresql.ssl.NonValidatingFactory to disable SSL validation in development environments");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withValidation((config, field, output) -> {
                if (config.getString(field).toLowerCase().equals(SnapshotMode.EXPORTED.getValue())) {
                    LOGGER.warn("Value '{}' of 'snapshot.mode' option is deprecated, use '{}' instead",
                            SnapshotMode.EXPORTED.getValue(), SnapshotMode.INITIAL.getValue());
                }
                return 0;
            })
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Options include: "
                    + "'always' to specify that the connector run a snapshot each time it starts up; "
                    + "'initial' (the default) to specify the connector can run a snapshot only when no offsets are available for the logical server name; "
                    + "'initial_only' same as 'initial' except the connector should stop after completing the snapshot and before it would normally start emitting changes;"
                    + "'never' to specify the connector should never run a snapshot and that upon first startup the connector should read from the last position (LSN) recorded by the server; and"
                    + "'exported' deprecated, use 'initial' instead; "
                    + "'custom' to specify a custom class with 'snapshot.custom_class' which will be loaded and used to determine the snapshot, see docs for more details.");

    // This property is not supported by YugabyteDB and has been copied from PostgreSQL connector to keep things
    // compilable. In the code, wherever this field has been referenced, it is purely for completeness perspective
    // and we do not use it with YugabyteDB.
    public static final Field SNAPSHOT_LOCKING_MODE = Field.create("snapshot.locking.mode")
            .withDisplayName("Snapshot locking mode")
            .withEnum(SnapshotLockingMode.class, SnapshotLockingMode.NONE)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 13))
            .withDescription("Controls how the connector holds locks on tables while performing the schema snapshot. The 'shared' "
                    + "which means the connector will hold a table lock that prevents exclusive table access for just the initial portion of the snapshot "
                    + "while the database schemas and other metadata are being read. The remaining work in a snapshot involves selecting all rows from "
                    + "each table, and this is done using a flashback query that requires no locks. However, in some cases it may be desirable to avoid "
                    + "locks entirely which can be done by specifying 'none'. This mode is only safe to use if no schema changes are happening while the "
                    + "snapshot is taken.");

    public static final Field AUTO_ADD_NEW_TABLES = Field.create("auto.add.new.tables")
            .withDisplayName("Auto add new tables")
            .withImportance(Importance.LOW)
            .withType(Type.BOOLEAN)
            .withDefault(true)
            .withValidation(Field::isBoolean)
            .withDescription("Whether or not to automatically fetch and add new tables to the connector.");

    public static final Field NEW_TABLE_POLL_INTERVAL_MS = Field.create("new.table.poll.interval.ms")
            .withDisplayName("New table poll interval ms")
            .withImportance(Importance.MEDIUM)
            .withType(Type.LONG)
            .withDefault(DEFAULT_NEW_TABLE_POLL_INTERVAL_MS)
            .withValidation(Field::isNonNegativeLong)
            .withDescription("Interval at which the poller thread should poll to check if there are any new tables added to the stream");

    public static final Field LOG_GET_CHANGES = Field.create("log.get.changes")
            .withDisplayName("Whether to log GetChanges requests")
            .withImportance(Importance.LOW)
            .withType(Type.BOOLEAN)
            .withDefault(DEFAULT_LOG_GET_CHANGES)
            .withValidation(Field::isBoolean)
            .withDescription("Whether the connector should log GetChanges requests it is making to the service");

    public static final Field LOG_GET_CHANGES_INTERVAL_MS = Field.create("log.get.changes.interval.ms")
            .withDisplayName("Interval to log GetChanges request in milliseconds")
            .withImportance(Importance.LOW)
            .withType(Type.LONG)
            .withDefault(DEFAULT_LOG_GET_CHANGES_INTERVAL_MS)
            .withValidation(Field::isNonNegativeLong);

    public static final Field SNAPSHOT_MODE_CLASS = Field.create("snapshot.custom.class")
            .withDisplayName("Snapshot Mode Custom Class")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 9))
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withValidation((config, field, output) -> {
                if (config.getString(SNAPSHOT_MODE).toLowerCase().equals("custom") && config.getString(field).isEmpty()) {
                    output.accept(field, "", "snapshot.custom_class cannot be empty when snapshot.mode 'custom' is defined");
                    return 1;
                }
                return 0;
            })
            .withDescription(
                    "When 'snapshot.mode' is set as custom, this setting must be set to specify a fully qualified class name to load (via the default class loader)."
                            + "This class must implement the 'Snapshotter' interface and is called on each app boot to determine whether to do a snapshot and how to build queries.");

    public static final Field TRUNCATE_HANDLING_MODE = Field.create("truncate.handling.mode")
            .withDisplayName("Truncate handling mode")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 23))
            .withEnum(TruncateHandlingMode.class, TruncateHandlingMode.SKIP)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withValidation(YugabyteDBConnectorConfig::validateTruncateHandlingMode)
            .withDescription("Specify how TRUNCATE operations are handled for change events (supported only on pg11+ pgoutput plugin), including: " +
                    "'skip' to skip / ignore TRUNCATE events (default), " +
                    "'include' to handle and include TRUNCATE events");

    public static final Field OVERRIDE_TRANSACTION_ORDERING_DEPRECATION = Field.create("TEST.override.transaction.ordering.deprecation")
            .withDisplayName("Internal config to override and forcefully use transaction ordering")
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withType(Type.BOOLEAN);

    // This field is now deprecated and its usage will be removed in future releases.
    public static final Field TRANSACTION_ORDERING = Field.create("transaction.ordering")
           .withDisplayName("Order transactions")
           .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 23))
           .withImportance(Importance.HIGH)
           .withDefault(false)
           .withType(Type.BOOLEAN)
           .withValidation((config, field, output) -> {
               if (config.getBoolean(field) && !config.getBoolean(OVERRIDE_TRANSACTION_ORDERING_DEPRECATION)) {
                   final String errorMessage =
                     "transaction.ordering is disabled with gRPC connector, use CDC with logical " +
                       "replication to leverage transaction ordering " +
                       "https://docs.yugabyte.com/preview/explore/change-data-capture/using-logical-replication/";
                   LOGGER.error(errorMessage);
                   output.accept(field, "", errorMessage);
                   return 1;
               }

               return 0;
           })
           .withDescription("Specify whether the transactions need to be ordered");

    public static final Field CONSISTENCY_MODE = Field.create("consistency.mode")
            .withDisplayName("Transaction Consistency mode")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 23))
            .withEnum(ConsistencyMode.class, ConsistencyMode.DEFAULT)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withValidation(YugabyteDBConnectorConfig::validateTruncateHandlingMode)
            .withDescription("Specify how transactions should be managed when streaming events: " +
                    "'default' no consistency grouping is done. Events are generated when received, " +
                    "'key' Consistency grouping is at primary key level, " +
                    "'global' Consistency grouping is at global level across all transactions");

    public static final Field LAST_CALLBACK_TIMEOUT_MS = Field.create("last.callback.timeout.ms")
           .withDisplayName("Last callback timeout")
           .withImportance(Importance.LOW)
           .withValidation(Field::isNonNegativeLong)
           .withDefault(DEFAULT_LAST_CALLBACK_TIMEOUT_MS)
           .withDescription("Time to wait for commit callback before attempting " +
                            "a failure handling assuming that we have not received any callback");

    /**
     * A comma-separated list of regular expressions that match the prefix of logical decoding messages to be excluded
     * from monitoring. Must not be used with {@link #LOGICAL_DECODING_MESSAGE_PREFIX_INCLUDE_LIST}
     */
    public static final Field LOGICAL_DECODING_MESSAGE_PREFIX_EXCLUDE_LIST = Field.create("message.prefix.exclude.list")
            .withDisplayName("Exclude Logical Decoding Message Prefixes")
            .withType(Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 25))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isListOfRegex, YugabyteDBConnectorConfig::validateLogicalDecodingMessageExcludeList)
            .withDescription("A comma-separated list of regular expressions that match the logical decoding message prefixes to be excluded from monitoring.");

    /**
     * A comma-separated list of regular expressions that match the prefix of logical decoding messages to be monitored.
     * Must not be used with {@link #LOGICAL_DECODING_MESSAGE_PREFIX_EXCLUDE_LIST}
     */
    public static final Field LOGICAL_DECODING_MESSAGE_PREFIX_INCLUDE_LIST = Field.create("message.prefix.include.list")
            .withDisplayName("Include Logical Decoding Message Prefixes")
            .withType(Type.LIST)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 24))
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withValidation(Field::isListOfRegex)
            .withDescription(
                    "A comma-separated list of regular expressions that match the logical decoding message prefixes to be monitored. All prefixes are monitored by default.");

    public static final Field HSTORE_HANDLING_MODE = Field.create("hstore.handling.mode")
            .withDisplayName("HStore Handling")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 22))
            .withEnum(HStoreHandlingMode.class, HStoreHandlingMode.JSON)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Specify how HSTORE columns should be represented in change events, including:"
                    + "'json' represents values as string-ified JSON (default)"
                    + "'map' represents values as a key/value map");

    public static final Field INTERVAL_HANDLING_MODE = Field.create("interval.handling.mode")
            .withDisplayName("Interval Handling")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR, 21))
            .withEnum(IntervalHandlingMode.class, IntervalHandlingMode.NUMERIC)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDescription("Specify how INTERVAL columns should be represented in change events, including:"
                    + "'string' represents values as an exact ISO formatted string"
                    + "'numeric' (default) represents values using the inexact conversion into microseconds");

    public static final Field STATUS_UPDATE_INTERVAL_MS = Field.create("status.update.interval.ms")
            .withDisplayName("Status update interval (ms)")
            .withType(Type.INT) // Postgres doesn't accept long for this value
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 6))
            .withDefault(10_000)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Frequency for sending replication connection status updates to the server, given in milliseconds. Defaults to 10 seconds (10,000 ms).")
            .withValidation(Field::isPositiveInteger);

    public static final Field TCP_KEEPALIVE = Field.create(DATABASE_CONFIG_PREFIX + "tcpKeepAlive")
            .withDisplayName("TCP keep-alive probe")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED, 0))
            .withDefault(true)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Enable or disable TCP keep-alive probe to avoid dropping TCP connection")
            .withValidation(Field::isBoolean);

    public static final Field INCLUDE_UNKNOWN_DATATYPES = Field.create("include.unknown.datatypes")
            .withDisplayName("Include unknown datatypes")
            .withType(Type.BOOLEAN)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 1))
            .withDefault(false)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specify whether the fields of data type not supported by Debezium should be processed:"
                    + "'false' (the default) omits the fields; "
                    + "'true' converts the field into an implementation dependent binary representation.");

    public static final Field SCHEMA_REFRESH_MODE = Field.create("schema.refresh.mode")
            .withDisplayName("Schema refresh mode")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 0))
            .withEnum(SchemaRefreshMode.class, SchemaRefreshMode.COLUMNS_DIFF)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("Specify the conditions that trigger a refresh of the in-memory schema for a table. " +
                    "'columns_diff' (the default) is the safest mode, ensuring the in-memory schema stays in-sync with " +
                    "the database table's schema at all times. " +
                    "'columns_diff_exclude_unchanged_toast' instructs the connector to refresh the in-memory schema cache if there is a discrepancy between it " +
                    "and the schema derived from the incoming message, unless unchanged TOASTable data fully accounts for the discrepancy. " +
                    "This setting can improve connector performance significantly if there are frequently-updated tables that " +
                    "have TOASTed data that are rarely part of these updates. However, it is possible for the in-memory schema to " +
                    "become outdated if TOASTable columns are dropped from the table.");

    /*
     * public static final Field XMIN_FETCH_INTERVAL = Field.create("xmin.fetch.interval.ms")
     * .withDisplayName("Xmin fetch interval (ms)")
     * .withType(Type.LONG)
     * .withGroup(Field.createGroupEntry(Field.Group.CONNECTION_ADVANCED_REPLICATION, 7))
     * .withWidth(Width.SHORT)
     * .withDefault(0L)
     * .withImportance(Importance.MEDIUM)
     * .withDescription("Specify how often (in ms) the xmin will be fetched from the replication slot. " +
     * "This xmin value is exposed by the slot which gives a lower bound of where a new replication slot could start from. " +
     * "The lower the value, the more likely this value is to be the current 'true' value, but the bigger the performance cost. " +
     * "The bigger the value, the less likely this value is to be the current 'true' value, but the lower the performance penalty. " +
     * "The default is set to 0 ms, which disables tracking xmin.")
     * .withValidation(Field::isNonNegativeLong);
     */

    public static final Field TOASTED_VALUE_PLACEHOLDER = Field.create("toasted.value.placeholder")
            .withDisplayName("Toasted value placeholder")
            .withType(Type.STRING)
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_ADVANCED, 2))
            .withWidth(Width.MEDIUM)
            .withDefault("__debezium_unavailable_value")
            .withImportance(Importance.MEDIUM)
            .withDescription("Specify the constant that will be provided by Debezium to indicate that " +
                    "the original value is a toasted value not provided by the database. " +
                    "If starts with 'hex:' prefix it is expected that the rest of the string repesents hexadecimally encoded octets.");

    private final TruncateHandlingMode truncateHandlingMode;
    private final ConsistencyMode consistencyMode;
    private final HStoreHandlingMode hStoreHandlingMode;
    private final IntervalHandlingMode intervalHandlingMode;
    private final SnapshotMode snapshotMode;
    private final SchemaRefreshMode schemaRefreshMode;

    private final TableFilter databaseFilter;
    private final TableFilter cqlTableFilter;

    private final boolean isYSQL;

    private final LogicalDecodingMessageFilter logicalDecodingMessageFilter;

    protected final SnapshotLockingMode snapshotLockingMode;

    public YugabyteDBConnectorConfig(Configuration config) {
        // YCQL doesn't have any schema name associated with the tables so in that case we use the table name as is.
        super(config, new SystemTablesPredicate(), YBClientUtils.isYSQLStream(config) ? x -> x.schema() + "." + x.table() : x -> x.table(),
              DEFAULT_SNAPSHOT_FETCH_SIZE, ColumnFilterMode.SCHEMA, !YBClientUtils.isYSQLStream(config) /* useCatalogBeforeSchema */);

        this.isYSQL = YBClientUtils.isYSQLStream(config);
        this.truncateHandlingMode = TruncateHandlingMode.parse(config.getString(YugabyteDBConnectorConfig.TRUNCATE_HANDLING_MODE));
        this.consistencyMode = ConsistencyMode.parse(config.getString(YugabyteDBConnectorConfig.CONSISTENCY_MODE));
        this.logicalDecodingMessageFilter = new LogicalDecodingMessageFilter(config.getString(LOGICAL_DECODING_MESSAGE_PREFIX_INCLUDE_LIST),
                config.getString(LOGICAL_DECODING_MESSAGE_PREFIX_EXCLUDE_LIST));
        String hstoreHandlingModeStr = config.getString(YugabyteDBConnectorConfig.HSTORE_HANDLING_MODE);
        this.hStoreHandlingMode = HStoreHandlingMode.parse(hstoreHandlingModeStr);
        this.intervalHandlingMode = IntervalHandlingMode.parse(config.getString(YugabyteDBConnectorConfig.INTERVAL_HANDLING_MODE));
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
        this.snapshotLockingMode = SnapshotLockingMode.parse(config.getString(SNAPSHOT_LOCKING_MODE), SNAPSHOT_LOCKING_MODE.defaultValueAsString());
        this.schemaRefreshMode = SchemaRefreshMode.parse(config.getString(SCHEMA_REFRESH_MODE));

        this.databaseFilter = new DatabasePredicate();
        this.cqlTableFilter = new CQLTablesPredicate();
    }

    protected String hostname() {
        return getConfig().getString(HOSTNAME);
    }

    protected int port() {
        return getConfig().getInteger(PORT);
    }

    public String databaseName() {
        return getConfig().getString(DATABASE_NAME);
    }

    public String masterAddresses() {
        return getConfig().getString(MASTER_ADDRESSES);
    }

    public String streamId() {
        return getConfig().getString(STREAM_ID);
    };

    public boolean ignoreExceptions() {
        return getConfig().getBoolean(IGNORE_EXCEPTIONS);
    }

    public boolean forceUseImplicitStream() {
        return getConfig().getBoolean(FORCE_USE_IMPLICIT_STREAM);
    }

    public int maxNumTablets() {
        return getConfig().getInteger(MAX_NUM_TABLETS);
    }

    public long adminOperationTimeoutMs() {
        return getConfig().getLong(ADMIN_OPERATION_TIMEOUT_MS);
    }

    public long operationTimeoutMs() {
        return getConfig().getLong(OPERATION_TIMEOUT_MS);
    }

    public int maxRPCRetryAttempts() {
        return getConfig().getInteger(MAX_RPC_RETRY_ATTEMPTS);
    }

    public int rpcRetrySleepTime() {
        return getConfig().getInteger(RPC_RETRY_SLEEP_TIME);
    }

    public long socketReadTimeoutMs() {
        return getConfig().getLong(SOCKET_READ_TIMEOUT_MS);
    }

    public long cdcPollIntervalms() {
        return getConfig().getLong(CDC_POLL_INTERVAL_MS);
    }

    public boolean cdcLimitPollPerIteration() {
        return getConfig().getBoolean(CDC_LIMIT_POLL_PER_ITERATION);
    }

    public int maxConnectorRetries() {
        return getConfig().getInteger(MAX_CONNECTOR_RETRIES);
    }

    public long connectorRetryDelayMs() {
        return getConfig().getLong(CONNECTOR_RETRY_DELAY_MS);
    }

    public boolean logGetChanges() {
        return getConfig().getBoolean(LOG_GET_CHANGES);
    }

    public long logGetChangesIntervalMs() {
        return getConfig().getLong(LOG_GET_CHANGES_INTERVAL_MS);
    }

    public String sslRootCert() {
        return getConfig().getString(SSL_ROOT_CERT);
    }

    public String sslClientCert() {
        return getConfig().getString(SSL_CLIENT_CERT);
    }

    public String sslClientKey() {
        return getConfig().getString(SSL_CLIENT_KEY);
    }

    protected LogicalDecoder plugin() {
        return LogicalDecoder.parse(getConfig().getString(PLUGIN_NAME));
    }

    protected String slotName() {
        return getConfig().getString(SLOT_NAME);
    }

    protected boolean dropSlotOnStop() {
        if (getConfig().hasKey(DROP_SLOT_ON_STOP.name())) {
            return getConfig().getBoolean(DROP_SLOT_ON_STOP);
        }

        // Return default value
        return getConfig().getBoolean(DROP_SLOT_ON_STOP);
    }

    public String publicationName() {
        return getConfig().getString(PUBLICATION_NAME);
    }

    protected AutoCreateMode publicationAutocreateMode() {
        return AutoCreateMode.parse(getConfig().getString(PUBLICATION_AUTOCREATE_MODE));
    }

    protected String streamParams() {
        return getConfig().getString(STREAM_PARAMS);
    }

    protected int maxRetries() {
        return getConfig().getInteger(MAX_RETRIES);
    }

    protected Duration retryDelay() {
        return Duration.ofMillis(getConfig().getInteger(RETRY_DELAY_MS));
    }

    protected Duration statusUpdateInterval() {
        return Duration.ofMillis(getConfig().getLong(YugabyteDBConnectorConfig.STATUS_UPDATE_INTERVAL_MS));
    }

    public TruncateHandlingMode truncateHandlingMode() {
        return truncateHandlingMode;
    }

    public boolean transactionOrdering() {
        return getConfig().getBoolean(TRANSACTION_ORDERING);
    }
    public ConsistencyMode consistencyMode() {
        return consistencyMode;
    }

    protected HStoreHandlingMode hStoreHandlingMode() {
        return hStoreHandlingMode;
    }

    public LogicalDecodingMessageFilter getMessageFilter() {
        return logicalDecodingMessageFilter;
    }

    protected IntervalHandlingMode intervalHandlingMode() {
        return intervalHandlingMode;
    }

    protected boolean includeUnknownDatatypes() {
        return getConfig().getBoolean(INCLUDE_UNKNOWN_DATATYPES);
    }

    public Map<String, ConfigValue> validate() {
        return getConfig().validate(ALL_FIELDS);
    }

    protected Snapshotter getSnapshotter() {
        return this.snapshotMode.getSnapshotter(getConfig());
    }

    protected boolean skipRefreshSchemaOnMissingToastableData() {
        return SchemaRefreshMode.COLUMNS_DIFF_EXCLUDE_UNCHANGED_TOAST == this.schemaRefreshMode;
    }

    protected TableFilter databaseFilter() {
        return this.databaseFilter;
    }

    protected TableFilter cqlTableFilter() {
        return this.cqlTableFilter;
    }

    protected boolean autoAddNewTables() {
        return getConfig().getBoolean(AUTO_ADD_NEW_TABLES);
    }

    protected long newTablePollIntervalMs() {
        return getConfig().getLong(NEW_TABLE_POLL_INTERVAL_MS);
    }

    protected boolean isYSQLDbType() {
        return this.isYSQL;
    }

    public int mbeanRegistrationRetries() {
        return getConfig().getInteger(MBEAN_REGISTRATION_RETRIES);
    }

    public long mbeanRegistrationRetryDelayMs() {
        return getConfig().getLong(MBEAN_REGISTRATION_RETRY_DELAY_MS);
    }

    public long lastCallbackTimeoutMs() {
        return getConfig().getLong(LAST_CALLBACK_TIMEOUT_MS);
    }

    /*
     * protected Duration xminFetchInterval() {
     * return Duration.ofMillis(getConfig().getLong(PostgresConnectorConfig.XMIN_FETCH_INTERVAL));
     * }
     */

    protected byte[] toastedValuePlaceholder() {
        final String placeholder = getConfig().getString(TOASTED_VALUE_PLACEHOLDER);
        if (placeholder.startsWith("hex:")) {
            return Strings.hexStringToByteArray(placeholder.substring(4));
        }
        return placeholder.getBytes();
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return new YugabyteDBSourceInfoStructMaker(Module.name(), Module.version(), this);
    }

    @Override
    public Optional<? extends EnumeratedValue> getSnapshotLockingMode() {
        return Optional.of(this.snapshotLockingMode);
    }

    private static final ConfigDefinition CONFIG_DEFINITION = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
            .name("YugabyteDB")
            .type(
                    HOSTNAME,
                    PORT,
                    USER,
                    PASSWORD,
                    DATABASE_NAME,
                    PLUGIN_NAME,
                    SLOT_NAME,
                    PUBLICATION_NAME,
                    PUBLICATION_AUTOCREATE_MODE,
                    DROP_SLOT_ON_STOP,
                    MASTER_ADDRESSES,
                    STREAM_ID,
                    STREAM_PARAMS,
                    ON_CONNECT_STATEMENTS,
                    SSL_MODE,
                    SSL_CLIENT_CERT,
                    SSL_CLIENT_KEY_PASSWORD,
                    SSL_ROOT_CERT,
                    SSL_CLIENT_KEY,
                    MAX_RETRIES,
                    RETRY_DELAY_MS,
                    SSL_SOCKET_FACTORY,
                    STATUS_UPDATE_INTERVAL_MS,
                    TCP_KEEPALIVE,
                    MAX_NUM_TABLETS,
                    AUTO_ADD_NEW_TABLES,
                    NEW_TABLE_POLL_INTERVAL_MS,
                    LOG_GET_CHANGES,
                    LOG_GET_CHANGES_INTERVAL_MS,
              MBEAN_REGISTRATION_RETRIES,
                    MBEAN_REGISTRATION_RETRY_DELAY_MS)
            .events(
                    INCLUDE_UNKNOWN_DATATYPES)
            .connector(
                    SNAPSHOT_MODE,
                    SNAPSHOT_MODE_CLASS,
                    HSTORE_HANDLING_MODE,
                    BINARY_HANDLING_MODE,
                    INTERVAL_HANDLING_MODE,
                    SCHEMA_REFRESH_MODE,
                    TRUNCATE_HANDLING_MODE,
                    INCREMENTAL_SNAPSHOT_CHUNK_SIZE,
                    TRANSACTION_ORDERING)
            .excluding(INCLUDE_SCHEMA_CHANGES)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    // Source of the validation rules - https://doxygen.postgresql.org/slot_8c.html#afac399f07320b9adfd2c599cf822aaa3
    private static int validateReplicationSlotName(Configuration config, Field field, Field.ValidationOutput problems) {
        final String name = config.getString(field);
        int errors = 0;
        if (name != null) {
            if (!name.matches("[a-z0-9_]{1,63}")) {
                problems.accept(field, name,
                        "Valid replication slot name must contain only digits, lowercase characters and underscores with length <= 63");
                ++errors;
            }
        }
        return errors;
    }

    protected static int validateYBHostname(Configuration config, Field field, Field.ValidationOutput problems) {
        String hostName = config.getString(field);
        int problemCount = 0;

        if (!Strings.isNullOrBlank(hostName)) {
            if (hostName.contains(",") && !hostName.contains(":")) {
                // Basic validation for cases when a user has only specified comma separated IPs which is not the correct format.
                problems.accept(field, hostName, hostName + " has invalid format (specify mutiple hosts in the format ip1:port1,ip2:port2,ip3:port3)");
                ++problemCount;
            }

            if (!YB_HOSTNAME_PATTERN.asPredicate().test(hostName)) {
                problems.accept(field, hostName, hostName + " has invalid format (only the underscore, hyphen, dot, comma, colon and alphanumeric characters are allowed)");
                ++problemCount;
            }
        }

        return problemCount;
    }

    private static int validateTruncateHandlingMode(Configuration config, Field field, Field.ValidationOutput problems) {
        final String value = config.getString(field);
        int errors = 0;
        if (value != null) {
            TruncateHandlingMode truncateHandlingMode = TruncateHandlingMode.parse(value);
            if (truncateHandlingMode == null) {
                List<String> validModes = Arrays.stream(TruncateHandlingMode.values()).map(TruncateHandlingMode::getValue).collect(Collectors.toList());
                String message = String.format("Valid values for %s are %s, but got '%s'", field.name(), validModes, value);
                problems.accept(field, value, message);
                errors++;
                return errors;
            }
            if (truncateHandlingMode == TruncateHandlingMode.INCLUDE) {
                final LogicalDecoder logicalDecoder = LogicalDecoder.parse(config.getString(PLUGIN_NAME));
                if (!logicalDecoder.supportsTruncate()) {
                    String message = String.format(
                            "%s '%s' is not supported with configuration %s '%s'",
                            field.name(), truncateHandlingMode.getValue(), PLUGIN_NAME.name(), logicalDecoder.getValue());
                    problems.accept(field, value, message);
                    errors++;
                }
            }
        }
        return errors;
    }

    private static int validateLogicalDecodingMessageExcludeList(Configuration config, Field field, Field.ValidationOutput problems) {
        String includeList = config.getString(LOGICAL_DECODING_MESSAGE_PREFIX_INCLUDE_LIST);
        String excludeList = config.getString(LOGICAL_DECODING_MESSAGE_PREFIX_EXCLUDE_LIST);

        if (includeList != null && excludeList != null) {
            problems.accept(LOGICAL_DECODING_MESSAGE_PREFIX_EXCLUDE_LIST, excludeList,
                    "\"logical_decoding_message.prefix.include.list\" is already specified");
            return 1;
        }
        return 0;
    }

    public static String extractStreamIdFromSlot(Configuration config) {
        RuntimeException exception = null;
        try (YugabyteDBConnection ybConnection = new YugabyteDBConnection(getJdbcConfig(config),
                YugabyteDBConnection.CONNECTION_GENERAL)) {
            int retryCount = 0;
            while (retryCount <= config.getInteger(MAX_RETRIES)) {
                try (Connection connection = ybConnection.connection();
                      final Statement statement = connection.createStatement()) {
                    String query = "SELECT yb_stream_id FROM pg_replication_slots WHERE slot_name = '"+ config.getString(SLOT_NAME) + "';";
                    final ResultSet rs = statement.executeQuery(query);
                    if (rs.next()) {
                        String streamId = rs.getString("yb_stream_id");
                        return streamId;
                    } else {
                        LOGGER.error("Could not find replication slot with name " + config.getString(SLOT_NAME));
                        return "";
                    }
                } catch (SQLException e) {
                    exception = new RuntimeException(e);
                    retryCount++;
                    if (retryCount > config.getInteger(MAX_RETRIES)) {
                        LOGGER.error("Failed to execute the query on pg_replication_slots. All {} retries failed",
                                config.getInteger(MAX_RETRIES), e);
                        throw new DebeziumException(e);
                    }
                    LOGGER.warn(
                            "Error while trying to execute the query on pg_replication_slots. Will retry, attempt {} out of {}",
                            retryCount, config.getInteger(MAX_RETRIES));
                    e.printStackTrace();

                    pauseBetweenRetries(config.getLong(RETRY_DELAY_MS));
                }
            }
        }

        if (exception != null) {
            throw exception;
        }

        return "";
    }

    public static String extractTableListFromPublication(Configuration config) {
        String publicationName = config.getString(PUBLICATION_NAME);
        Exception exception = null;

        int retryCount = 0;
        while (retryCount <= config.getInteger(MAX_CONNECTOR_RETRIES)) {
            try (YugabyteDBConnection ybConnection = new YugabyteDBConnection(getJdbcConfig(config), YugabyteDBConnection.CONNECTION_GENERAL)) {
                return fetchTableIncludeList(ybConnection, publicationName);
            } catch (SQLException e) {
                retryCount++;
                exception = e;
                if (retryCount > config.getInteger(MAX_CONNECTOR_RETRIES)) {
                    LOGGER.error("Failed to extract table list from publication. All {} retries failed",config.getInteger(MAX_CONNECTOR_RETRIES), e);
                    throw new DebeziumException(e);
                }
                LOGGER.warn("Error while trying to extract table list from publication. Will retry, attempt {} out of {}",
                            retryCount, config.getInteger(MAX_CONNECTOR_RETRIES));
                e.printStackTrace();

                pauseBetweenRetries(config.getLong(CONNECTOR_RETRY_DELAY_MS));
            }
        }

        if (exception != null) {
            throw new DebeziumException(exception);
        }

        return null;
    }

    public static JdbcConfiguration getJdbcConfig(Configuration config) {
            return JdbcConfiguration.copy(Configuration.empty())
                    .with(JdbcConfiguration.DATABASE, config.getString(DATABASE_NAME))
                    .with(JdbcConfiguration.HOSTNAME, config.getString(HOSTNAME))
                    .with(JdbcConfiguration.PORT, config.getString(PORT))
                    .with(JdbcConfiguration.USER, config.getString(USER))
                    .with(JdbcConfiguration.PASSWORD, config.getString(PASSWORD))
                    .withDefault(YugabyteDBConnectorConfig.MAX_RETRIES, config.getInteger(MAX_RETRIES))
                    .with(YugabyteDBConnectorConfig.RETRY_DELAY_MS, config.getLong(RETRY_DELAY_MS))
                    .build();
    }

    public static String fetchTableIncludeList(YugabyteDBConnection ybConnection, String publicationName) throws SQLException {
        try (Connection connection = ybConnection.connection();
              final Statement statement = connection.createStatement()) {
            String query = "SELECT * FROM pg_publication_tables WHERE pubname = '" + publicationName + "';";
            final ResultSet rs = statement.executeQuery(query);
            String tableList = "";
            while (rs.next()) {
                String tableName = rs.getString("tablename");
                String schemaName = rs.getString("schemaname");
                tableList += schemaName + "." + tableName + ",";
            }
            return tableList.substring(0, tableList.length() - 1);
        }
    }

    public static boolean shouldUsePublication(Configuration config) {
        String streamId = config.getString(YugabyteDBConnectorConfig.STREAM_ID);

        // For CQL tables as well as for the config not using Publication/Replication streamId will be non null
        if (streamId == null || streamId.isEmpty()) {
            return true;
        }

        try (YBClient ybClient = YBClientUtils.getYbClient(config)) {
            GetDBStreamInfoResponse getDBStreamInfoResponse = ybClient.getDBStreamInfo(streamId);
            ListCDCStreamsResponse resp = ybClient.listCDCStreams(null, getDBStreamInfoResponse.getNamespaceId(), null);
            for (CDCStreamInfo stream : resp.getStreams()) {
                if (stream.getStreamId().equals(streamId)) {
                    String slotName = stream.getCdcsdkYsqlReplicationSlotName();
                    if (slotName == null || slotName.isEmpty()) {
                        return false;
                    } else {
                        String errorFormat = "Stream ID %s is associated with replication slot %s. Please use slot name in the config instead of Stream ID.";
                        String errorMessage = String.format(errorFormat, streamId, slotName);
                        LOGGER.error(errorMessage);
                        throw new DebeziumException(errorMessage);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Exception while making RPC calls to server");
            throw new DebeziumException(e);
        }

        return false;
    }

    /**
     * This method is borrowed from Debezium Postgres connector. We have moved this
     * method from ReplicationConnection class to config class because we need to
     * extract the stream ID and table include list from replication slot and
     * publication respectively before initializing YugabyteDBConnectorConfig instance
     * @param config Configuration object containg details about Publication, Autocreate modes and Plugin name
     */
    protected static void initPublication(Configuration config) {
        String publicationName = config.getString(PUBLICATION_NAME);
        String plugin = config.getString(PLUGIN_NAME);
        int maxAttempts = config.getInteger(MAX_RETRIES);
        int retryCount = 0;
        YugabyteDBConnectorConfig.AutoCreateMode publicationAutocreateMode = YugabyteDBConnectorConfig.AutoCreateMode
                .parse(config.getString(PUBLICATION_AUTOCREATE_MODE));
        if (YugabyteDBConnectorConfig.LogicalDecoder.YBOUTPUT.equals(YugabyteDBConnectorConfig.LogicalDecoder.parse(plugin))) {
            LOGGER.info("Initializing YbOutput logical decoder publication");
            while (retryCount <= maxAttempts) {
                try {
                    YugabyteDBConnection ybConnection = new YugabyteDBConnection(getJdbcConfig(config),YugabyteDBConnection.CONNECTION_GENERAL);
                    Connection conn = ybConnection.connection();
                    conn.setAutoCommit(false);

                    String selectPublication = String.format("SELECT puballtables FROM pg_publication WHERE pubname = '%s'", publicationName);
                    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(selectPublication)) {
                        if (!rs.next()) {
                            LOGGER.info("Creating new publication '{}' for plugin '{}'", publicationName, plugin);
                            switch (publicationAutocreateMode) {
                                case DISABLED:
                                    throw new ConnectException("Publication autocreation is disabled, please create one and restart the connector.");
                                case ALL_TABLES:
                                    String createPublicationStmt = String.format("CREATE PUBLICATION %s FOR ALL TABLES;", publicationName);
                                    LOGGER.info("Creating Publication with statement '{}'", createPublicationStmt);
                                    // Publication doesn't exist, create it.
                                    stmt.execute(createPublicationStmt);
                                    break;
                                case FILTERED:
                                    createOrUpdatePublicationModeFilterted(stmt, false, config);
                                    break;
                            }
                        } else {
                            switch (publicationAutocreateMode) {
                                case FILTERED:
                                    // Checking that publication can be altered
                                    Boolean allTables = rs.getBoolean(1);
                                    if (allTables) {
                                        throw new DebeziumException(String.format(
                                                "A logical publication for all tables named '%s' for plugin '%s' " +
                                                        "is already active on the server and can not be altered. " +
                                                        "If you need to exclude some tables or include only specific subset, " +
                                                        "please recreate the publication with necessary configuration " +
                                                        "or let plugin recreate it by dropping existing publication. " +
                                                        "Otherwise please change the 'publication.autocreate.mode' property to 'all_tables'.",
                                                publicationName, plugin));
                                    } else {
                                        createOrUpdatePublicationModeFilterted(stmt, true, config);
                                    }
                                    break;
                                default:
                                    LOGGER.trace(
                                            "A logical publication named '{}' for plugin '{}' is already active on the server " +
                                                    "and will be used by the plugin",
                                            publicationName, plugin);

                            }
                        }
                    }
                    conn.commit();
                    conn.setAutoCommit(true);
                    return;
                } catch (SQLException e) {
                    retryCount++;

                    if (retryCount > maxAttempts) {
                        throw new JdbcConnectionException(e);
                    }

                    LOGGER.warn("Error while initializing publication. Will retry, attempt {} out of {}", retryCount, maxAttempts);
                    pauseBetweenRetries(config.getLong(RETRY_DELAY_MS));
                }
            }
        }
    }

    private static void createOrUpdatePublicationModeFilterted(Statement stmt, boolean isUpdate, Configuration config) {
        String publicationName = config.getString(PUBLICATION_NAME);
        String tableFilterString = null;
        String createOrUpdatePublicationStmt;
        int maxAttempts = config.getInteger(MAX_RETRIES);
        int retryCount = 0;
        while (retryCount <= maxAttempts) {
            try {
                Set<TableId> tablesToCapture = determineCapturedTables(config);
                tableFilterString = tablesToCapture.stream().map(TableId::toDoubleQuotedString).collect(Collectors.joining(", "));
                if (tableFilterString.isEmpty()) {
                    throw new DebeziumException(String.format("No table filters found for filtered publication %s", publicationName));
                }

                if (isUpdate) {
                    createOrUpdatePublicationStmt = String.format("ALTER PUBLICATION %s SET TABLE %s;", publicationName, tableFilterString);
                    LOGGER.info("Updating Publication with statement '{}'", createOrUpdatePublicationStmt);
                } else {
                    createOrUpdatePublicationStmt = String.format("CREATE PUBLICATION %s FOR TABLE %s;", publicationName, tableFilterString);
                    LOGGER.info("Creating Publication with statement '{}'", createOrUpdatePublicationStmt);
                }

                stmt.execute(createOrUpdatePublicationStmt);
                break;
            } catch (Exception e) {
                retryCount++;

                if (retryCount > maxAttempts) {
                    throw new ConnectException(
                        String.format("Unable to %s filtered publication %s for %s. %s", isUpdate ? "update" : "create",
                                        publicationName, tableFilterString, e.getMessage()), e);
                }

                LOGGER.warn(
                        String.format("Error while trying to %s filtered publication. Will retry, attempt %d out of %d",
                                isUpdate ? "update" : "create", retryCount, maxAttempts));
                    
                pauseBetweenRetries(config.getLong(RETRY_DELAY_MS));
            }
        }
    }

    private static Set<TableId> determineCapturedTables(Configuration config) throws Exception {
        try (YugabyteDBConnection ybConnection = new YugabyteDBConnection(getJdbcConfig(config),YugabyteDBConnection.CONNECTION_GENERAL)) {
            Set<TableId> allTableIds = ybConnection.readTableNames(config.getString(DATABASE_NAME), null, null, null);

            Set<TableId> capturedTables = new HashSet<>();
            RelationalTableFilters tableFilter = new RelationalTableFilters(config, new SystemTablesPredicate(), x -> x.schema() + "." + x.table(), false /* useCatalogBeforeSchema */);

            for (TableId tableId : allTableIds) {
                if (!tableFilter.dataCollectionFilter().isIncluded(tableId)) {
                    LOGGER.trace("Ignoring table {} as it's not included in the filter configuration", tableId);
                    continue;
                }
                
                LOGGER.trace("Adding table {} to the list of captured tables", tableId);
                capturedTables.add(tableId);
            }

            return capturedTables
                    .stream()
                    .sorted()
                    .collect(Collectors.toCollection(LinkedHashSet::new));
        } catch (Exception e) {
            LOGGER.error("Error while determining captured tables", e);
            throw new DebeziumException(e);
        }
    }

    protected static void initReplicationSlot(Configuration config) {
        boolean createNeeded = !checkIfReplicationSlotExists(config);
        if (!createNeeded) {
            return;
        }
        createReplicationSlot(config);
    }

    private static void createReplicationSlot(Configuration config) {
        String slotName = config.getString(SLOT_NAME);
        String plugin = config.getString(PLUGIN_NAME);
        int maxAttempts = config.getInteger(MAX_RETRIES);
        int retryCount = 0;
        while(retryCount <= maxAttempts) {
            try (YugabyteDBConnection ybConnection = new YugabyteDBConnection(getJdbcConfig(config),YugabyteDBConnection.CONNECTION_GENERAL);
                Connection connection = ybConnection.connection()) {
                String query = "SELECT pg_create_logical_replication_slot('" + slotName + "', '" + plugin + "');";
                ybConnection.execute(query);
                break;
            } catch (Exception e) {
                retryCount++;
                if (retryCount > maxAttempts) {
                    LOGGER.error("Failed to create a replication slot with name {}. All {} attempts failed.", slotName, maxAttempts);
                    throw new DebeziumException(e);
                }

                LOGGER.warn("Error while creating replication slot {}. Will retry, attempt {} out of {}", slotName, retryCount, maxAttempts);
                pauseBetweenRetries(config.getLong(RETRY_DELAY_MS));
            }
        }
    }

    private static boolean checkIfReplicationSlotExists(Configuration config) {
        int maxAttempts = config.getInteger(MAX_RETRIES);
        int retryCount = 0;
        RuntimeException exception = null;
        while(retryCount <= maxAttempts) {
            try (YugabyteDBConnection ybConnection = new YugabyteDBConnection(getJdbcConfig(config),YugabyteDBConnection.CONNECTION_GENERAL);
                Connection connection = ybConnection.connection();
                final Statement statement = connection.createStatement()) {
                String query = "SELECT * FROM pg_replication_slots WHERE slot_name = '" + config.getString(SLOT_NAME) + "';";
                final ResultSet rs = statement.executeQuery(query);
                return rs.next();
            } catch (Exception e) {
                    retryCount++;
                    if (retryCount > maxAttempts) {
                        LOGGER.error("Failed to query pg_replication_slots. All {} attempts failed.", maxAttempts);
                        throw new DebeziumException(e);
                    }
                    exception = new RuntimeException(e);
                    LOGGER.warn("Error while querying pg_replication_slots. Will retry, attempt {} out of {}", retryCount, maxAttempts);
                    pauseBetweenRetries(config.getLong(RETRY_DELAY_MS));
            }
        }

        if (exception != null) {
            throw exception;
        }

        return false;
    }

    public static void pauseBetweenRetries(long delay) {
        try {
            final Metronome retryMetronome = Metronome.parker(Duration.ofMillis(delay), Clock.SYSTEM);
            retryMetronome.pause();
        } catch (InterruptedException ie) {
            LOGGER.warn("Connector retry sleep interrupted by exception: {}", ie);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    public ConnectionFactory getConnectionFactory() {
        String hostName = getJdbcConfig().getHostname();
        return hostName.contains(":")
                ? JdbcConnection.patternBasedFactory(YugabyteDBConnection.MULTI_HOST_URL_PATTERN,
                        org.postgresql.Driver.class.getName(),
                        YugabyteDBConnection.class.getClassLoader(),
                        JdbcConfiguration.PORT.withDefault(YugabyteDBConnectorConfig.PORT.defaultValueAsString()))
                : JdbcConnection.patternBasedFactory(YugabyteDBConnection.SINGLE_HOST_URL_PATTERN,
                        org.postgresql.Driver.class.getName(),
                        YugabyteDBConnection.class.getClassLoader(),
                        JdbcConfiguration.PORT.withDefault(YugabyteDBConnectorConfig.PORT.defaultValueAsString()));
    }

    private static class SystemTablesPredicate implements TableFilter {
        protected static final List<String> SYSTEM_SCHEMAS = Arrays.asList("pg_catalog", "information_schema");

        @Override
        public boolean isIncluded(TableId t) {
            return !SYSTEM_SCHEMAS.contains(t.schema().toLowerCase());
        }
    }

    private class DatabasePredicate implements TableFilter {
        @Override
        public boolean isIncluded(TableId tableId) {
            return Objects.equals(tableId.catalog(), getConfig().getString(DATABASE_NAME));
        }
    }

    private class CQLTablesPredicate implements TableFilter {
        @Override
        public boolean isIncluded(TableId tableId) {
            if (Objects.equals(tableId.catalog(), getConfig().getString(DATABASE_NAME))) {
                return tableIncludeList().contains(tableId.table());
            }
            return false;
        }
    }
}
