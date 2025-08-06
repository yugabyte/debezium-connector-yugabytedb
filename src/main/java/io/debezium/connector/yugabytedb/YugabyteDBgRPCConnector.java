/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import io.debezium.connector.yugabytedb.connection.HashPartition;
import io.debezium.connector.yugabytedb.util.YugabyteDBConnectorUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService.TabletCheckpointPair;
import org.yb.client.*;

import com.google.common.net.HostAndPort;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.spi.schema.DataCollectionId;

/**
 * A Kafka Connect source connector that creates tasks which use YugabyteDB CDC API
 * to receive incoming changes for a database and publish them to Kafka.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in
 * {@link YugabyteDBConnectorConfig}.
 *
 * @author Suranjan Kumar (skumar@yugabyte.com), Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBgRPCConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBgRPCConnector.class);
    private static final long MAX_TIMEOUT = 10000L;

    private Map<String, String> props;
    private Set<String> tableIds;
    private List<HashPartition> hashRanges;
    private YugabyteDBConnectorConfig yugabyteDBConnectorConfig;
    private boolean usePublication;

    private YugabyteDBTablePoller tableMonitorThread;

    public YugabyteDBgRPCConnector() {
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return YugabyteDBConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        LOGGER.debug("Props " + props);
        Configuration config = Configuration.from(this.props);

        String streamId = config.getString(YugabyteDBConnectorConfig.STREAM_ID);
        String tableIncludeList = config.getString(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST);
        
        usePublication = YugabyteDBConnectorConfig.shouldUsePublication(config);

        if (usePublication) {
            YugabyteDBConnectorConfig.initPublication(config);
            tableIncludeList = YugabyteDBConnectorConfig.extractTableListFromPublication(config);
            YugabyteDBConnectorConfig.initReplicationSlot(config);
            streamId = YugabyteDBConnectorConfig.extractStreamIdFromSlot(config);
        }

        config = config.edit()
                        .with(YugabyteDBConnectorConfig.STREAM_ID, streamId)
                        .with(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST, tableIncludeList)
                        .build();
        this.yugabyteDBConnectorConfig = new YugabyteDBConnectorConfig(config);
        
        tableMonitorThread = new YugabyteDBTablePoller(yugabyteDBConnectorConfig, context, usePublication);
        if (this.yugabyteDBConnectorConfig.autoAddNewTables()) {
            tableMonitorThread.start();
        }

        // This guard is to disallow users to deploy a connector with more than a single task when
        // transaction ordering is enabled
        if (this.yugabyteDBConnectorConfig.transactionOrdering() && config.getInteger("tasks.max") != 1) {
            throw new ConnectException("Transaction ordering is only supported with 1 task, "
                                        + "change number of tasks and try again");
        }
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs;

        if (props == null) {
            LOGGER.error("Configuring a maximum of {} tasks with no connector configuration" +
                    " available", maxTasks);
            return Collections.emptyList();
        }
        String serializedNameToType = "";
        String serializedOidToType = "";
        try (YugabyteDBConnection connection = new YugabyteDBConnection(yugabyteDBConnectorConfig.getJdbcConfig(), YugabyteDBConnection.CONNECTION_GENERAL)) {
            if (yugabyteDBConnectorConfig.isYSQLDbType()) {
                YugabyteDBTypeRegistry typeRegistry = new YugabyteDBTypeRegistry(connection);
                Map<String, YugabyteDBType> nameToType = typeRegistry.getNameToType();
                Map<Integer, YugabyteDBType> oidToType = typeRegistry.getOidToType();
                try {
                    serializedNameToType = ObjectUtil.serializeObjectToString(nameToType);
                    LOGGER.debug("The serializedNameToType " + serializedNameToType);
                    Object test = ObjectUtil.deserializeObjectFromString(serializedNameToType);
                    LOGGER.debug("The deserializedNameToType " + test);
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }

                try {
                    serializedOidToType = ObjectUtil.serializeObjectToString(oidToType);
                    LOGGER.debug("The serializedOidToType " + serializedOidToType);
                } catch (IOException e) {
                    e.printStackTrace();
                }

                if (usePublication) {
                    // Refresh the table include list in case of Alter publication
                    Configuration config = this.yugabyteDBConnectorConfig.getConfig();
                    String tableIncludeList =  YugabyteDBConnectorConfig.extractTableListFromPublication(config);
                    config  = config.edit()
                                    .with(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST, tableIncludeList)
                                    .build();
                    this.yugabyteDBConnectorConfig = new YugabyteDBConnectorConfig(config);
                }
            }
        }
        String charSetName = Charset.forName("UTF-8").name();
        Configuration config = Configuration.from(this.props);
        Map<String, ConfigValue> results = validateAllFields(config);

        validateTServerConnection(results, config);

        String streamIdValue = this.yugabyteDBConnectorConfig.streamId();
        LOGGER.debug("The streamid in config is" + this.yugabyteDBConnectorConfig.streamId());
        LOGGER.debug("The port in config is "+ this.yugabyteDBConnectorConfig.port());

        if (streamIdValue == null) {
            streamIdValue = results.get(YugabyteDBConnectorConfig.STREAM_ID.name()).value().toString();
        }

        boolean sendBeforeImage = false;
        boolean enableExplicitCheckpointing = false;
        try {
            sendBeforeImage = YBClientUtils.isBeforeImageEnabled(this.yugabyteDBConnectorConfig);
            enableExplicitCheckpointing = YBClientUtils.isExplicitCheckpointingEnabled(this.yugabyteDBConnectorConfig);
            LOGGER.info("Before image status: {}", sendBeforeImage);
            LOGGER.info("Explicit checkpointing enabled: {}", enableExplicitCheckpointing);
        } catch (Exception e) {
            LOGGER.error("Error while trying to get before image status", e);
            throw new DebeziumException(e);
        }

        if (!enableExplicitCheckpointing && !this.yugabyteDBConnectorConfig.forceUseImplicitStream()) {
            final String errorMessage = "The provided stream " + this.yugabyteDBConnectorConfig.streamId()
                    + " is an IMPLICIT stream, create a stream with EXPLICIT checkpointing and try again."
                    + " To forcefully use an IMPLICIT stream, set configuration property force.use.implicit.stream"
                    + " to true";
            throw new DebeziumException(errorMessage);
        }

        if (this.yugabyteDBConnectorConfig.transactionOrdering() && !enableExplicitCheckpointing) {
            final String errorMessage = "Explicit checkpointing not enabled in consistent streaming mode, "
                    + "create a stream with explicit checkpointing and try again";
            throw new DebeziumException(errorMessage);
        }

        LOGGER.info("DB stream ID being used: {}", streamIdValue);

        int numGroups = Math.min(hashRanges.size(), maxTasks);
        LOGGER.info("Total tablets to be grouped: " + hashRanges.size() + " within maximum tasks: " + maxTasks);

        List<List<HashPartition>> hashRangesGrouped =
          YugabyteDBConnectorUtils.groupPartitionsSmartly(this.hashRanges, numGroups);

        taskConfigs = new ArrayList<>(hashRangesGrouped.size());

        for (List<HashPartition> taskTables : hashRangesGrouped) {
            Map<String, String> taskProps = new HashMap<>(this.props);
            int taskId = taskConfigs.size();
            taskProps.put(YugabyteDBConnectorConfig.TASK_ID.toString(), String.valueOf(taskId));
            LOGGER.info("Task tables for task {}: {}", taskId, taskTables);

            String hashRangesSerialized = "";
            try {
                hashRangesSerialized = ObjectUtil.serializeObjectToString(taskTables);
                LOGGER.debug("The taskTablesSerialized " + hashRangesSerialized);
            } catch (IOException e) {
                LOGGER.error("Error while serializing task tables");
                e.printStackTrace();
                throw new DebeziumException(e);
            }

            taskProps.put(YugabyteDBConnectorConfig.CHAR_SET.toString(), charSetName);
            taskProps.put(YugabyteDBConnectorConfig.HASH_RANGES_LIST.toString(), hashRangesSerialized);

            if (yugabyteDBConnectorConfig.isYSQLDbType()) {
                taskProps.put(YugabyteDBConnectorConfig.NAME_TO_TYPE.toString(), serializedNameToType);
                taskProps.put(YugabyteDBConnectorConfig.OID_TO_TYPE.toString(), serializedOidToType);
            }

            taskProps.put(YugabyteDBConnectorConfig.STREAM_ID.toString(), streamIdValue);
            taskProps.put(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST.toString(), this.yugabyteDBConnectorConfig.tableIncludeList());
            taskProps.put(YugabyteDBConnectorConfig.SEND_BEFORE_IMAGE.toString(), String.valueOf(sendBeforeImage));
            taskProps.put(YugabyteDBConnectorConfig.ENABLE_EXPLICIT_CHECKPOINTING.toString(), String.valueOf(enableExplicitCheckpointing));
            taskConfigs.add(taskProps);
        }

        LOGGER.debug("Configuring {} YugabyteDB connector task(s)", taskConfigs.size());

        return taskConfigs;
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping table monitoring thread");
        tableMonitorThread.shutdown();
        try {
            tableMonitorThread.join(MAX_TIMEOUT);
        } catch (InterruptedException ie) {
            // Ignore after a warning, shouldn't be interrupted.
            LOGGER.warn("Table monitor thread interrupted by exception: {}", ie.getMessage());
            if (LOGGER.isDebugEnabled()) {
                ie.printStackTrace();
            }
            Thread.currentThread().interrupt();
        }
        this.props = null;
    }

    @Override
    public ConfigDef config() {
        return YugabyteDBConnectorConfig.configDef();
    }

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        final ConfigValue databaseValue = configValues.get(RelationalDatabaseConnectorConfig.DATABASE_NAME.name());
        if (!databaseValue.errorMessages().isEmpty()) {
            return;
        }

        String streamId = config.getString(YugabyteDBConnectorConfig.STREAM_ID);
        String tableIncludeList = config.getString(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST);
        
        usePublication = YugabyteDBConnectorConfig.shouldUsePublication(config);

        if (usePublication) {
            YugabyteDBConnectorConfig.initPublication(config);
            tableIncludeList = YugabyteDBConnectorConfig.extractTableListFromPublication(config);
            YugabyteDBConnectorConfig.initReplicationSlot(config);
            streamId = YugabyteDBConnectorConfig.extractStreamIdFromSlot(config);
        }

        config = config.edit()
                        .with(YugabyteDBConnectorConfig.STREAM_ID, streamId)
                        .with(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST, tableIncludeList)
                        .build();

        this.yugabyteDBConnectorConfig = new YugabyteDBConnectorConfig(config);

        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());

        // Try to connect to the database ...
        if (yugabyteDBConnectorConfig.isYSQLDbType()) {
            try (YugabyteDBConnection connection = new YugabyteDBConnection(yugabyteDBConnectorConfig.getJdbcConfig(), YugabyteDBConnection.CONNECTION_GENERAL)) {
                int retryCount = 0;
                while (retryCount <= yugabyteDBConnectorConfig.maxConnectorRetries()) {
                    try {
                        // Prepare connection without initial statement execution
                        connection.connection(false);
                        // check connection
                        connection.execute("SELECT version()");
                        LOGGER.info("Successfully tested connection for {} with user '{}'",
                                connection.connectionString(),
                                connection.username());
                        break;
                    } catch (SQLException e) {
                        retryCount++;
                        if (retryCount > yugabyteDBConnectorConfig.maxConnectorRetries()) {
                            LOGGER.error("Failed testing connection for {} with user '{}'",
                                    connection.connectionString(),
                                    connection.username(), e);
                            hostnameValue.addErrorMessage("Error while validating connector config: "
                                    + e.getMessage());
                        }
                        LOGGER.warn("Error while trying to test the connection for {} with user '{}' will retry, attempt {} out of {}",
                                connection.connectionString(), connection.username(), retryCount,
                                yugabyteDBConnectorConfig.maxConnectorRetries());
                    }
                }
            }
        }

        validateTServerConnection(configValues, config);
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(YugabyteDBConnectorConfig.ALL_FIELDS);
    }

    protected void validateTServerConnection(Map<String, ConfigValue> configValues,
                                             Configuration config) {
        try (YBClient ybClient = YBClientUtils.getYbClient(yugabyteDBConnectorConfig)) {
            String hostAddress = config.getString(YugabyteDBConnectorConfig.MASTER_ADDRESSES.toString());
            // so whenever they are null, they will just be ignored
            LOGGER.debug("The master host address is " + hostAddress);
            HostAndPort masterHostPort = ybClient.getLeaderMasterHostAndPort();
            if (masterHostPort == null) {
                LOGGER.error("Failed testing connection at {}", yugabyteDBConnectorConfig.hostname());
            }

            // Do a get and check if the streamid exists.
            // TODO: Find out where to do validation for table whitelist
            String streamId = yugabyteDBConnectorConfig.streamId();
            if (streamId == null || streamId.isEmpty()) {
                // Coming to this block means the auto.create.stream is set to false and no stream ID is provided, the connector should not proceed forward.
                throw new DebeziumException("DB Stream ID not provided, please provide a DB stream ID to proceed");
            }

            final ConfigValue streamIdConfig = configValues.get(YugabyteDBConnectorConfig.STREAM_ID.name());

            if (yugabyteDBConnectorConfig.tableIncludeList() == null || yugabyteDBConnectorConfig.tableIncludeList().isEmpty()) {
                throw new DebeziumException("The table.include.list is empty, please provide a list of tables to get the changes from");
            }

            // If the stream ID is not associated with any table then we should throw an exception
            // early, this would save us some work on the connector side since we would then not be
            // going to fetch all the tables then verify if they are a part of the stream ID,
            // eventually saving us some network calls as well
            GetDBStreamInfoResponse getStreamInfoResp = null;
            try {
                getStreamInfoResp = ybClient.getDBStreamInfo(streamId);
            } catch (Exception e) {
                String errorMessage = String.format("Failed fetching all tables for the streamid %s", streamId);
                LOGGER.error(errorMessage, e);
                throw new DebeziumException(errorMessage, e);
            }

            if (getStreamInfoResp.getTableInfoList().isEmpty()) {
                String errorMessage = "The provided stream ID is not associated with any table";
                LOGGER.error(errorMessage);
                throw new DebeziumException(errorMessage);
            }

            this.tableIds = YBClientUtils.fetchTableList(ybClient, this.yugabyteDBConnectorConfig);

            if (tableIds == null || tableIds.isEmpty()) {
                throw new DebeziumException("The tables provided in table.include.list do not exist");
            }

            this.hashRanges = new ArrayList<>();
            try {
                for (String tableId : tableIds) {
                    YBTable table = ybClient.openTableByUUID(tableId);
                    GetTabletListToPollForCDCResponse resp = YBClientUtils.getTabletListToPollForCDCWithRetry(table,
                            tableId, yugabyteDBConnectorConfig);
                    List<HashPartition> partitions = new ArrayList<>();
                    LOGGER.info("TabletCheckpointPair list size for table {}: {}", tableId, resp.getTabletCheckpointPairListSize());
                    for (TabletCheckpointPair pair : resp.getTabletCheckpointPairList()) {
                        HashPartition tempPartition = HashPartition.from(pair, tableId);

                        partitions.add(tempPartition);
                        this.hashRanges.add(tempPartition);
                    }

                    // Validate that we have received the complete range of partitions.
                    // For colocated tables, validating complete ranges does not make sense as
                    // there will not be a full range.
                    if (!table.isColocated()) {
                        HashPartition.validateCompleteRanges(partitions);
                    }

                    LOGGER.info("Received tablet list for table {} ({}): {}", table.getTableId(), table.getName(),
                                partitions.stream().map(HashPartition::getTabletId).collect(Collectors.toSet()));
                }
            } catch (Exception e) {
                final String errorMessage = "Error while fetching all the tablets";
                LOGGER.error(errorMessage, e);
                throw new DebeziumException(errorMessage, e);
            }
        }
        catch (Exception e) {
            LOGGER.warn("Exception while validating connection", e);
            throw new DebeziumException("Exception while validating connection", e);
        }
    }

    // TODO Vaibhav: Can be used to get the matching tableIDs for streaming.
    //  We can take out some logic from validateTServerConnection and use it here.
    @Override
    public <T extends DataCollectionId> List<T> getMatchingCollections(Configuration config) {
        return null;
    }

}
