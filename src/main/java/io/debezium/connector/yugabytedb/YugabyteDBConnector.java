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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.util.ConnectorUtils;
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

/**
 * A Kafka Connect source connector that creates tasks which use YugabyteDB CDC API
 * to receive incoming changes for a database and publish them to Kafka.
 * <h2>Configuration</h2>
 * <p>
 * This connector is configured with the set of properties described in
 * {@link YugabyteDBConnectorConfig}.
 *
 * @author Suranjan Kumar (skumar@yugabyte.com)
 */
public class YugabyteDBConnector extends RelationalBaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBConnector.class);
    private Map<String, String> props;
    private YBClient ybClient;
    private volatile YugabyteDBConnection connection;
    private Set<String> tableIds;
    private List<Pair<String, String>> tabletIds;
    private YugabyteDBConnectorConfig yugabyteDBConnectorConfig;

    public YugabyteDBConnector() {
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
        this.yugabyteDBConnectorConfig = new YugabyteDBConnectorConfig(config);
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (props == null) {
            LOGGER.error("Configuring a maximum of {} tasks with no connector configuration" +
                    " available", maxTasks);
            return Collections.emptyList();
        }

        connection = new YugabyteDBConnection(yugabyteDBConnectorConfig.getJdbcConfig(), YugabyteDBConnection.CONNECTION_GENERAL);
        final Charset databaseCharset = connection.getDatabaseCharset();
        String charSetName = databaseCharset.name();

        YugabyteDBTypeRegistry typeRegistry = new YugabyteDBTypeRegistry(connection);

        Map<String, YugabyteDBType> nameToType = typeRegistry.getNameToType();
        Map<Integer, YugabyteDBType> oidToType = typeRegistry.getOidToType();
        String serializedNameToType = "";
        try {
            serializedNameToType = ObjectUtil.serializeObjectToString(nameToType);
            LOGGER.debug("The serializedNameToType " + serializedNameToType);
            Object test = ObjectUtil.deserializeObjectFromString(serializedNameToType);
            LOGGER.debug("The deserializedNameToType " + test);
        }
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        String serializedOidToType = "";
        try {
            serializedOidToType = ObjectUtil.serializeObjectToString(oidToType);
            LOGGER.debug("The serializedOidToType " + serializedOidToType);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        Configuration config = Configuration.from(this.props);
        Map<String, ConfigValue> results = validateAllFields(config);

        validateTServerConnection(results, config);
        
        String streamIdValue = this.yugabyteDBConnectorConfig.streamId();
        LOGGER.debug("The streamid in config is" + this.yugabyteDBConnectorConfig.streamId());

        if (streamIdValue == null) {
            streamIdValue = results.get(YugabyteDBConnectorConfig.STREAM_ID.name()).value().toString();
        }

        LOGGER.debug("The streamid being used is " + streamIdValue);

        int numGroups = Math.min(this.tabletIds.size(), maxTasks);
        LOGGER.debug("The tabletIds size are " + tabletIds.size() + " maxTasks" + maxTasks);

        List<List<Pair<String, String>>> tabletIdsGrouped = ConnectorUtils.groupPartitions(this.tabletIds, numGroups);
        LOGGER.debug("The grouped tabletIds are " + tabletIdsGrouped.size());
        List<Map<String, String>> taskConfigs = new ArrayList<>(tabletIdsGrouped.size());

        for (List<Pair<String, String>> taskTables : tabletIdsGrouped) {
            LOGGER.debug("The taskTables are " + taskTables);
            Map<String, String> taskProps = new HashMap<>(this.props);
            int taskId = taskConfigs.size();
            taskProps.put(YugabyteDBConnectorConfig.TASK_ID.toString(), String.valueOf(taskId));
            String taskTablesSerialized = "";
            try {
                taskTablesSerialized = ObjectUtil.serializeObjectToString(taskTables);
                LOGGER.debug("The taskTablesSerialized " + taskTablesSerialized);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            taskProps.put(YugabyteDBConnectorConfig.TABLET_LIST.toString(), taskTablesSerialized);
            taskProps.put(YugabyteDBConnectorConfig.CHAR_SET.toString(), charSetName);
            taskProps.put(YugabyteDBConnectorConfig.NAME_TO_TYPE.toString(), serializedNameToType);
            taskProps.put(YugabyteDBConnectorConfig.OID_TO_TYPE.toString(), serializedOidToType);
            taskProps.put(YugabyteDBConnectorConfig.STREAM_ID.toString(), streamIdValue);
            taskConfigs.add(taskProps);
        }

        LOGGER.debug("Configuring {} YugabyteDB connector task(s)", taskConfigs.size());
        closeYBClient();
        return taskConfigs;
    }

    private void closeYBClient() {
        try {
            ybClient.close();
        }
        catch (Exception e) {
            LOGGER.warn("Received exception while shutting down the client", e);
        }
    }

    private YBClient getYBClientBase(String hostAddress, long adminTimeout, long operationTimeout, long socketReadTimeout,
                                     int maxNumTablets, String certFile, String clientCert, String clientKey) {
        if (maxNumTablets == -1) {
            maxNumTablets = yugabyteDBConnectorConfig.maxNumTablets();
        }

        if (adminTimeout == -1) {
            adminTimeout = yugabyteDBConnectorConfig.adminOperationTimeoutMs();
        }

        if (operationTimeout == -1) {
            operationTimeout = yugabyteDBConnectorConfig.operationTimeoutMs();
        }

        if (socketReadTimeout == -1) {
            socketReadTimeout = yugabyteDBConnectorConfig.socketReadTimeoutMs();
        }

        AsyncYBClient asyncClient = new AsyncYBClient.AsyncYBClientBuilder(hostAddress)
                .defaultAdminOperationTimeoutMs(adminTimeout)
                .defaultOperationTimeoutMs(operationTimeout)
                .defaultSocketReadTimeoutMs(socketReadTimeout)
                .numTablets(maxNumTablets)
                .sslCertFile(certFile)
                .sslClientCertFiles(clientCert, clientKey)
                .build();

        return new YBClient(asyncClient);
    }

    @Override
    public void stop() {
        this.props = null;
        if (this.ybClient != null) {
          try {
            ybClient.close();
          } catch (Exception e) {
            LOGGER.warn("Received exception while shutting down the client", e);
          }
        }
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

        this.yugabyteDBConnectorConfig = new YugabyteDBConnectorConfig(config);
        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        // Try to connect to the database ...
        try (YugabyteDBConnection connection = new YugabyteDBConnection(yugabyteDBConnectorConfig.getJdbcConfig(), YugabyteDBConnection.CONNECTION_GENERAL)) {
            try {
                // Prepare connection without initial statement execution
                connection.connection(false);
                // check connection
                connection.execute("SELECT version()");
                LOGGER.info("Successfully tested connection for {} with user '{}'",
                        connection.connectionString(),
                        connection.username());
            }
            catch (SQLException e) {
                LOGGER.error("Failed testing connection for {} with user '{}'",
                        connection.connectionString(),
                        connection.username(), e);
                hostnameValue.addErrorMessage("Error while validating connector config: "
                        + e.getMessage());
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
        String hostAddress = config.getString(YugabyteDBConnectorConfig.MASTER_ADDRESSES.toString());
        this.ybClient = getYBClientBase(hostAddress,
                yugabyteDBConnectorConfig.adminOperationTimeoutMs(),
                yugabyteDBConnectorConfig.operationTimeoutMs(),
                yugabyteDBConnectorConfig.socketReadTimeoutMs(),
                yugabyteDBConnectorConfig.maxNumTablets(),
                yugabyteDBConnectorConfig.sslRootCert(),
                yugabyteDBConnectorConfig.sslClientCert(),
                yugabyteDBConnectorConfig.sslClientKey()); // always passing the ssl root certs,
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
            getStreamInfoResp = this.ybClient.getDBStreamInfo(streamId);
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

        this.tableIds = YBClientUtils.fetchTableList(this.ybClient, this.yugabyteDBConnectorConfig);

        if (tableIds == null || tableIds.isEmpty()) {
            throw new DebeziumException("The tables provided in table.include.list do not exist");
        }

        this.tabletIds = new ArrayList<>();
        try {
            for (String tableId : tableIds) {
                YBTable table = ybClient.openTableByUUID(tableId);
                GetTabletListToPollForCDCResponse resp = ybClient.getTabletListToPollForCdc(
                    table, this.yugabyteDBConnectorConfig.streamId(), tableId);
                for (TabletCheckpointPair pair : resp.getTabletCheckpointPairList()) {
                    this.tabletIds.add(
                        new ImmutablePair<String,String>(tableId, pair.getTabletId().toStringUtf8()));
                }
            }
            Collections.sort(this.tabletIds, (a, b) -> a.getRight().compareTo(b.getRight()));
        }
        catch (Exception e) {
            LOGGER.error("Error while fetching all the tablets", e);
        }
    }
}
