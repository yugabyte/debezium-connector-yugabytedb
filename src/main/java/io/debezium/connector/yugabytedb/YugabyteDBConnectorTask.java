/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

import io.debezium.heartbeat.HeartbeatFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.postgresql.core.Encoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.OffsetReader;
import io.debezium.connector.yugabytedb.connection.ReplicationConnection;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection.YugabyteDBValueConverterBuilder;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;
import io.debezium.util.Metronome;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Kafka connect source task which uses YugabyteDB CDC API to process DB changes.
 *
 * @author Suranjan Kumar (skumar@yugabyte.com)
 */
public class YugabyteDBConnectorTask
        extends BaseSourceTask<YBPartition, YugabyteDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBConnectorTask.class);
    private static final String CONTEXT_NAME = "yugabytedb-connector-task";

    private volatile YugabyteDBTaskContext taskContext;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile YugabyteDBConnection jdbcConnection;
    private volatile YugabyteDBConnection heartbeatConnection;
    private volatile YugabyteDBSchema schema;

    @Override
    public ChangeEventSourceCoordinator<YBPartition, YugabyteDBOffsetContext> start(Configuration config) {
        final YugabyteDBConnectorConfig connectorConfig = new YugabyteDBConnectorConfig(config);
        final TopicSelector<TableId> topicSelector = YugabyteDBTopicSelector.create(connectorConfig);
        final Snapshotter snapshotter = connectorConfig.getSnapshotter();
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

        LOGGER.debug("The config is " + config);

        String tabletList = config.getString(YugabyteDBConnectorConfig.TABLET_LIST);
        List<Pair<String, String>> tabletPairList = null;
        try {
            tabletPairList = (List<Pair<String, String>>) ObjectUtil.deserializeObjectFromString(tabletList);
            LOGGER.debug("The tablet list is " + tabletPairList);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        if (snapshotter == null) {
            throw new ConnectException("Unable to load snapshotter, if using custom snapshot mode," +
                    " double check your settings");
        }

        final String databaseCharsetName = config.getString(YugabyteDBConnectorConfig.CHAR_SET);
        final Charset databaseCharset = Charset.forName(databaseCharsetName);

        String nameToTypeStr = config.getString(YugabyteDBConnectorConfig.NAME_TO_TYPE.toString());
        String oidToTypeStr = config.getString(YugabyteDBConnectorConfig.OID_TO_TYPE.toString());
        Encoding encoding = Encoding.defaultEncoding(); // UTF-8

        Map<String, YugabyteDBType> nameToType = null;
        Map<Integer, YugabyteDBType> oidToType = null;
        try {
            nameToType = (Map<String, YugabyteDBType>) ObjectUtil
                    .deserializeObjectFromString(nameToTypeStr);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            oidToType = (Map<Integer, YugabyteDBType>) ObjectUtil
                    .deserializeObjectFromString(oidToTypeStr);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        YugabyteDBTaskConnection taskConnection = new YugabyteDBTaskConnection(encoding);
        final YugabyteDBValueConverterBuilder valueConverterBuilder = (typeRegistry) -> YugabyteDBValueConverter.of(
                connectorConfig,
                databaseCharset,
                typeRegistry);

        // Global JDBC connection used both for snapshotting and streaming.
        // Must be able to resolve datatypes.
        // connection = new YugabyteDBConnection(connectorConfig.getJdbcConfig());
        jdbcConnection = new YugabyteDBConnection(connectorConfig.getJdbcConfig(), valueConverterBuilder, YugabyteDBConnection.CONNECTION_GENERAL);
        // try {
        // jdbcConnection.setAutoCommit(false);
        // }
        // catch (SQLException e) {
        // throw new DebeziumException(e);
        // }

        // CDCSDK We can just build the type registry on the co-ordinator and then send the
        // map of Postgres Type and Oid to the Task using Config
        final YugabyteDBTypeRegistry yugabyteDBTypeRegistry = new YugabyteDBTypeRegistry(taskConnection,
                nameToType, oidToType);
        // jdbcConnection.getTypeRegistry();

        schema = new YugabyteDBSchema(connectorConfig, yugabyteDBTypeRegistry, topicSelector,
                valueConverterBuilder.build(yugabyteDBTypeRegistry));
        this.taskContext = new YugabyteDBTaskContext(connectorConfig, schema, topicSelector);
        // get the tablet ids and load the offsets

        final Map<YBPartition, YugabyteDBOffsetContext> previousOffsets = getPreviousOffsetss(
                new YugabyteDBPartition.Provider(connectorConfig),
                new YugabyteDBOffsetContext.Loader(connectorConfig));
        final Clock clock = Clock.system();

        YugabyteDBOffsetContext context = new YugabyteDBOffsetContext(previousOffsets, 
                                                                      connectorConfig);

        LoggingContext.PreviousContext previousContext = taskContext
                .configureLoggingContext(CONTEXT_NAME);
        try {
            // Print out the server information
            // CDCSDK Get the table,

            queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                    .build();

            ErrorHandler errorHandler = new YugabyteDBErrorHandler(connectorConfig, queue);

            final YugabyteDBEventMetadataProvider metadataProvider = new YugabyteDBEventMetadataProvider();

            Configuration configuration = connectorConfig.getConfig();
            HeartbeatFactory heartbeatFactory = new HeartbeatFactory<>(
                    connectorConfig,
                    topicSelector,
                    schemaNameAdjuster,
                    () -> new YugabyteDBConnection(connectorConfig.getJdbcConfig(), YugabyteDBConnection.CONNECTION_GENERAL),
                    exception -> {
                        String sqlErrorId = exception.getSQLState();
                        switch (sqlErrorId) {
                            case "57P01":
                                // Postgres error admin_shutdown, see https://www.postgresql.org/docs/12/errcodes-appendix.html
                                throw new DebeziumException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                            case "57P03":
                                // Postgres error cannot_connect_now, see https://www.postgresql.org/docs/12/errcodes-appendix.html
                                throw new RetriableException("Could not execute heartbeat action query (Error: " + sqlErrorId + ")", exception);
                            default:
                                break;
                        }
                    });

            final YugabyteDBEventDispatcher<TableId> dispatcher = new YugabyteDBEventDispatcher<>(
                    connectorConfig,
                    topicSelector,
                    schema,
                    queue,
                    connectorConfig.getTableFilters().dataCollectionFilter(),
                    DataChangeEvent::new,
                    YugabyteDBChangeRecordEmitter::updateSchema,
                    metadataProvider,
                    heartbeatFactory,
                    schemaNameAdjuster,
                    jdbcConnection);

            YugabyteDBChangeEventSourceCoordinator coordinator = new YugabyteDBChangeEventSourceCoordinator(
                    Offsets.of(new YBPartition(), context) ,
                    errorHandler,
                    YugabyteDBConnector.class,
                    connectorConfig,
                    new YugabyteDBChangeEventSourceFactory(
                            connectorConfig,
                            snapshotter,
                            jdbcConnection,
                            errorHandler,
                            dispatcher,
                            clock,
                            schema,
                            taskContext,
                            null,
                            null/* slotCreatedInfo */,
                            null/* slotInfo */,
                            queue),
                    new DefaultChangeEventSourceMetricsFactory(),
                    dispatcher,
                    schema,
                    snapshotter,
                    null/* slotInfo */);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        }
        finally {
            previousContext.restore();
        }
    }

    Map<YBPartition, YugabyteDBOffsetContext> getPreviousOffsetss(
        Partition.Provider<YBPartition> provider,
        OffsetContext.Loader<YugabyteDBOffsetContext> loader) {
        // return super.getPreviousOffsets(provider, loader);
        Set<YBPartition> partitions = provider.getPartitions();
        LOGGER.debug("The size of partitions is " + partitions.size());
        OffsetReader<YBPartition, YugabyteDBOffsetContext, OffsetContext.Loader<YugabyteDBOffsetContext>> reader = new OffsetReader<>(
                context.offsetStorageReader(), loader);
        Map<YBPartition, YugabyteDBOffsetContext> offsets = reader.offsets(partitions);

        boolean found = false;
        for (YBPartition partition : partitions) {
            YugabyteDBOffsetContext offset = offsets.get(partition);

            if (offset != null) {
                found = true;
                LOGGER.info("Found previous partition offset {}: {}", partition, offset);
            }
        }

        if (!found) {
            LOGGER.info("No previous offsets found");
        }

        return offsets;
    }

    public ReplicationConnection createReplicationConnection(YugabyteDBTaskContext taskContext,
                                                             boolean doSnapshot,
                                                             int maxRetries,
                                                             Duration retryDelay)
            throws ConnectException {
        final Metronome metronome = Metronome.parker(retryDelay, Clock.SYSTEM);
        short retryCount = 0;
        ReplicationConnection replicationConnection = null;
        while (retryCount <= maxRetries) {
            try {
                return taskContext.createReplicationConnection(doSnapshot);
            }
            catch (SQLException ex) {
                retryCount++;
                if (retryCount > maxRetries) {
                    LOGGER.error("Too many errors connecting to server." +
                            " All {} retries failed.", maxRetries);
                    throw new ConnectException(ex);
                }

                LOGGER.warn("Error connecting to server; will attempt retry {} of {} after {} " +
                        "seconds. Exception message: {}", retryCount,
                        maxRetries, retryDelay.getSeconds(), ex.getMessage());
                try {
                    metronome.pause();
                }
                catch (InterruptedException e) {
                    LOGGER.warn("Connection retry sleep interrupted by exception: " + e);
                    Thread.currentThread().interrupt();
                }
            }
        }
        return replicationConnection;
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {

        // if all the tablets have been polled in a loop
        // the poll the queue
        // and notify.
        final List<DataChangeEvent> records = queue.poll();
        LOGGER.debug("Got the records from queue: " + records);
        final List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    protected void doStop() {
        if (jdbcConnection != null) {
            jdbcConnection.close();
        }

        if (heartbeatConnection != null) {
            heartbeatConnection.close();
        }

        if (schema != null) {
            schema.close();
        }
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return YugabyteDBConnectorConfig.ALL_FIELDS;
    }

    public YugabyteDBTaskContext getTaskContext() {
        return taskContext;
    }
}
