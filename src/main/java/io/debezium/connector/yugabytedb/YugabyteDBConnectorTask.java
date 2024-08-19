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
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import io.debezium.connector.yugabytedb.connection.OpId;
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
import io.debezium.connector.yugabytedb.metrics.YugabyteDBMetricsFactory;
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

    private YugabyteDBChangeEventSourceCoordinator coordinator;

    private YBPartition.Provider partitionProvider;
    private YugabyteDBOffsetContext.Loader offsetContextLoader;

    private final ReentrantLock commitLock = new ReentrantLock();

    protected volatile Map<String, ?> ybOffset;

    @Override
    public ChangeEventSourceCoordinator<YBPartition, YugabyteDBOffsetContext> start(Configuration config) {
        final YugabyteDBConnectorConfig connectorConfig = new YugabyteDBConnectorConfig(config);
        final TopicSelector<TableId> topicSelector = YugabyteDBTopicSelector.create(connectorConfig);
        final Snapshotter snapshotter = connectorConfig.getSnapshotter();
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();

        LOGGER.debug("The config is " + config);

        if (snapshotter == null) {
            throw new ConnectException("Unable to load snapshotter, if using custom snapshot mode," +
                    " double check your settings");
        }

        final String databaseCharsetName = config.getString(YugabyteDBConnectorConfig.CHAR_SET);
        final Charset databaseCharset = Charset.forName(databaseCharsetName);

        Encoding encoding = Encoding.defaultEncoding(); // UTF-8
        YugabyteDBTaskConnection taskConnection = new YugabyteDBTaskConnection(encoding);

        this.partitionProvider = new YBPartition.Provider(connectorConfig);
        this.offsetContextLoader = new YugabyteDBOffsetContext.Loader(connectorConfig);

        final YugabyteDBValueConverterBuilder valueConverterBuilder = (typeRegistry) -> YugabyteDBValueConverter.of(
                connectorConfig,
                databaseCharset,
                typeRegistry);



        if (connectorConfig.isYSQLDbType()) {

            String nameToTypeStr = config.getString(YugabyteDBConnectorConfig.NAME_TO_TYPE.toString());
            String oidToTypeStr = config.getString(YugabyteDBConnectorConfig.OID_TO_TYPE.toString());

            Map<String, YugabyteDBType> nameToType = null;
            Map<Integer, YugabyteDBType> oidToType = null;
            try {
                nameToType = (Map<String, YugabyteDBType>) ObjectUtil
                        .deserializeObjectFromString(nameToTypeStr);
            } catch (IOException | ClassNotFoundException e) {
                LOGGER.error("Error while deserializing name to type string", e);
                throw new RuntimeException(e);
            }

            try {
                oidToType = (Map<Integer, YugabyteDBType>) ObjectUtil
                        .deserializeObjectFromString(oidToTypeStr);
            } catch (IOException | ClassNotFoundException e) {
                LOGGER.error("Error while deserializing object to type string", e);
            }

            // Global JDBC connection used both for snapshotting and streaming.
            // Must be able to resolve datatypes.
            jdbcConnection = new YugabyteDBConnection(connectorConfig.getJdbcConfig(), valueConverterBuilder,
                    YugabyteDBConnection.CONNECTION_GENERAL);

            // CDCSDK We can just build the type registry on the co-ordinator and then send
            // the map of Postgres Type and Oid to the Task using Config
            final YugabyteDBTypeRegistry yugabyteDBTypeRegistry = new YugabyteDBTypeRegistry(taskConnection, nameToType,
                    oidToType, jdbcConnection);

            schema = new YugabyteDBSchema(connectorConfig, yugabyteDBTypeRegistry, topicSelector,
                    valueConverterBuilder.build(yugabyteDBTypeRegistry));

        } else {
            final YugabyteDBCQLValueConverter cqlValueConverter = YugabyteDBCQLValueConverter.of(connectorConfig,
                    databaseCharset);
            schema = new YugabyteDBSchema(connectorConfig, topicSelector, cqlValueConverter);
        }

        String taskId = config.getString(YugabyteDBConnectorConfig.TASK_ID.toString());
        boolean sendBeforeImage = config.getBoolean(YugabyteDBConnectorConfig.SEND_BEFORE_IMAGE.toString());
        boolean enableExplicitCheckpointing = config.getBoolean(YugabyteDBConnectorConfig.ENABLE_EXPLICIT_CHECKPOINTING.toString());

        this.taskContext = new YugabyteDBTaskContext(connectorConfig, schema, topicSelector, taskId, sendBeforeImage, enableExplicitCheckpointing);

        // Get the tablet ids and load the offsets
        final Offsets<YBPartition, YugabyteDBOffsetContext> previousOffsets =
            getPreviousOffsetsFromProviderAndLoader(
                new YBPartition.Provider(connectorConfig),
                new YugabyteDBOffsetContext.Loader(connectorConfig));
        final Clock clock = Clock.system();

        YugabyteDBOffsetContext context = new YugabyteDBOffsetContext(previousOffsets,
                                                                      connectorConfig);

        LoggingContext.PreviousContext previousContext = taskContext
                .configureLoggingContext(CONTEXT_NAME + "|" + taskId);
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

            this.coordinator = new YugabyteDBChangeEventSourceCoordinator(
                    previousOffsets,
                    errorHandler,
                    YugabyteDBgRPCConnector.class,
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
                    new YugabyteDBMetricsFactory(previousOffsets.getPartitions(), connectorConfig, taskId),
                    dispatcher,
                    schema,
                    snapshotter,
                    null/* slotInfo */);

            this.coordinator.start(taskContext, this.queue, metadataProvider);

            return this.coordinator;
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

    Offsets<YBPartition, YugabyteDBOffsetContext> getPreviousOffsetsFromProviderAndLoader(
        Partition.Provider<YBPartition> provider,
        OffsetContext.Loader<YugabyteDBOffsetContext> loader) {
        Optional<Set<YBPartition>> ybPartitions = coordinator.getPartitions();
        Set<YBPartition> partitions = ybPartitions.orElse(provider.getPartitions());
        LOGGER.debug("The size of partitions is " + partitions.size());
        OffsetReader<YBPartition, YugabyteDBOffsetContext,
                     OffsetContext.Loader<YugabyteDBOffsetContext>> reader = new OffsetReader<>(
                        context.offsetStorageReader(), loader);
        Offsets<YBPartition, YugabyteDBOffsetContext> offsets =
            Offsets.of(reader.offsets(partitions));

        boolean found = false;
        if (offsets != null) {
            found = true;

            if (LOGGER.isDebugEnabled()) {
                for (Map.Entry<YBPartition, YugabyteDBOffsetContext> entry : offsets.getOffsets().entrySet()) {
                    if (entry.getKey() != null && entry.getValue() != null) {
                        LOGGER.debug("Read offset map {} for partition {} from topic", entry.getValue().getOffset(), entry.getKey());
                    }
                }
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

    @Override
    public void commitRecord(SourceRecord record) throws InterruptedException {
        // Do nothing.
    }

    /*
     Let's say there are 3 partitions or 3 tablets
     - tablet_0 (0-1, 1-0, 2-0)
     - tablet_1 (0-1, 1-1, 2-0))
     - tablet_2 (0-1, 1-1, 2-1)
     The records are also published in the same order i.e. tablet_0, tablet_1, tablet_2
     But it is not guaranteed that while reading the offsets from kafka we will read in the same
     order, we can end up reading the partitions/tablets in the following order:
     - tablet_1
     - tablet_2
     - tablet_0
     Now, if we call commitOffset on each of the partition, we will basically be overriding the
     offsets with a lower value which we do not want to happen. And that is specifically the reason
     why we use the method getHigherOffsets() to get the highest offset (OpId for YugabyteDB tablet)
     for each tablet across all partitions.
     */
    @Override
    public void commit() throws InterruptedException {
        boolean locked = commitLock.tryLock();

        if (locked) {
            try {
                if (this.coordinator != null) {
                    Offsets<YBPartition, YugabyteDBOffsetContext> offsets = getPreviousOffsetsFromProviderAndLoader(this.partitionProvider, this.offsetContextLoader);
                    if (offsets.getOffsets() != null) {
                        offsets.getOffsets()
                          .entrySet()
                          .stream()
                          .filter(e -> e.getValue() != null)
                          .forEach(entry -> {
                              Map<String, ?> lastOffset = entry.getValue().getOffset();
                              this.ybOffset = getHigherOffsets(lastOffset);
                          });

                        if (LOGGER.isDebugEnabled()) {
                            for (Map.Entry<String, ?> entry : ybOffset.entrySet()) {
                                LOGGER.debug("Committing offset {} for partition {}", entry.getValue(), entry.getKey());
                            }
                        }

                        this.coordinator.commitOffset(ybOffset);
                    }
                }
            } finally {
                commitLock.unlock();
            }
        } else {
            LOGGER.warn("Couldn't commit processed checkpoints with the source database due to a concurrent connector shutdown or restart");
        }
    }

    /**
     * Get a map of keys with the higher values after comparing the cached map and the one we pass.
     * <br/><br/>
     * For example, suppose we have the ybOffset as <code>{a=1,b=12,c=6}</code> and offsets as
     * <code>{a=3,b=1,c=6}</code> then the value returned will be <code>{a=3,b=12,c=6}</code>.
     * @param offsets the offset map read from Kafka topic
     * @return a map with the values higher among the cached ybOffset and passed offsets map
     */
    protected Map<String, ?> getHigherOffsets(Map<String, ?> offsets) {
        if (this.ybOffset == null) {
            return offsets;
        }

        Map<String, String> finalOffsets = new HashMap<>();

        if (offsets == null) {
            // We do not have anything to commit here, returning an empty map should be fine.
            return finalOffsets;
        }

        for (Map.Entry<String, ?> entry : offsets.entrySet()) {
            if ((entry.getKey().contains(".") && !isTaskInSnapshotPhase())
                  || (!entry.getKey().contains(".") && isTaskInSnapshotPhase())) {
                LOGGER.debug("Skipping the offset for entry {}", entry.getKey());
                continue;
            }

            OpId currentEntry = OpId.valueOf((String) this.ybOffset.get(entry.getKey()));
            if (currentEntry == null || currentEntry.isLesserThanOrEqualTo(OpId.valueOf((String) entry.getValue()).toCdcSdkCheckpoint())) {
                finalOffsets.put(entry.getKey(), (String) entry.getValue());
            } else {
                finalOffsets.put(entry.getKey(), (String) this.ybOffset.get(entry.getKey()));
            }
        }

        return finalOffsets;
    }

    /**
     * @return true if the {@link YugabyteDBChangeEventSourceCoordinator} for this task has started
     * executing the streaming source, false otherwise. In other words, this method indicates the
     * status whether this task is in the snapshot phase.
     */
    protected boolean isTaskInSnapshotPhase() {
        return coordinator.isSnapshotInProgress();
    }

    public YugabyteDBTaskContext getTaskContext() {
        return taskContext;
    }
}
