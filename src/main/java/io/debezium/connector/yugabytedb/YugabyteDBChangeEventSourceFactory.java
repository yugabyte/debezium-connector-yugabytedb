/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.util.Optional;

import io.debezium.connector.yugabytedb.connection.ReplicationConnection;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating YugabyteDB-specific change event sources, including snapshot and streaming sources.
 * This class is responsible for instantiating the appropriate change event source implementations
 * based on the connector configuration and runtime context.
 *
 * @author Vaibhav Kushwaha
 */
public class YugabyteDBChangeEventSourceFactory implements ChangeEventSourceFactory<YBPartition, YugabyteDBOffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBChangeEventSourceFactory.class);

    private final YugabyteDBConnectorConfig configuration;
    private final MainConnectionProvidingConnectionFactory<YugabyteDBConnection> connectionFactory;
    private final ErrorHandler errorHandler;
    private final YugabyteDBEventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final YugabyteDBSchema schema;
    private final YugabyteDBTaskContext taskContext;
    private final SnapshotterService snapshotterService;
    private final ReplicationConnection replicationConnection;
    private final ChangeEventQueue<DataChangeEvent> queue;

    public YugabyteDBChangeEventSourceFactory(YugabyteDBConnectorConfig configuration, SnapshotterService snapshotterService,
                                              MainConnectionProvidingConnectionFactory<YugabyteDBConnection> connectionFactory,
                                              ErrorHandler errorHandler,
                                              YugabyteDBEventDispatcher<TableId> dispatcher,
                                              Clock clock, YugabyteDBSchema schema,
                                              YugabyteDBTaskContext taskContext,
                                              ReplicationConnection replicationConnection,
                                              ChangeEventQueue<DataChangeEvent> queue) {
        this.configuration = configuration;
        this.snapshotterService = snapshotterService;
        this.connectionFactory = connectionFactory;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.taskContext = taskContext;
        this.replicationConnection = replicationConnection;
        this.queue = queue;
    }

    @Override
    public SnapshotChangeEventSource<YBPartition, YugabyteDBOffsetContext> getSnapshotChangeEventSource(
                SnapshotProgressListener<YBPartition> snapshotProgressListener,
                NotificationService<YBPartition, YugabyteDBOffsetContext> notificationService) {
        return new YugabyteDBSnapshotChangeEventSource(
                configuration,
                snapshotterService,
                connectionFactory,
                taskContext,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener,
                notificationService);
    }

    @Override
    public StreamingChangeEventSource<YBPartition, YugabyteDBOffsetContext> getStreamingChangeEventSource() {
        LOGGER.info("Transaction ordering enabled: {}", configuration.transactionOrdering());
        if (!configuration.transactionOrdering()) {
            LOGGER.info("Instantiating Vanilla Streaming Source");
            return new YugabyteDBStreamingChangeEventSource(
                    configuration,
                    snapshotterService,
                    connectionFactory.mainConnection(),
                    dispatcher,
                    errorHandler,
                    clock,
                    schema,
                    taskContext,
                    replicationConnection,
                    queue);
        } else {
            LOGGER.info("Instantiating CONSISTENT Streaming Source");
            return new YugabyteDBConsistentStreamingSource(
                    configuration,
                    snapshotterService,
                    connectionFactory.mainConnection(),
                    dispatcher,
                    errorHandler,
                    clock,
                    schema,
                    taskContext,
                    replicationConnection,
                    queue);
        }
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<YBPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
            YugabyteDBOffsetContext offsetContext, SnapshotProgressListener<YBPartition> snapshotProgressListener,
            DataChangeEventListener<YBPartition> dataChangeEventListener,
            NotificationService<YBPartition, YugabyteDBOffsetContext> notificationService) {
        // YugabyteDB does not support incremental snapshots so when an incremental snapshot change
        // event source is requested, we can simply return an empty value so that no action can be
        // taken on this.
        return Optional.empty();
    }
}
