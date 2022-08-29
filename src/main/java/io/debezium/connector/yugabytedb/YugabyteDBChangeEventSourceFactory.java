/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.util.Optional;

import io.debezium.connector.yugabytedb.connection.ReplicationConnection;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.spi.SlotCreationResult;
import io.debezium.connector.yugabytedb.spi.SlotState;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;

public class YugabyteDBChangeEventSourceFactory implements ChangeEventSourceFactory<YBPartition, YugabyteDBOffsetContext> {

    private final YugabyteDBConnectorConfig configuration;
    private final YugabyteDBConnection jdbcConnection;
    private final ErrorHandler errorHandler;
    private final YugabyteDBEventDispatcher<TableId> dispatcher;
    private final Clock clock;
    private final YugabyteDBSchema schema;
    private final YugabyteDBTaskContext taskContext;
    private final Snapshotter snapshotter;
    private final ReplicationConnection replicationConnection;
    private final SlotCreationResult slotCreatedInfo;
    private final SlotState startingSlotInfo;
    private final ChangeEventQueue<DataChangeEvent> queue;

    public YugabyteDBChangeEventSourceFactory(YugabyteDBConnectorConfig configuration,
                                              Snapshotter snapshotter,
                                              YugabyteDBConnection jdbcConnection,
                                              ErrorHandler errorHandler,
                                              YugabyteDBEventDispatcher<TableId> dispatcher,
                                              Clock clock, YugabyteDBSchema schema,
                                              YugabyteDBTaskContext taskContext,
                                              ReplicationConnection replicationConnection,
                                              SlotCreationResult slotCreatedInfo,
                                              SlotState startingSlotInfo,
                                              ChangeEventQueue<DataChangeEvent> queue) {
        this.configuration = configuration;
        this.jdbcConnection = jdbcConnection;
        this.errorHandler = errorHandler;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.schema = schema;
        this.taskContext = taskContext;
        this.snapshotter = snapshotter;
        this.replicationConnection = replicationConnection;
        this.slotCreatedInfo = slotCreatedInfo;
        this.startingSlotInfo = startingSlotInfo;
        this.queue = queue;
    }

    @Override
    public SnapshotChangeEventSource<YBPartition, YugabyteDBOffsetContext> getSnapshotChangeEventSource(
                                                                                                                SnapshotProgressListener snapshotProgressListener) {
        return new YugabyteDBSnapshotChangeEventSource(
                configuration,
                taskContext,
                snapshotter,
                schema,
                dispatcher,
                clock,
                snapshotProgressListener);
    }

    @Override
    public StreamingChangeEventSource<YBPartition, YugabyteDBOffsetContext> getStreamingChangeEventSource() {
        return new YugabyteDBStreamingChangeEventSource(
                configuration,
                snapshotter,
                jdbcConnection,
                dispatcher,
                errorHandler,
                clock,
                schema,
                taskContext,
                replicationConnection,
                queue);
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<YBPartition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(YugabyteDBOffsetContext offsetContext,
                                                                                                                              SnapshotProgressListener snapshotProgressListener,
                                                                                                                              DataChangeEventListener dataChangeEventListener) {
        final SignalBasedIncrementalSnapshotChangeEventSource<YBPartition, TableId> incrementalSnapshotChangeEventSource = new SignalBasedIncrementalSnapshotChangeEventSource<YBPartition, TableId>(
                configuration,
                jdbcConnection,
                dispatcher,
                schema,
                clock,
                snapshotProgressListener,
                dataChangeEventListener);
        return Optional.of(incrementalSnapshotChangeEventSource);
    }
}
