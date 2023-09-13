package io.debezium.connector.yugabytedb;

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

import io.debezium.data.Envelope;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.signal.Signal;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.schema.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.yugabytedb.connection.LogicalDecodingMessage;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.util.SchemaNameAdjuster;

import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;

/**
 * Custom extension of the {@link EventDispatcher} to accommodate routing {@link LogicalDecodingMessage} events to the change event queue.
 *
 * @author Rajat Venkatesh, Vaibhav Kushwaha
 */
public class YugabyteDBEventDispatcher<T extends DataCollectionId> extends EventDispatcher<YBPartition, T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBEventDispatcher.class);
    private final YugabyteDBConnectorConfig connectorConfig;
    private final ChangeEventCreator changeEventCreator;
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final boolean emitTombstonesOnDelete;
    private final LogicalDecodingMessageMonitor logicalDecodingMessageMonitor;
    private final LogicalDecodingMessageFilter messageFilter;
    private final TopicSelector<T> topicSelector;
    private final DatabaseSchema<T> schema;
    private DataChangeEventListener<YBPartition> eventListener = DataChangeEventListener.NO_OP();
    private final InconsistentSchemaHandler<YBPartition, T> inconsistentSchemaHandler;
    private final Signal<YBPartition> signal;
    private final boolean neverSkip;
    private final Heartbeat heartbeat;
    private final EnumSet<Envelope.Operation> skippedOperations;
    private final DataCollectionFilters.DataCollectionFilter<T> filter;
    private final YugabyteDBTransactionMonitor transactionMonitor;
    private final YugabyteDBStreamingChangeRecordReceiver streamingReceiver;

    public YugabyteDBEventDispatcher(YugabyteDBConnectorConfig connectorConfig, TopicSelector<T> topicSelector,
                                   DatabaseSchema<T> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilters.DataCollectionFilter<T> filter,
                                   ChangeEventCreator changeEventCreator, InconsistentSchemaHandler<YBPartition, T> inconsistentSchemaHandler,
                                   EventMetadataProvider metadataProvider, HeartbeatFactory<T> heartbeatFactory, SchemaNameAdjuster schemaNameAdjuster,
                                   JdbcConnection jdbcConnection) {
        super(connectorConfig, topicSelector, schema, queue, filter, changeEventCreator, inconsistentSchemaHandler, metadataProvider,
                heartbeatFactory, schemaNameAdjuster);
        this.connectorConfig = connectorConfig;
        this.changeEventCreator = changeEventCreator;
        this.queue = queue;
        this.logicalDecodingMessageMonitor = new LogicalDecodingMessageMonitor(connectorConfig, this::enqueueLogicalDecodingMessage);
        this.messageFilter = connectorConfig.getMessageFilter();
        this.topicSelector = topicSelector;
        this.heartbeat = heartbeatFactory.createHeartbeat();
        this.streamingReceiver = new YugabyteDBStreamingChangeRecordReceiver();
        this.inconsistentSchemaHandler = inconsistentSchemaHandler != null ? inconsistentSchemaHandler : this::errorOnMissingSchema;
        this.signal = new Signal<>(connectorConfig, this);
        this.skippedOperations = connectorConfig.getSkippedOperations();
        this.emitTombstonesOnDelete = connectorConfig.isEmitTombstoneOnDelete();
        this.neverSkip = connectorConfig.supportsOperationFiltering() || this.skippedOperations.isEmpty();
        this.filter = filter;
        this.schema = schema;
        this.transactionMonitor = new YugabyteDBTransactionMonitor(connectorConfig, metadataProvider, schemaNameAdjuster, this::enqueueTransactionMessage);
    }

    public void dispatchLogicalDecodingMessage(Partition partition, OffsetContext offset, Long decodeTimestamp,
                                               LogicalDecodingMessage message)
            throws InterruptedException {
        if (messageFilter.isIncluded(message.getPrefix())) {
            logicalDecodingMessageMonitor.logicalDecodingMessageEvent(partition, offset, decodeTimestamp, message);
        } else {
            LOGGER.trace("Filtered data change event for logical decoding message with prefix{}", message.getPrefix());
        }
    }

    public void setEventListener(DataChangeEventListener<YBPartition> eventListener) {
        this.eventListener = eventListener;
    }

    @Override
    public boolean dispatchDataChangeEvent(YBPartition partition, T dataCollectionId, ChangeRecordEmitter<YBPartition> changeRecordEmitter) throws InterruptedException {
        try {
            boolean handled = false;
            if (!filter.isIncluded(dataCollectionId)) {
                LOGGER.info("Filtered data change event for {}", dataCollectionId);
                eventListener.onFilteredEvent(partition, "source = " + dataCollectionId, changeRecordEmitter.getOperation());
                dispatchFilteredEvent(changeRecordEmitter.getPartition(), changeRecordEmitter.getOffset());
            } else {
                DataCollectionSchema dataCollectionSchema = schema.schemaFor(dataCollectionId); //Doubt: we dont have schemaFor in our code, should we get tableSchema here
                LOGGER.info("Sumukh: the datacollectionschema inside dispatch change event = " + dataCollectionSchema);
                // TODO handle as per inconsistent schema info option
                if (dataCollectionSchema == null) {
                    final Optional<DataCollectionSchema> replacementSchema = inconsistentSchemaHandler.handle(partition,
                      dataCollectionId, changeRecordEmitter);
                    if (!replacementSchema.isPresent()) {
                        LOGGER.info("Sumukh no replacement schema");
                        return false;
                    }
                    dataCollectionSchema = replacementSchema.get();
                    LOGGER.info(
                            "Sumukh: the datacollectionschema was null, replacement schema = " + dataCollectionSchema);
                }

                changeRecordEmitter.emitChangeRecords(dataCollectionSchema, new ChangeRecordEmitter.Receiver<YBPartition>() {
                    @Override
                    public void changeRecord(YBPartition partition,
                                             DataCollectionSchema schema,
                                             Envelope.Operation operation,
                                             Object key, Struct value,
                                             OffsetContext offset,
                                             ConnectHeaders headers)
                      throws InterruptedException {
                        if (operation == Envelope.Operation.CREATE && signal.isSignal(dataCollectionId)) {
                            signal.process(partition, value, offset);
                        }

                        if (neverSkip || !skippedOperations.contains(operation)) {
                            transactionMonitor.dataEvent(partition, dataCollectionId, offset, key, value);
                            eventListener.onEvent(partition, dataCollectionId, offset, key, value, operation);

                            streamingReceiver.changeRecord(partition, schema, operation, key, value, offset, headers);
                        }
                    }
                });
                handled = true;
            }

            // TODO: Add heartbeat event processing here if required.

            return handled;
        } catch (Exception e) {
          switch (connectorConfig.getEventProcessingFailureHandlingMode()) {
            case FAIL:
               throw new ConnectException("Error while processing event at offset " + changeRecordEmitter.getOffset().getOffset(), e);
            case WARN:
                LOGGER.warn(
                   "Error while processing event at offset {}",
                   changeRecordEmitter.getOffset().getOffset());
                   break;
            case SKIP:
                LOGGER.debug(
                  "Error while processing event at offset {}",
                  changeRecordEmitter.getOffset().getOffset());
                break;
            }
          return false;
        }
    }

    @Override
    public void dispatchTransactionStartedEvent(YBPartition partition, String transactionId, OffsetContext offset) throws InterruptedException {
        this.transactionMonitor.transactionStartedEvent(partition, transactionId, offset);
    }

    @Override
    public void dispatchTransactionCommittedEvent(YBPartition partition, OffsetContext offset) throws InterruptedException {
        this.transactionMonitor.transactionCommittedEventImpl(partition, (YugabyteDBOffsetContext) offset);
    }

    private void enqueueTransactionMessage(SourceRecord record) throws InterruptedException {
        queue.enqueue(new DataChangeEvent(record));
    }

    private void enqueueLogicalDecodingMessage(SourceRecord record) throws InterruptedException {
        queue.enqueue(new DataChangeEvent(record));
    }

    private final class YugabyteDBStreamingChangeRecordReceiver implements ChangeRecordEmitter.Receiver<YBPartition> {
        @Override
        public void changeRecord(YBPartition partition,
                                 DataCollectionSchema dataCollectionSchema,
                                 Envelope.Operation operation,
                                 Object key, Struct value,
                                 OffsetContext offsetContext,
                                 ConnectHeaders headers) throws InterruptedException {
            Objects.requireNonNull(value, "value must not be null");

            LOGGER.info("Received change record for {} operation on key {}", operation, key);

            // Truncate events must have null key schema as they are sent to table topics without keys
            Schema keySchema = (key == null && operation == Envelope.Operation.TRUNCATE) ? null
                                 : dataCollectionSchema.keySchema();
            String topicName = topicSelector.topicNameFor((T) dataCollectionSchema.id());

            SourceRecord record = new SourceRecord(partition.getSourcePartition(),
              offsetContext.getOffset(),
              topicName, null,
              keySchema, key,
              dataCollectionSchema.getEnvelopeSchema().schema(),
              value,
              null,
              headers);

            queue.enqueue(changeEventCreator.createDataChangeEvent(record));
            LOGGER.info("Queued change event source record " + record);

            if (emitTombstonesOnDelete && operation == Envelope.Operation.DELETE) {
                SourceRecord tombStone = record.newRecord(
                  record.topic(),
                  record.kafkaPartition(),
                  record.keySchema(),
                  record.key(),
                  null, // value schema
                  null, // value
                  record.timestamp(),
                  record.headers());

                queue.enqueue(changeEventCreator.createDataChangeEvent(tombStone));
                LOGGER.info("Queued change event tombstone record " + tombStone);
            }
        }
    }
}
