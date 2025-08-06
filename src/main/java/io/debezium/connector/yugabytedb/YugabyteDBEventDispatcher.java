package io.debezium.connector.yugabytedb;

/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatFactory;
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

import io.debezium.config.CommonConnectorConfig.WatermarkStrategy;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.yugabytedb.connection.LogicalDecodingMessage;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.signal.channels.SourceSignalChannel;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;

import java.time.Instant;
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
    private final TopicNamingStrategy<T> topicNamingStrategy;
    private final DatabaseSchema<T> schema;
    private DataChangeEventListener<YBPartition> eventListener = DataChangeEventListener.NO_OP();
    private final InconsistentSchemaHandler<YBPartition, T> inconsistentSchemaHandler;
    private final boolean neverSkip;
    private final EnumSet<Envelope.Operation> skippedOperations;
    private final DataCollectionFilters.DataCollectionFilter<T> filter;
    private final YugabyteDBTransactionMonitor transactionMonitor;
    private final YugabyteDBStreamingChangeRecordReceiver streamingReceiver;
    
    protected SignalProcessor<YBPartition, YugabyteDBOffsetContext> signalProcessor;
    protected final SourceSignalChannel sourceSignalChannel;

    public YugabyteDBEventDispatcher(YugabyteDBConnectorConfig connectorConfig, TopicNamingStrategy<T> topicNamingStrategy,
                                   DatabaseSchema<T> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilters.DataCollectionFilter<T> filter,
                                   ChangeEventCreator changeEventCreator, InconsistentSchemaHandler<YBPartition, T> inconsistentSchemaHandler,
                                   EventMetadataProvider metadataProvider, HeartbeatFactory<T> heartbeatFactory, SchemaNameAdjuster schemaNameAdjuster,
                                   SignalProcessor<YBPartition, YugabyteDBOffsetContext> signalProcessor, JdbcConnection jdbcConnection) {
        super(connectorConfig, topicNamingStrategy, schema, queue, filter, changeEventCreator, metadataProvider,
                Heartbeat.DEFAULT_NOOP_HEARTBEAT, schemaNameAdjuster, signalProcessor);
        this.connectorConfig = connectorConfig;
        this.changeEventCreator = changeEventCreator;
        this.queue = queue;
        this.logicalDecodingMessageMonitor = new LogicalDecodingMessageMonitor(connectorConfig, this::enqueueLogicalDecodingMessage);
        this.messageFilter = connectorConfig.getMessageFilter();
        this.topicNamingStrategy = topicNamingStrategy;
        this.streamingReceiver = new YugabyteDBStreamingChangeRecordReceiver();
        this.inconsistentSchemaHandler = inconsistentSchemaHandler != null ? inconsistentSchemaHandler : this::errorOnMissingSchema;
        this.skippedOperations = connectorConfig.getSkippedOperations();
        this.emitTombstonesOnDelete = connectorConfig.isEmitTombstoneOnDelete();
        this.neverSkip = connectorConfig.supportsOperationFiltering() || this.skippedOperations.isEmpty();
        this.filter = filter;
        this.schema = schema;
        this.signalProcessor = signalProcessor;
        this.transactionMonitor = new YugabyteDBTransactionMonitor(connectorConfig, metadataProvider, schemaNameAdjuster, this::enqueueTransactionMessage, topicNamingStrategy.transactionTopic());

        this.sourceSignalChannel = null;
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
                LOGGER.trace("Filtered data change event for {}", dataCollectionId);
                eventListener.onFilteredEvent(partition, "source = " + dataCollectionId, changeRecordEmitter.getOperation());
                dispatchFilteredEvent(changeRecordEmitter.getPartition(), changeRecordEmitter.getOffset());
            } else {
                DataCollectionSchema dataCollectionSchema = schema.schemaFor(dataCollectionId);

                // TODO handle as per inconsistent schema info option
                if (dataCollectionSchema == null) {
                    final Optional<DataCollectionSchema> replacementSchema = inconsistentSchemaHandler.handle(partition,
                      dataCollectionId, changeRecordEmitter);
                    if (!replacementSchema.isPresent()) {
                        return false;
                    }
                    dataCollectionSchema = replacementSchema.get();
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
            default:
                LOGGER.debug("Error while processing event at offset {}: {}",
                  changeRecordEmitter.getOffset().getOffset(), e.getMessage());
            }
          return false;
        }
    }

    @Override
    public void dispatchTransactionStartedEvent(YBPartition partition, String transactionId, OffsetContext offset, Instant timestamp) throws InterruptedException {
        this.transactionMonitor.transactionStartedEvent(partition, transactionId, offset, timestamp);
    }

    @Override
    public void dispatchTransactionCommittedEvent(YBPartition partition, OffsetContext offset, Instant timestamp) throws InterruptedException {
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

            LOGGER.trace("Received change record for {} operation on key {}", operation, key);

            // Truncate events must have null key schema as they are sent to table topics without keys
            Schema keySchema = (key == null && operation == Envelope.Operation.TRUNCATE) ? null
                                 : dataCollectionSchema.keySchema();
            String topicName = topicNamingStrategy.dataChangeTopic((T) dataCollectionSchema.id());

            SourceRecord record = new SourceRecord(partition.getSourcePartition(),
              offsetContext.getOffset(),
              topicName, null,
              keySchema, key,
              dataCollectionSchema.getEnvelopeSchema().schema(),
              value,
              null,
              headers);

            queue.enqueue(changeEventCreator.createDataChangeEvent(record));

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
            }
        }
    }
}
