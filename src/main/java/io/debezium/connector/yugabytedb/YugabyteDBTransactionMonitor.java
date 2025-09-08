package io.debezium.connector.yugabytedb;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.data.Envelope;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.pipeline.txmetadata.TransactionStatus;
import io.debezium.pipeline.txmetadata.TransactionStructMaker;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.schema.SchemaNameAdjuster;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

/**
 * Custom implementation of transaction monitor for YugabyteDB.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBTransactionMonitor extends TransactionMonitor {
	private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBTransactionMonitor.class);

	private static final String PARTITION_ID_KEY = "partition_id";

	private final Schema transactionKeySchema;
	private final Schema transactionValueSchema;
	private final EventMetadataProvider eventMetadataProvider;
	private final String topicName;
	private final BlockingConsumer<SourceRecord> sender;
	private final CommonConnectorConfig connectorConfig;

	public static final Schema TRANSACTION_BLOCK_SCHEMA = SchemaBuilder.struct().optional()
		.field(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, Schema.STRING_SCHEMA)
		.field(TransactionStructMaker.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, Schema.INT64_SCHEMA)
		.field(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, Schema.INT64_SCHEMA)
		.build();

	private static final Schema EVENT_COUNT_PER_DATA_COLLECTION_SCHEMA = SchemaBuilder.struct()
		.field(TransactionStructMaker.DEBEZIUM_TRANSACTION_COLLECTION_KEY, Schema.STRING_SCHEMA)
		.field(TransactionStructMaker.DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY, Schema.INT64_SCHEMA)
		.build();

	public YugabyteDBTransactionMonitor(CommonConnectorConfig connectorConfig,
										EventMetadataProvider eventMetadataProvider,
										SchemaNameAdjuster schemaNameAdjuster,
										BlockingConsumer<SourceRecord> sender,
										String topicName) {
		super(connectorConfig, eventMetadataProvider, schemaNameAdjuster, sender, topicName);
		Objects.requireNonNull(eventMetadataProvider);

		transactionKeySchema = SchemaBuilder.struct()
			.name(schemaNameAdjuster.adjust("io.debezium.connector.common.TransactionMetadataKey"))
			.field(DEBEZIUM_TRANSACTION_ID_KEY, Schema.STRING_SCHEMA)
			.build();

		transactionValueSchema = SchemaBuilder.struct()
			.name(schemaNameAdjuster.adjust("io.debezium.connector.common.TransactionMetadataValue"))
			.field(TransactionStructMaker.DEBEZIUM_TRANSACTION_STATUS_KEY, Schema.STRING_SCHEMA)
			.field(TransactionStructMaker.DEBEZIUM_TRANSACTION_ID_KEY, Schema.STRING_SCHEMA)
			.field(TransactionStructMaker.DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY, Schema.OPTIONAL_INT64_SCHEMA)
			.field(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTIONS_KEY, SchemaBuilder.array(EVENT_COUNT_PER_DATA_COLLECTION_SCHEMA).optional().build())
			.field(PARTITION_ID_KEY, SchemaBuilder.STRING_SCHEMA)
			.build();

		this.topicName = topicName;
		this.eventMetadataProvider = eventMetadataProvider;
		this.sender = sender;
		this.connectorConfig = connectorConfig;
	}

	@Override
	public void dataEvent(Partition partition, DataCollectionId source, OffsetContext offset, Object key, Struct value) throws InterruptedException {
		dataEventImpl((YBPartition) partition, source, (YugabyteDBOffsetContext) offset, key, value);
	}

	private void dataEventImpl(YBPartition partition, DataCollectionId source, YugabyteDBOffsetContext offset, Object key, Struct value) throws InterruptedException {
		if (!connectorConfig.shouldProvideTransactionMetadata()) {
			return;
		}
		final YugabyteDBTransactionContext transactionContext = (YugabyteDBTransactionContext) offset.getTransactionContext();

		final String txId = eventMetadataProvider.getTransactionId(source, offset, key, value);
		if (txId == null) {
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("Event '{}' has no transaction id", eventMetadataProvider.toSummaryString(source, offset, key, value));
			}
			return;
		}

		if (!transactionContext.isTransactionInProgress(partition)) {
			transactionContext.beginTransaction(partition, txId);
			beginTransaction(partition, offset);
		}
		else if (!transactionContext.getTransactionId(partition).equals(txId)) {
			LOGGER.info("Received a different transaction ID ({}) for the partition {} " +
									"with another transaction ({}) in progress", txId, partition.getFullPartitionName(),
									transactionContext.getTransactionId(partition));
		}
		transactionEvent(partition, offset, source, value);
	}

	private void transactionEvent(YBPartition partition, YugabyteDBOffsetContext offsetContext, DataCollectionId source, Struct value) {
		YugabyteDBTransactionContext transactionContext = (YugabyteDBTransactionContext) offsetContext.getTransactionContext();
		final long dataCollectionEventOrder = transactionContext.event(partition, source);
		if (value == null) {
			LOGGER.debug("Event with key {} without value. Cannot enrich source block.");
			return;
		}
		final Struct txStruct = new Struct(TRANSACTION_BLOCK_SCHEMA);
		txStruct.put(DEBEZIUM_TRANSACTION_ID_KEY, transactionContext.getTransactionId(partition));
		txStruct.put(TransactionStructMaker.DEBEZIUM_TRANSACTION_TOTAL_ORDER_KEY, transactionContext.getTotalEventCount(partition));
		txStruct.put(TransactionStructMaker.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY, dataCollectionEventOrder);
		value.put(Envelope.FieldName.TRANSACTION, txStruct);
	}

	public void transactionStartedEvent(Partition partition, String transactionId, OffsetContext offset, Instant timestamp) throws InterruptedException {
		if (!connectorConfig.shouldProvideTransactionMetadata()) {
			return;
		}
		YugabyteDBTransactionContext transactionContext = (YugabyteDBTransactionContext) offset.getTransactionContext();
		transactionContext.beginTransaction((YBPartition) partition, transactionId);
		beginTransaction((YBPartition) partition, (YugabyteDBOffsetContext) offset);
	}

	public void transactionCommittedEvent(Partition partition, OffsetContext offset) throws InterruptedException {
		transactionCommittedEventImpl((YBPartition) partition, (YugabyteDBOffsetContext) offset);
	}

	public void transactionCommittedEventImpl(YBPartition partition, YugabyteDBOffsetContext offsetContext) throws InterruptedException {
		if (!connectorConfig.shouldProvideTransactionMetadata()) {
			return;
		}

		YugabyteDBTransactionContext transactionContext = (YugabyteDBTransactionContext) offsetContext.getTransactionContext();
		if (transactionContext.isTransactionInProgress(partition)) {
			endTransaction(partition, offsetContext);
		}

		transactionContext.endTransaction(partition);
	}

	private void beginTransaction(YBPartition partition, YugabyteDBOffsetContext offsetContext) throws InterruptedException {
		YugabyteDBTransactionContext transactionContext = (YugabyteDBTransactionContext) offsetContext.getTransactionContext();
		final Struct key = new Struct(transactionKeySchema);
		key.put(DEBEZIUM_TRANSACTION_ID_KEY, transactionContext.getTransactionId(partition));
		final Struct value = new Struct(transactionValueSchema);
		value.put(TransactionStructMaker.DEBEZIUM_TRANSACTION_STATUS_KEY, TransactionStatus.BEGIN.name());
		value.put(DEBEZIUM_TRANSACTION_ID_KEY, transactionContext.getTransactionId(partition));
		value.put(PARTITION_ID_KEY, partition.getFullPartitionName());

		sender.accept(new SourceRecord(partition.getSourcePartition(), offsetContext.getOffset(),
			topicName, null, key.schema(), key, value.schema(), value));
	}

	private void endTransaction(YBPartition partition, OffsetContext offsetContext) throws InterruptedException {
		YugabyteDBTransactionContext transactionContext = (YugabyteDBTransactionContext) offsetContext.getTransactionContext();
		final Struct key = new Struct(transactionKeySchema);
		key.put(DEBEZIUM_TRANSACTION_ID_KEY, transactionContext.getTransactionId(partition));
		final Struct value = new Struct(transactionValueSchema);
		value.put(TransactionStructMaker.DEBEZIUM_TRANSACTION_STATUS_KEY, TransactionStatus.END.name());
		value.put(DEBEZIUM_TRANSACTION_ID_KEY, transactionContext.getTransactionId(partition));
		value.put(TransactionStructMaker.DEBEZIUM_TRANSACTION_EVENT_COUNT_KEY, transactionContext.getTotalEventCount(partition));
		value.put(PARTITION_ID_KEY, partition.getFullPartitionName());

		// TODO: Process and add per table event count here if required.

		sender.accept(new SourceRecord(partition.getSourcePartition(), offsetContext.getOffset(),
			topicName, null, key.schema(), key, value.schema(), value));
	}
}
