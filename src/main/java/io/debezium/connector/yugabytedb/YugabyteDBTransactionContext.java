package io.debezium.connector.yugabytedb;

import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.schema.DataCollectionId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Class to manage the distributed transaction related events for YugabyteDB.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBTransactionContext extends TransactionContext {
	private static final String OFFSET_TRANSACTION_ID = TransactionMonitor.DEBEZIUM_TRANSACTION_KEY + "_" + TransactionMonitor.DEBEZIUM_TRANSACTION_ID_KEY;
	private static final String OFFSET_TABLE_COUNT_PREFIX = TransactionMonitor.DEBEZIUM_TRANSACTION_KEY + "_"
																														+ TransactionMonitor.DEBEZIUM_TRANSACTION_DATA_COLLECTION_ORDER_KEY + "_";
	private static final int OFFSET_TABLE_COUNT_PREFIX_LENGTH = OFFSET_TABLE_COUNT_PREFIX.length();
	private String transactionId = null;

	private Map<String, String> partitionTransactions;
	private Map<String, Long> partitionTotalEventCount = new HashMap<>();
	private final Map<String, Long> perTableEventCount = new HashMap<>();
	private final Map<String, Long> viewPerTableEventCount = Collections.unmodifiableMap(perTableEventCount);
	private long totalEventCount = 0;

	private void reset() {
		transactionId = null;
		totalEventCount = 0;
		perTableEventCount.clear();
	}

	private void reset(String partitionId) {
		partitionTransactions.put(partitionId, null);
		partitionTotalEventCount.put(partitionId, 0L);
	}

	public static YugabyteDBTransactionContext load(Map<String, ?> offsets) {
		final Map<String, Object> o = (Map<String, Object>) offsets;
		final YugabyteDBTransactionContext context = new YugabyteDBTransactionContext();

		context.transactionId = (String) o.get(OFFSET_TRANSACTION_ID);

		for (final Map.Entry<String, Object> offset : o.entrySet()) {
			if (offset.getKey().startsWith(OFFSET_TABLE_COUNT_PREFIX)) {
				final String dataCollectionId = offset.getKey().substring(OFFSET_TABLE_COUNT_PREFIX_LENGTH);
				final Long count = (Long) offset.getValue();
				context.perTableEventCount.put(dataCollectionId, count);
			}
		}

		context.totalEventCount = context.perTableEventCount.values().stream().mapToLong(x -> x).sum();

		return context;
	}

	public boolean isTransactionInProgress(YBPartition partition) {
		return partitionTransactions.get(partition.getId()) != null;
	}

	public String getTransactionId(YBPartition partition) {
		return partitionTransactions.get(partition.getId());
	}

	public long getTotalEventCount(YBPartition partition) {
		return partitionTotalEventCount.get(partition.getId());
	}

	public void beginTransaction(YBPartition partition, String txId) {
		partitionTransactions.put(partition.getId(), txId);
	}

	public void endTransaction(YBPartition partition) {
		reset(partition.getId());
	}

	public long event(YBPartition partition, DataCollectionId source) {
		return partitionTotalEventCount.merge(partition.getId(), 1L, Long::sum);
	}

	public long event(DataCollectionId source) {
		totalEventCount++;
		final String sourceName = source.toString();
		final long dataCollectionEventOrder = perTableEventCount.getOrDefault(sourceName, 0L) + 1;
		perTableEventCount.put(sourceName, dataCollectionEventOrder);
		return dataCollectionEventOrder;
	}

	public Map<String, Long> getPerTableEventCount() {
		return viewPerTableEventCount;
	}

	@Override
	public String toString() {
		return "YugabyteDBTransactionContext [currentTransactionId=" + transactionId + ", perTableEventCount="
						 + perTableEventCount + ", totalEventCount=" + totalEventCount + "]";
	}
}
