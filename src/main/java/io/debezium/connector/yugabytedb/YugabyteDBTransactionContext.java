package io.debezium.connector.yugabytedb;

import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.spi.schema.DataCollectionId;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to manage the distributed transaction related events for YugabyteDB.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBTransactionContext extends TransactionContext {
	private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBTransactionContext.class);

	private Map<String, String> partitionTransactions = new HashMap<>();
	private Map<String, Long> partitionTotalEventCount = new HashMap<>();

	private void reset(String partitionId) {
		partitionTransactions.put(partitionId, null);
		partitionTotalEventCount.put(partitionId, 0L);
	}

	public static YugabyteDBTransactionContext load(Map<String, ?> offsets) {
		// TODO Vaibhav: Do we actually load any transaction context from offsets? If not, remove this.
		final Map<String, Object> o = (Map<String, Object>) offsets;
		final YugabyteDBTransactionContext context = new YugabyteDBTransactionContext();

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
		partitionTotalEventCount.put(partition.getId(), 0L);
	}

	public void endTransaction(YBPartition partition) {
		reset(partition.getId());
	}

	public long event(YBPartition partition, DataCollectionId source) {
		return partitionTotalEventCount.merge(partition.getId(), 1L, Long::sum);
	}

	public String toString(YBPartition partition) {
		if (partitionTransactions.get(partition.getId()) == null) {
			LOGGER.warn("No transaction in progress for given partition ID {}, returning empty string", partition.getId());
			return "";
		}

		return String.format("YugabyteDBTransactionContext[partition=%s transaction_id=%s totalEventCount=%d]",
			partition.getId(), partitionTransactions.get(partition.getId()),
			partitionTotalEventCount.get(partition.getId()));
	}

	// Override the existing functions to throw exceptions if used anywhere to restrict their usage
	// as they may cause issues.

	@Override
	public long event(DataCollectionId source) {
		throw new UnsupportedOperationException("event(DataCollectionId) is not implemented, use event(YBPartition, DataCollectionId)");
	}

	@Override
	public boolean isTransactionInProgress() {
		throw new UnsupportedOperationException("isTransactionInProgress() is not implemented, use isTransactionInProgress(YBPartition)");
	}

	@Override
	public void beginTransaction(String txId) {
		throw new UnsupportedOperationException("beginTransaction(String) is not implemented, use beginTransaction(YBPartition, String)");
	}

	@Override
	public long getTotalEventCount() {
		throw new UnsupportedOperationException("getTotalEventCount() is not implemented, use getTotalEventCount(YBPartition)");
	}

	@Override
	public void endTransaction() {
		throw new UnsupportedOperationException("endTransaction() is not implemented, use endTransaction(YBPartition)");
	}

	@Override
	public String getTransactionId() {
		throw new UnsupportedOperationException("getTransactionId() is not supported, use getTransactionId(YBPartition)");
	}
}
