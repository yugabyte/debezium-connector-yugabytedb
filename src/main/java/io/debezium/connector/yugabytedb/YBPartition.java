package io.debezium.connector.yugabytedb;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.debezium.connector.yugabytedb.connection.HashPartition;
import io.debezium.connector.yugabytedb.util.YugabyteDBConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

/**
 * Partition class to represent the Debezium partitions for YugabyteDB.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YBPartition implements Partition {
    private static final String PARTITION_KEY = "yb_partition";

    private final String tabletId;
    private final String tableId;

    public YBPartition(String tableId, String tabletId) {
        this.tableId = tableId;
        this.tabletId = tabletId;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(PARTITION_KEY, getId());
    }

    public String getTableId() {
        return this.tableId;
    }

    public String getTabletId() {
        return this.tabletId;
    }

    /**
     * @return the ID of this partition in the format {@code tableId.tabletId}
     */
    public String getId() {
        return getFullPartitionName();
    }

    /**
     * Get the full ID of this partition identified by {@code tableId.tabletId} - this will be used
     * to form the metric names.
     * @return
     */
    public String getFullPartitionName() {
        return getTableId() + "." + getTabletId();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final YBPartition other = (YBPartition) obj;

        return this.tabletId.equals(other.getTabletId()) && this.tableId.equals(other.getTableId());
    }

    @Override
    public int hashCode() {
        return getFullPartitionName().hashCode();
    }

    @Override
    public String toString() {
        return String.format("YBPartition {tableId=%s, tabletId=%s}", this.tableId, this.tabletId);
    }

    public static YBPartition fromFullPartitionId(String fullPartitionId) {
        String[] tableTablet = fullPartitionId.split("\\.");

        if (tableTablet.length == 1) {
            throw new RuntimeException("Full partition ID expected of the form tabletId.tabletId, provided " + fullPartitionId);
        }

        return new YBPartition(tableTablet[0], tableTablet[1]);
    }

    static class Provider implements Partition.Provider<YBPartition> {
        private final YugabyteDBConnectorConfig connectorConfig;
        private static final Logger LOGGER = LoggerFactory.getLogger(YBPartition.class);

        Provider(YugabyteDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<YBPartition> getPartitions() {
            String tabletListSerialized = this.connectorConfig.getConfig().getString(YugabyteDBConnectorConfig.HASH_RANGES_LIST);
            List<HashPartition> tabletPairList;
            try {
                tabletPairList = YugabyteDBConnectorUtils.populatePartitionRanges(tabletListSerialized);
                LOGGER.debug("The tablet list is " + tabletPairList);
            } catch (IOException | ClassNotFoundException e) {
                // The task should fail if tablet list cannot be deserialized
                throw new DebeziumException("Error while deserializing tablet list", e);
            }

            Set<YBPartition> partitions = new HashSet<>();
            for (HashPartition partition : tabletPairList) {
                partitions.add(partition.toYBPartition());
            }
            LOGGER.debug("The partition being returned is " + partitions);
            return partitions;
        }
    }
}
