package io.debezium.connector.yugabytedb;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

public class YBPartition implements Partition {
    private static final String TABLET_PARTITION_KEY = "tabletid";

    private final String tabletId;

    public YBPartition() {
        this.tabletId = "";
    }

    public YBPartition(String tabletId) {
        this.tabletId = tabletId;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(TABLET_PARTITION_KEY, tabletId);
    }

    public String getTabletId() {
        return this.tabletId;
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
        return Objects.equals(tabletId, other.tabletId);
    }

    @Override
    public int hashCode() {
        return tabletId.hashCode();
    }

    @Override
    public String toString() {
        return "YBPartition{" +
                "tabletId='" + tabletId + '\'' +
                '}';
    }

    static class Provider implements Partition.Provider<YBPartition> {
        private final YugabyteDBConnectorConfig connectorConfig;
        private static final Logger LOGGER = LoggerFactory.getLogger(YBPartition.class);

        Provider(YugabyteDBConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public Set<YBPartition> getPartitions() {
            String tabletList = this.connectorConfig.getConfig().getString(YugabyteDBConnectorConfig.TABLET_LIST);
            List<Pair<String, String>> tabletPairList = null;
            try {
                tabletPairList = (List<Pair<String, String>>) ObjectUtil.deserializeObjectFromString(tabletList);
                LOGGER.debug("The tablet list is " + tabletPairList);
            } catch (IOException | ClassNotFoundException e) {
                // The task should fail if tablet list cannot be deserialized
                throw new DebeziumException("Error while deserializing tablet list", e);
            }

            Set<YBPartition> partititons = new HashSet<>();
            for (Pair<String, String> tabletPair : tabletPairList) {
                partititons.add(new YBPartition(tabletPair.getRight()));
            }
            LOGGER.debug("The partition being returned is " + partititons);
            return partititons;
        }
    }
}
