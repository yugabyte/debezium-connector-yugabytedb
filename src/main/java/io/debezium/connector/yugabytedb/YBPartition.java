package io.debezium.connector.yugabytedb;

import java.util.Map;
import java.util.Objects;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

public class YBPartition implements Partition {
    private static final String TABLET_PARTITION_KEY = "tabletid";

    private final String tabletId;

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
}
