package io.debezium.connector.yugabytedb;

import java.util.*;

import org.yb.client.*;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

public class YBPartition implements Partition {

    private static final String TABLETS_PARTITION_KEY = "tabletids";

    private final String listOfTablets;

    public YBPartition(String listOfTablets) {
        this.listOfTablets = listOfTablets;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(TABLETS_PARTITION_KEY, listOfTablets);
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
        return Objects.equals(listOfTablets, other.listOfTablets);
    }

    @Override
    public int hashCode() {
        return listOfTablets.hashCode();
    }

}
