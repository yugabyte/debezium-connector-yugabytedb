/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.spi.Partition;

public class YugabyteDBPartition implements Partition {
    @Override
    public Map<String, String> getSourcePartition() {
        throw new UnsupportedOperationException("Currently unsupported by the YugabyteDB " +
                "connector");
    }

    @Override
    public boolean equals(Object obj) {
        throw new UnsupportedOperationException("Currently unsupported by the YugabyteDB connector");
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("Currently unsupported by the YugabyteDB connector");
    }

    static class Provider implements Partition.Provider<YBPartition> {
        private final YugabyteDBConnectorConfig connectorConfig;
        private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBPartition.class);

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
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            catch (ClassNotFoundException e) {
                e.printStackTrace();
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
