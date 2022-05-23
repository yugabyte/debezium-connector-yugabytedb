package io.debezium.connector.yugabytedb;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

public class YugabyteDBConnectorTaskConfig extends YugabyteDBConnectorConfig {
    public YugabyteDBConnectorTaskConfig(Configuration config) {
        super(config);
    }

    public static final Field TABLET_LSIT = Field.create("TASK." + "tabletList")
            .withDisplayName("YugabyteDB Tablet LIST for a Task ")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.MEDIUM)
            .withDescription("Internal task config: List of TabletIds to be fetched by this task");
}
