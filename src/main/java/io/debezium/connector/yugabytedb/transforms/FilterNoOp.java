package io.debezium.connector.yugabytedb.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterNoOp<R extends ConnectRecord<R>> implements Transformation<R>  {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilterNoOp.class);

    @Override
    public R apply(final R record) {
        if (record.value() == null && record.key() == null && record.keySchema() == null && record.valueSchema() == null) {
            LOGGER.info("Found a NO_OP record, filtering it out!");
            return null;
        } else {
            return record;
        }
     }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
    }
}
