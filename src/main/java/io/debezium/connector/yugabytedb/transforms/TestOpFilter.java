package io.debezium.connector.yugabytedb.transforms;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REPRO-only SMT: drops records whose Debezium envelope 'op' is in the configured set.
 * Filters at the KAFKA CONNECT layer (after poll()), unlike skipped.operations which
 * filters inside the connector. Non-envelope records (heartbeats etc.) pass through.
 */
public class TestOpFilter<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestOpFilter.class);

    private final Set<String> ops = new HashSet<>();

    @Override
    public R apply(R record) {
        if (record == null || record.value() == null || !(record.value() instanceof Struct)) {
            return record;
        }
        Struct value = (Struct) record.value();
        if (value.schema().field("op") == null) {
            return record; // not a CDC envelope (heartbeat/schema message) -> pass through
        }
        String op = value.getString("op");
        if (op != null && ops.contains(op)) {
            Object sourceOffset = (record instanceof SourceRecord) ? ((SourceRecord) record).sourceOffset() : null;
            LOGGER.info("REPRO: SMT TestOpFilter DROPPING op={} topic={} sourceOffset={}", op, record.topic(), sourceOffset);
            return null; // filtered at the SMT layer
        }
        return record;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef().define("ops", ConfigDef.Type.LIST, "u,d,t",
                ConfigDef.Importance.HIGH, "Comma-separated Debezium op codes to drop (c,u,d,t,r)");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Map<String, ?> configs) {
        ops.clear();
        Object v = configs.get("ops");
        List<String> parts;
        if (v instanceof List) {
            parts = (List<String>) v;
        }
        else if (v != null) {
            parts = Arrays.asList(v.toString().split(","));
        }
        else {
            parts = Arrays.asList("u", "d", "t");
        }
        for (String o : parts) {
            if (o != null && !o.trim().isEmpty()) {
                ops.add(o.trim().toLowerCase(Locale.ROOT));
            }
        }
        LOGGER.info("REPRO: TestOpFilter configured to drop ops {}", ops);
    }

    @Override
    public void close() {
    }
}
