package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.config.Field;

/**
 * Validation of the GetChanges concurrency settings; needs no database.
 */
public class YugabyteDBPollConcurrencyConfigTest {

    private static final Field.Set FIELDS = Field.setOf(
            YugabyteDBConnectorConfig.CDC_POLL_MAX_CONCURRENT_TABLETS,
            YugabyteDBConnectorConfig.CDC_POLL_MAX_INFLIGHT_BYTES);

    private static Map<String, ConfigValue> validate(Configuration config) {
        return config.validate(FIELDS);
    }

    @Test
    public void defaultsAreSequentialWithTheServerResponseLimit() {
        Configuration config = Configuration.create().build();

        assertEquals(1, config.getInteger(YugabyteDBConnectorConfig.CDC_POLL_MAX_CONCURRENT_TABLETS));
        assertEquals(0L, config.getLong(YugabyteDBConnectorConfig.CDC_POLL_MAX_INFLIGHT_BYTES));
        validate(config).values().forEach(v -> assertTrue(v.errorMessages().isEmpty(), v.name()));
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 8, YugabyteDBConnectorConfig.MAX_CDC_POLL_MAX_CONCURRENT_TABLETS })
    public void acceptsConcurrencyWithinBounds(int value) {
        Configuration config = Configuration.create()
                .with(YugabyteDBConnectorConfig.CDC_POLL_MAX_CONCURRENT_TABLETS, value).build();

        assertTrue(validate(config).get(YugabyteDBConnectorConfig.CDC_POLL_MAX_CONCURRENT_TABLETS.name())
                .errorMessages().isEmpty());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, -1, YugabyteDBConnectorConfig.MAX_CDC_POLL_MAX_CONCURRENT_TABLETS + 1 })
    public void rejectsConcurrencyOutOfBounds(int value) {
        Configuration config = Configuration.create()
                .with(YugabyteDBConnectorConfig.CDC_POLL_MAX_CONCURRENT_TABLETS, value).build();

        assertFalse(validate(config).get(YugabyteDBConnectorConfig.CDC_POLL_MAX_CONCURRENT_TABLETS.name())
                .errorMessages().isEmpty());
    }

    @Test
    public void rejectsANegativeInflightBudget() {
        Configuration config = Configuration.create()
                .with(YugabyteDBConnectorConfig.CDC_POLL_MAX_INFLIGHT_BYTES, -1L).build();

        assertFalse(validate(config).get(YugabyteDBConnectorConfig.CDC_POLL_MAX_INFLIGHT_BYTES.name())
                .errorMessages().isEmpty());
    }
}
