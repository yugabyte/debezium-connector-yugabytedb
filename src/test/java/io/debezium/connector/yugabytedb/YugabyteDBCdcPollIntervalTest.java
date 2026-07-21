package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 * Unit tests to verify the resolution of the deprecated {@code cdc.poll.interval.ms}
 * property against {@code cdc.poll.interval.active.ms} and {@code cdc.poll.interval.idle.ms}.
 *
 * @author Suranjan Kumar
 */
public class YugabyteDBCdcPollIntervalTest {

    @Test
    public void shouldUseDefaultsWhenNothingIsConfigured() {
        Configuration config = Configuration.create().build();

        long[] intervals = YugabyteDBConnectorConfig.resolveCdcPollIntervals(config);

        assertEquals(YugabyteDBConnectorConfig.DEFAULT_CDC_POLL_INTERVAL_ACTIVE_MS, intervals[0]);
        assertEquals(YugabyteDBConnectorConfig.DEFAULT_CDC_POLL_INTERVAL_IDLE_MS, intervals[1]);
    }

    @Test
    public void shouldUseActiveAndIdleValuesWhenDeprecatedPropertyIsNotSet() {
        Configuration config = Configuration.create()
                .with(YugabyteDBConnectorConfig.CDC_POLL_INTERVAL_ACTIVE_MS, 25)
                .with(YugabyteDBConnectorConfig.CDC_POLL_INTERVAL_IDLE_MS, 750)
                .build();

        long[] intervals = YugabyteDBConnectorConfig.resolveCdcPollIntervals(config);

        assertEquals(25, intervals[0]);
        assertEquals(750, intervals[1]);
    }

    @Test
    public void shouldUseDeprecatedValueForBothIntervalsWhenSet() {
        Configuration config = Configuration.create()
                .with(YugabyteDBConnectorConfig.CDC_POLL_INTERVAL_MS, 300)
                .build();

        long[] intervals = YugabyteDBConnectorConfig.resolveCdcPollIntervals(config);

        assertEquals(300, intervals[0]);
        assertEquals(300, intervals[1]);
    }

    @Test
    public void shouldIgnoreActiveAndIdleValuesWhenDeprecatedPropertyIsSet() {
        Configuration config = Configuration.create()
                .with(YugabyteDBConnectorConfig.CDC_POLL_INTERVAL_MS, 300)
                .with(YugabyteDBConnectorConfig.CDC_POLL_INTERVAL_ACTIVE_MS, 25)
                .with(YugabyteDBConnectorConfig.CDC_POLL_INTERVAL_IDLE_MS, 750)
                .build();

        long[] intervals = YugabyteDBConnectorConfig.resolveCdcPollIntervals(config);

        assertEquals(300, intervals[0]);
        assertEquals(300, intervals[1]);
    }
}
