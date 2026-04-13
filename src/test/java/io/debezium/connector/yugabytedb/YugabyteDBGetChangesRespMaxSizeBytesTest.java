package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests to verify the {@code getchangesRespMaxSizeBytes} configuration
 * property is correctly parsed and surfaced by {@link YugabyteDBConnectorConfig}.
 */
public class YugabyteDBGetChangesRespMaxSizeBytesTest {

    /**
     * Builds a minimal {@link YugabyteDBConnectorConfig} from the supplied
     * builder, adding only the properties required by the constructor.
     */
    private static YugabyteDBConnectorConfig buildConfig(Configuration.Builder builder) {
        builder.with(RelationalDatabaseConnectorConfig.SERVER_NAME, "test-server")
               .with(YugabyteDBConnectorConfig.MASTER_ADDRESSES, "127.0.0.1:7100")
               .with(YugabyteDBConnectorConfig.DATABASE_NAME, "yugabyte")
               .with(YugabyteDBConnectorConfig.HOSTNAME, "127.0.0.1")
               .with(YugabyteDBConnectorConfig.PORT, 5433)
               .with(YugabyteDBConnectorConfig.USER, "yugabyte")
               .with(YugabyteDBConnectorConfig.PASSWORD, "yugabyte");
        return new YugabyteDBConnectorConfig(builder.build());
    }

    @Test
    public void shouldReturnNullWhenPropertyIsNotSet() {
        YugabyteDBConnectorConfig config = buildConfig(Configuration.create());

        assertNull(config.getchangesRespMaxSizeBytes(),
                "Expected null when cdc.getchanges.resp.max.size.bytes is not configured");
    }

    @Test
    public void shouldReturnConfiguredValueWhenPropertyIsSet() {
        long expectedBytes = 8_388_608L; // 8 MB
        YugabyteDBConnectorConfig config = buildConfig(
                Configuration.create()
                        .with(YugabyteDBConnectorConfig.GETCHANGES_RESP_MAX_SIZE_BYTES, expectedBytes));

        assertEquals(expectedBytes, config.getchangesRespMaxSizeBytes(),
                "Expected the explicitly configured value to be returned");
    }

    @Test
    public void shouldReturnZeroWhenPropertyIsSetToZero() {
        YugabyteDBConnectorConfig config = buildConfig(
                Configuration.create()
                        .with(YugabyteDBConnectorConfig.GETCHANGES_RESP_MAX_SIZE_BYTES, 0L));

        assertEquals(0L, config.getchangesRespMaxSizeBytes(),
                "Expected 0 when the property is explicitly set to zero");
    }
}
