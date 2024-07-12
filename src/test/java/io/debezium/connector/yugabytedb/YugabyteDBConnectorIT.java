package io.debezium.connector.yugabytedb;

import static org.fest.assertions.Assertions.*;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class YugabyteDBConnectorIT extends YugabyteDBContainerTestBase {

    private YugabyteDBConnector connector;

    @BeforeEach
    public void before() {
        initializeConnectorTestFramework();
    }

    @AfterEach
    public void after() throws Exception {
        stopConnector();
    }

    private void validateFieldDef(Field expected) {
        ConfigDef configDef = connector.config();
        assertThat(configDef.names()).contains(expected.name());
        ConfigDef.ConfigKey key = configDef.configKeys().get(expected.name());
        assertThat(key).isNotNull();
        assertThat(key.name).isEqualTo(expected.name());
        assertThat(key.displayName).isEqualTo(expected.displayName());
        assertThat(key.importance).isEqualTo(expected.importance());
        assertThat(key.documentation).isEqualTo(expected.description());
        assertThat(key.type).isEqualTo(expected.type());
    }

    @Test
    public void shouldValidateConnectorConfigDef() {
        connector = new YugabyteDBConnector();
        ConfigDef configDef = connector.config();
        assertThat(configDef).isNotNull();
        YugabyteDBConnectorConfig.ALL_FIELDS.forEach(this::validateFieldDef);
    }

    @Test
    public void shouldNotStartWithInvalidConfiguration() {
        // use an empty configuration which should be invalid because of the lack of DB connection details
        Configuration.Builder configBuilder = Configuration.create();

        // we expect the engine will log at least one error, so preface it ...
        LOGGER.info("Attempting to start the connector with an INVALID configuration, so MULTIPLE error messages & one exception will appear in the log");
        startEngine(configBuilder, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }
}
