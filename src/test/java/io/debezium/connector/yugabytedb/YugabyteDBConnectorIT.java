package io.debezium.connector.yugabytedb;

import static org.fest.assertions.Assertions.*;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;

public class YugabyteDBConnectorIT extends YugabyteDBTestBase {
    private final static Logger LOGGER = Logger.getLogger(YugabyteDBConnectorIT.class);

    private YugabyteDBConnector connector;

    @Before
    public void before() {
        initializeConnectorTestFramework();
    }

    @After
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
    public void shouldNotStartWithInvalidConfiguration() throws Exception {
        // use an empty configuration which should be invalid because of the lack of DB connection details
        Configuration config = Configuration.create().build();

        // we expect the engine will log at least one error, so preface it ...
        LOGGER.info("Attempting to start the connector with an INVALID configuration, so MULTIPLE error messages & one exception will appear in the log");
        start(YugabyteDBConnector.class, config, (success, msg, error) -> {
            assertThat(success).isFalse();
            assertThat(error).isNotNull();
        });
        assertConnectorNotRunning();
    }
}
