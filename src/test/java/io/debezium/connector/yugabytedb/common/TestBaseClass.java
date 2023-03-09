package io.debezium.connector.yugabytedb.common;

import io.debezium.embedded.AbstractConnectorTest;
import org.awaitility.Awaitility;
import org.testcontainers.containers.YugabyteYSQLContainer;

import java.time.Duration;

/**
 * Base class to have common methods and attributes for the containers to be run.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class TestBaseClass extends AbstractConnectorTest {
    protected static YugabyteYSQLContainer ybContainer;

    protected void awaitUntilConnectorIsReady() throws Exception {
        Awaitility.await()
                .pollDelay(Duration.ofSeconds(5))
                .atMost(Duration.ofSeconds(10))
                .until(() -> {
                    return engine.isRunning();
                });
    }
}
