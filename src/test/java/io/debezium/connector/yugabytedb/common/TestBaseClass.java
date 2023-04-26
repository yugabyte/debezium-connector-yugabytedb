package io.debezium.connector.yugabytedb.common;

import io.debezium.connector.yugabytedb.container.YugabyteCustomContainer;
import io.debezium.connector.yugabytedb.rules.YugabyteDBLogTestName;
import io.debezium.embedded.AbstractConnectorTest;
import org.awaitility.Awaitility;

import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.YugabyteYSQLContainer;

import java.time.Duration;

/**
 * Base class to have common methods and attributes for the containers to be run.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
@ExtendWith(YugabyteDBLogTestName.class)
public class TestBaseClass extends AbstractConnectorTest {
    public Logger LOGGER = LoggerFactory.getLogger(getClass());
    protected static YugabyteCustomContainer ybContainer;

    protected final String DEFAULT_DB_NAME = "yugabyte";
    protected final String DEFAULT_COLOCATED_DB_NAME = "colocated_database";

    protected static String yugabytedStartCommand = "";

    protected void awaitUntilConnectorIsReady() throws Exception {
        Awaitility.await()
                .pollDelay(Duration.ofSeconds(5))
                .atMost(Duration.ofSeconds(10))
                .until(() -> {
                    return engine.isRunning();
                });
    }

    protected void stopYugabyteDB() throws Exception {
        throw new UnsupportedOperationException("Method stopYugabyteDB not implemented for base test class");
    }

    protected void startYugabyteDB() throws Exception {
        throw new UnsupportedOperationException("Method startYugabyteDB not implemented for base test class");
    }

    protected void restartYugabyteDB(long millisecondsToWait) throws Exception {
        throw new UnsupportedOperationException("Method restartYugabyteDB not implemented for base test class");
    }

    protected static String getYugabytedStartCommand() {
        return yugabytedStartCommand;
    }
}
