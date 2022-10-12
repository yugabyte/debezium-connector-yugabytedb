package io.debezium.connector.yugabytedb.common;

import java.time.Duration;

import org.awaitility.Awaitility;

import io.debezium.embedded.AbstractConnectorTest;

public class YugabyteDBTestBase extends AbstractConnectorTest {
    public void awaitUntilConnectorIsReady() throws Exception {
        Awaitility.await()
                  .pollDelay(Duration.ofSeconds(10))
                  .atMost(Duration.ofSeconds(15))
                  .until(() -> {
                    return engine.isRunning();
                  });
    }
}
