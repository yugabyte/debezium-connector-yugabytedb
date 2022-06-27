package io.debezium.connector.yugabytedb.common;

import java.time.Duration;

import org.awaitility.Awaitility;

import io.debezium.embedded.AbstractConnectorTest;

public class YugabyteDBTestBase extends AbstractConnectorTest {
    public void awaitUntilConnectorIsReady() throws Exception {
        Awaitility.await()
                  .atMost(Duration.ofSeconds(5))
                  .until(() -> {
                    if (engine.isRunning()) {
                        return true;
                    } else {
                        return false;
                    }
                  });
    }
}
