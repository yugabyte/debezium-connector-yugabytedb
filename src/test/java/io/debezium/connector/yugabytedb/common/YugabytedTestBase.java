package io.debezium.connector.yugabytedb.common;

import io.debezium.embedded.AbstractConnectorTest;
import org.awaitility.Awaitility;

import java.time.Duration;

/**
 * Class which can be extended to make sure that the tests are run against a local instance
 * of yugabyted. Just make sure that your yugabyted service is running on 127.0.0.1 <br><br>
 *
 * <strong>Usage:</strong>
 * Simply extend the {@code YugabytedTestBase} instead of {@code YugabyteDBContainerTestBase}
 * <code>
 *     public class XYZ extends YugabytedTestBase {
 *         // Contents of test class
 *     }
 * </code>
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabytedTestBase extends TestBaseClass {
    // The methods for initialization and shutdown do nothing as running tests against yugabyted
    // doesn't require any container to be setup or killed.
    public static void initializeYBContainer(String masterFlags, String tserverFlags) {
        // Do nothing.
    }

    public static void initializeYBContainer() {
        // Do nothing.
    }

    public static void shutdownYBContainer() {
        // Do nothing.
    }

    public String getMasterAddress() {
        return "127.0.0.1:7100";
    }
}
