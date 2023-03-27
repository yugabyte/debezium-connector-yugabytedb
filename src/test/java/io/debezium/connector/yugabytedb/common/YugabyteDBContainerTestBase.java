package io.debezium.connector.yugabytedb.common;

import io.debezium.connector.yugabytedb.TestHelper;

/**
 * Utility class to be extended in order to run tests against a containerized instance for
 * YugabyteDB.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBContainerTestBase extends TestBaseClass {
    protected static void initializeYBContainer(String masterFlags, String tserverFlags) {
        ybContainer = TestHelper.getYbContainer(masterFlags, tserverFlags);
        ybContainer.start();

        TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433));
        TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));
    }

    protected static void initializeYBContainer() {
        initializeYBContainer(null, null);
    }

    protected static void shutdownYBContainer() {
        ybContainer.stop();
    }

    protected static String getMasterAddress() {
        return ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100);
    }
}
