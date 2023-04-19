package io.debezium.connector.yugabytedb.common;

import org.testcontainers.containers.Container.ExecResult;

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

    @Override
    protected long getIntentsCount() throws Exception {
        ExecResult result = ybContainer.execInContainer("/home/yugabyte/bin/yb-ts-cli", "--server_address", "0.0.0.0", "count_intents");

        // Assuming the result is just a number, so simply convert the string to long and return.
        return Long.valueOf(result.getStdout().split("\n")[0]);
    }
}
