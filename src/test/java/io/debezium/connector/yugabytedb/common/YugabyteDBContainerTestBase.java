package io.debezium.connector.yugabytedb.common;

import java.time.Duration;

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
    protected void stopYugabyteDB() throws Exception {
        ExecResult stopResult = ybContainer.execInContainer("/home/yugabyte/bin/yugabyted", "stop");
        LOGGER.info("YugabyteDB stopped with output: {} error: {} toString: {}", stopResult.getStdout(), stopResult.getStderr(), stopResult.toString());

        // ExecResult stopResult2 = ybContainer.execInContainer("/home/yugabyte/bin/yb-admin", "--master_addresses", "0.0.0.0:7100", "create_change_data_stream", "ysql.yugabyte");
        // LOGGER.info("Stop result 2: {}", stopResult2.getStdout());
    }

    @Override
    protected void startYugabyteDB() throws Exception {
        // This assumes that the yugabyted process will start back up again with the same value of flags which
        // were there before stopping it.
        ExecResult startResult = ybContainer.execInContainer("/home/yugabyte/bin/yugabyted", "start");
        LOGGER.info("YugabyteDB started with output: {}", startResult.getStdout());
    }

    /**
     * Restart the yugabyted process running in the TestContainer
     * 
     * @param milliseconds amount of time (in ms) to wait before starting after stopping
     */
    @Override
    protected void restartYugabyteDB(long milliseconds) throws Exception {
        stopYugabyteDB();

        TestHelper.waitFor(Duration.ofMillis(milliseconds));
        startYugabyteDB();
    }
}
