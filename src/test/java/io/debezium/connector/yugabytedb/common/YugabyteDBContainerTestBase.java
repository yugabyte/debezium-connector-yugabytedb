package io.debezium.connector.yugabytedb.common;

import java.time.Duration;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.testcontainers.containers.Container.ExecResult;

import io.debezium.connector.yugabytedb.TestHelper;

/**
 * Utility class to be extended in order to run tests against a containerized instance for
 * YugabyteDB.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBContainerTestBase extends TestBaseClass {
    private static final Logger logger = LoggerFactory.getLogger(YugabyteDBContainerTestBase.class);
    protected static void initializeYBContainer() {
        ybContainer = TestHelper.getYbContainer();
        ybContainer.start();

        logger.info("Container startup command: {}", getYugabytedStartCommand());

        try {
            ExecResult result = ybContainer.execInContainer(getYugabytedStartCommand().split("\\s+"));

            logger.info("Started yugabyted inside container: {}", result.getStdout());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433), ybContainer.getMappedPort(9042));
        TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));
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
        return Long.parseLong(result.getStdout().split("\n")[0]);
    }

    protected void stopYugabyteDB() throws Exception {
        ExecResult stopResult = ybContainer.execInContainer("/sbin/tini", "-s", "--", "/home/yugabyte/bin/yugabyted", "stop");
        LOGGER.debug("YugabyteDB stopped with output: {} error: {} toString: {}", stopResult.getStdout(), stopResult.getStderr(), stopResult.toString());
    }

    @Override
    protected void startYugabyteDB() throws Exception {
        // This assumes that the yugabyted process will start back up again with the same value of flags which
        // were there before stopping it.
        ExecResult startResult = ybContainer.execInContainer("/sbin/tini", "-s", "--", "/home/yugabyte/bin/yugabyted", "start");
        LOGGER.debug("YugabyteDB started with output: {}", startResult.getStdout());

        // Wait for sometime for the process to be initialized properly.
        TestHelper.waitFor(Duration.ofSeconds(10));
    }

    /**
     * Restart the yugabyted process running in the TestContainer
     *
     * @param millisecondsToWait amount of time (in ms) to wait before starting after stopping
     */
    @Override
    protected void restartYugabyteDB(long millisecondsToWait) throws Exception {
        stopYugabyteDB();

        TestHelper.waitFor(Duration.ofMillis(millisecondsToWait));
        startYugabyteDB();
    }
}
