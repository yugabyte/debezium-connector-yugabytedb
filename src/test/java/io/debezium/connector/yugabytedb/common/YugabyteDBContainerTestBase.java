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
    protected static void initializeYBContainer(String masterFlags, String tserverFlags) {
        ybContainer = TestHelper.getYbContainer(masterFlags, tserverFlags);
        ybContainer.start();

        if (tserverFlags == null || tserverFlags.isEmpty()) {
            tserverFlags = "";
        } else {
            tserverFlags = " --tserver_flags=" + tserverFlags;
        }

        if (masterFlags == null || masterFlags.isEmpty()) {
            masterFlags = "--master_flags=rpc_bind_addresses=0.0.0.0";
        } else {
            masterFlags = "--master_flags=rpc_bind_addresses=0.0.0.0," + masterFlags;
        }
        
        logger.info("tserver flags: {}", tserverFlags);
        logger.info("master flags: {}", masterFlags);

        yugabytedStartCommand = "/home/yugabyte/bin/yugabyted start --listen=0.0.0.0 "
                                    + masterFlags + tserverFlags + " --daemon=true";
        logger.info("Container startup command: {}", yugabytedStartCommand);

        try {
            ExecResult result = ybContainer.execInContainer(getYugabytedStartCommand().split("\\s+"));

            logger.info("Started yugabyted inside container: {}", result.getStdout());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

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
     * @param milliseconds amount of time (in ms) to wait before starting after stopping
     */
    @Override
    protected void restartYugabyteDB(long millisecondsToWait) throws Exception {
        stopYugabyteDB();

        TestHelper.waitFor(Duration.ofMillis(millisecondsToWait));
        startYugabyteDB();
    }
}
