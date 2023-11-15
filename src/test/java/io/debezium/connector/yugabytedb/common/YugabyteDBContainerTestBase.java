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

        // Set the GFLAG: "cdc_state_checkpoint_update_interval_ms" to 0 in all tests, forcing every
        // instance of explicit_checkpoint to be added to the 'cdc_state' table in the service.
        String finalTserverFlags = " --tserver_flags=" +
                                   ((tserverFlags == null || tserverFlags.isEmpty())
                                        ? "cdc_state_checkpoint_update_interval_ms=0"
                                        : tserverFlags + ",cdc_state_checkpoint_update_interval_ms=0");

        if (masterFlags == null || masterFlags.isEmpty()) {
            masterFlags = "--master_flags=rpc_bind_addresses=0.0.0.0";
        } else {
            masterFlags = "--master_flags=rpc_bind_addresses=0.0.0.0," + masterFlags;
        }

        logger.info("tserver flags: {}", finalTserverFlags);
        logger.info("master flags: {}", masterFlags);

        yugabytedStartCommand = "/home/yugabyte/bin/yugabyted start --listen=0.0.0.0 "
                                    + masterFlags + finalTserverFlags + " --daemon=true";
        logger.info("Container startup command: {}", yugabytedStartCommand);

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

    protected static void initializeYBContainerNoStartYB(String masterFlags, String tserverFlags) {
        ybContainer = TestHelper.getYbContainer(masterFlags, tserverFlags);
        ybContainer.start();

        // Set the GFLAG: "cdc_state_checkpoint_update_interval_ms" to 0 in all tests, forcing every
        // instance of explicit_checkpoint to be added to the 'cdc_state' table in the service.
//        String finalTserverFlags = " --tserver_flags=" +
//                                     ((tserverFlags == null || tserverFlags.isEmpty())
//                                        ? "cdc_state_checkpoint_update_interval_ms=0"
//                                        : tserverFlags + ",cdc_state_checkpoint_update_interval_ms=0");
//
//        if (masterFlags == null || masterFlags.isEmpty()) {
//            masterFlags = "--master_flags=rpc_bind_addresses=0.0.0.0";
//        } else {
//            masterFlags = "--master_flags=rpc_bind_addresses=0.0.0.0," + masterFlags;
//        }
//
//        logger.info("tserver flags: {}", finalTserverFlags);
//        logger.info("master flags: {}", masterFlags);
//
//        yugabytedStartCommand = "/home/yugabyte/bin/yugabyted start --listen=0.0.0.0 "
//                                  + masterFlags + finalTserverFlags + " --daemon=true";
//        logger.info("Container startup command: {}", yugabytedStartCommand);

        TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433), ybContainer.getMappedPort(9042));
        TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));
    }

    protected static void setMasterTserverFlags(String masterFlags, String tserverFlags) {
        String finalTserverFlags = " --tserver_flags=" +
                                     ((tserverFlags == null || tserverFlags.isEmpty())
                                        ? "cdc_state_checkpoint_update_interval_ms=0"
                                        : tserverFlags + ",cdc_state_checkpoint_update_interval_ms=0");

        if (masterFlags == null || masterFlags.isEmpty()) {
            masterFlags = "--master_flags=rpc_bind_addresses=0.0.0.0";
        } else {
            masterFlags = "--master_flags=rpc_bind_addresses=0.0.0.0," + masterFlags;
        }

        logger.info("tserver flags: {}", finalTserverFlags);
        logger.info("master flags: {}", masterFlags);

        yugabytedStartCommand = "/home/yugabyte/bin/yugabyted start --listen=0.0.0.0 "
                                  + masterFlags + finalTserverFlags + " --daemon=true";
    }

    protected static void initializeYBContainer() {
        initializeYBContainer(null, null);
    }

    protected static void shutdownYBContainer() {
        if (ybContainer != null && ybContainer.isRunning()) {
            ybContainer.stop();
        }
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
