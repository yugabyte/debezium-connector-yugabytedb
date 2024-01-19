package io.debezium.connector.yugabytedb.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.Arrays;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import com.google.common.base.Throwables;
import org.awaitility.Awaitility;
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
    private static final String CONTAINER_IP_FORMAT_STRING = "docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' %s";

    private static String containerIpAddress;

    protected static void initializeYBContainer(String masterFlags, String tserverFlags) {
        ybContainer = TestHelper.getYbContainer(masterFlags, tserverFlags);
        ybContainer.start();

        containerIpAddress = getContainerIp(ybContainer.getContainerId());
        logger.info("YugabyteDB container IP: {}", containerIpAddress);

        // Set the GFLAG: "cdc_state_checkpoint_update_interval_ms" to 0 in all tests, forcing every
        // instance of explicit_checkpoint to be added to the 'cdc_state' table in the service.
        String finalTserverFlags = " --tserver_flags=" +
                                   ((tserverFlags == null || tserverFlags.isEmpty())
                                        ? "cdc_state_checkpoint_update_interval_ms=0"
                                        : tserverFlags + ",cdc_state_checkpoint_update_interval_ms=0");

        if (masterFlags == null || masterFlags.isEmpty()) {
            masterFlags = "--master_flags=allowed_preview_flags_csv=yb_enable_cdc_consistent_snapshot_streams,yb_enable_cdc_consistent_snapshot_streams=true";
        } else {
            masterFlags = "--master_flags=allowed_preview_flags_csv=yb_enable_cdc_consistent_snapshot_streams,yb_enable_cdc_consistent_snapshot_streams=true," + masterFlags;
        }

        logger.info("tserver flags: {}", finalTserverFlags);
        logger.info("master flags: {}", masterFlags);

        yugabytedStartCommand = "/home/yugabyte/bin/yugabyted start --advertise_address=" + containerIpAddress + " "
                                    + masterFlags + finalTserverFlags + " --daemon=true";
        logger.info("Container startup command: {}", yugabytedStartCommand);

        try {
            for (String ele : getYugabytedStartCommand().split("\\s+")) {
                logger.info(ele);
            }
            ExecResult result = ybContainer.execInContainer(getYugabytedStartCommand().split("\\s+"));

            logger.info("Started yugabyted inside container: {}", result.getStdout());
            logger.info("Error output (if any): {}", result.getStderr());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        TestHelper.setContainerHostPort(containerIpAddress, ybContainer.getMappedPort(5433), ybContainer.getMappedPort(9042));
        TestHelper.setMasterAddress(containerIpAddress + ":" + ybContainer.getMappedPort(7100));
    }

    protected static void initializeYBContainer() {
        initializeYBContainer(null, null);
    }

    /**
     * @param containerId the container ID
     * @return a string representation of the IP address of the passed container ID
     */
    protected static String getContainerIp(String containerId) {
        try {
            Process process =
              Runtime.getRuntime().exec(String.format(CONTAINER_IP_FORMAT_STRING, containerId));
            int exitCode = process.waitFor();

            if (exitCode != 0) {
                throw new Exception("Command exited with exit code " + exitCode);
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

            // Assuming we have just one line of the container IP.
            String line = reader.readLine();
            return line.substring(1, line.length() - 1);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    protected static void shutdownYBContainer() {
        String containerId = ybContainer.getContainerId();
        logger.info("Shutting down container with ID {}", containerId);

        ybContainer.stop();

        // Wait until container is fully stopped.
        Awaitility.await()
            .atMost(Duration.ofSeconds(20))
            .pollInterval(Duration.ofSeconds(2))
            .ignoreNoExceptions()
            .until(() -> {
              try {
                InspectContainerResponse containerInfo = ybContainer.getDockerClient().inspectContainerCmd(containerId).exec();
                return containerInfo.getState() == null && !Boolean.TRUE.equals(containerInfo.getState().getRunning());
              } catch (NotFoundException e) {
                logger.warn("Was going to stop container but it apparently no longer exists: {}", containerId);
                return true;
              } catch (Exception e) {
                logger.warn("Error encountered when checking container for shutdown (ID: {}) - it may not have been stopped, or may already be stopped. Root cause: {}",
                    containerId, Throwables.getRootCause(e).getMessage());
                return true;
              }
            });
    }

    protected static String getMasterAddress() {
        return ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100);
    }

    @Override
    protected long getIntentsCount() throws Exception {
        ExecResult result = ybContainer.execInContainer("/home/yugabyte/bin/yb-ts-cli", "--server_address", containerIpAddress, "count_intents");

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
