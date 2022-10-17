package io.debezium.connector.yugabytedb;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.client.GetDBStreamInfoResponse;
import org.yb.client.YBClient;
import org.yb.master.MasterReplicationOuterClass.GetCDCDBStreamInfoResponsePB.TableInfo;

import io.debezium.DebeziumException;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * Poller class which implements the {@link Runnable} to keep polling for new tables
 * on the YugabyteDB server side.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBTablePoller implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBTablePoller.class);

  private final YugabyteDBConnectorConfig connectorConfig;
  private final ErrorHandler errorHandler;
  private final YBClient ybClient;
  private final Metronome metronome;

  private boolean running = true;
  
  private List<TableInfo> tableInfoList = null;

  public YugabyteDBTablePoller(YugabyteDBConnectorConfig connectorConfig,
                               ErrorHandler errorHandler,
                               YBClient ybClient) {
    super();
    this.connectorConfig = connectorConfig;
    this.errorHandler = errorHandler;
    this.ybClient = ybClient;
    // TODO: See if this interval can be made configurable
    this.metronome = Metronome.parker(Duration.ofSeconds(5), Clock.SYSTEM);
  }

  private boolean isRunning() {
    return running;
  }

  public void kill() {
    this.running = false;
  }

  @Override
  public void run() {
    // Continuously poll on the stream to get the associated table list.
    while (isRunning()) {
      try {
        GetDBStreamInfoResponse dbStreamInfoResponse = ybClient.getDBStreamInfo(
                                                         connectorConfig.streamId());

        if (tableInfoList == null) {
          tableInfoList = dbStreamInfoResponse.getTableInfoList();
        } else {
          // Check if the table info size has changed.
          if (tableInfoList.size() != dbStreamInfoResponse.getTableInfoList().size()) {
            // The change of the list size indicates that a new table has been added to the list,
            // restart the connector at this stage so that it can start polling on the newly added
            // table as well.
            int newTableCount = dbStreamInfoResponse.getTableInfoList().size()
                                  - tableInfoList.size();
            String exceptionMessage = "Found " + newTableCount
                                        + " new tables, restarting the connector";
            errorHandler.setProducerThrowable(new NewTableFoundException(exceptionMessage));

            // Kill the current thread once producer throwable has been set
            LOGGER.info("Killing the table poller thread");
            kill();
          }
        }
      } catch (Exception e) {
        LOGGER.warn("Cannot retrive the DB stream info from the server, will try again");
      }

      // Wait for the interval before polling again.
      try {
        metronome.pause();
      } catch (InterruptedException ie) {
        LOGGER.error("Metronome pause interrupted", ie);
        throw new DebeziumException(ie);
      }
    }
  }
}
