package io.debezium.connector.yugabytedb;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.client.GetDBStreamInfoResponse;
import org.yb.client.ListTablesResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;
import org.yb.master.MasterReplicationOuterClass.GetCDCDBStreamInfoResponsePB.TableInfo;

import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.relational.TableId;

/**
 * Poller thread which extends {@link Thread} to keep polling for new tables on YugabyteDB
 * server side to the provided stream ID.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBTablePoller extends Thread {
  private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBTablePoller.class);
  private final short MAX_RETRY_COUNT = 5;

  private final YugabyteDBConnectorConfig connectorConfig;
  private final ConnectorContext connectorContext;
  private final YBClient ybClient;
  private final CountDownLatch shutdownLatch;
  private final long pollMs;
  private final boolean usePublication;
  
  private Set<TableInfo> cachedTableInfoSet = null;
  private Set<String> cachedTableNameSet = null;

  public YugabyteDBTablePoller(YugabyteDBConnectorConfig connectorConfig,
                               ConnectorContext connectorContext, boolean usePublication) {
    super();
    this.connectorConfig = connectorConfig;
    this.connectorContext = connectorContext;
    this.ybClient = YBClientUtils.getYbClient(connectorConfig);
    this.shutdownLatch = new CountDownLatch(1);
    this.pollMs = connectorConfig.newTablePollIntervalMs();
    this.usePublication = usePublication;
  }

  @Override
  public void run() {
    LOGGER.info("Starting thread to monitor the tables");
    try {
      while (shutdownLatch.getCount() > 0) {
        if (areThereNewTables()) {
          this.connectorContext.requestTaskReconfiguration();
        }
        try {
          LOGGER.debug("Waiting for {} ms to poll again for new tables", pollMs);
          boolean shuttingDown = shutdownLatch.await(pollMs, TimeUnit.MILLISECONDS);
          if (shuttingDown) {
            return;
          }
        } catch (InterruptedException ie) {
          LOGGER.error("Unexpected interrupted exception, ignoring", ie);
          Thread.currentThread().interrupt();
        }
      }
    } finally {
      if (this.ybClient != null) {
        LOGGER.info("Closing the ybclient in the Poller thread.");
        try {
          this.ybClient.close();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

  }

  /**
   * Verify if there are any new tables added to the change data stream. This function continuously
   * fetches the DB stream info for a given stream and as soon as there is a change in the stream
   * info, it returns a signal to the connector.
   * @return true if there is a new table in the stream info
   */
  private boolean areThereNewTablesInStream() {
    short retryCount = 0;
    while (retryCount <= MAX_RETRY_COUNT) {
      try {
        boolean shouldRestart = false;
        GetDBStreamInfoResponse resp = this.ybClient.getDBStreamInfo(this.connectorConfig.streamId());

        if (cachedTableInfoSet == null) {
          LOGGER.debug("Cached table list in the poller thread is null, initializing it now");
          cachedTableInfoSet = resp.getTableInfoList().stream().collect(Collectors.toSet());
        } else {
          if (cachedTableInfoSet.size() != resp.getTableInfoList().size()) {
            Set<TableInfo> tableInfoSetFromResponse =
              resp.getTableInfoList().stream().collect(Collectors.toSet());

            Set<TableInfo> cachedSet = new HashSet<>(cachedTableInfoSet);
            Set<TableInfo> responseSet = new HashSet<>(tableInfoSetFromResponse);

            Set<TableInfo> intersection = new HashSet<>(cachedSet);
            intersection.retainAll(responseSet);

            Set<TableInfo> difference = new HashSet<>(responseSet);
            difference.removeAll(cachedSet);
            
            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Common tables between the cached table info set and the set received "
                          + "from GetDBStreamInfoResponse:");
              intersection.forEach(tableInfo -> {
                LOGGER.debug(tableInfo.getTableId().toStringUtf8());
              });

              LOGGER.debug("New tables as received in the GetDBStreamInfoResponse: ");
              difference.forEach(tableInfo -> {
                LOGGER.debug(tableInfo.getTableId().toStringUtf8());
              });
            }

            for (TableInfo tableInfo : difference) {
              if (isTableIncludedForStreaming(tableInfo.getTableId().toStringUtf8())) {
                String message = "Found {} new table(s), signalling context reconfiguration";
                LOGGER.info(message, difference.size());
                shouldRestart = true;
              }
            }
          
            // Update the cached table list.
            cachedTableInfoSet = tableInfoSetFromResponse;
          }
        }

        return shouldRestart;
      } catch (Exception e) {
        ++retryCount;

        if (retryCount > MAX_RETRY_COUNT) {
          LOGGER.error("Retries exceeded the maximum retry count in table poller thread,"
                    + " all {} retries failed", MAX_RETRY_COUNT);
          throw fail(e);
        }

        LOGGER.warn("Exception while trying to get DB stream Info in poller thread,"
                  + "will retry again", e);
        return false;
      }
    }

    return false;
  }

  private boolean areThereNewTablesInPublication() {
    short retryCount = 0;

    while (retryCount <= MAX_RETRY_COUNT) {
      try {
        boolean shouldRestart = false;
        Set<String> tablesInPublication = getTablesInPublication();


        if (cachedTableNameSet == null) {
          LOGGER.debug("Cached table list in the poller thread is null, initializing it now");
          cachedTableNameSet = tablesInPublication;
        } else {
          if (cachedTableNameSet.size() != tablesInPublication.size()) {
            Set<String> cachedSet = new HashSet<>(cachedTableNameSet);
            Set<String> responseSet = new HashSet<>(tablesInPublication);

            Set<String> intersection = new HashSet<>(cachedSet);
            intersection.retainAll(responseSet);

            Set<String> difference = new HashSet<>(responseSet);
            difference.removeAll(cachedSet);

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug("Common tables between the cached table names set and the set received "
                          + "from pg_publication_tables: ");
              intersection.forEach(table -> LOGGER.debug(table));

              LOGGER.debug("New tables as received from pg_publication_tables : ");
              difference.forEach(table -> LOGGER.debug(table));
            }

            
            String message = "Found {} new table(s), signalling context reconfiguration";
            LOGGER.info(message, difference.size());
            shouldRestart = true;
              
            // Update the cached table list.
            cachedTableNameSet = tablesInPublication;
          }
        }

        return shouldRestart;
      } catch (Exception e) {
        ++retryCount;

        if (retryCount > MAX_RETRY_COUNT) {
          LOGGER.error("Retries exceeded the maximum retry count in table poller thread,"
                    + " all {} retries failed", MAX_RETRY_COUNT);
          throw fail(e);
        }

        LOGGER.warn("Exception while trying to get DB stream Info in poller thread,"
                  + "will retry again", e);
        return false;
      }
    }

    return false;
  }

  private boolean areThereNewTables() {
    if (usePublication) {
      return areThereNewTablesInPublication();
    } else {
      return areThereNewTablesInStream();    
    }
  }

  /**
   * Check whether the table with the given table UUID is included for streaming.
   * @param tableUUID the UUID of the table
   * @return true if it is included in the `table.include.list`, false otherwise
   * @throws Exception
   */
  public boolean isTableIncludedForStreaming(String tableUUID) throws Exception {
    YBTable ybTable = this.ybClient.openTableByUUID(tableUUID);

    ListTablesResponse resp = this.ybClient.getTablesList(ybTable.getName(),
                                                          true, null);

    for (org.yb.master.MasterDdlOuterClass.ListTablesResponsePB.TableInfo tableInfo :
            resp.getTableInfoList()) {
      String fqlTableName = tableInfo.getNamespace().getName() + "."
                            + tableInfo.getPgschemaName() + "."
                            + tableInfo.getName();
      TableId tableId = new TableId(
        null,  // catalog is null since Debezium's tableFilter uses schema.table
        tableInfo.getPgschemaName(),
        tableInfo.getName());

      if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)
            && connectorConfig.databaseFilter().isIncluded(tableId)) {
        return true;
      }
    }

    return false;
  }

  private Set<String> getTablesInPublication() throws Exception {
    try (YugabyteDBConnection ybConnection = new YugabyteDBConnection(this.connectorConfig.getJdbcConfig(), YugabyteDBConnection.CONNECTION_GENERAL);
        Connection connection = ybConnection.connection()) {
          Set<String> tablesInPublication = new HashSet<String>();
          Statement statement = connection.createStatement();
          String getTablesFromPublicationQuery = "SELECT * FROM pg_publication_tables WHERE pubname = '" + this.connectorConfig.publicationName() + "' ;";
          ResultSet rs = statement.executeQuery(getTablesFromPublicationQuery);
          while(rs.next()) {
            String tableName = rs.getString("tablename");
            String schemaName = rs.getString("schemaname");
            tablesInPublication.add(schemaName + "." + tableName);
          }
          return tablesInPublication;
    } 
  }

  /**
   * Shutdown the table poller thread
   */
  public void shutdown() {
    LOGGER.info("Shutting down the poller thread to monitor tables");

    // Close YBClient instance
    if (this.ybClient != null) {
      try {
        this.ybClient.close();
      } catch (Exception e) {
        LOGGER.warn("Exception while closing YBClient instance", e);
      }
    }

    shutdownLatch.countDown();
  }

  /**
   * Return a failure with {@link RuntimeException}
   * @param t the throwable object
   * @return a RuntimeException
   */
  private RuntimeException fail(Throwable t) {
    String errorMessage = "Error while trying to get the DB stream Info in poller thread";
    LOGGER.error(errorMessage, t);

    RuntimeException runtimeException = new ConnectException(errorMessage, t);
    connectorContext.raiseError(runtimeException);

    // Shutdown the monitoring thread.
    shutdownLatch.countDown();

    return runtimeException;
  }
}
