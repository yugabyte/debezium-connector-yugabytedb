package io.debezium.connector.yugabytedb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.client.AsyncYBClient;
import org.yb.cdc.CdcService.TabletCheckpointPair;
import org.yb.client.GetDBStreamInfoResponse;
import org.yb.client.GetTabletListToPollForCDCResponse;
import org.yb.client.ListTablesResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;
import org.yb.master.MasterDdlOuterClass;

import io.debezium.DebeziumException;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.relational.TableId;

import org.yb.master.MasterReplicationOuterClass;
import org.yb.master.MasterTypes;
import org.yb.master.MasterDdlOuterClass.ListTablesResponsePB.TableInfo;

/**
 * Utility class to provide function to help functioning of the connector processes.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YBClientUtils {
  private final static Logger LOGGER = LoggerFactory.getLogger(YBClientUtils.class);

  public static boolean isTableIncludedInStreamId(GetDBStreamInfoResponse resp, String tableId) {
    for (MasterReplicationOuterClass.GetCDCDBStreamInfoResponsePB.TableInfo tableInfo : resp.getTableInfoList()) {
        if (Objects.equals(tableId, tableInfo.getTableId().toStringUtf8())) {
            return true;
        }
    }

    // This signifies that the table ID we have provided is not a part of the stream ID
    return false;
  }

  /**
   * Get the list of all the table UUIDs to be included for streaming
   * @param ybClient the {@link YBClient} instance
   * @param connectorConfig connector configuration for the connector
   * @return a Set of the tableIDs
   */
  public static Set<String> fetchTableList(YBClient ybClient,
                                           YugabyteDBConnectorConfig connectorConfig) {
    LOGGER.info("Fetching all the tables from the source");
    
    Set<String> tableIds = new HashSet<>();
      try {
          ListTablesResponse tablesResp = ybClient.getTablesList();
          for (MasterDdlOuterClass.ListTablesResponsePB.TableInfo tableInfo : 
              tablesResp.getTableInfoList()) {
              if (tableInfo.getRelationType() == MasterTypes.RelationType.INDEX_TABLE_RELATION ||
                    tableInfo.getRelationType() == MasterTypes.RelationType.SYSTEM_TABLE_RELATION) {
                  // Ignoring the index and system tables from getting added for streaming.
                  continue;
              }

              // Ignore the tables without a pgschema_name, these tables are the ones created with 
              // the older versions of YugabyteDB where the changes for CDCSDK were not present. 
              // For more details, visit https://github.com/yugabyte/yugabyte-db/issues/11976
              if (tableInfo.getPgschemaName() == null || tableInfo.getPgschemaName().isEmpty()) {
                  LOGGER.warn(String.format("Ignoring the table %s.%s since it does not have" 
                    + " a pgschema_name value (possibly because it was created using an older"
                    + " YugabyteDB version)", tableInfo.getNamespace().getName(),
                      tableInfo.getName()));
                  continue;
              }

              String fqlTableName = tableInfo.getNamespace().getName() + "." 
                                    + tableInfo.getPgschemaName() + "." 
                                    + tableInfo.getName();
              TableId tableId = YugabyteDBSchema.parseWithSchema(fqlTableName, 
                                                                 tableInfo.getPgschemaName());

              // Retrieve the list of tables in the stream ID,
              GetDBStreamInfoResponse dbStreamInfoResponse = ybClient.getDBStreamInfo(
                                                               connectorConfig.streamId());

              if (connectorConfig.getTableFilters().dataCollectionFilter().isIncluded(tableId)
                      && connectorConfig.databaseFilter().isIncluded(tableId)) {
                  // Throw an exception if the table in the include list is not a part of stream ID
                  if (!isTableIncludedInStreamId(dbStreamInfoResponse, 
                                                 tableInfo.getId().toStringUtf8())) {
                      String warningMessageFormat = "The table %s is not a part of the "
                                                            + "stream ID %s. Ignoring the table.";
                      if (connectorConfig.ignoreExceptions()) {
                          LOGGER.warn(warningMessageFormat, tableId, connectorConfig.streamId());
                          continue;
                      }
                      throw new DebeziumException(String.format(warningMessageFormat, tableId, 
                                                                connectorConfig.streamId()));
                  }

                  LOGGER.info(String.format("Adding table %s for streaming (%s)", 
                                            tableInfo.getId().toStringUtf8(), fqlTableName));
                  tableIds.add(tableInfo.getId().toStringUtf8());
              }
              else {
                  LOGGER.warn("Filtering out the table {} since it was not in the include list", 
                              tableId);
              }
          }
      }
      catch (Exception e) {
          // We are ultimately throwing this exception since this will be thrown while initializing 
          // the connector and at this point if this exception is thrown, we should not proceed 
          // forward with the connector.
          throw new DebeziumException(e);
      }
      return tableIds;
  }
  
  /**
   * Helper function to get the mapped values for the table to tablet IDs. The function returns a
   * list in which each element is a pair like Pair<tableID, tabletId>
   * @param ybClient {@link YBClient} instance
   * @param tableIds set of table UUIDs for which to find the tablet UUIDs
   * @param dbStreamId the stream ID for which we need to read the tablets
   * @return a list containing the pairs where tableID is mapped to tabletIDs
   */
  public static List<Pair<String, String>> getTabletListMappedToTableIds(YBClient ybClient, 
                                                                         Set<String> tableIds,
                                                                         String dbStreamId) {
    List<Pair<String, String>> tableToTabletIds = new ArrayList<>();
    try {
      for (String tableId : tableIds) {
          YBTable table = ybClient.openTableByUUID(tableId);
          GetTabletListToPollForCDCResponse resp = ybClient.getTabletListToPollForCdc(
              table, dbStreamId, tableId);
          for (TabletCheckpointPair pair : resp.getTabletCheckpointPairList()) {
            tableToTabletIds.add(
                new ImmutablePair<String,String>(tableId, pair.getTabletId().toStringUtf8()));
          }
      }
      Collections.sort(tableToTabletIds, (a, b) -> a.getRight().compareTo(b.getRight()));
    }
    catch (Exception e) {
        LOGGER.error("Error while fetching all the tablets", e);
        throw new DebeziumException(e);
    }

    return tableToTabletIds;
  }

  /**
   * Helper function to set the checkpoint on YugabyteDB server side
   * @param ybClient {@link YBClient} instance
   * @param streamId DB stream ID
   * @param tableId table UUID
   * @param tabletId tablet UUID
   * @param term term of the checkpoint
   * @param index index of the checkpoint
   * @param initialCheckpoint flag to indicate whether to start retaining intents
   * @param bootstrap flag to indicate whether to bootstrap the tablet
   * @throws Exception if things go wrong
   */
  public static void setCheckpoint(YBClient ybClient, String streamId, String tableId, 
                                   String tabletId, long term, long index, 
                                   boolean initialCheckpoint, boolean bootstrap) throws Exception {
    String logFormatString = "Connector setting checkpoint for tablet {} with streamId {} - " 
                             + "term: {} index: {} initialCheckpoint: {} bootstrap: {}";
    LOGGER.debug(logFormatString, tabletId, streamId, term, index, initialCheckpoint, bootstrap);
    ybClient.bootstrapTablet(ybClient.openTableByUUID(tableId), streamId, tabletId, term, 
                             index, initialCheckpoint, bootstrap);
  }

  /**
   * Helper function to get the Debezium style TableId of a table from table UUID
   * @param ybClient the {@link YBClient} instance
   * @param table the {@link YBTable} instance
   * @return the {@link TableId}
   * @throws Exception if a {@link YBTable} cannot be opened by the client
   */
  public static TableId getTableIdFromYbTable(YBClient ybClient, YBTable table) throws Exception {
    ListTablesResponse resp = ybClient.getTablesList(table.getName());
    for (TableInfo tInfo : resp.getTableInfoList()) {
      if (tInfo.getName().equals(table.getName()) && tInfo.getNamespace().getName().equals(table.getKeyspace())) {
        return new TableId(tInfo.getNamespace().getName(), tInfo.getPgschemaName(), tInfo.getName());
      }
    }

    return null;
  }

  /**
   * Get a {@link YBClient} instance to perform client operations on YugabyteDB server
   * @param connectorConfig configuration for the connector
   * @return a YBClient instance
   */
  public static YBClient getYbClient(YugabyteDBConnectorConfig connectorConfig) {
    AsyncYBClient asyncClient = new AsyncYBClient.AsyncYBClientBuilder(connectorConfig.masterAddresses())
                                  .defaultAdminOperationTimeoutMs(connectorConfig.adminOperationTimeoutMs())
                                  .defaultOperationTimeoutMs(connectorConfig.operationTimeoutMs())
                                  .defaultSocketReadTimeoutMs(connectorConfig.socketReadTimeoutMs())
                                  .numTablets(connectorConfig.maxNumTablets())
                                  .sslCertFile(connectorConfig.sslRootCert())
                                  .sslClientCertFiles(connectorConfig.sslClientCert(), connectorConfig.sslClientKey())
                                  .build();
    return new YBClient(asyncClient);
  }
  
  public static OpId getOpIdFromGetTabletListResponse(GetTabletListToPollForCDCResponse resp, String tabletId) {
    List<TabletCheckpointPair> tabletCheckpointPairs = resp.getTabletCheckpointPairList();
    for (TabletCheckpointPair p : tabletCheckpointPairs) {
      if (p.getTabletId().toStringUtf8().equals(tabletId)) {
        return new OpId((long) p.getCdcSdkCheckpoint().getTerm(),
                        (long) p.getCdcSdkCheckpoint().getIndex(),
                        p.getCdcSdkCheckpoint().getKey().toByteArray(),
                        p.getCdcSdkCheckpoint().getWriteId(),
                        p.getCdcSdkCheckpoint().getSnapshotTime());
      }
    }

    // Return null if no match is found, ideally this shouldn't happen in any case
    return null;
  }
}
