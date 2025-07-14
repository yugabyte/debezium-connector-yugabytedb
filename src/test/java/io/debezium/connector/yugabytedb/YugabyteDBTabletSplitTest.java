package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.yb.client.GetTabletListToPollForCDCResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

/**
 * Unit tests to verify that the connector gracefully handles the tablet splitting on the server.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBTabletSplitTest extends YugabyteDBContainerTestBase {

  private static String masterAddresses;

  @BeforeAll
  public static void beforeClass() throws SQLException {
      initializeYBContainer();
      masterAddresses = getMasterAddress();

      TestHelper.dropAllSchemas();
  }

  @BeforeEach
  public void before() {
      initializeConnectorTestFramework();
  }

  @AfterEach
  public void after() throws Exception {
      stopConnector();
      TestHelper.executeDDL("drop_tables_and_databases.ddl");
  }

  @AfterAll
  public static void afterClass() {
      shutdownYBContainer();
  }

  @Order(1)
  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void shouldConsumeDataAfterTabletSplit(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 1 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    startEngine(configBuilder, (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    int recordsCount = 50;

    String insertFormat = "INSERT INTO t1 VALUES (%d, 'value for split table');";

    for (int i = 0; i < recordsCount; ++i) {
      TestHelper.execute(String.format(insertFormat, i));
    }

    YBClient ybClient = TestHelper.getYbClient(masterAddresses);
    YBTable table = TestHelper.getYbTable(ybClient, "t1");
    
    // Verify that there is just a single tablet.
    Set<String> tablets = ybClient.getTabletUUIDs(table);
    int tabletCountBeforeSplit = tablets.size();
    assertEquals(1, tabletCountBeforeSplit);

    // Also verify that the new API to get the tablets is returning the correct tablets.
    GetTabletListToPollForCDCResponse getTabletsResponse =
      ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());
    assertEquals(tabletCountBeforeSplit, getTabletsResponse.getTabletCheckpointPairListSize());

    // Compact the table to ready it for splitting.
    ybClient.flushTable(table.getTableId());

    // Wait for 20s for the table to be flushed.
    TestHelper.waitFor(Duration.ofSeconds(20));

    // Split the tablet. There is just one tablet so it is safe to assume that the iterator will
    // return just the desired tablet.
    ybClient.splitTablet(tablets.iterator().next());

    // Wait till there are 2 tablets for the table.
    TestHelper.waitForTablets(ybClient, table, 2);

    // Insert more records
    for (int i = recordsCount; i < 100; ++i) {
      TestHelper.execute(String.format(insertFormat, i));
    }

    // Consume the records now - there will be 100 records in total.
    SourceRecords records = consumeByTopic(100);
    
    // Verify that the records are there in the topic.
    assertEquals(100, records.recordsForTopic("test_server.public.t1").size());

    // Also call the CDC API to fetch tablets to verify the new tablets have been added in the
    // cdc_state table.
    GetTabletListToPollForCDCResponse getTabletResponse2 =
      ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());

    assertEquals(2, getTabletResponse2.getTabletCheckpointPairListSize());
  }

  @Order(2)
  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void reproduceSplitWhileTransactionIsNotFinishedWithAutomaticSplitting(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    // Stop, if ybContainer already running.
    if (ybContainer.isRunning()) {
      ybContainer.stop();
      // Wait till the container stops
      Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> {
        return !ybContainer.isRunning();
      });
    }

    // Get ybContainer with required master and tserver flags.
    setMasterFlags("enable_automatic_tablet_splitting=true",
                   "tablet_split_high_phase_shard_count_per_node=10000",
                   "tablet_split_high_phase_size_threshold_bytes=52428880",
                   "tablet_split_low_phase_size_threshold_bytes=5242888",
                   "tablet_split_low_phase_shard_count_per_node=16");
    setTserverFlags("enable_automatic_tablet_splitting=true");
    ybContainer = TestHelper.getYbContainer();
    ybContainer.start();

    LOGGER.info("Container startup command in test: {}", getYugabytedStartCommand());

    try {
      ybContainer.execInContainer(getYugabytedStartCommand().split("\\s+"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433), ybContainer.getMappedPort(9042));
    TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));
    
    TestHelper.dropAllSchemas();
    String generatedColumns = "";
    
    for (int i = 1; i <= 99; ++i) {
      generatedColumns += "v" + i
                          + " varchar(400) default "
                          + "'123456789012345678901234567890123456789012345678901234567890"
                          + "123456789012345678901234567890123456789012345678901234567890"
                          + "123456789012345678901234567890123456789012345678901234567890"
                          + "123456789012345678901234567890123456789012345678901234567890"
                          + "1234567890123456789012345678901234567890123456789012345', ";
    }

    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT, "
                       + generatedColumns
                       + "v100 varchar(400) default '1234567890123456789012345678901234567890"
                       + "1234567890123456789012345678901234567890123456789012345')"
                       + " SPLIT INTO 1 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    startEngine(configBuilder, (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    int recordsCount = 20000;

    String insertFormat = "INSERT INTO t1(id, name) VALUES (%d, 'value for split table');";

    int beginKey = 0;
    int endKey = beginKey + 100;
    for (int i = 0; i < 200; ++i) {
      TestHelper.executeBulkWithRange(insertFormat, beginKey, endKey);
      beginKey = endKey;
      endKey = beginKey + 100;
    }

    // Wait for splitting here
    TestHelper.waitFor(Duration.ofSeconds(15));
    verifyRecordCount(recordsCount);
  }

  private void verifyRecordCount(long recordsCount) {
    Set<Integer> recordKeySet = new HashSet<>();
    int failureCounter = 0;
    while (recordKeySet.size() < recordsCount) {
        int consumed = consumeAvailableRecords(record -> {
            Struct s = (Struct) record.key();
            int value = s.getStruct("id").getInt32("value");
            recordKeySet.add(value);
        });
        if (consumed > 0) {
            failureCounter = 0;
        } else {
          ++failureCounter;
          TestHelper.waitFor(Duration.ofSeconds(2));
        }

        if (failureCounter == 100) {
          LOGGER.error("Breaking becauase failure counter hit limit");
          break;
        }
    }

    LOGGER.debug("Record key set size: " + recordKeySet.size());
    List<Integer> rList = recordKeySet.stream().collect(Collectors.toList());
    Collections.sort(rList);

    assertEquals(recordsCount, recordKeySet.size());
  }
}
