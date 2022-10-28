package io.debezium.connector.yugabytedb;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Set;

import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.YugabyteYSQLContainer;
import org.yb.client.GetTabletListToPollForCDCResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;

/**
 * Unit tests to verify that the connector gracefully handles the tablet splitting on the server.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBTabletSplitTest extends YugabyteDBTestBase {
  private final static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBPartitionTest.class);
  private static YugabyteYSQLContainer ybContainer;
  private static String masterAddresses;

  @BeforeClass
  public static void beforeClass() throws SQLException {
      ybContainer = TestHelper.getYbContainer();
      ybContainer.start();

      TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433));
      TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));
      masterAddresses = ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100);

      TestHelper.dropAllSchemas();
  }

  @Before
  public void before() {
      initializeConnectorTestFramework();
  }

  @After
  public void after() throws Exception {
      stopConnector();
      TestHelper.executeDDL("drop_tables_and_databases.ddl");
  }

  @AfterClass
  public static void afterClass() throws Exception {
      ybContainer.stop();
  }

  @Test
  public void shouldConsumeDataAfterTabletSplit() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 1 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
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
    Awaitility.await()
      .pollDelay(Duration.ofSeconds(2))
      .atMost(Duration.ofSeconds(20))
      .until(() -> {
        return ybClient.getTabletUUIDs(table).size() == 2;
      });

    // Insert more records
    for (int i = recordsCount; i < 100; ++i) {
      TestHelper.execute(String.format(insertFormat, i));
    }

    // Consume the records now - there will be 100 records in total.
    SourceRecords records = consumeRecordsByTopic(100);
    
    // Verify that the records are there in the topic.
    assertEquals(100, records.recordsForTopic("test_server.public.t1").size());

    // Also call the CDC API to fetch tablets to verify the new tablets have been added in the
    // cdc_state table.
    GetTabletListToPollForCDCResponse getTabletResponse2 =
      ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());

    assertEquals(2, getTabletResponse2.getTabletCheckpointPairListSize());
  }
}
