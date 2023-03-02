package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.YugabyteYSQLContainer;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;
import io.debezium.util.Strings;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Test to verify that the connector is able to work smoothly for a table with multiple tablets.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBPartitionTest extends YugabyteDBTestBase {
  private final static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBPartitionTest.class);
  private static YugabyteYSQLContainer ybContainer;

  @BeforeAll
  public static void beforeClass() throws SQLException {
      ybContainer = TestHelper.getYbContainer();
      ybContainer.start();

      TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433));
      TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));

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
  public static void afterClass() throws Exception {
      ybContainer.stop();
  }

  @Test
  public void taskShouldNotFail() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 20 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    int recordsCount = 5;
    String insertFormat = "INSERT INTO t1 VALUES (%d, 'value for split table');";
    for (int i = 0; i < recordsCount; ++i) {
      TestHelper.execute(String.format(insertFormat, i));
    }

    CompletableFuture.runAsync(() -> verifyRecordCount(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
  }

  private void verifyRecordCount(long recordsCount) {
    int totalConsumedRecords = 0;
    long start = System.currentTimeMillis();
    while (totalConsumedRecords < recordsCount) {
        int consumed = super.consumeAvailableRecords(record -> {
            LOGGER.info("The record being consumed is " + record);
        });
        if (consumed > 0) {
            totalConsumedRecords += consumed;
            LOGGER.info("Consumed " + totalConsumedRecords + " records");
        }
    }
    LOGGER.info("Total duration to consume " + recordsCount + " records: " + Strings.duration(System.currentTimeMillis() - start));

    assertEquals(recordsCount, totalConsumedRecords);
  }
}
