package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import io.debezium.util.Strings;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Test to verify that the connector is able to work smoothly for a table with multiple tablets.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBPartitionTest extends YugabyteDBContainerTestBase {

  @BeforeAll
  public static void beforeClass() throws SQLException {
      initializeYBContainer();
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
      shutdownYBContainer();
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void taskShouldNotFail(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 20 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    startEngine(configBuilder, (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    int recordsCount = 5;
    String insertFormat = "INSERT INTO t1 VALUES (%d, 'value for split table');";
    for (int i = 0; i < recordsCount; ++i) {
      TestHelper.execute(String.format(insertFormat, i));
    }

    verifyRecordCount(recordsCount);
  }

  private void verifyRecordCount(long recordsCount) {
    int totalConsumedRecords = 0;
    long start = System.currentTimeMillis();
    while (totalConsumedRecords < recordsCount) {
        int consumed = consumeAvailableRecords(record -> {
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
