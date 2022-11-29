package io.debezium.connector.yugabytedb;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.YugabyteYSQLContainer;
import org.yb.client.GetTabletListToPollForCDCResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;

/**
 * Unit tests to verify that the connector works with schema changes gracefully.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBSchemaEvolutionTest extends YugabyteDBTestBase {
  private final static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBSchemaEvolutionTest.class);
  
  // Keeping the id part as a string only so that it is easier to use generate_series as well.
  private final String insertFormatString = "INSERT INTO t1 VALUES (%s, 'name_value');";
  
  private static YugabyteYSQLContainer ybContainer;
  private static String masterAddresses;

  @BeforeClass
  public static void beforeClass() throws SQLException {
      String tserverFlags = "cdc_max_stream_intent_records=200";
      ybContainer = TestHelper.getYbContainer(null, tserverFlags);
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
  public void shouldHandleSchemaChangesGracefully() throws Exception {
    /**
     * 1. Create 2 tablets with range sharding
     * 2. Start the CDC pipeline and insert data - make sure one of the tablet gets way more data
     *    than the other one (use generate_series maybe)
     * 3. Execute an ALTER command
     * 4. Now when the connector will try to poll the records for the tablet with less data, it will
     *    also try to get the schema and since the schema has changed by this time in the records,
     *    the connector should throw an error.
     */
    TestHelper.dropAllSchemas();
    TestHelper.execute("CREATE TABLE t1 (id INT, name TEXT, PRIMARY KEY(id ASC)) SPLIT AT VALUES ((30000));");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
    configBuilder.with(YugabyteDBConnectorConfig.CDC_POLL_INTERVAL_MS, 10_000);
    configBuilder.with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, 10000);

    start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    TestHelper.execute(String.format(insertFormatString, "1"));
    TestHelper.execute(String.format(insertFormatString, "generate_series(40001,45000)"));

    // Now by the time connector is consuming all these records, execute an ALTER COMMAND and
    // insert records in the tablet with lesser data.
    TestHelper.execute("ALTER TABLE t1 ADD COLUMN new_column VARCHAR(128) DEFAULT 'new_val';");
    // TestHelper.execute("ALTER TABLE t1 DROP COLUMN new_column;");
    TestHelper.execute(String.format(insertFormatString, "2"));

    // Consume the records now.
    CompletableFuture.runAsync(() -> verifyRecordCount(5000 + 2))
      .exceptionally(throwable -> {
        throw new RuntimeException(throwable);
      }).get();
  }

  private void verifyRecordCount(long recordsCount) {
    waitAndFailIfCannotConsume(new ArrayList<>(), recordsCount, 10 * 60 * 1000);
  }

  private void waitAndFailIfCannotConsume(List<SourceRecord> records, long recordsCount,
                                            long milliSecondsToWait) {
      AtomicLong totalConsumedRecords = new AtomicLong();
      long seconds = milliSecondsToWait / 1000;
      try {
          Awaitility.await()
              .atMost(Duration.ofSeconds(seconds))
              .until(() -> {
                  int consumed = super.consumeAvailableRecords(record -> {
                      LOGGER.debug("The record being consumed is " + record);
                      records.add(record);
                  });
                  if (consumed > 0) {
                      totalConsumedRecords.addAndGet(consumed);
                      LOGGER.debug("Consumed " + totalConsumedRecords + " records");
                  }

                  return totalConsumedRecords.get() == recordsCount;
              });
      } catch (ConditionTimeoutException exception) {
          fail("Failed to consume " + recordsCount + " records in " + seconds + " seconds, total consumed: " + totalConsumedRecords.get(), exception);
      }

      assertEquals(recordsCount, totalConsumedRecords.get());
    }
}
