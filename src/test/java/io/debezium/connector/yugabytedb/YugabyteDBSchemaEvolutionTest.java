package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.YugabyteYSQLContainer;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;

import static org.junit.jupiter.api.Assertions.*;

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

  @BeforeAll
  public static void beforeClass() throws SQLException {
      String tserverFlags = "cdc_max_stream_intent_records=200";
      ybContainer = TestHelper.getYbContainer(null, tserverFlags);
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
  public static void afterClass() {
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
     *    the connector should get the older schema for that tablet and keep working.
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

    TestHelper.execute(String.format(insertFormatString, "2"));

    // Consume the records now.
    CompletableFuture.runAsync(() -> verifyRecordCount(new ArrayList<>() /* dummy list */, 5000 + 2))
      .exceptionally(throwable -> {
        throw new RuntimeException(throwable);
      }).get();
  }

  @Test
  public void shouldHandleDropColumnWithSingleTablet() throws Exception {
    /**
     * 1. Create a table having 10 columns (+1 for primary key)
     * 2. Start the CDC pipeline and keep inserting data
     * 3. Execute ALTER TABLE...DROP commands randomly.
     */
    int columnCount = 10;
    TestHelper.dropAllSchemas();

    int sum = 0;
    String createTableStatement = "CREATE TABLE t1 (id INT PRIMARY KEY";
    for (int i = 1; i <= columnCount; ++i) {
      createTableStatement += ", col_" + i + " INT DEFAULT 404";
      sum += i;
    }
    createTableStatement += ") SPLIT INTO 1 TABLETS;";
    final int totalExpectedRecords = sum;
    LOGGER.info("Creating table: {}", createTableStatement);
    
    TestHelper.execute(createTableStatement);

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
    configBuilder.with(YugabyteDBConnectorConfig.CDC_POLL_INTERVAL_MS, 5_000);
    configBuilder.with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, 10000);

    start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    Thread executorThread = new Thread(new Executor(columnCount));
    executorThread.start();

    // Wait for the thread to finish.
    executorThread.join();

    // Verify the record count now
    List<SourceRecord> records = new ArrayList<>();
    CompletableFuture.runAsync(() -> verifyRecordCount(records, totalExpectedRecords))
      .exceptionally(throwable -> {
        throw new RuntimeException(throwable);
      }).get();
  }

  @Test
  public void shouldHandleSchemaChangesForHighTabletCount() throws Exception {
    /**
     * 1. Create a table having 40 columns (+1 for primary key) with 40 tablets
     * 2. Start the CDC pipeline and keep inserting data
     * 3. Execute ALTER TABLE...DROP commands randomly.
     */
    int columnCount = 40;
    TestHelper.dropAllSchemas();

    int sum = 0;
    String createTableStatement = "CREATE TABLE t1 (id INT PRIMARY KEY";
    for (int i = 1; i <= 40; ++i) {
      createTableStatement += ", col_" + i + " INT DEFAULT 404";
      sum += i;
    }
    createTableStatement += ") SPLIT INTO 40 TABLETS;";
    final int totalExpectedRecords = sum;
    LOGGER.info("Creating table: {}", createTableStatement);
    
    TestHelper.execute(createTableStatement);

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
    configBuilder.with(YugabyteDBConnectorConfig.CDC_POLL_INTERVAL_MS, 5_000);
    configBuilder.with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, 10000);

    start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    Thread executorThread = new Thread(new Executor(columnCount));
    executorThread.start();

    // Wait for the thread to finish.
    executorThread.join();
    LOGGER.info("Expected record count after thread finish: {}", totalExpectedRecords);

    // Verify the record count now
    List<SourceRecord> records = new ArrayList<>();
    CompletableFuture.runAsync(() -> verifyRecordCount(records, totalExpectedRecords))
      .exceptionally(throwable -> {
        throw new RuntimeException(throwable);
      }).get();
  }

  @Test
  public void shouldWorkWithColumnRenaming() throws Exception {
    /**
     * 1. Create tablets with range sharding
     * 2. Start the CDC pipeline and insert data
     * 3. Execute an ALTER command to rename a column
     * 4. Now when the connector will try to poll the records for the tablet with less data, it will
     *    also try to get the schema and since the schema has changed by this time in the records,
     *    the connector should get the older schema for that tablet and keep working.
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
    TestHelper.execute(String.format(insertFormatString, "generate_series(40001,42000)"));

    // Now by the time connector is consuming all these records, execute an ALTER COMMAND and
    // insert records in the tablet with lesser data.
    TestHelper.execute("ALTER TABLE t1 RENAME COLUMN name TO full_name;");

    TestHelper.execute(String.format(insertFormatString, "generate_series(2, 10)"));

    List<SourceRecord> records = new ArrayList<>();

    // Consume the records now.
    CompletableFuture.runAsync(() -> verifyRecordCount(records, 2000 + 1 + 9))
      .exceptionally(throwable -> {
        throw new RuntimeException(throwable);
      }).get();

    // Filter records which have 2 <= id and id <= 10 so as to verify that they are being published
    // with the new column name.
    List<SourceRecord> filteredRecords = new ArrayList<>();
    for (int i = 0; i < records.size(); ++i) {
      Struct record = (Struct) records.get(i).value();
      int value = record.getStruct("after").getStruct("id").getInt32("value");
      if (value >= 2 && value <= 10) {
        filteredRecords.add(records.get(i));
      }
    }

    for (int i = 0; i < filteredRecords.size(); ++i) {
      assertValueField(filteredRecords.get(i), "after/full_name/value", "name_value");
    }
  }

  private void verifyRecordCount(List<SourceRecord> records, long recordsCount) {
    waitAndFailIfCannotConsume(records, recordsCount, 10 * 60 * 1000);
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

    protected class Executor implements Runnable {
      private final String generateSeries = "INSERT INTO t1 VALUES (generate_series(%d, %d));";
      private final int columnCount;
      public Executor(int columnCount) {
        this.columnCount = columnCount;
      }

      @Override
      public void run() {
        int startKey = 1;
        for (int i = 1; i <= columnCount; ++i) {
          // Pick a random index from the list and drop the column.
          int colToDrop = i;

          // Drop the column and then insert some records
          TestHelper.execute("ALTER TABLE t1 DROP COLUMN col_" + colToDrop + ";");

          TestHelper.execute(String.format(generateSeries, startKey, startKey + colToDrop - 1));
          startKey += colToDrop;
      }
    }
  }
}
