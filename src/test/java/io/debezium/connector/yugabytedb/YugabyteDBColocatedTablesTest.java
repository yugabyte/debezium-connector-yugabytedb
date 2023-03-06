package io.debezium.connector.yugabytedb;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.YugabyteYSQLContainer;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;

/**
 * Unit tests to verify connector functionality with colocated tables in YugabyteDB.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBColocatedTablesTest extends YugabyteDBTestBase {
  private final static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBColocatedTablesTest.class);
  private static YugabyteYSQLContainer ybContainer;

  private static final String COLOCATED_DB_NAME = "colocated_database";  

  private final String INSERT_TEST_1 = "INSERT INTO test_1 VALUES (%d, 'sample insert');";
  private final String INSERT_TEST_2 = "INSERT INTO test_2 VALUES (%d::text);";
  private final String INSERT_TEST_3 = "INSERT INTO test_3 VALUES (%d::float, 'hours in varchar');";
  private final String INSERT_TEST_NO_COLOCATED =
    "INSERT INTO test_no_colocated VALUES (%d, 'not a colocated table');";

  @BeforeClass
  public static void beforeClass() throws Exception {
    ybContainer = TestHelper.getYbContainer();
    ybContainer.start();

    TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433));
    TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");
  }

  @Before
  public void before() throws Exception {
    initializeConnectorTestFramework();
    TestHelper.dropAllSchemas();
  }

  @After
  public void after() throws Exception {
    stopConnector();
    dropTables();
  }

  @AfterClass
  public static void afterClass() {
    ybContainer.stop();
  }

  @Test
  public void shouldSupportBasicColocatedTableStreaming() throws Exception {
    createTables();

    String dbStreamId = TestHelper.getNewDbStreamId(COLOCATED_DB_NAME, "test_1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder(COLOCATED_DB_NAME,
        "public.test_1", dbStreamId);

    start(YugabyteDBConnector.class, configBuilder.build());

    awaitUntilConnectorIsReady();

    TestHelper.executeBulk(INSERT_TEST_1, 10, COLOCATED_DB_NAME);

    // Dummy wait for 10 seconds
    TestHelper.waitFor(Duration.ofSeconds(10));

    SourceRecords records = consumeRecordsByTopic(10);

    assertNotNull(records);

    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_1").size());

    // Since other tables were not included in the list, plus no data has been inserted in these
    // tables so the topic themselves should not exist
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_2"));
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_3"));
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_no_colocated"));
  }

  // This test also verifies that the connector works for a subset of the colocated tables as well
  @Test
  public void shouldWorkForTablesInIncludeListOnly() throws Exception {
    createTables();

    String dbStreamId = TestHelper.getNewDbStreamId(COLOCATED_DB_NAME, "test_1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder(COLOCATED_DB_NAME,
        "public.test_1,public.test_2", dbStreamId);

    start(YugabyteDBConnector.class, configBuilder.build());

    awaitUntilConnectorIsReady();

    TestHelper.executeBulk(INSERT_TEST_1, 10, COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_2, 10, COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_3, 10, COLOCATED_DB_NAME);

    // Dummy wait for 10 seconds
    TestHelper.waitFor(Duration.ofSeconds(10));

    SourceRecords records = consumeRecordsByTopic(20);

    assertNotNull(records);

    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_1").size());
    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_2").size());

    // The table test_3 was not included in the list so the topic should not get created
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_3"));

    // The table test_no_colocated was not included in the list as well as no data was inserted
    // into it so the topic should not get created
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_no_colocated"));
  }

  @Test
  public void shouldWorkWithMixOfColocatedAndNonColocatedTables() throws Exception {
    createTables();

    String dbStreamId = TestHelper.getNewDbStreamId(COLOCATED_DB_NAME, "test_1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder(COLOCATED_DB_NAME,
        "public.test_1,public.test_2,public.test_no_colocated", dbStreamId);

    start(YugabyteDBConnector.class, configBuilder.build());

    awaitUntilConnectorIsReady();

    TestHelper.executeBulk(INSERT_TEST_1, 10, COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_2, 10, COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_3, 10, COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_NO_COLOCATED, 10, COLOCATED_DB_NAME);

    // Dummy wait for 10 seconds
    TestHelper.waitFor(Duration.ofSeconds(10));

    SourceRecords records = consumeRecordsByTopic(30);

    assertNotNull(records);

    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_1").size());
    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_2").size());

    // The table test_3 was not included in the list so the topic should not get created
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_3"));

    assertEquals(
      10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_no_colocated").size());
  }

  @Test
  public void shouldWorkAfterAddingTableAfterRestart() throws Exception {
    createTables();

    String dbStreamId = TestHelper.getNewDbStreamId(COLOCATED_DB_NAME, "test_1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder(COLOCATED_DB_NAME,
        "public.test_1,public.test_2", dbStreamId);

    start(YugabyteDBConnector.class, configBuilder.build());

    awaitUntilConnectorIsReady();

    TestHelper.executeBulk(INSERT_TEST_1, 10, COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_2, 10, COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_3, 10, COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_NO_COLOCATED, 10, COLOCATED_DB_NAME);

    // Dummy wait for 10 seconds
    TestHelper.waitFor(Duration.ofSeconds(10));

    SourceRecords records = consumeRecordsByTopic(20);

    assertNotNull(records);

    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_1").size());
    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_2").size());

    // The other tables were not included in the list so the topics should not get created
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_3"));
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_no_colocated"));

    // Stop the connector and modify the configuration
    stopConnector();

    configBuilder.with(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST,
                       "public.test_1,public.test_2,public.test_3");

    // The connector would now start polling for the included tables. Note that the earlier data for
    // table test_3 won't be streamed since it might have gotten garbage collected since it resides
    // on the same tablet i.e. colocated
    start(YugabyteDBConnector.class, configBuilder.build());

    // The below statements will insert records of the respective types with keys in the
    // range [11,21)
    TestHelper.executeBulkWithRange(INSERT_TEST_1, 11, 21, COLOCATED_DB_NAME);
    TestHelper.executeBulkWithRange(INSERT_TEST_2, 11, 21, COLOCATED_DB_NAME);
    TestHelper.executeBulkWithRange(INSERT_TEST_3, 11, 21, COLOCATED_DB_NAME);

    // Dummy wait for 10 more seconds
    TestHelper.waitFor(Duration.ofSeconds(10));

    SourceRecords recordsAfterRestart = consumeRecordsByTopic(30);

    assertNotNull(recordsAfterRestart);

    assertEquals(
      10, recordsAfterRestart.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_1").size());
    assertEquals(
      10, recordsAfterRestart.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_2").size());
    assertEquals(
      10, recordsAfterRestart.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_3").size());

    // The topic for table test_no_colocated would still not exist since it was not included in
    // the list as well as no data was inserted into it
    assertFalse(
      recordsAfterRestart.topics().contains(TestHelper.TEST_SERVER + ".public.test_no_colocated"));
  }

  @Test
  public void dropColumnsForTablesAndHandleSchemaChanges() throws Exception {
    /**
     * 1. Create colocated tables having 40 columns (+2 for other columns)
     * 2. Start the CDC pipeline and keep inserting data
     * 3. Execute ALTER TABLE...DROP commands randomly.
     */
    int columnCount = 40;
    TestHelper.dropAllSchemas();

    createTables(columnCount);

    String dbStreamId = TestHelper.getNewDbStreamId(COLOCATED_DB_NAME, "test_1");
    Configuration.Builder configBuilder =
        TestHelper.getConfigBuilder(COLOCATED_DB_NAME, "public.test_1,public.test_2,public.test_3", dbStreamId);
    configBuilder.with(YugabyteDBConnectorConfig.CDC_POLL_INTERVAL_MS, 5_000);
    configBuilder.with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, 10000);

    start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> assertTrue(success));

    awaitUntilConnectorIsReady();

    // Start threads to perform schema change operations on the colocated tables.
    Executor ex1 = new Executor("test_1", columnCount);
    Executor ex2 = new Executor("test_2", columnCount);
    Executor ex3 = new Executor("test_3", columnCount);
    Thread thread1 = new Thread(ex1);
    Thread thread2 = new Thread(ex2);
    Thread thread3 = new Thread(ex3);

    // We can get the total number of records to be inserted for one table and the actual number
    // would be 3 times of that value since all the tables are having same inserts.
    long totalExpectedRecords = 3 * ex1.getTotalRecordsToBeInserted();

    thread1.start();
    thread2.start();
    thread3.start();

    // Wait for the threads to finish.
    thread1.join();
    thread2.join();
    thread3.join();

    LOGGER.info("Expected record count after thread finish: {}", totalExpectedRecords);

    // TODO: Figure out a way to verify the column info in records as well.
    // Maybe use record.schema().fields().size() to verify column count
    // Verify the record count now
    List<SourceRecord> records = new ArrayList<>();
    CompletableFuture.runAsync(() -> verifyRecordCount(records, totalExpectedRecords))
            .exceptionally(throwable -> {
              throw new RuntimeException(throwable);
            }).get();
  }

  /**
   * Helper function to create the required tables in the database COLOCATED_DB_NAME
   */
  private void createTables(int columnCount) {
    StringBuilder createTest1 = new StringBuilder("CREATE TABLE test_1 (id INT PRIMARY KEY, name TEXT");
    StringBuilder createTest2 = new StringBuilder("CREATE TABLE test_2 (text_key TEXT PRIMARY KEY, random_val TEXT DEFAULT 'random value'");
    StringBuilder createTest3 = new StringBuilder("CREATE TABLE test_3 (hours FLOAT PRIMARY KEY, hours_in_text VARCHAR(40)");
    for (int i = 1; i <= columnCount; ++i) {
      createTest1.append(", col_").append(i).append(" INT DEFAULT 404");
      createTest2.append(", col_").append(i).append(" INT DEFAULT 404");
      createTest3.append(", col_").append(i).append(" INT DEFAULT 404");
    }

    createTest1.append(") WITH (COLOCATED = true);");
    createTest2.append(") WITH (COLOCATED = true);");
    createTest3.append(") WITH (COLOCATED = true);");

    final String createTestNoColocated = 
      "CREATE TABLE test_no_colocated (id INT PRIMARY KEY, name TEXT) WITH (COLOCATED = false)"
      + " SPLIT INTO 3 TABLETS;";

    TestHelper.executeInDatabase(createTest1.toString(), COLOCATED_DB_NAME);
    TestHelper.executeInDatabase(createTest2.toString(), COLOCATED_DB_NAME);
    TestHelper.executeInDatabase(createTest3.toString(), COLOCATED_DB_NAME);
    TestHelper.executeInDatabase(createTestNoColocated, COLOCATED_DB_NAME);
  }

  private void createTables() {
    createTables(0);
  }

  /**
   * Helper function to drop the tables from the database COLOCATED_DB_NAME
   */
  private void dropTables() {
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_1;", COLOCATED_DB_NAME);
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_2;", COLOCATED_DB_NAME);
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_3;", COLOCATED_DB_NAME);
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_no_colocated;", COLOCATED_DB_NAME);
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

  protected static class Executor implements Runnable {
    private final String tableName;
    private final int columnCount;

    public Executor(String tableName, int columnCount) {
      this.tableName = tableName;
      this.columnCount = columnCount;
    }

    public long getTotalRecordsToBeInserted() {
      long sum = 0;
      for (int i = 1; i <= this.columnCount; ++i) {
        sum += i;
      }

      return sum;
    }

    @Override
    public void run() {
      int startKey = 1;
      for (int i = 1; i <= columnCount; ++i) {
        // Drop the column and then insert some records
        TestHelper.executeInDatabase("ALTER TABLE " + tableName + " DROP COLUMN col_" + i + ";", COLOCATED_DB_NAME);

        String generateSeries = "INSERT INTO %s VALUES (generate_series(%d, %d));";
        TestHelper.executeInDatabase(String.format(generateSeries, tableName, startKey, startKey + i - 1), COLOCATED_DB_NAME);
        startKey += i;
      }
    }
  }
}
