package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.*;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.TestBaseClass;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests to verify connector functionality with colocated tables in YugabyteDB.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBColocatedTablesTest extends YugabyteDBContainerTestBase {
  private final String INSERT_TEST_1 = "INSERT INTO test_1 VALUES (%d, 'sample insert');";
  private final String INSERT_TEST_2 = "INSERT INTO test_2 VALUES (%d::text);";
  private final String INSERT_TEST_3 = "INSERT INTO test_3 VALUES (%d::float, 'hours in varchar');";
  private final String INSERT_TEST_NO_COLOCATED =
    "INSERT INTO test_no_colocated VALUES (%d, 'not a colocated table');";

  @BeforeAll
  public static void beforeClass() throws Exception {
    initializeYBContainer();
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");
  }

  @BeforeEach
  public void before() throws Exception {
    initializeConnectorTestFramework();
    TestHelper.dropAllSchemas();
  }

  @AfterEach
  public void after() throws Exception {
    stopConnector();
    dropTables();
  }

  @AfterAll
  public static void afterClass() {
    shutdownYBContainer();
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void shouldSupportBasicColocatedTableStreaming(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    createTables();

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME,
        "public.test_1", dbStreamId);

    startEngine(configBuilder);

    awaitUntilConnectorIsReady();

    TestHelper.executeBulk(INSERT_TEST_1, 10, DEFAULT_COLOCATED_DB_NAME);

    // Dummy wait for 10 seconds
    TestHelper.waitFor(Duration.ofSeconds(10));

    SourceRecords records = consumeByTopic(10);

    assertNotNull(records);

    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_1").size());

    // Since other tables were not included in the list, plus no data has been inserted in these
    // tables so the topic themselves should not exist
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_2"));
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_3"));
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_no_colocated"));
  }

  // This test also verifies that the connector works for a subset of the colocated tables as well
  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void shouldWorkForTablesInIncludeListOnly(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    createTables();

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME,
        "public.test_1,public.test_2", dbStreamId);

    startEngine(configBuilder);

    awaitUntilConnectorIsReady();

    TestHelper.executeBulk(INSERT_TEST_1, 10, DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_2, 10, DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_3, 10, DEFAULT_COLOCATED_DB_NAME);

    // Dummy wait for 10 seconds
    TestHelper.waitFor(Duration.ofSeconds(10));

    SourceRecords records = consumeByTopic(20);

    assertNotNull(records);

    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_1").size());
    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_2").size());

    // The table test_3 was not included in the list so the topic should not get created
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_3"));

    // The table test_no_colocated was not included in the list as well as no data was inserted
    // into it so the topic should not get created
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_no_colocated"));
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void shouldWorkWithMixOfColocatedAndNonColocatedTables(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    createTables();

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME,
        "public.test_1,public.test_2,public.test_no_colocated", dbStreamId);

    startEngine(configBuilder);

    awaitUntilConnectorIsReady();

    TestHelper.executeBulk(INSERT_TEST_1, 10, DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_2, 10, DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_3, 10, DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_NO_COLOCATED, 10, DEFAULT_COLOCATED_DB_NAME);

    // Dummy wait for 10 seconds
    TestHelper.waitFor(Duration.ofSeconds(10));

    SourceRecords records = consumeByTopic(30);

    assertNotNull(records);

    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_1").size());
    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_2").size());

    // The table test_3 was not included in the list so the topic should not get created
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_3"));

    assertEquals(
      10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_no_colocated").size());
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void shouldWorkAfterAddingTableAfterRestart(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    createTables();

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME,
        "public.test_1,public.test_2", dbStreamId);

    startEngine(configBuilder);

    awaitUntilConnectorIsReady();

    TestHelper.executeBulk(INSERT_TEST_1, 10, DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_2, 10, DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_3, 10, DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeBulk(INSERT_TEST_NO_COLOCATED, 10, DEFAULT_COLOCATED_DB_NAME);

    // Dummy wait for 10 seconds
    TestHelper.waitFor(Duration.ofSeconds(10));

    SourceRecords records = consumeByTopic(20);

    assertNotNull(records);

    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_1").size());
    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_2").size());

    // The other tables were not included in the list so the topics should not get created
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_3"));
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_no_colocated"));

    // Stop the connector and modify the configuration
    stopConnector();

    TestHelper.executeBulkWithRange(INSERT_TEST_2, 11, 21, DEFAULT_COLOCATED_DB_NAME);

    configBuilder.with(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST,
                       "public.test_1,public.test_2,public.test_3");

    // The connector would now start polling for the included tables. Note that the earlier data for
    // table test_3 won't be streamed since it might have gotten garbage collected since it resides
    // on the same tablet i.e. colocated
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // The below statements will insert records of the respective types with keys in the
    // range [11,21)
    TestHelper.executeBulkWithRange(INSERT_TEST_1, 11, 21, DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeBulkWithRange(INSERT_TEST_2, 21, 101, DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeBulkWithRange(INSERT_TEST_3, 11, 21, DEFAULT_COLOCATED_DB_NAME);

    // Dummy wait for 10 more seconds
    TestHelper.waitFor(Duration.ofSeconds(10));

    SourceRecords recordsAfterRestart = consumeByTopic(110);

    assertNotNull(recordsAfterRestart);

    assertEquals(
      10, recordsAfterRestart.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_1").size());
    assertEquals(
      90, recordsAfterRestart.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_2").size());
    assertEquals(
      10, recordsAfterRestart.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_3").size());

    // The topic for table test_no_colocated would still not exist since it was not included in
    // the list as well as no data was inserted into it
    assertFalse(
      recordsAfterRestart.topics().contains(TestHelper.TEST_SERVER + ".public.test_no_colocated"));
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void dropColumnsForTablesAndHandleSchemaChanges(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    /*
     * 1. Create colocated tables having 40 columns (+2 for other columns)
     * 2. Start the CDC pipeline and keep inserting data
     * 3. Execute ALTER TABLE...DROP commands randomly.
     */
    int columnCount = 40;
    TestHelper.dropAllSchemas();

    createTables(columnCount);

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder =
        TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1,public.test_2,public.test_3", dbStreamId);
    configBuilder.with(YugabyteDBConnectorConfig.CDC_POLL_INTERVAL_MS, 5_000);
    configBuilder.with(YugabyteDBConnectorConfig.CONNECTOR_RETRY_DELAY_MS, 10000);

    startEngine(configBuilder, (success, message, error) -> assertTrue(success));
    awaitUntilConnectorIsReady();

    // Start threads to perform schema change operations on the colocated tables.
    Executor ex = new Executor(Arrays.asList("test_1", "test_2", "test_3"), columnCount);
    Thread thread = new Thread(ex);

    // We can get the total number of records to be inserted for one table and the actual number
    // would be 3 times of that value since all the tables are having same inserts.
    long totalExpectedRecords = 3 * ex.getTotalRecordsToBeInserted();

    thread.start();
    thread.join();

    LOGGER.info("Expected record count after thread finish: {}", totalExpectedRecords);

    // TODO: Figure out a way to verify the column info in records as well.
    // Maybe use record.schema().fields().size() to verify column count
    // Verify the record count now
    List<SourceRecord> records = new ArrayList<>();
    verifyRecordCount(records, totalExpectedRecords);
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

    TestHelper.executeInDatabase(createTest1.toString(), DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeInDatabase(createTest2.toString(), DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeInDatabase(createTest3.toString(), DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeInDatabase(createTestNoColocated, DEFAULT_COLOCATED_DB_NAME);
  }

  private void createTables() {
    createTables(0);
  }

  /**
   * Helper function to drop the tables from the database COLOCATED_DB_NAME
   */
  private void dropTables() {
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_1;", DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_2;", DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_3;", DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_no_colocated;", DEFAULT_COLOCATED_DB_NAME);
  }

  private void verifyRecordCount(List<SourceRecord> records, long recordsCount) {
    waitAndFailIfCannotConsume(records, recordsCount, 10 * 60 * 1000);
  }

  protected static class Executor extends TestBaseClass implements Runnable {
    private final List<String> tables;
    private final int columnCount;

    public Executor(List<String> tables, int columnCount) {
      this.tables = tables;
      this.columnCount = columnCount;
    }

    public long getTotalRecordsToBeInserted() {
      long sum = 0;
      for (int i = 1; i <= this.columnCount; ++i) {
        sum += i;
      }

      return sum;
    }

    /**
     * This function will start dropping columns for the tables and will insert a few records
     * after dropping the column. The flow will be as follows:<br>
     * <ul>
     *     Iterate over the column count starting from 1, in every iteration, go over all the tables
     *     and drop a column and insert a few rows.
     * </ul>
     *
     */
    @Override
    public void run() {
      int startKey = 1;
      for (int i = 1; i <= columnCount; ++i) {
        for (String tableName : tables) {
          // Drop a column and then insert some records.
          TestHelper.executeInDatabase("ALTER TABLE " + tableName + " DROP COLUMN col_" + i + ";", DEFAULT_COLOCATED_DB_NAME);

          String generateSeries = "INSERT INTO %s VALUES (generate_series(%d, %d));";
          TestHelper.executeInDatabase(String.format(generateSeries, tableName, startKey, startKey + i - 1), DEFAULT_COLOCATED_DB_NAME);
        }

        startKey += i;
      }
    }
  }
}
