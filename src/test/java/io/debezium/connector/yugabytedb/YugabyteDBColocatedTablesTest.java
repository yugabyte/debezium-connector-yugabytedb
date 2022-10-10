package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.SQLException;
import java.time.Duration;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.YugabyteYSQLContainer;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;

/**
 * Unit tests to verify connector functionality with colocated tables in YugabyteDB.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBColocatedTablesTest extends YugabyteDBTestBase {
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
    createColocatedTables();
  }

  @After
  public void after() throws Exception {
    stopConnector();
    dropColocatedTables();
    // TestHelper.executeDDL("drop_tables_and_databases.ddl");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    ybContainer.stop();
  }

  @Test
  public void shouldSupportBasicColocatedTableStreaming() throws Exception {
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

  /**
   * Helper function to create the colocated tables in the database COLOCATED_DB_NAME
   */
  private void createColocatedTables() {
    final String createTest1 =
      "CREATE TABLE test_1 (id INT PRIMARY KEY, name TEXT) WITH (COLOCATED = true);";
    final String createTest2 =
      "CREATE TABLE test_2 (text_key TEXT PRIMARY KEY) WITH (COLOCATED = true);";
    final String createTest3 = 
      "CREATE TABLE test_3 (hours FLOAT PRIMARY KEY, hours_in_text VARCHAR(40))"
      + " WITH (COLOCATED = true);";
    final String createTestNoColocated = 
      "CREATE TABLE test_no_colocated (id INT PRIMARY KEY, name TEXT) WITH (COLOCATED = false)"
      + " SPLIT INTO 3 TABLETS;";

    TestHelper.executeInDatabase(createTest1, COLOCATED_DB_NAME);
    TestHelper.executeInDatabase(createTest2, COLOCATED_DB_NAME);
    TestHelper.executeInDatabase(createTest3, COLOCATED_DB_NAME);
    TestHelper.executeInDatabase(createTestNoColocated, COLOCATED_DB_NAME);
  }

  /**
   * Helper function to drop the colocated tables from the database COLOCATED_DB_NAME
   */
  private void dropColocatedTables() {
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_1;", COLOCATED_DB_NAME);
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_2;", COLOCATED_DB_NAME);
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_3;", COLOCATED_DB_NAME);
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_no_colocated;", COLOCATED_DB_NAME);
  }
}
