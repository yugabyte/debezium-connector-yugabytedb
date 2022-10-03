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

public class YugabyteDBColocatedTablesTest extends YugabyteDBTestBase {
  private static YugabyteYSQLContainer ybContainer;

  private final String INSERT_TEST_1 = "INSERT INTO test_1 VALUES (%d, 'sample insert');";
  private final String INSERT_TEST_2 = "INSERT INTO test_2 VALUES (%d::text);";
  private final String INSERT_TEST_3 = "INSERT INTO test_3 VALUES (%f, '%s');";

  @BeforeClass
  public static void beforeClass() throws SQLException {
    ybContainer = TestHelper.getYbContainer();
    ybContainer.start();

    TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433));
    TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));
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
  public void shouldSupportBasicColocatedTableStreaming() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("colocated_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId("colocated_database", "test_1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("colocated_database", "public.test_1", dbStreamId);

    start(YugabyteDBConnector.class, configBuilder.build());

    awaitUntilConnectorIsReady();

    TestHelper.executeBulk(INSERT_TEST_1, 10);

    // Dummy wait for 10 seconds
    TestHelper.waitFor(Duration.ofSeconds(10));

    SourceRecords records = consumeRecordsByTopic(10);

    assertNotNull(records);

    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_1").size());

    // Since other tables were not included in the list, the topic themselves should not exist
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_2"));
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_3"));
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_no_colocated"));
  }

  @Test
  public void shouldWorkForTablesInIncludeListOnly() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("colocated_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId("colocated_database", "test_1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("colocated_database", "public.test_1,public.test_2", dbStreamId);

    start(YugabyteDBConnector.class, configBuilder.build());

    awaitUntilConnectorIsReady();

    TestHelper.executeBulk(INSERT_TEST_1, 10);
    TestHelper.executeBulk(INSERT_TEST_2, 10);

    // Dummy wait for 10 seconds
    TestHelper.waitFor(Duration.ofSeconds(10));

    SourceRecords records = consumeRecordsByTopic(20);

    assertNotNull(records);

    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_1").size());
    assertEquals(10, records.recordsForTopic(TestHelper.TEST_SERVER + ".public.test_2").size());

    // Since no records were inserted in other tables, the topic themselves should not exist
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_3"));
    assertFalse(records.topics().contains(TestHelper.TEST_SERVER + ".public.test_no_colocated"));
  }
}
