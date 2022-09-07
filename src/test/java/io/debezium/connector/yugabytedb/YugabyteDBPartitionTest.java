package io.debezium.connector.yugabytedb;

import static org.junit.Assert.assertTrue;

import java.sql.SQLException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.YugabyteYSQLContainer;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;

public class YugabyteDBPartitionTest extends YugabyteDBTestBase {
  private final static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBPartitionTest.class);
  private static YugabyteYSQLContainer ybContainer;

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
  public void taskShouldNotFail() throws Exception {
    TestHelper.dropAllSchemas();
    LOGGER.info("Creating table with 20 tablets in YB");
    TestHelper.executeDDL("partition_table.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    LOGGER.info("Initiating the connector");
    start(YugabyteDBConnector.class, configBuilder.build(), (success, message, error) -> {
      // Commenting out the assertion to print the error only
      // assertTrue(success);

      LOGGER.info("===================================================================");
      LOGGER.info("ERROR message: {}", error.getMessage());
      LOGGER.info("-------------------------------------------------------------------");
      LOGGER.info("Stacktrace: ");
      error.printStackTrace();
      LOGGER.info("===================================================================");
    });

    // TODO Vaibhav: This line is to make the test fail at present (before the fix)
    assertConnectorIsRunning();
  }
}
