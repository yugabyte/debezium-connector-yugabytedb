package io.debezium.connector.yugabytedb;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.connect.source.SourceRecord;
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
import io.debezium.util.Strings;

/**
 * Basic unit tests to check the behaviour of Composite types with CDC in YugabyteDB.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBCompositeTypesTest extends YugabyteDBTestBase {
  private final static Logger LOGGER = 
    LoggerFactory.getLogger(YugabyteDBCompositeTypesTest.class);
  private static YugabyteYSQLContainer ybContainer;

  private final String TABLE_NAME = "composite_types_table";
  
  // Fully qualified table name with schema
  private final String FQ_TABLE_NAME = "public." + TABLE_NAME;

  // Helper SQL statements for creation and insertion into the table
  private final String CREATE_TYPE =
    "CREATE TYPE my_name_type AS (first_name text, last_name varchar(40));";
  private final String CREATE_TABLE =
    "CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, name_col my_name_type);";
  private final String INSERT_FORMAT =
    "INSERT INTO " + TABLE_NAME + " VALUES (%s, ('Vaibhav', 'Kushwaha'));";

  @BeforeClass
  public static void beforeClass() throws Exception {
    ybContainer = TestHelper.getYbContainer();
    ybContainer.start();

    TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433));
    TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));
    TestHelper.dropAllSchemas();
  }

  @Before
  public void beforeEachTest() throws Exception {
    initializeConnectorTestFramework();
  }

  @After
  public void afterEachTest() throws Exception {
    stopConnector();
    TestHelper.executeDDL("drop_tables_and_databases.ddl");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    ybContainer.stop();
  }

  @Test
  public void validateBasicTestBasedUdt() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.execute(CREATE_TYPE);
    TestHelper.execute(CREATE_TABLE);

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", TABLE_NAME);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder(FQ_TABLE_NAME, dbStreamId);
    start(YugabyteDBConnector.class, configBuilder.build());

    final int recordsCount = 5;
    awaitUntilConnectorIsReady();

    TestHelper.executeBulk(INSERT_FORMAT, recordsCount);

    CompletableFuture.runAsync(() -> verifyUDTValue(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
  }

  private void verifyUDTValue(long recordsCount) {
    int totalConsumedRecords = 0;
    long start = System.currentTimeMillis();
    List<SourceRecord> records = new ArrayList<>();
    while (totalConsumedRecords < recordsCount) {
        int consumed = super.consumeAvailableRecords(record -> {
            LOGGER.info("The record being consumed is " + record);
            records.add(record);
        });
        if (consumed > 0) {
            totalConsumedRecords += consumed;
            LOGGER.debug("Consumed " + totalConsumedRecords + " records");
        }
    }
    LOGGER.info("Total duration to consume " + recordsCount + " records: "
                 + Strings.duration(System.currentTimeMillis() - start));

    for (int i = 0; i < records.size(); ++i) {
        // Verify the records with values
        assertValueField(records.get(i), "after/id/value", i);
        assertValueField(records.get(i), "after/name_col/value", "(Vaibhav,Kushwaha)");
    }
  }
}
