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
 * Basic unit tests to check the behaviour of User Defined types with CDC in YugabyteDB.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBUserDefinedTypesTest extends YugabyteDBTestBase {
  private final static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBUserDefinedTypesTest.class);
  private static YugabyteYSQLContainer ybContainer;

  // Fully qualified table name
  private final String FQ_TABLE_NAME = "public.udt_table";

  private final String INSERT_FORMAT = "INSERT INTO udt_table VALUES (%s, ('Vaibhav', 'Kushwaha'));";

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Make sure this test is using the YugabyteDB version which has the changes to support
    // composite types with Change data capture
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
    // ybContainer.stop();
  }

  @Test
  public void validateBasicTestBasedUdt() throws Exception {
    TestHelper.dropAllSchemas();
    System.out.println("executing the ddl file");
    TestHelper.executeDDL("yugabyte_user_defined_types.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "udt_table");
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder(FQ_TABLE_NAME, dbStreamId);
    System.out.println("starting the connector");
    start(YugabyteDBConnector.class, configBuilder.build());

    final int recordsCount = 5;
    awaitUntilConnectorIsReady();
    System.out.println("Sleeping for 10 seconds now");
    Thread.sleep(10000);

    System.out.println("executing the bulk");
    TestHelper.executeBulk(INSERT_FORMAT, recordsCount);

    System.out.println("verifying the inserted value");
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
    LOGGER.info("Total duration to consume " + recordsCount + " records: " + Strings.duration(System.currentTimeMillis() - start));

    for (int i = 0; i < records.size(); ++i) {
        // verify the records with values
        assertValueField(records.get(i), "after/id/value", i);
        assertValueField(records.get(i), "after/name_col/value", "(Vaibhav,Kushwaha)");
    }
  }
}
