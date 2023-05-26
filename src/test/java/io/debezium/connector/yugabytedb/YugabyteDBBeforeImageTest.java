package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.*;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

public class YugabyteDBBeforeImageTest extends YugabytedTestBase {
  private final String formatInsertString =
      "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 12.345);";

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

  @Test
  public void isBeforeGettingPublished() throws Exception {
      TestHelper.initDB("yugabyte_create_tables.ddl");

      String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", true /* withBeforeImage */);
      Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
      startEngine(configBuilder);

      awaitUntilConnectorIsReady();

      // Insert a record and update it.
      TestHelper.execute(String.format(formatInsertString, 1));
      TestHelper.execute("UPDATE t1 SET first_name='VKVK', hours=56.78 where id = 1;");

      // Consume the records and verify that the records should have the relevant information.
      List<SourceRecord> records = new ArrayList<>();
      CompletableFuture.runAsync(() -> getRecords(records, 2, 20000)).get();

      // The first record is an insert record with before image as null.
      SourceRecord insertRecord = records.get(0);
      assertValueField(insertRecord, "before", null);
      assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

      // The second record will be an update record having a before image.
      SourceRecord updateRecord = records.get(1);
      assertBeforeImage(updateRecord, 1, "Vaibhav", "Kushwaha", 12.345);
      assertAfterImage(updateRecord, 1, "VKVK", "Kushwaha", 56.78);
  }

  @Test
  public void consecutiveSingleShardTransactions() throws Exception {
      TestHelper.initDB("yugabyte_create_tables.ddl");

      String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", true /* withBeforeImage */);
      Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
      startEngine(configBuilder);

      awaitUntilConnectorIsReady();

      // Insert a record and update it.
      TestHelper.execute(String.format(formatInsertString, 1));
      TestHelper.execute("UPDATE t1 SET last_name='some_last_name' where id = 1;");
      TestHelper.execute("UPDATE t1 SET first_name='V', last_name='K', hours=0.05 where id = 1;");

      // Consume the records and verify that the records should have the relevant information.
      List<SourceRecord> records = new ArrayList<>();
      CompletableFuture.runAsync(() -> getRecords(records, 3, 20000)).get();

      // The first record is an insert record with before image as null.
      SourceRecord insertRecord = records.get(0);
      assertValueField(insertRecord, "before", null);
      assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

      // The second record will be an update record having a before image.
      SourceRecord updateRecord = records.get(1);
      assertBeforeImage(updateRecord, 1, "Vaibhav", "Kushwaha", 12.345);
      assertAfterImage(updateRecord, 1, "Vaibhav", "some_last_name", 12.345);

      // The third record will be an update record too.
      SourceRecord updateRecord2 = records.get(2);
      assertBeforeImage(updateRecord2, 1, "Vaibhav", "some_last_name", 12.345);
      assertAfterImage(updateRecord2, 1, "V", "K", 0.05);
  }

  @Test
  public void multiShardTransactions() throws Exception {
    TestHelper.initDB("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", true /* withBeforeImage */);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
    startEngine(configBuilder);

    awaitUntilConnectorIsReady();

    // Perform operations on the table
    TestHelper.execute("BEGIN; " + String.format(formatInsertString, 1) + " COMMIT;");
    TestHelper.execute("BEGIN; " + String.format(formatInsertString, 2) + " COMMIT;");
    TestHelper.execute("BEGIN; UPDATE t1 SET hours=98.765 where id = 1; COMMIT;");
    TestHelper.execute("BEGIN; UPDATE t1 SET first_name='first_name_12345' where id = 2; COMMIT;");
    TestHelper.execute("DELETE from t1 WHERE id = 1;");
    TestHelper.execute("DELETE from t1 WHERE id = 2;");

    // The above statements will generate 8 records:
    // INSERT + INSERT + UPDATE + UPDATE + DELETE + TOMBSTONE + DELETE + TOMBSTONE.
    int totalRecordsToConsume = 8;

    // Consume the records and verify that the records should have the relevant information.
    List<SourceRecord> records = new ArrayList<>();
    CompletableFuture.runAsync(() -> getRecords(records, totalRecordsToConsume, 20000)).get();

    // The first and second records will be insert records with before image as null.
    SourceRecord record0 = records.get(0);
    assertValueField(record0, "before", null);
    assertAfterImage(record0, 1, "Vaibhav", "Kushwaha", 12.345);

    SourceRecord record1 = records.get(1);
    assertValueField(record1, "before", null);
    assertAfterImage(record1, 2, "Vaibhav", "Kushwaha", 12.345);

    // The third and fourth records will be update records.
    SourceRecord record2 = records.get(2);
    assertBeforeImage(record2, 1, "Vaibhav", "Kushwaha", 12.345);
    assertAfterImage(record2, 1, "Vaibhav", "Kushwaha", 98.765);

    SourceRecord record3 = records.get(3);
    assertBeforeImage(record3, 2, "Vaibhav", "Kushwaha", 12.345);
    assertAfterImage(record3, 2, "first_name_12345", "Kushwaha", 12.345);

    // For deletes, we will be getting 4 records i.e. DELETE+TOMBSTONE+DELETE+TOMBSTONE.
    SourceRecord record4 = records.get(4);
    assertBeforeImage(record4, 1, "Vaibhav", "Kushwaha", 98.765);
    assertValueField(record4, "after", null); // Delete records have null after field.

    assertTombstone(records.get(5));

    SourceRecord record6 = records.get(6);
    assertBeforeImage(record6, 2, "first_name_12345", "Kushwaha", 12.345);
    assertValueField(record6, "after", null); // Delete records have null after field.

    assertTombstone(records.get(7));
  }

  @Test
  public void updateWithNullValues() throws Exception {
    TestHelper.initDB("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", true /* withBeforeImage */);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
    startEngine(configBuilder);

    awaitUntilConnectorIsReady();

    // Insert a record and update it.
    TestHelper.execute(String.format(formatInsertString, 1));
    TestHelper.execute("UPDATE t1 SET last_name=null, hours=null where id = 1;");

    // Consume the records and verify that the records should have the relevant information.
    List<SourceRecord> records = new ArrayList<>();
    CompletableFuture.runAsync(() -> getRecords(records, 2, 20000)).get();

    // The first record is an insert record with before image as null.
    SourceRecord insertRecord = records.get(0);
    assertValueField(insertRecord, "before", null);
    assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

    // The second record will be an update record having a before image.
    SourceRecord updateRecord = records.get(1);
    assertBeforeImage(updateRecord, 1, "Vaibhav", "Kushwaha", 12.345);
    assertAfterImage(updateRecord, 1, "Vaibhav", null, null);
  }

  @Test
  public void modifyPrimaryKey() throws Exception {
    // NOTE: The modification of primary key will not lead to any different behaviour, it will
    // simply give us two records, DELETE + INSERT.

    TestHelper.initDB("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", true /* withBeforeImage */);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
    startEngine(configBuilder);

    awaitUntilConnectorIsReady();

    // Insert a record and update it.
    TestHelper.execute(String.format(formatInsertString, 1));
    TestHelper.execute("UPDATE t1 SET last_name='some_last_name', hours=98.765 where id = 1;");
    TestHelper.execute("UPDATE t1 SET id = 404 WHERE id = 1;");

    // Consume the records and verify that the records should have the relevant information.
    List<SourceRecord> records = new ArrayList<>();
    CompletableFuture.runAsync(() -> getRecords(records, 5, 20000)).get();

    // The first record is an insert record with before image as null.
    SourceRecord record0 = records.get(0);
    assertValueField(record0, "before", null);
    assertAfterImage(record0, 1, "Vaibhav", "Kushwaha", 12.345);

    // The second record will be an update record having a before image.
    SourceRecord record1 = records.get(1);
    assertBeforeImage(record1, 1, "Vaibhav", "Kushwaha", 12.345);
    assertAfterImage(record1, 1, "Vaibhav", "some_last_name", 98.765);

    // For updating the primary key, we will get a delete record along with a tombstone and one
    // insert record.
    SourceRecord record2 = records.get(2);
    assertBeforeImage(record2, 1, "Vaibhav", "some_last_name", 98.765);
    assertValueField(record2, "after", null);

    assertTombstone(records.get(3));

    SourceRecord record4 = records.get(4);
    assertValueField(record4, "before", null);
    assertAfterImage(record4, 404, "Vaibhav", "some_last_name", 98.765);
  }

  @Test
  public void operationsOnTableWithDefaultValues() throws Exception {
    TestHelper.initDB("yugabyte_create_tables.ddl");

    // Create a table with default values.
    TestHelper.execute("CREATE TABLE table_with_defaults (id INT PRIMARY KEY, first_name TEXT"
                       + " DEFAULT 'first_name_d', last_name VARCHAR(40) DEFAULT 'last_name_d',"
                       + " hours DOUBLE PRECISION DEFAULT 12.345);");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "table_with_defaults",
                                                    true /* withBeforeImage */);
    Configuration.Builder configBuilder =
        TestHelper.getConfigBuilder("public.table_with_defaults", dbStreamId);
    startEngine(configBuilder);

    awaitUntilConnectorIsReady();

    TestHelper.execute("INSERT INTO table_with_defaults VALUES (1);");
    TestHelper.execute(
        "UPDATE table_with_defaults SET first_name='updated_first_name' WHERE id = 1");
    TestHelper.execute("UPDATE table_with_defaults SET first_name='updated_first_name_2',"
                       + " last_name='updated_last_name', hours=98.765 WHERE id = 1");

    // Consume the records and verify that the records should have the relevant information.
    List<SourceRecord> records = new ArrayList<>();
    CompletableFuture.runAsync(() -> getRecords(records, 3, 20000)).get();

    // The first record is an insert record with before image as null.
    SourceRecord record0 = records.get(0);
    assertValueField(record0, "before", null);
    assertAfterImage(record0, 1, "first_name_d", "last_name_d", 12.345);

    // The second and third records will be update records having a before image.
    SourceRecord record1 = records.get(1);
    assertBeforeImage(record1, 1, "first_name_d", "last_name_d", 12.345);
    assertAfterImage(record1, 1, "updated_first_name", "last_name_d", 12.345);

    SourceRecord record2 = records.get(2);
    assertBeforeImage(record2, 1, "updated_first_name", "last_name_d", 12.345);
    assertAfterImage(record2, 1, "updated_first_name_2", "updated_last_name", 98.765);
  }

  private void assertBeforeImage(SourceRecord record, Integer id, String firstName, String lastName,
                                 Double hours) {
      assertValueField(record, "before/id/value", id);
      assertValueField(record, "before/first_name/value", firstName);
      assertValueField(record, "before/last_name/value", lastName);
      assertValueField(record, "before/hours/value", hours);
  }

  private void assertAfterImage(SourceRecord record, Integer id, String firstName, String lastName,
                                Double hours) {
      assertValueField(record, "after/id/value", id);
      assertValueField(record, "after/first_name/value", firstName);
      assertValueField(record, "after/last_name/value", lastName);
      assertValueField(record, "after/hours/value", hours);
  }

  private void getRecords(List<SourceRecord> records, long totalRecordsToConsume,
                          long milliSecondsToWait) {
      AtomicLong totalConsumedRecords = new AtomicLong();
      long seconds = milliSecondsToWait / 1000;
      try {
          Awaitility.await()
              .atMost(Duration.ofSeconds(seconds))
              .until(() -> {
                  int consumed = consumeAvailableRecords(record -> {
                      LOGGER.debug("The record being consumed is " + record);
                      records.add(record);
                  });
                  if (consumed > 0) {
                      totalConsumedRecords.addAndGet(consumed);
                      LOGGER.debug("Consumed " + totalConsumedRecords + " records");
                  }

                  return totalConsumedRecords.get() == totalRecordsToConsume;
              });
      } catch (ConditionTimeoutException exception) {
          fail("Failed to consume " + totalRecordsToConsume + " records in " + seconds + " seconds",
               exception);
      }

      assertEquals(totalRecordsToConsume, totalConsumedRecords.get());
  }
}
