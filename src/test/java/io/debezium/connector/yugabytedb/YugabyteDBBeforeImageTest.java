package io.debezium.connector.yugabytedb;

import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.yb.client.CDCStreamInfo;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import io.debezium.connector.yugabytedb.HelperBeforeImageModes.BeforeImageMode;

public class YugabyteDBBeforeImageTest extends YugabyteDBContainerTestBase {
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
      TestHelper.execute("DROP TABLE IF EXISTS test_table;");
      TestHelper.executeDDL("drop_tables_and_databases.ddl");
  }

  @AfterAll
  public static void afterClass() throws Exception {
      shutdownYBContainer();
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void isBeforeGettingPublished(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
      TestHelper.initDB("yugabyte_create_tables.ddl");

      String dbStreamId = TestHelper.getNewDbStreamId(
          "yugabyte", "t1", true /* withBeforeImage */, true,
          BeforeImageMode.FULL, consistentSnapshot, useSnapshot);
      Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
      startEngine(configBuilder);

      awaitUntilConnectorIsReady();

      // Insert a record and update it.
      TestHelper.execute(String.format(formatInsertString, 1));
      TestHelper.execute("UPDATE t1 SET first_name='VKVK', hours=56.78 where id = 1;");

      // Consume the records and verify that the records should have the relevant information.
      List<SourceRecord> records = new ArrayList<>();
      getRecords(records, 2, 20000);

      // The first record is an insert record with before image as null.
      SourceRecord insertRecord = records.get(0);
      assertValueField(insertRecord, "before", null);
      assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

      // The second record will be an update record having a before image.
      SourceRecord updateRecord = records.get(1);
      assertBeforeImage(updateRecord, 1, "Vaibhav", "Kushwaha", 12.345);
      assertAfterImage(updateRecord, 1, "VKVK", "Kushwaha", 56.78);
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void consecutiveSingleShardTransactions(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
      TestHelper.initDB("yugabyte_create_tables.ddl");

      String dbStreamId = TestHelper.getNewDbStreamId(
          "yugabyte", "t1", true /* withBeforeImage */, true,
          BeforeImageMode.FULL, consistentSnapshot, useSnapshot);
      Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
      startEngine(configBuilder);

      awaitUntilConnectorIsReady();

      // Insert a record and update it.
      TestHelper.execute(String.format(formatInsertString, 1));
      TestHelper.execute("UPDATE t1 SET last_name='some_last_name' where id = 1;");
      TestHelper.execute("UPDATE t1 SET first_name='V', last_name='K', hours=0.05 where id = 1;");

      // Consume the records and verify that the records should have the relevant information.
      List<SourceRecord> records = new ArrayList<>();
      getRecords(records, 3, 20000);

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

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void consecutiveSingleShardTransactionsForChange(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
      TestHelper.initDB("yugabyte_create_tables.ddl");

      String dbStreamId = TestHelper.getNewDbStreamId(
          "yugabyte", "t1", true /* withBeforeImage */, true,
          BeforeImageMode.CHANGE, consistentSnapshot, useSnapshot);
      Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
      startEngine(configBuilder);

      awaitUntilConnectorIsReady();

      // Insert a record and update it.
      TestHelper.execute(String.format(formatInsertString, 1));
      TestHelper.execute("UPDATE t1 SET last_name='some_last_name' where id = 1;");
      TestHelper.execute("DELETE from t1 WHERE id = 1;");

      // Consume the records and verify that the records should have the relevant information.
      List<SourceRecord> records = new ArrayList<>();
      getRecords(records, 4, 20000);

      // The first record is an insert record with before image as null.
      SourceRecord insertRecord = records.get(0);
      assertValueField(insertRecord, "before", null);
      assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

      // The second record will be an update record having no before image.
      SourceRecord updateRecord = records.get(1);
      assertValueField(updateRecord, "before", null);
      assertValueField(updateRecord, "after/id/value", 1);
      assertValueField(updateRecord, "after/last_name/value", "some_last_name");

      // The third record will be a delete record.
      SourceRecord deleteRecord = records.get(2);
      assertValueField(deleteRecord, "before/id/value", 1);
      assertValueField(deleteRecord, "after", null);
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void consecutiveSingleShardTransactionsForChangeOldNew(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
      TestHelper.initDB("yugabyte_create_tables.ddl");

      String dbStreamId = TestHelper.getNewDbStreamId(
          "yugabyte", "t1", true /* withBeforeImage */, true, 
          BeforeImageMode.CHANGE_OLD_NEW, consistentSnapshot, useSnapshot);
      Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
      startEngine(configBuilder);

      awaitUntilConnectorIsReady();

      // Insert a record and update it.
      TestHelper.execute(String.format(formatInsertString, 1));
      TestHelper.execute("UPDATE t1 SET last_name='some_last_name' where id = 1;");
      TestHelper.execute("DELETE from t1 WHERE id = 1;");

      // Consume the records and verify that the records should have the relevant information.
      List<SourceRecord> records = new ArrayList<>();
      getRecords(records, 4, 20000);

      // The first record is an insert record with before image as null.
      SourceRecord insertRecord = records.get(0);
      assertValueField(insertRecord, "before", null);
      assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

      // The second record will be an update record having no before image.
      SourceRecord updateRecord = records.get(1);
      assertValueField(updateRecord, "before/id/value", 1);
      assertValueField(updateRecord, "before/last_name/value", "Kushwaha");
      assertValueField(updateRecord, "after/id/value", 1);
      assertValueField(updateRecord, "after/last_name/value", "some_last_name");

      // The third record will be a delete record.
      SourceRecord deleteRecord = records.get(2);
      assertValueField(deleteRecord, "before/id/value", 1);
      assertValueField(deleteRecord, "after", null);
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void consecutiveSingleShardTransactionsForDefault(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
      TestHelper.initDB("yugabyte_create_tables.ddl");

      String dbStreamId = TestHelper.getNewDbStreamId(
          "yugabyte", "t1", true /* withBeforeImage */, true,
          BeforeImageMode.DEFAULT, consistentSnapshot, useSnapshot);
      Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
      startEngine(configBuilder);

      awaitUntilConnectorIsReady();

      // Insert a record and update it.
      TestHelper.execute(String.format(formatInsertString, 1));
      TestHelper.execute("UPDATE t1 SET last_name='some_last_name' where id = 1;");
      TestHelper.execute("DELETE from t1 WHERE id = 1;");

      // Consume the records and verify that the records should have the relevant information.
      List<SourceRecord> records = new ArrayList<>();
      getRecords(records, 4, 20000);

      // The first record is an insert record with before image as null.
      SourceRecord insertRecord = records.get(0);
      assertValueField(insertRecord, "before", null);
      assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

      // The second record will be an update record having no before image.
      SourceRecord updateRecord = records.get(1);
      assertValueField(updateRecord, "before", null);
      assertAfterImage(updateRecord, 1, "Vaibhav", "some_last_name", 12.345);

      // The third record will be a delete record.
      SourceRecord deleteRecord = records.get(2);
      assertValueField(deleteRecord, "before/id/value", 1);
      assertValueField(deleteRecord, "after", null);
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void consecutiveSingleShardTransactionsForNothing(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
      TestHelper.initDB("yugabyte_create_tables.ddl");

      String dbStreamId = TestHelper.getNewDbStreamId(
          "yugabyte", "t1", true /* withBeforeImage */, true,
          BeforeImageMode.NOTHING, consistentSnapshot, useSnapshot);
      Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
      startEngine(configBuilder);

      awaitUntilConnectorIsReady();

      // Insert a record and update it.
      TestHelper.execute(String.format(formatInsertString, 1));
      TestHelper.execute("UPDATE t1 SET last_name='some_last_name' where id = 1;");
      TestHelper.execute("DELETE from t1 WHERE id = 1;");

      // Consume the records and verify that the records should have the relevant information.
      List<SourceRecord> records = new ArrayList<>();
      getRecords(records, 4, 20000);

      // The first record is an insert record with before image as null.
      SourceRecord insertRecord = records.get(0);
      assertValueField(insertRecord, "before", null);
      assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

      // The second record will be an update record having no before image.
      SourceRecord updateRecord = records.get(1);
      assertValueField(updateRecord, "before", null);
      assertAfterImage(updateRecord, 1, "Vaibhav", "some_last_name", 12.345);

      // The third record will be a delete record.
      SourceRecord deleteRecord = records.get(2);
      assertValueField(deleteRecord, "before", null);
      assertValueField(deleteRecord, "after", null);
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void multiShardTransactions(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    TestHelper.initDB("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId(
        "yugabyte", "t1", true /* withBeforeImage */, true,
        BeforeImageMode.FULL, consistentSnapshot, useSnapshot);
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
    getRecords(records, totalRecordsToConsume, 20000);

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

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void updateWithNullValues(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    TestHelper.initDB("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId(
        "yugabyte", "t1", true /* withBeforeImage */, true,
        BeforeImageMode.FULL, consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
    startEngine(configBuilder);

    awaitUntilConnectorIsReady();

    // Insert a record and update it.
    TestHelper.execute(String.format(formatInsertString, 1));
    TestHelper.execute("UPDATE t1 SET last_name=null, hours=null where id = 1;");

    // Consume the records and verify that the records should have the relevant information.
    List<SourceRecord> records = new ArrayList<>();
    getRecords(records, 2, 20000);

    // The first record is an insert record with before image as null.
    SourceRecord insertRecord = records.get(0);
    assertValueField(insertRecord, "before", null);
    assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 12.345);

    // The second record will be an update record having a before image.
    SourceRecord updateRecord = records.get(1);
    assertBeforeImage(updateRecord, 1, "Vaibhav", "Kushwaha", 12.345);
    assertAfterImage(updateRecord, 1, "Vaibhav", null, null);
  }

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void modifyPrimaryKey(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    // NOTE: The modification of primary key will not lead to any different behaviour, it will
    // simply give us two records, DELETE + INSERT.

    TestHelper.initDB("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId(
        "yugabyte", "t1", true /* withBeforeImage */, true,
        BeforeImageMode.FULL, consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
    startEngine(configBuilder);

    awaitUntilConnectorIsReady();

    // Insert a record and update it.
    TestHelper.execute(String.format(formatInsertString, 1));
    TestHelper.execute("UPDATE t1 SET last_name='some_last_name', hours=98.765 where id = 1;");
    TestHelper.execute("UPDATE t1 SET id = 404 WHERE id = 1;");

    // Consume the records and verify that the records should have the relevant information.
    List<SourceRecord> records = new ArrayList<>();
    getRecords(records, 5, 20000);

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

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void operationsOnTableWithDefaultValues(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    TestHelper.initDB("yugabyte_create_tables.ddl");

    // Create a table with default values.
    TestHelper.execute("CREATE TABLE table_with_defaults (id INT PRIMARY KEY, first_name TEXT"
                       + " DEFAULT 'first_name_d', last_name VARCHAR(40) DEFAULT 'last_name_d',"
                       + " hours DOUBLE PRECISION DEFAULT 12.345);");

    String dbStreamId = TestHelper.getNewDbStreamId(
        "yugabyte", "table_with_defaults", true /* withBeforeImage */, true,
        BeforeImageMode.FULL, consistentSnapshot, useSnapshot);
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
    getRecords(records, 3, 20000);

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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldWorkWhenDefaultOldValueIsNull(boolean shouldUpdateToNull) throws Exception {
    TestHelper.execute("CREATE TABLE test_table (id INT PRIMARY KEY, bigint_col bigint);");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "test_table", true /* withBeforeImage */,
        true, BeforeImageMode.ALL, true, true);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.test_table", dbStreamId);
    startEngine(configBuilder);

    awaitUntilConnectorIsReady();

    TestHelper.execute("INSERT INTO test_table VALUES (1);");

    TestHelper.execute(
      String.format("UPDATE test_table SET bigint_col = %s WHERE id = 1;", shouldUpdateToNull ? "NULL" : "12345"));

    TestHelper.execute("INSERT INTO test_table VALUES (2, 202);");

    List<SourceRecord> records = new ArrayList<>();
    waitAndFailIfCannotConsume(records, 3);

    // Assert insert record.
    SourceRecord insertRecord = records.get(0);
    Struct recordVal = (Struct) insertRecord.value();
    assertEquals("c", TestHelper.getOpValue(insertRecord));
    assertEquals(1, recordVal.getStruct("after").getStruct("id").getInt32("value"));
    assertNull(recordVal.getStruct("after").getStruct("bigint_col").get("value"));

    SourceRecord updateRecord = records.get(1);
    Struct updateRecordVal = (Struct) updateRecord.value();
    assertEquals("u", TestHelper.getOpValue(updateRecord));

    if (shouldUpdateToNull) {
      assertNull(updateRecordVal.getStruct("after").getStruct("bigint_col").get("value"));
    } else {
      assertEquals(12345, updateRecordVal.getStruct("after").getStruct("bigint_col").getInt64("value"));
    }

    assertEquals(1, updateRecordVal.getStruct("before").getStruct("id").getInt32("value"));
    assertNull(updateRecordVal.getStruct("before").getStruct("bigint_col"));

    // Assert second insert record.
    SourceRecord insertRecordAfterUpdate = records.get(2);
    Struct insertValAfterUpdate = (Struct) insertRecordAfterUpdate.value();
    assertEquals("c", TestHelper.getOpValue(insertRecord));
    assertEquals(2, insertValAfterUpdate.getStruct("after").getStruct("id").getInt32("value"));
    assertEquals(202, insertValAfterUpdate.getStruct("after").getStruct("bigint_col").getInt64("value"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldWorkWhenDefaultHasOldValue(boolean shouldUpdateToNull) throws Exception {
    TestHelper.execute("CREATE TABLE test_table (id INT PRIMARY KEY, bigint_col bigint DEFAULT 123);");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "test_table", true /* withBeforeImage */,
      true, BeforeImageMode.ALL, true, true);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.test_table", dbStreamId);
    startEngine(configBuilder);

    awaitUntilConnectorIsReady();

    TestHelper.execute("INSERT INTO test_table VALUES (1);");

    TestHelper.execute(
      String.format("UPDATE test_table SET bigint_col = %s WHERE id = 1;", shouldUpdateToNull ? "NULL" : "12345"));

    List<SourceRecord> records = new ArrayList<>();
    waitAndFailIfCannotConsume(records, 2);

    // Assert insert record.
    SourceRecord insertRecord = records.get(0);
    Struct recordVal = (Struct) insertRecord.value();
    assertEquals("c", TestHelper.getOpValue(insertRecord));
    assertEquals(1, recordVal.getStruct("after").getStruct("id").getInt32("value"));
    assertEquals(123, recordVal.getStruct("after").getStruct("bigint_col").getInt64("value"));

    SourceRecord updateRecord = records.get(1);
    Struct updateRecordVal = (Struct) updateRecord.value();
    assertEquals("u", TestHelper.getOpValue(updateRecord));

    if (shouldUpdateToNull) {
      assertNull(updateRecordVal.getStruct("after").getStruct("bigint_col").get("value"));
    } else {
      assertEquals(12345, updateRecordVal.getStruct("after").getStruct("bigint_col").getInt64("value"));
    }

    assertEquals(1, updateRecordVal.getStruct("before").getStruct("id").getInt32("value"));
    assertEquals(123, updateRecordVal.getStruct("before").getStruct("bigint_col").getInt64("value"));
  }

  @Test
  public void shouldHaveBeforeImageWhenUpdatesArePerformedInTransaction() throws Exception {
    TestHelper.execute("CREATE TABLE test_table (id INT PRIMARY KEY, bigint_col bigint);");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "test_table", true /* withBeforeImage */,
      true, BeforeImageMode.ALL, true, true);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.test_table", dbStreamId);
    startEngine(configBuilder);

    awaitUntilConnectorIsReady();

    TestHelper.execute("INSERT INTO test_table VALUES (1);");

    TestHelper.execute("BEGIN; " +
                       "UPDATE test_table SET bigint_col = NULL WHERE id = 1; " +
                       "UPDATE test_table SET bigint_col = 123456 WHERE id = 1; " +
                       "COMMIT;");

    List<SourceRecord> records = new ArrayList<>();
    waitAndFailIfCannotConsume(records, 3);

    // Assert insert record.
    SourceRecord insertRecord = records.get(0);
    Struct recordVal = (Struct) insertRecord.value();
    assertEquals("c", TestHelper.getOpValue(insertRecord));
    assertEquals(1, recordVal.getStruct("after").getStruct("id").getInt32("value"));
    assertNull(recordVal.getStruct("after").getStruct("bigint_col").get("value"));

    SourceRecord updateRecordOne = records.get(1);
    Struct updateRecordOneVal = (Struct) updateRecordOne.value();
    assertEquals("u", TestHelper.getOpValue(updateRecordOne));
    assertNull(updateRecordOneVal.getStruct("after").getStruct("bigint_col").get("value"));

    assertEquals(1, updateRecordOneVal.getStruct("before").getStruct("id").getInt32("value"));
    assertNull(updateRecordOneVal.getStruct("before").getStruct("bigint_col"));

    SourceRecord updateRecordTwo = records.get(2);
    Struct updateRecordTwoVal = (Struct) updateRecordTwo.value();
    assertEquals("u", TestHelper.getOpValue(updateRecordTwo));
    assertEquals(123456, updateRecordTwoVal.getStruct("after").getStruct("bigint_col").getInt64("value"));

    assertEquals(1, updateRecordTwoVal.getStruct("before").getStruct("id").getInt32("value"));
    assertNull(updateRecordTwoVal.getStruct("before").getStruct("bigint_col"));
  }

    @Test
    public void shouldSendNullValueForAddedColumns() throws Exception {
        TestHelper.execute("CREATE TABLE test_table (id INT PRIMARY KEY, v1 INT DEFAULT 1, v2 INT DEFAULT 123);");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "test_table", true /* withBeforeImage */,
                true, BeforeImageMode.ALL, true, true);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.test_table", dbStreamId);
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        // Perform 2 inserts.
        TestHelper.execute("INSERT INTO test_table VALUES (1);");
        TestHelper.execute("INSERT INTO test_table VALUES (2);");

        // Add an integer column and update the previously added rows but keep v3 null.
        TestHelper.execute("ALTER TABLE test_table ADD COLUMN v3 int;");
        TestHelper.execute("UPDATE test_table SET v1 = 2;");

        // Add a money column. Insert 1 row with null values for v3 and v4 and update all the rows keeping v3 and v4 null.
        TestHelper.execute("ALTER TABLE test_table ADD COLUMN v4 MONEY;");
        TestHelper.execute("INSERT INTO test_table VALUES (3, 2, 123, null, null);");
        TestHelper.execute("UPDATE test_table SET v1 = 3;");

        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, 8 /* 2 inserts, 2 updates, 1 insert, 3 updates */);

        // Assert that first two updates have null v3 column values.
        int i = 2;
        for (; i < 4; i++) {
            SourceRecord record = records.get(i);
            Struct recordVal = (Struct) record.value();
            assertEquals("u", TestHelper.getOpValue(record));
            assertEquals(i - 1, recordVal.getStruct("before").getStruct("id").getInt32("value"));
            assertEquals(1, recordVal.getStruct("before").getStruct("v1").getInt32("value"));
            assertEquals(123, recordVal.getStruct("before").getStruct("v2").getInt32("value"));
            assertNull(recordVal.getStruct("before").getStruct("v3").get("value"));

            assertEquals(i - 1, recordVal.getStruct("after").getStruct("id").getInt32("value"));
            assertEquals(2, recordVal.getStruct("after").getStruct("v1").getInt32("value"));
            assertEquals(123, recordVal.getStruct("after").getStruct("v2").getInt32("value"));
            assertNull(recordVal.getStruct("after").getStruct("v3").get("value"));
        }

        // Assert that the insert and the updates performed after adding v4 column have null v3 and v4 column values.
        SourceRecord insertRecord = records.get(i);
        Struct insertVal = (Struct) insertRecord.value();
        assertEquals("c", TestHelper.getOpValue(insertRecord));
        assertEquals(i - 1, insertVal.getStruct("after").getStruct("id").getInt32("value"));
        assertEquals(2, insertVal.getStruct("after").getStruct("v1").getInt32("value"));
        assertEquals(123, insertVal.getStruct("after").getStruct("v2").getInt32("value"));
        assertNull(insertVal.getStruct("after").getStruct("v3").get("value"));
        assertNull(insertVal.getStruct("after").getStruct("v4").get("value"));
        ++i;

        for (; i < 8; i++) {
            SourceRecord record = records.get(i);
            Struct recordVal = (Struct) record.value();
            assertEquals("u", TestHelper.getOpValue(record));
            assertEquals(i - 4, recordVal.getStruct("before").getStruct("id").getInt32("value"));
            assertEquals(2, recordVal.getStruct("before").getStruct("v1").getInt32("value"));
            assertEquals(123, recordVal.getStruct("before").getStruct("v2").getInt32("value"));
            assertNull(recordVal.getStruct("before").getStruct("v3").get("value"));
            assertNull(recordVal.getStruct("before").getStruct("v4").get("value"));

            assertEquals(i - 4, recordVal.getStruct("after").getStruct("id").getInt32("value"));
            assertEquals(3, recordVal.getStruct("after").getStruct("v1").getInt32("value"));
            assertEquals(123, recordVal.getStruct("after").getStruct("v2").getInt32("value"));
            assertNull(recordVal.getStruct("after").getStruct("v3").get("value"));
            assertNull(recordVal.getStruct("after").getStruct("v4").get("value"));
        }
    }

    @Test
    public void shouldSendNullValuesForAllTypesInBeforeImage() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
        Thread.sleep(1000);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "all_types", true /* withBeforeImage */,
                true, BeforeImageMode.ALL, true, true);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.all_types", dbStreamId);
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        // Perform one insert having only pk and all other columns null and delete the same row.
        TestHelper.execute("INSERT INTO all_types (id) VALUES (1)");
        TestHelper.execute("DELETE FROM all_types where id = 1");

        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, 3);

        // Assert that the before image sent for all the columns other than the pk is null.
        SourceRecord deleteRecord = records.get(1);
        Struct recordVal = (Struct) deleteRecord.value();
        assertEquals("d", TestHelper.getOpValue(deleteRecord));
        assertEquals(1, recordVal.getStruct("before").getStruct("id").getInt32("value"));
        assertNull(recordVal.getStruct("before").getStruct("bitcol").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("varbitcol").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("booleanval").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("byteaval").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("ch").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("vchar").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("cidrval").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("dt").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("dp").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("inetval").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("intervalval").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("jsonval").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("jsonbval").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("mc").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("mc8").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("mn").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("nm").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("rl").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("si").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("i4r").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("i8r").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("nr").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("tsr").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("tstzr").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("dr").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("txt").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("tm").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("tmtz").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("ts").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("tstz").get("value"));
        assertNull(recordVal.getStruct("before").getStruct("uuidval").get("value"));
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
