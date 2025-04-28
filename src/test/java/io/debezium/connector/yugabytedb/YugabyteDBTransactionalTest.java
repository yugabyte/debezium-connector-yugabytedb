package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.yb.CommonTypes;
import org.yb.client.*;

import com.yugabyte.util.PSQLException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests to validate that streaming is not affected nor is there any data loss when there are
 * operations to be streamed in combination with different failure/restart scenarios.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class YugabyteDBTransactionalTest extends YugabytedTestBase {
  @BeforeAll
  public static void beforeClass() throws SQLException {
    setMasterFlags("cdc_wal_retention_time_secs=60");
    setTserverFlags("cdc_max_stream_intent_records=10",
                    "cdc_intent_retention_ms=60000");
    initializeYBContainer();
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
    shutdownYBContainer();
  }

  @Order(1)
  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void shouldResumeLargeTransactions(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    /*
     * The goal behind this test is to make sure that when explicit checkpoints are not being
     * updated, the safe time should also not be moving forward since it will cause data loss
     * by filtering the records.
     *
     * 1. Start connector
     * 2. Stop updating the explicit checkpoint
     * 3. Execute transactions - since the checkpoints are not being updated, nothing will go
     *    to the cdc_state table
     * 4. Restart the connector - once the connector restarts, it should consume all the events
     *    from the starting without any data loss
     */
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    YugabyteDBStreamingChangeEventSource.TEST_TRACK_EXPLICIT_CHECKPOINTS = true;

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress());
    YBTable ybTable = TestHelper.getYbTable(ybClient, "t1");
    assertNotNull(ybTable);

    Set<String> tablets = ybClient.getTabletUUIDs(ybTable);

    String tabletId = tablets.iterator().next();
    assertNotNull(tabletId);
    assertEquals(1, tablets.size());

    // This check will verify that we are getting some explicit checkpoint values, once that is confirmed, stop updating the explicit checkpoints.
    Awaitility.await()
      .atMost(Duration.ofMinutes(1))
      .until(() -> YugabyteDBStreamingChangeEventSource.TEST_explicitCheckpoints.get(tabletId) != null);
    YugabyteDBStreamingChangeEventSource.TEST_UPDATE_EXPLICIT_CHECKPOINT = false;

    final int recordsToBeInserted = 200;
    TestHelper.execute(
      String.format("INSERT INTO t1 VALUES (generate_series(0, %d), 'Vaibhav', 'Kushwaha');",
        99));
    TestHelper.execute(
      String.format("INSERT INTO t1 VALUES (generate_series(%d, %d), 'Vaibhav', 'Kushwaha');",
        100, recordsToBeInserted - 1));

    // Consume all the records.
    List<SourceRecord> recordsBeforeRestart = new ArrayList<>();
    Set<Integer> primaryKeysBeforeRestart = new HashSet<>();
    waitAndConsume(recordsBeforeRestart, recordsToBeInserted, 2 * 60 * 1000);

    // Now that we have some value for explicit checkpoint, we can stop connector.
    stopConnector();

    for (SourceRecord record : recordsBeforeRestart) {
      Struct value = (Struct) record.value();
      primaryKeysBeforeRestart.add(value.getStruct("after").getStruct("id").getInt32("value"));
    }

    // Assert that we have consumed all the records.
    assertEquals(recordsToBeInserted, primaryKeysBeforeRestart.size());

    // Since we have stopped updating the checkpoints, enable it again and consume the records,
    // We should get all the records again. Note that enabling the checkpointing will have no
    // impact on the test result here, it could have been kept disabled as well.
    YugabyteDBStreamingChangeEventSource.TEST_UPDATE_EXPLICIT_CHECKPOINT = true;

    // Consume records.
    List<SourceRecord> recordsAfterRestart = new ArrayList<>();
    Set<Integer> primaryKeysAfterRestart = new HashSet<>();

    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    waitAndConsume(recordsAfterRestart, recordsToBeInserted, 2 * 60 * 1000);

    for (SourceRecord record : recordsAfterRestart) {
      Struct value = (Struct) record.value();
      primaryKeysAfterRestart.add(value.getStruct("after").getStruct("id").getInt32("value"));
    }

    assertEquals(recordsToBeInserted, primaryKeysAfterRestart.size());

    YugabyteDBStreamingChangeEventSource.TEST_TRACK_EXPLICIT_CHECKPOINTS = false;
  }

  @Order(2)
  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void shouldBeNoDataLossWithExplicitCheckpointAdvancing(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    YugabyteDBStreamingChangeEventSource.TEST_TRACK_EXPLICIT_CHECKPOINTS = true;

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    // Start connector.
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // Insert a record.
    TestHelper.execute("INSERT INTO t1 VALUES (generate_series(0,99), 'Vaibhav', 'Kushwaha');");

    // Wait for a few seconds so that the checkpoint can be advanced.
    TestHelper.waitFor(Duration.ofSeconds(3));

    // Stop updating checkpoints in callbacks.
    YugabyteDBStreamingChangeEventSource.TEST_UPDATE_EXPLICIT_CHECKPOINT = false;

    TestHelper.waitFor(Duration.ofSeconds(10));

    // Stop the connector.
    stopConnector();

    // Consume all available records.
    List<SourceRecord> recordsBeforeRestart = new ArrayList<>();
    int consumedRecords = consumeAvailableRecords(recordsBeforeRestart::add);
    assertNoRecordsToConsume();

    LOGGER.info("Total consumed records before restart: {}", recordsBeforeRestart.size());

    // We will update the checkpoints now.
    YugabyteDBStreamingChangeEventSource.TEST_UPDATE_EXPLICIT_CHECKPOINT = true;

    // Restart the connector.
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // We should be consuming only 1 record.
    List<SourceRecord> recordsAfterRestart = new ArrayList<>();
    waitAndConsume(recordsAfterRestart, 100, 3 * 60 * 1000);

    Set<Integer> primaryKeysAfterRestart = new HashSet<>();
    for (SourceRecord record : recordsAfterRestart) {
      Struct value = (Struct) record.value();
      primaryKeysAfterRestart.add(value.getStruct("after").getStruct("id").getInt32("value"));
    }

    assertEquals(100, primaryKeysAfterRestart.size());

    assertNoRecordsToConsume();

    YugabyteDBStreamingChangeEventSource.TEST_TRACK_EXPLICIT_CHECKPOINTS = false;
  }

  @Order(3)
  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void verifyNoHoldingOfResourcesOnServiceAfterSplit(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    /*
     * The goal of this test is to verify that if a tablet split is there then we are not holding
     * up any resources on the children tablets if there are no operations on the children or there
     * are no records for children.
     *
     * Basically, we are verifying that if there is no callback on the children tablets (which
     * would be the case if there are no records) then we do not hold up any resources. This will be
     * verified by reading the GetCheckpointResponse on the children and verifying that the OpId
     * and snapshot_time is equal to the explicit checkpoint for the parent.
     */
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    YugabyteDBStreamingChangeEventSource.TEST_TRACK_EXPLICIT_CHECKPOINTS = true;

    // Start connector.
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // Insert records.
    TestHelper.execute("INSERT INTO t1 VALUES (generate_series(0, 49), 'Vaibhav', 'Kushwaha');");

    YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress());
    YBTable ybTable = TestHelper.getYbTable(ybClient, "t1");
    assertNotNull(ybTable);
    Set<String> tablets = ybClient.getTabletUUIDs(ybTable);
    assertEquals(1, tablets.size());
    String parentTablet = tablets.iterator().next();

    List<SourceRecord> records = new ArrayList<>();
    waitAndFailIfCannotConsume(records, 50);

    // Stop the advancing of checkpoints - this will ensure that we are not advancing the
    // checkpoints from the GetChangesResponse. Additionally, this will also ensure that we are
    // calling GetChanges on children with the explicit checkpoint as the checkpoint we have
    // received from the GetTabletListToPollForCDCResponse.
    YugabyteDBStreamingChangeEventSource.TEST_STOP_ADVANCING_CHECKPOINTS = true;

    // Flush the table and split its tablet.
    ybClient.flushTable(ybTable.getTableId());
    TestHelper.waitFor(Duration.ofSeconds(20));
    ybClient.splitTablet(parentTablet);
    TestHelper.waitForTablets(ybClient, ybTable, 2);

    // Once the split happens, children will be initialised with an explicit checkpoint value
    // returned by GetTabletListToPoll call, and since there will be no further records for the
    // children tablets, the explicit checkpoint will never change from this assigned value, we
    // can now use GetCheckpoint to compare the values.
    List<String> tabletsAfterSplit = new ArrayList<>(ybClient.getTabletUUIDs(ybTable));
    assertEquals(2, tabletsAfterSplit.size());

    CdcSdkCheckpoint parentExplicitCheckpoint = YugabyteDBStreamingChangeEventSource.TEST_explicitCheckpoints.get(parentTablet);
    assertNotNull(parentExplicitCheckpoint);

    GetCheckpointResponse childCheckpoint1 = ybClient.getCheckpoint(ybTable, dbStreamId, tabletsAfterSplit.get(0));
    assertEquals(parentExplicitCheckpoint.getTerm(), childCheckpoint1.getTerm());
    assertEquals(parentExplicitCheckpoint.getIndex(), childCheckpoint1.getIndex());
    assertEquals(parentExplicitCheckpoint.getTime(), childCheckpoint1.getSnapshotTime());

    GetCheckpointResponse childCheckpoint2 = ybClient.getCheckpoint(ybTable, dbStreamId, tabletsAfterSplit.get(1));
    assertEquals(parentExplicitCheckpoint.getTerm(), childCheckpoint2.getTerm());
    assertEquals(parentExplicitCheckpoint.getIndex(), childCheckpoint2.getIndex());
    assertEquals(parentExplicitCheckpoint.getTime(), childCheckpoint2.getSnapshotTime());
  }

  @Order(4)
  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void shouldNotFailWithLowRetentionPeriod(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    // Do not retry as it will only increase the duration of the test run and end up giving a false
    // success result.
    configBuilder.with(YugabyteDBConnectorConfig.MAX_CONNECTOR_RETRIES, 0);

    // Start connector.
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // Execute a transaction.
    TestHelper.execute("INSERT INTO t1 VALUES (generate_series(1,5), 'Vaibhav', 'Kushwaha');");

    LOGGER.info("Creating snapshot schedule on database: {} with a retention of 15 minutes", DEFAULT_DB_NAME);
    YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress());
    CreateSnapshotScheduleResponse snapshotScheduleResponse =
      ybClient.createSnapshotSchedule(CommonTypes.YQLDatabase.YQL_DATABASE_PGSQL, DEFAULT_DB_NAME, 15 * 60, 1 /* interval */);

    // Wait for a minute.
    TestHelper.waitFor(Duration.ofMinutes(1));

    // Restart the connector.
    stopConnector();
    TestHelper.waitFor(Duration.ofSeconds(5));
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    waitAndFailIfCannotConsume(new ArrayList<>(), 5);

    // Keep asserting that the connector is still running.
    for (int i = 0; i < 10; ++i) {
      assertConnectorIsRunning();
    }

    // Delete the snapshot schedule at the end of the test.
    ybClient.deleteSnapshotSchedule(snapshotScheduleResponse.getSnapshotScheduleUUID());
  }

  @Test
  public void shouldNotFailWithIndexCreation() throws Exception {
    final ReentrantLock lock = new ReentrantLock();
    YugabyteDBStreamingChangeEventSource.TEST_TRACK_EXPLICIT_CHECKPOINTS = true;
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    TestHelper.executeInDatabase("CREATE TABLE t1_colocated (id varchar(36) primary key, status varchar(64) not null, "
                                  + "roundid int not null, userid bigint not null, shardid int not null, "
                                  + "createdat timestamp with time zone not null default now(), "
                                  + "updatedat timestamp with time zone not null default now()) WITH (COLOCATION = true);",
                                  DEFAULT_COLOCATED_DB_NAME);

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "t1_colocated", false, false);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1_colocated", dbStreamId)
                                            .with(YugabyteDBConnectorConfig.DATABASE_NAME, DEFAULT_COLOCATED_DB_NAME)
                                            .with(YugabyteDBConnectorConfig.CDC_POLL_INTERVAL_MS, 1000);
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    YBClient ybClient = TestHelper.getYbClient(TestHelper.getMasterAddress());
    YBTable ybTable = TestHelper.getYbTable(ybClient, "t1_colocated");
    assertNotNull(ybTable);

    Set<String> tablets = ybClient.getTabletUUIDs(ybTable);

    String tabletId = tablets.iterator().next();
    assertNotNull(tabletId);
    assertEquals(1, tablets.size());

    final int iterations = 30;
    final int batchSize = 1500;

    // Launch insertion thread here.
    ExecutorService exec = Executors.newFixedThreadPool(3);
    Future<?> future = exec.submit(() -> {
        long idBegin = 1;
        LOGGER.info("Starting the insertion thread");
        try (YugabyteDBConnection pgConn = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME)) {
            Statement st = pgConn.connection().createStatement();

            for (int i = 0; i < iterations; ++i) {
                lock.lock();
                try {
                    LOGGER.info("Inserting records in batch {}", i);
                    st.execute(String.format("insert into t1_colocated values (generate_series(%d,%d), 'SHIPPED', 12, 1234, 2345);",
                                idBegin, idBegin + batchSize - 1));
                }
                finally {
                    lock.unlock();
                }
                // Sleep for 1 second to allow the index creation thread to acquire the lock.
                Thread.sleep(1000);

                idBegin += batchSize;
            }
        }
        catch (Exception ex) {
            LOGGER.error("Exception in the insertion thread: ", ex);
            throw new RuntimeException(ex);
        }
    });

    Future<?> indexCreationFuture = exec.submit(() -> {
      try (YugabyteDBConnection ybConn = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME)) {
          Statement st = ybConn.connection().createStatement();
          for (int i = 0; i < iterations; ++i) {
              final String columnName = "new_column_" + i;
              lock.lock();
              try {
                  // Add a new column to s2.orders of type text and default value equal to its name.
                  st.execute("ALTER TABLE t1_colocated ADD COLUMN IF NOT EXISTS " + columnName + " text DEFAULT '" + columnName + "';");
                  LOGGER.info("Creating index on column {}", columnName);
                  st.execute("CREATE INDEX idx_t1_colocated_" + i + " ON t1_colocated (" + columnName + ");");
              }
              catch (Exception e) {
                  LOGGER.error("Exception in the index creation thread, will retry: ", e);
              }
              finally {
                  lock.unlock();
                  Thread.sleep(2 * 1000);
              }
          }
      }
      catch (Exception ex) {
          LOGGER.error("Exception in the index creation thread: ", ex);
          throw new RuntimeException(ex);
      }
    });

    // Consume all the records.
    List<SourceRecord> consumedRecords = new ArrayList<>();
    Set<Integer> primaryKeys = new HashSet<>();

    int recordCount = 0;
    while (!future.isDone()) {
      // Consume records.
      int local = consumeAvailableRecords(consumedRecords::add);
      if (local > 0) {
        recordCount += local;
        LOGGER.info("Consumed {} records", recordCount);
      }

      Thread.sleep(1000);
    }

    for (SourceRecord record : consumedRecords) {
      Struct value = (Struct) record.value();
      primaryKeys.add(value.getStruct("after").getStruct("id").getInt32("value"));
    }

    // Connect to the database and get the count of records and assert with the primary key size.
    try (YugabyteDBConnection conn = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME)) {
      Statement st = conn.connection().createStatement();
      st.execute("SELECT count(*) FROM t1_colocated;");
      ResultSet rs = st.getResultSet();
      assertTrue(rs.next());
      int count = rs.getInt(1);
      assertEquals(count, primaryKeys.size());
    }

    // assertEquals(recordsToBeInserted, primaryKeys.size());
    indexCreationFuture.cancel(true);
    YugabyteDBStreamingChangeEventSource.TEST_TRACK_EXPLICIT_CHECKPOINTS = false;
  }

  @Test
  public void shouldNotFailWithSchemaPackingNotFound() throws Exception {
    final ReentrantLock lock = new ReentrantLock();
    YugabyteDBStreamingChangeEventSource.TEST_TRACK_EXPLICIT_CHECKPOINTS = true;
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    TestHelper.execute("DROP DATABASE IF EXISTS colocated_database;");
    TestHelper.execute("CREATE DATABASE colocated_database WITH COLOCATED = true;");

    TestHelper.executeInDatabase("CREATE TABLE t1_colocated (id varchar(36) primary key, status varchar(64) not null, "
                                  + "roundid int not null, userid bigint not null, shardid int not null, "
                                  + "createdat timestamp with time zone not null default now(), "
                                  + "updatedat timestamp with time zone not null default now()) WITH (COLOCATION = false) split into 3 tablets;",
                                  DEFAULT_COLOCATED_DB_NAME);

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "t1_colocated", false, false);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1_colocated", dbStreamId)
                                            .with(YugabyteDBConnectorConfig.HOSTNAME, "127.0.0.1:5433,127.0.0.2:5433,127.0.0.3:5433")
                                            .with(YugabyteDBConnectorConfig.MASTER_ADDRESSES, "127.0.0.1:7100,127.0.0.2:7100,127.0.0.3:7100")
                                            .with(YugabyteDBConnectorConfig.DATABASE_NAME, DEFAULT_COLOCATED_DB_NAME)
                                            .with(YugabyteDBConnectorConfig.CDC_POLL_INTERVAL_MS, 1000)
                                            .with(YugabyteDBConnectorConfig.MAX_RPC_RETRY_ATTEMPTS, 100)
                                            .with(YugabyteDBConnectorConfig.RPC_RETRY_SLEEP_TIME, 100);
    LOGGER.info("Starting the connector with stream id: {}", dbStreamId);
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    final int iterations = 200;
    final int batchSize = 1500;

    // Launch insertion thread here.
    ExecutorService exec = Executors.newFixedThreadPool(5);
    Future<?> insertFuture = exec.submit(() -> {
        long idBegin = 1;
        LOGGER.info("Starting the insertion thread");
        YugabyteDBConnection pgConn = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME, "127.0.0.1");
        try {
            Statement st = pgConn.connection().createStatement();

            for (int i = 0; i < iterations; ++i) {
                // lock.lock();
                try {
                    LOGGER.info("Inserting records in batch {}", i);
                    st.execute(String.format("insert into t1_colocated values (generate_series(%d,%d), 'SHIPPED', 12, 1234, 2345);",
                                idBegin, idBegin + batchSize - 1));
                } catch (Exception psqle) {
                  LOGGER.warn("Exception caught, will continue: {}", psqle.getMessage());
                }
                // Sleep for 1 second to allow the index creation thread to acquire the lock.
                Thread.sleep(1000);

                idBegin += batchSize;
            }
        }
        catch (Exception ex) {
            LOGGER.error("Exception in the insertion thread: ", ex);
            throw new RuntimeException(ex);
        }
    });

    Future<?> reverseInsertFuture = exec.submit(() -> {
      long idBegin = -1;
      LOGGER.info("Starting the reverse insertion thread");
      YugabyteDBConnection pgConn = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME,  "127.0.0.2");
      try {
          Statement st = pgConn.connection().createStatement();

          for (int i = 0; i < iterations; ++i) {
              // lock.lock();
              try {
                  LOGGER.info("Inserting reverse records in batch {}", i);
                  st.execute(String.format("insert into t1_colocated values (generate_series(%d,%d), 'SHIPPED', 12, 1234, 2345);",
                             idBegin - batchSize + 1, idBegin));
              } catch (Exception psqle) {
                LOGGER.warn("Exception caught, will continue: {}", psqle.getMessage());
              }
              // Sleep for 1 second to allow the index creation thread to acquire the lock.
              Thread.sleep(1000);

              idBegin -= batchSize;
          }
      }
      catch (Exception ex) {
          LOGGER.error("Exception in the reverse insertion thread: ", ex);
          throw new RuntimeException(ex);
      }
  });

  /*
  * 1. BEGIN + perform inserts - schema 0 (no apply in WAL yet)
  * 2. Second session, create index - schema 1 (incremented after index creation)
  * 3. Third session, BEGIN + perform inserts + COMMIT (commit will be on the new schema version)
  * 4. COMMIT the first session - check if this does not fail 
  */

  // Future<?> thirdInsertFuture = exec.submit(() -> {
  //   long idBegin = iterations * batchSize + 100;
  //   LOGGER.info("Starting the reverse insertion thread");
  //   YugabyteDBConnection pgConn = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME,  "127.0.0.3");
  //   try {
  //       Statement st = pgConn.connection().createStatement();

  //       for (int i = 0; i < iterations; ++i) {
  //           // lock.lock();
  //           try {
  //               LOGGER.info("Inserting thirdPart records in batch {}", i);
  //               st.execute(String.format("insert into t1_colocated values (generate_series(%d,%d), 'SHIPPED', 12, 1234, 2345);",
  //                          idBegin, idBegin + batchSize - 1));
  //           } catch (Exception psqle) {
  //             LOGGER.warn("Exception caught, will continue: {}", psqle.getMessage());
  //           }
  //           // Sleep for 1 second to allow the index creation thread to acquire the lock.
  //           Thread.sleep(1000);

  //           idBegin += batchSize;
  //       }
  //   }
  //   catch (Exception ex) {
  //       LOGGER.error("Exception in the reverse insertion thread: ", ex);
  //       throw new RuntimeException(ex);
  //   }
  // });

    Future<?> indexCreationFuture = exec.submit(() -> {
      YugabyteDBConnection pgConn = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME,  "127.0.0.3");
      try {
          Statement st = pgConn.connection().createStatement();
          for (int i = 0; i < iterations; ++i) {
              final String columnName = "new_column_" + i;
              // lock.lock();
              try {
                  // Add a new column to s2.orders of type text and default value equal to its name.
                  st.execute("ALTER TABLE t1_colocated ADD COLUMN IF NOT EXISTS " + columnName + " text DEFAULT '" + columnName + "';");
                  LOGGER.info("Creating index on column {}", columnName);
                  st.execute("CREATE INDEX idx_t1_colocated_" + i + " ON t1_colocated (" + columnName + ");");
              }
              catch (Exception e) {
                  LOGGER.error("Exception in the index creation thread, will continue: {}", e.getMessage());
              }
              finally {
                  // lock.unlock();
                  Thread.sleep(2 * 1000);
              }
          }
      }
      catch (Exception ex) {
          LOGGER.error("Exception in the index creation thread: ", ex);
          throw new RuntimeException(ex);
      }
    });

    // Future<?> mvCreationFuture = exec.submit(() -> {
    //   YugabyteDBConnection pgConn = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME, "127.0.0.3");
    //   try {
    //       LOGGER.info("Waiting for 10s before starting mv creation");
    //       TestHelper.waitFor(Duration.ofSeconds(10));
    //       Statement st = pgConn.connection().createStatement();
    //       for (int i = 0; i < iterations; ++i) {
    //           // final String columnName = "new_column_" + i;
    //           try {
    //               st.execute("CREATE MATERIALIZED VIEW IF NOT EXISTS mv_t1_colocated_" + i + " AS SELECT * FROM t1_colocated;");
    //           } catch (Exception e) {
    //               LOGGER.error("Exception in the MV creation thread, will continue: {}", e.getMessage());
    //           }
    //           finally {
    //               Thread.sleep(3 * 1000);
    //           }
    //       }
    //   }
    //   catch (Exception ex) {
    //       LOGGER.error("Exception in the MV creation thread: ", ex);
    //       throw new RuntimeException(ex);
    //   }
    // });

    // Future<?> tserverRestartFuture = exec.submit(() -> {

    //   for (int i = 0; i < iterations; ++i) {
    //     try {
    //       LOGGER.info("Restarting node {}", (i % 3) + 1);
    //       // Restart the tserver node.
    //       Process proc = Runtime.getRuntime().exec("/home/ec2-user/code/vaibhav/yugabyte-db/bin/yb-ctl restart_node " + (i % 3) + 1);
    //       proc.waitFor();
    //       TestHelper.waitFor(Duration.ofSeconds(10));
    //     }
    //     catch (Exception e) {
    //       LOGGER.error("Exception in the tserver restart thread, will continue: {}", e.getMessage());
    //     }
    //   }
    // });

    // Consume all the records.
    List<SourceRecord> consumedRecords = new ArrayList<>();
    Set<String> primaryKeys = new HashSet<>();

    int recordCount = 0;
    while (!insertFuture.isDone() || !reverseInsertFuture.isDone() || areThereRecordsToConsume()) {
      // Consume records.
      int local = consumeAvailableRecords(consumedRecords::add);
      if (local > 0) {
        recordCount += local;
        LOGGER.info("Consumed {} records", recordCount);
      }

      Thread.sleep(1000);
    }

    for (int i = 0; i < 10; ++i) {
      // Check for 10 iterations that there are no records to consume.
      assertNoRecordsToConsume();
      TestHelper.waitFor(Duration.ofSeconds(1));
    }

    for (SourceRecord record : consumedRecords) {
      Struct value = (Struct) record.value();
      primaryKeys.add(value.getStruct("after").getStruct("id").getString("value"));
    }

    // Connect to the database and get the count of records and assert with the primary key size.
    try (YugabyteDBConnection conn = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME)) {
      Statement st = conn.connection().createStatement();
      st.execute("SELECT count(*) FROM t1_colocated;");
      ResultSet rs = st.getResultSet();
      assertTrue(rs.next());
      int count = rs.getInt(1);
      assertEquals(count, primaryKeys.size());
    }

    // assertEquals(recordsToBeInserted, primaryKeys.size());
    indexCreationFuture.cancel(true);
    // mvCreationFuture.cancel(true);
  }

  @Test
  public void reproError() throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");

    TestHelper.execute("DROP DATABASE IF EXISTS colocated_database;");
    TestHelper.execute("CREATE DATABASE colocated_database WITH COLOCATED = true;");

    TestHelper.executeInDatabase("CREATE TABLE t1_colocated (id varchar(36) primary key, status varchar(64) not null, "
                                  + "roundid int not null, userid bigint not null, shardid int not null, "
                                  + "createdat timestamp with time zone not null default now(), "
                                  + "updatedat timestamp with time zone not null default now()) WITH (COLOCATION = false) split into 3 tablets;",
                                  DEFAULT_COLOCATED_DB_NAME);

    String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "t1_colocated", false, false);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1_colocated", dbStreamId)
                                            .with(YugabyteDBConnectorConfig.HOSTNAME, "127.0.0.1:5433,127.0.0.2:5433,127.0.0.3:5433")
                                            .with(YugabyteDBConnectorConfig.MASTER_ADDRESSES, "127.0.0.1:7100,127.0.0.2:7100,127.0.0.3:7100")
                                            .with(YugabyteDBConnectorConfig.DATABASE_NAME, DEFAULT_COLOCATED_DB_NAME)
                                            .with(YugabyteDBConnectorConfig.CDC_POLL_INTERVAL_MS, 1000)
                                            .with(YugabyteDBConnectorConfig.MAX_RPC_RETRY_ATTEMPTS, 100)
                                            .with(YugabyteDBConnectorConfig.RPC_RETRY_SLEEP_TIME, 100);
    LOGGER.info("Starting the connector with stream id: {}", dbStreamId);
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    // Begin a transaction and insert but do not commit.
    Connection conn1 = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME, "127.0.0.1").connection();
    Connection conn2 = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME, "127.0.0.2").connection();
    Connection conn3 = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME, "127.0.0.3").connection();
    
    conn1.setAutoCommit(false);
    Statement st1 = conn1.createStatement();
    st1.execute("BEGIN;");
    st1.execute("INSERT INTO t1_colocated VALUES ('1', 'SHIPPED', 12, 1234, 2345);");

    Statement st2 = conn2.createStatement();
    Statement st3 = conn3.createStatement();

    // st2.execute("ALTER TABLE t1_colocated ADD COLUMN IF NOT EXISTS new_column text DEFAULT 'new_column';");

    ExecutorService exec = Executors.newFixedThreadPool(1);
    Future<?> insertFuture = exec.submit(() -> {
      try {
        st2.execute("CREATE INDEX idx_t1_colocated_status ON t1_colocated (new_column);");
      } catch (SQLException e) {
        e.printStackTrace();
      }
    });
    
    // Using st3, begin a transaction and insert a record and commit it.
    st3.execute("BEGIN; INSERT INTO t1_colocated VALUES ('2', 'SHIPPED', 12, 1234, 2345); COMMIT;");

    // Immediately commit st1.
    st1.execute("COMMIT;");

    // Consume all the records.
    List<SourceRecord> consumedRecords = new ArrayList<>();
    Set<String> primaryKeys = new HashSet<>();

    int recordCount = 0;
    int noRecordIterations = 0;
    while (areThereRecordsToConsume() || noRecordIterations < 10) {
      // Consume records.
      int local = consumeAvailableRecords(consumedRecords::add);
      if (local > 0) {
        recordCount += local;
        LOGGER.info("Consumed {} records", recordCount);
        noRecordIterations = 0;
      } else {
        noRecordIterations++;
      }

      Thread.sleep(1000);
    }

    for (SourceRecord record : consumedRecords) {
      Struct value = (Struct) record.value();
      primaryKeys.add(value.getStruct("after").getStruct("id").getString("value"));
    }

    // Connect to the database and get the count of records and assert with the primary key size.
    try (YugabyteDBConnection conn = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME)) {
      Statement st = conn.connection().createStatement();
      st.execute("SELECT count(*) FROM t1_colocated;");
      ResultSet rs = st.getResultSet();
      assertTrue(rs.next());
      int count = rs.getInt(1);
      assertEquals(count, primaryKeys.size());
    }
  }
}
