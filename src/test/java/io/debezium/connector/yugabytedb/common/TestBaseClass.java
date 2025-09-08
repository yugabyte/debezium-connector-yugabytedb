package io.debezium.connector.yugabytedb.common;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.TestHelper;
import io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.connector.yugabytedb.container.YugabyteCustomContainer;
import io.debezium.connector.yugabytedb.rules.YugabyteDBLogTestName;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.TestingDebeziumEngine;
import io.debezium.embedded.TestingEmbeddedEngine;
import io.debezium.embedded.async.AsyncEmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.Builder;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.util.LoggingContext;
import io.debezium.util.Testing;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Base class to have common methods and attributes for the containers to be run.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
@ExtendWith(YugabyteDBLogTestName.class)
public class TestBaseClass extends AbstractConnectorTest {
    public Logger LOGGER = LoggerFactory.getLogger(getClass());
    protected static YugabyteCustomContainer ybContainer;
    protected CountDownLatch countDownLatch;
    protected static final String DEFAULT_DB_NAME = "yugabyte";
    protected static final String DEFAULT_COLOCATED_DB_NAME = "colocated_database";
    protected static String containerIpAddress = "127.0.0.1";
    protected Map<String, ?> offsetMapForRecords = new HashMap<>();
    protected ExecutorService engineExecutor;
    protected static BlockingArrayQueue<SourceRecord> linesConsumed;
    protected long callbackDelay = 0;
    protected static List<String> masterFlags =
      new ArrayList<>(
        List.of(
          "enable_tablet_split_of_cdcsdk_streamed_tables=true"
        ));

    // Set the GFLAG: "cdc_state_checkpoint_update_interval_ms" to 0 in all tests, forcing every
    // instance of explicit_checkpoint to be added to the 'cdc_state' table in the service.
    protected static List<String> tserverFlags =
      new ArrayList<>(
        List.of(
          "cdc_state_checkpoint_update_interval_ms=0"
        ));
    protected static String yugabytedLocation = "/home/yugabyte/bin/yugabyted";

    protected void awaitUntilConnectorIsReady() throws Exception {
        Awaitility.await()
                .pollDelay(Duration.ofSeconds(15))
                .atMost(Duration.ofSeconds(65))
                .until(() -> {
                    return isEngineRunning.get();
                });
    }

  @BeforeAll
  public static void initializeTestFramework() {
    LoggingContext.forConnector(YugabyteDBgRPCConnector.class.getSimpleName(), "", "test");
    linesConsumed = new BlockingArrayQueue<>();
  }

  @Override
  protected String assertBeginTransaction(SourceRecord record) {
    final Struct begin = (Struct) record.value();
    final Struct beginKey = (Struct) record.key();

    assertEquals("BEGIN", begin.getString("status"));
    assertNull(begin.getInt64("event_count"));

    final String txId = begin.getString("id");
    assertEquals(txId, beginKey.getString("id"));

    assertNotNull(begin.getString("partition_id"));

    return txId;
  }

  protected static void setMasterFlags(String...flags) {
    masterFlags.addAll(Arrays.asList(flags));
  }

  protected static String getMasterFlags() {
    return String.join(",", masterFlags);
  }

  protected static void setTserverFlags(String...flags) {
    tserverFlags.addAll(Arrays.asList(flags));
  }

  protected static String getTserverFlags() {
    return String.join(",", tserverFlags);
  }

  protected void setCommitCallbackDelay(long milliseconds) {
    LOGGER.info("Setting commit callback delay to {} milliseconds", milliseconds);
    this.callbackDelay = milliseconds;
  }

  protected void resetCommitCallbackDelay() {
    this.callbackDelay = 0;
  }

  /**
   * Assert that the passed {@link SourceRecord} is a record for END transaction
   * @param record the record to assert
   * @param expectedTxId expected transaction ID this record should have
   * @param expectedEventCount expected event count in the transaction
   * @param partitionId the partition to which the record belongs, pass null if this assertion needs
   *                    to be skipped
   */
  protected void assertEndTransaction(SourceRecord record, String expectedTxId, long expectedEventCount, String partitionId) {
    final Struct end = (Struct) record.value();
    final Struct endKey = (Struct) record.key();

    assertEquals("END", end.getString("status"));
    assertEquals(expectedTxId, end.getString("id"));
    assertEquals(expectedEventCount, end.getInt64("event_count"));

    if (partitionId != null) {
      assertNotNull(end.getString("partition_id"));
    }

    assertEquals(expectedTxId, endKey.getString("id"));
  }

  protected void stopYugabyteDB() throws Exception {
      throw new UnsupportedOperationException("Method stopYugabyteDB not implemented for base test class");
  }

  protected void startYugabyteDB() throws Exception {
      throw new UnsupportedOperationException("Method startYugabyteDB not implemented for base test class");
  }

  protected void restartYugabyteDB(long millisecondsToWait) throws Exception {
      throw new UnsupportedOperationException("Method restartYugabyteDB not implemented for base test class");
  }

  protected static String getYugabytedStartCommand() {
    String finalTserverFlags = "--tserver_flags=" + getTserverFlags();
    String finalMasterFlags = "--master_flags=" + getMasterFlags();

    return String.format("%s start --advertise_address=%s %s %s --daemon=true",
        yugabytedLocation, containerIpAddress, finalMasterFlags, finalTserverFlags);
  }

  protected long getIntentsCount() throws Exception {
    throw new UnsupportedOperationException("Method getIntentCount is not implemented for " + TestBaseClass.class.toString());
  }

  public void startEngine(Configuration.Builder configBuilder) {
    startEngine(configBuilder, (success, message, error) -> {});
  }

  public void startEngineWithPartialConsumptionOfLastBatch(Configuration.Builder configBuilder, int snapshotRecords) {
    startEngineWithPartialConsumptionOfLastBatch(configBuilder, snapshotRecords, (success, message, error) -> {});
  }

  @Override
  protected Builder<SourceRecord> createEngineBuilder() {
    return new EmbeddedEngine.EngineBuilder();
  }

  @Override
  protected TestingDebeziumEngine<SourceRecord> createEngine(Builder<SourceRecord> builder) {
      return new TestingEmbeddedEngine((EmbeddedEngine)builder.build());
  }

  public void startEngine(Configuration.Builder configBuilder,
                          DebeziumEngine.CompletionCallback callback) {
    configBuilder
      .with(AsyncEmbeddedEngine.ENGINE_NAME, "test-connector")
      .with(AsyncEmbeddedEngine.OFFSET_STORAGE, MemoryOffsetBackingStore.class.getName())
      .with(AsyncEmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
      .with(AsyncEmbeddedEngine.CONNECTOR_CLASS, YugabyteDBgRPCConnector.class);

    countDownLatch = new CountDownLatch(1);
    DebeziumEngine.CompletionCallback wrapperCallback = (success, msg, error) -> {
      try {
        if (callback != null) {
          callback.handle(success, msg, error);
        }
      }
      finally {
        if (!success) {
          // we only unblock if there was an error; in all other cases we're unblocking when a task has been started
          countDownLatch.countDown();
          countDownLatch.await(10, TimeUnit.SECONDS);
        }
      }
      Testing.debug("Stopped connector");
    };

    DebeziumEngine.ConnectorCallback connectorCallback = new DebeziumEngine.ConnectorCallback() {
      @Override
      public void taskStarted() {
        // if this is called, it means a task has been started successfully so we can continue
        countDownLatch.countDown();
      }

      @Override
      public void connectorStarted() {
          isEngineRunning.compareAndExchange(false, true);
      }

      @Override
      public void connectorStopped() {
          isEngineRunning.compareAndExchange(true, false);
      }
    };

    DebeziumEngine.Builder<SourceRecord> builder = createEngineBuilder();
    builder
      .using(configBuilder.build().asProperties())
      .using(OffsetCommitPolicy.always())
      .using(wrapperCallback)
      .using(connectorCallback)
      .using(this.getClass().getClassLoader())
      .notifying((records, committer) -> {
        for (SourceRecord record: records) {
          linesConsumed.add(record);
          committer.markProcessed(record);

          offsetMapForRecords = record.sourceOffset();
        }

        // This method here is responsible for calling the commit() method which later
        // invokes commitOffset() in the change event source classes.
        TestHelper.waitFor(Duration.ofMillis(callbackDelay));
        committer.markBatchFinished();
      }).build();

    engine = this.createEngine(builder);

    engineExecutor = Executors.newFixedThreadPool(1);
    engineExecutor.submit(() -> {
      LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
      engine.run();
    });
  }

  /**
   * Returns the available records in the queue linesConsumed
   * @return the available (non-consumed) records in the queue linesConsumed
   */
  public int getNonConsumedRecordCount() {
    return linesConsumed.size();
  }

  /**
   * Start an embeddedEngine but only consume records passed in totalRecordsToConsume.
   * @param configBuilder configurations for the engine
   * @param totalRecordsToConsume the total number of records to be added to linesConsumed i.e. the Kafka queue
   * @param callback completion callback
   */
  public void startEngineWithPartialConsumptionOfLastBatch(Configuration.Builder configBuilder, int totalRecordsToConsume, DebeziumEngine.CompletionCallback callback) {
    configBuilder
      .with(AsyncEmbeddedEngine.ENGINE_NAME, "test-connector")
      .with(AsyncEmbeddedEngine.OFFSET_STORAGE, MemoryOffsetBackingStore.class.getName())
      .with(AsyncEmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
      .with(AsyncEmbeddedEngine.CONNECTOR_CLASS, YugabyteDBgRPCConnector.class);

    countDownLatch = new CountDownLatch(1);
    DebeziumEngine.CompletionCallback wrapperCallback = (success, msg, error) -> {
      try {
        if (callback != null) {
          callback.handle(success, msg, error);
        }
      }
      finally {
        if (!success) {
          // we only unblock if there was an error; in all other cases we're unblocking when a task has been started
          countDownLatch.countDown();
        }
      }
      Testing.debug("Stopped connector");
    };

    DebeziumEngine.ConnectorCallback connectorCallback = new DebeziumEngine.ConnectorCallback() {
      @Override
      public void taskStarted() {
        // if this is called, it means a task has been started successfully so we can continue
        countDownLatch.countDown();
      }
    };

    DebeziumEngine.Builder<SourceRecord> builder = createEngineBuilder();
    builder
               .using(configBuilder.build().asProperties())
               .using(OffsetCommitPolicy.always())
               .using(wrapperCallback)
               .using(connectorCallback)
               .using(this.getClass().getClassLoader())
               .notifying((records, committer) -> {
                 for (SourceRecord record: records) {
                  // Partially consume the last batch
                  if (linesConsumed.size() >= totalRecordsToConsume) {
                    break;
                  }
                  linesConsumed.add(record);  
                  committer.markProcessed(record);
                  offsetMapForRecords = record.sourceOffset(); 
                 }

                 // This method here is responsible for calling the commit() method which later
                 // invokes commitOffset() in the change event source classes.
                 TestHelper.waitFor(Duration.ofMillis(callbackDelay));
                  committer.markBatchFinished();
               });
    engine = this.createEngine(builder);

    engineExecutor = Executors.newFixedThreadPool(1);
    engineExecutor.submit(() -> {
      LoggingContext.forConnector(getClass().getSimpleName(), "", "engine");
      engine.run();
    });
  }

  protected int consumeAvailableRecords(Consumer<SourceRecord> recordConsumer) {
    List<SourceRecord> records = new ArrayList<>();
    linesConsumed.drainTo(records);
    
    if (recordConsumer != null) {
        records.forEach(recordConsumer);
    }

    return records.size();
  }

  protected SourceRecords consumeByTopic(int numRecords) throws InterruptedException {
    SourceRecords records = new SourceRecords();
    int recordsConsumed = 0;
    while (recordsConsumed < numRecords) {
      if (!linesConsumed.isEmpty()) {
        records.add(linesConsumed.poll());
        ++recordsConsumed;
      }
    }

    return records;
  }

  protected OpId getExplicitCheckpointForTablet(String partitionId) {
    for (Map.Entry<String, ?> entry : offsetMapForRecords.entrySet()) {
      if (entry.getKey().equals(partitionId)) {
        return OpId.valueOf((String) entry.getValue());
      }
    }

    throw new RuntimeException("No checkpoint found for " + partitionId + " in offset map");
  }

  @Override
  protected void assertNoRecordsToConsume() {
    assertTrue(linesConsumed.isEmpty());
  }

  @Override
  protected boolean waitForAvailableRecords(long timeout, TimeUnit unit) {
    assertTrue(timeout >= 0);
    long now = System.currentTimeMillis();
    long stop = now + unit.toMillis(timeout);
    while (System.currentTimeMillis() < stop) {
        if (!linesConsumed.isEmpty()) {
            break;
        }
    }
    return !linesConsumed.isEmpty();
  }

  /**
   * Consume the records available and add them to a list for further assertion purposes.
   * @param records list to which we need to add the records we consume, pass a
   * {@code new ArrayList<>()} if you do not need assertions on the consumed values
   * @param recordsCount Max number of records that can be consumed if available
   * @param milliSecondsToWait duration in milliseconds to wait for while consuming
   */
  protected void waitAndConsume(List<SourceRecord> records, long recordsCount,
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
                      LOGGER.info("Consumed " + totalConsumedRecords + " records");
                  }

                  return totalConsumedRecords.get() >= recordsCount;
              });
      } catch (ConditionTimeoutException exception) {
        LOGGER.error("Consumed only {} in {} milli-seconds", totalConsumedRecords.get(), milliSecondsToWait);

        // Don't fail and throw exception.
        throw exception;
      }
  }

  /**
   * Consume the records available and add only the unique records to the list. Note that this
   * method expects the primary key name to be {@code id} of type {@link Integer}
   * @param records list to which we need to add the records we consume, pass a
   * {@code new ArrayList<>()} if you do not need assertions on the consumed values
   * @param recordsCount Max number of records that can be consumed if available
   * @param milliSecondsToWait duration in milliseconds to wait for while consuming
   */
  protected void waitAndConsumeUnique(List<SourceRecord> records, long recordsCount,
                                      long milliSecondsToWait) {
    AtomicLong totalConsumedRecords = new AtomicLong();
    long seconds = milliSecondsToWait / 1000;
    Set<Integer> primaryKeys = new HashSet<>();
    try {
      Awaitility.await()
        .atMost(Duration.ofSeconds(seconds))
        .until(() -> {
          int consumed = consumeAvailableRecords(record -> {
            LOGGER.debug("The record being consumed is " + record);
            Struct value = (Struct) record.value();
            int pkValue = value.getStruct("after").getStruct("id").getInt32("value");
            if (!primaryKeys.contains(pkValue)) {
              records.add(record);
              primaryKeys.add(pkValue);
            }
          });
          if (consumed > 0) {
            totalConsumedRecords.addAndGet(consumed);
            LOGGER.info("Consumed " + totalConsumedRecords + " records");
          }

          return primaryKeys.size() >= recordsCount;
        });
    } catch (ConditionTimeoutException exception) {
      LOGGER.error("Consumed only {} in {} milli-seconds", totalConsumedRecords.get(), milliSecondsToWait);

      // Don't fail and throw exception.
      throw exception;
    }
  }

  protected void waitAndFailIfCannotConsume(List<SourceRecord> records, long recordsCount,
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
                      LOGGER.info("Consumed " + totalConsumedRecords + " records");
                  }

                  return totalConsumedRecords.get() >= recordsCount;
              });
      } catch (ConditionTimeoutException exception) {
          fail("Failed to consume " + recordsCount + " in " + seconds + " seconds, consumed only " + totalConsumedRecords.get(), exception);
      }

      assertEquals(recordsCount, totalConsumedRecords.get());
  }

  protected void waitAndFailIfCannotConsume(List<SourceRecord> records, long recordsCount) {
    waitAndFailIfCannotConsume(records, recordsCount, 300 * 1000 /* 5 minutes */);
  }

  protected void insertBulkRecordsInColocatedDB(int numRecords, String fullTableName) {
    String formatInsertString = "INSERT INTO " + fullTableName + " VALUES (%d);";
    TestHelper.executeBulk(formatInsertString, numRecords, DEFAULT_COLOCATED_DB_NAME);
  }

  /**
   * Helper function to create the required tables in the database DEFAULT_COLOCATED_DB_NAME
   */
  protected void createTablesInColocatedDB(boolean colocation) {
    LOGGER.info("Creating tables with colocation: {}", colocation);
    final String createTest1 = String.format("CREATE TABLE test_1 (id INT PRIMARY KEY," +
                                              "name TEXT DEFAULT 'Vaibhav Kushwaha') " +
                                              "WITH (COLOCATION = %b);", colocation);
    final String createTest2 = String.format("CREATE TABLE test_2 (text_key TEXT PRIMARY " +
                                              "KEY) WITH (COLOCATION = %b);", colocation);
    final String createTest3 =
      String.format("CREATE TABLE test_3 (hours FLOAT PRIMARY KEY, " +
                    "hours_in_text VARCHAR(40) DEFAULT 'some_default_hour_value') " +
                    "WITH (COLOCATION = %b);", colocation);
    final String createTestNoColocated = "CREATE TABLE test_no_colocated (id INT PRIMARY KEY," +
                                          "name TEXT DEFAULT 'name_for_non_colocated') " +
                                          "WITH (COLOCATION = false) SPLIT INTO 3 TABLETS;";

    TestHelper.executeInDatabase(createTest1, DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeInDatabase(createTest2, DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeInDatabase(createTest3, DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeInDatabase(createTestNoColocated, DEFAULT_COLOCATED_DB_NAME);
  }

  /**
   * Helper function to drop all the tables being created as a part of this test.
   */
  protected void dropAllTablesInColocatedDB() {
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_1;", DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_2;", DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_3;", DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_no_colocated;", DEFAULT_COLOCATED_DB_NAME);
    TestHelper.executeInDatabase("DROP TABLE IF EXISTS all_types;", DEFAULT_COLOCATED_DB_NAME);
  }

  protected void insertBulkRecordsInRangeInColocatedDB(int beginKey, int endKey, String fullTableName) {
    String formatInsertString = "INSERT INTO " + fullTableName + " VALUES (%d);";
    TestHelper.executeBulkWithRange(formatInsertString, beginKey, endKey, DEFAULT_COLOCATED_DB_NAME);
  }

  protected class SourceRecords {
    private final List<SourceRecord> records = new ArrayList<>();
    private final Map<String, List<SourceRecord>> recordsByTopic = new HashMap<>();

    public void add(SourceRecord record) {
      records.add(record);
      recordsByTopic.computeIfAbsent(record.topic(), (topicName) -> new ArrayList<SourceRecord>()).add(record);
    }

    /**
     * Get the records on the given topic.
     *
     * @param topicName the name of the topic.
     * @return the records for the topic; possibly null if there were no records produced on the topic
     */
    public List<SourceRecord> recordsForTopic(String topicName) {
      return recordsByTopic.get(topicName);
    }

    /**
     * Get the set of topics for which records were received.
     *
     * @return the names of the topics; never null
     */
    public Set<String> topics() {
      return recordsByTopic.keySet();
    }

    public void forEachInTopic(String topic, Consumer<SourceRecord> consumer) {
      recordsForTopic(topic).forEach(consumer);
    }

    public void forEach(Consumer<SourceRecord> consumer) {
      records.forEach(consumer);
    }

    public List<SourceRecord> allRecordsInOrder() {
      return Collections.unmodifiableList(records);
    }
  }
}
