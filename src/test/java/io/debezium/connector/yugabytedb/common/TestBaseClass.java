package io.debezium.connector.yugabytedb.common;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.YugabyteDBConnector;
import io.debezium.connector.yugabytedb.container.YugabyteCustomContainer;
import io.debezium.connector.yugabytedb.rules.YugabyteDBLogTestName;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
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
import org.testcontainers.containers.YugabyteYSQLContainer;

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
    protected final String DEFAULT_DB_NAME = "yugabyte";
    protected final String DEFAULT_COLOCATED_DB_NAME = "colocated_database";
    protected static String yugabytedStartCommand = "";
    protected Map<String, ?> offsetMapForRecords = new HashMap<>();
    protected ExecutorService engineExecutor;
    protected static BlockingArrayQueue<SourceRecord> linesConsumed;

    protected void awaitUntilConnectorIsReady() throws Exception {
        Awaitility.await()
                .pollDelay(Duration.ofSeconds(15))
                .atMost(Duration.ofSeconds(65))
                .until(() -> {
                    return engine.isRunning();
                });
    }

  @BeforeAll
  public static void initializeTestFramework() {
    LoggingContext.forConnector(YugabyteDBConnector.class.getSimpleName(), "", "test");
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
        return yugabytedStartCommand;
  }

  protected long getIntentsCount() throws Exception {
    throw new UnsupportedOperationException("Method getIntentCount is not implemented for " + TestBaseClass.class.toString());
  }

  public void startEngine(Configuration.Builder configBuilder) {
    startEngine(configBuilder, (success, message, error) -> {});
  }

  public void startEngine(Configuration.Builder configBuilder,
                          DebeziumEngine.CompletionCallback callback) {
    configBuilder
      .with(EmbeddedEngine.ENGINE_NAME, "test-connector")
      .with(EmbeddedEngine.OFFSET_STORAGE, MemoryOffsetBackingStore.class.getName())
      .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
      .with(EmbeddedEngine.CONNECTOR_CLASS, YugabyteDBConnector.class);

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

    engine = (EmbeddedEngine) EmbeddedEngine.create()
               .using(configBuilder.build())
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
                 committer.markBatchFinished();
               }).build();

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
   * @param recordsCount total number of records which should be consumed
   * @param milliSecondsToWait duration in milliseconds to wait for while consuming
   */
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
