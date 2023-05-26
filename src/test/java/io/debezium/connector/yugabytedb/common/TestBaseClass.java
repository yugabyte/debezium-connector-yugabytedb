package io.debezium.connector.yugabytedb.common;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.container.YugabyteCustomContainer;
import io.debezium.connector.yugabytedb.rules.YugabyteDBLogTestName;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.util.LoggingContext;
import io.debezium.util.Testing;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;

import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.YugabyteYSQLContainer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    protected void awaitUntilConnectorIsReady() throws Exception {
        Awaitility.await()
                .pollDelay(Duration.ofSeconds(5))
                .atMost(Duration.ofSeconds(10))
                .until(() -> {
                    return engine.isRunning();
                });
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

  public void startEngine(Class<? extends SourceConnector> connectorClass, Configuration configuration) {
    startEngine(connectorClass, configuration, (success, message, error) -> {});
  }

  public void startEngine(Class<? extends SourceConnector> connectorClass,
                          Configuration configuration,
                          DebeziumEngine.CompletionCallback callback) {
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
               .using(OffsetCommitPolicy.always())
               .using(wrapperCallback)
               .using(connectorCallback)
               .using(this.getClass().getClassLoader())
               .notifying((records, committer) -> {
                 for (SourceRecord record: records) {
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
}
