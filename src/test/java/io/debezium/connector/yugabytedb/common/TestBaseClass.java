package io.debezium.connector.yugabytedb.common;

import io.debezium.connector.yugabytedb.rules.YugabyteDBLogTestName;
import io.debezium.embedded.AbstractConnectorTest;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;

import org.fest.assertions.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.YugabyteYSQLContainer;

import java.time.Duration;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Base class to have common methods and attributes for the containers to be run.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
@ExtendWith(YugabyteDBLogTestName.class)
public class TestBaseClass extends AbstractConnectorTest {
    public Logger LOGGER = LoggerFactory.getLogger(getClass());
    protected static YugabyteYSQLContainer ybContainer;

    protected final String DEFAULT_DB_NAME = "yugabyte";
    protected final String DEFAULT_COLOCATED_DB_NAME = "colocated_database";

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
}
