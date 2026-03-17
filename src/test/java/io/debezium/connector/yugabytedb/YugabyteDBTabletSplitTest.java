package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.yb.client.GetTabletListToPollForCDCResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

import org.yb.cdc.CdcService.TabletCheckpointPair;

/**
 * Unit tests to verify that the connector gracefully handles the tablet splitting on the server.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBTabletSplitTest extends YugabytedTestBase {

  private static String masterAddresses;

  @BeforeAll
  public static void beforeClass() throws SQLException {
      initializeYBContainer();
      masterAddresses = getMasterAddress();

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
  public static void afterClass() {
      shutdownYBContainer();
  }

  @Order(1)
  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void shouldConsumeDataAfterTabletSplit(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 1 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    startEngine(configBuilder, (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    int recordsCount = 50;

    String insertFormat = "INSERT INTO t1 VALUES (%d, 'value for split table');";

    for (int i = 0; i < recordsCount; ++i) {
      TestHelper.execute(String.format(insertFormat, i));
    }

    YBClient ybClient = TestHelper.getYbClient(masterAddresses);
    YBTable table = TestHelper.getYbTable(ybClient, "t1");
    
    // Verify that there is just a single tablet.
    Set<String> tablets = ybClient.getTabletUUIDs(table);
    int tabletCountBeforeSplit = tablets.size();
    assertEquals(1, tabletCountBeforeSplit);

    // Also verify that the new API to get the tablets is returning the correct tablets.
    GetTabletListToPollForCDCResponse getTabletsResponse =
      ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());
    assertEquals(tabletCountBeforeSplit, getTabletsResponse.getTabletCheckpointPairListSize());

    // Compact the table to ready it for splitting.
    ybClient.flushTable(table.getTableId());

    // Wait for 20s for the table to be flushed.
    TestHelper.waitFor(Duration.ofSeconds(20));

    // Split the tablet. There is just one tablet so it is safe to assume that the iterator will
    // return just the desired tablet.
    ybClient.splitTablet(tablets.iterator().next());

    // Wait till there are 2 tablets for the table.
    TestHelper.waitForTablets(ybClient, table, 2);

    // Insert more records
    for (int i = recordsCount; i < 100; ++i) {
      TestHelper.execute(String.format(insertFormat, i));
    }

    // Consume the records now - there will be 100 records in total.
    SourceRecords records = consumeByTopic(100);
    
    // Verify that the records are there in the topic.
    assertEquals(100, records.recordsForTopic("test_server.public.t1").size());

    // Also call the CDC API to fetch tablets to verify the new tablets have been added in the
    // cdc_state table.
    GetTabletListToPollForCDCResponse getTabletResponse2 =
      ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());

    assertEquals(2, getTabletResponse2.getTabletCheckpointPairListSize());
  }

  @Order(2)
  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void reproduceSplitWhileTransactionIsNotFinishedWithAutomaticSplitting(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    // Stop, if ybContainer already running.
    if (ybContainer.isRunning()) {
      ybContainer.stop();
      // Wait till the container stops
      Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> {
        return !ybContainer.isRunning();
      });
    }

    // Get ybContainer with required master and tserver flags.
    setMasterFlags("enable_automatic_tablet_splitting=true",
                   "tablet_split_high_phase_shard_count_per_node=10000",
                   "tablet_split_high_phase_size_threshold_bytes=52428880",
                   "tablet_split_low_phase_size_threshold_bytes=5242888",
                   "tablet_split_low_phase_shard_count_per_node=16");
    setTserverFlags("enable_automatic_tablet_splitting=true");
    ybContainer = TestHelper.getYbContainer();
    ybContainer.start();

    LOGGER.info("Container startup command in test: {}", getYugabytedStartCommand());

    try {
      ybContainer.execInContainer(getYugabytedStartCommand().split("\\s+"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433), ybContainer.getMappedPort(9042));
    TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));
    
    TestHelper.dropAllSchemas();
    String generatedColumns = "";
    
    for (int i = 1; i <= 99; ++i) {
      generatedColumns += "v" + i
                          + " varchar(400) default "
                          + "'123456789012345678901234567890123456789012345678901234567890"
                          + "123456789012345678901234567890123456789012345678901234567890"
                          + "123456789012345678901234567890123456789012345678901234567890"
                          + "123456789012345678901234567890123456789012345678901234567890"
                          + "1234567890123456789012345678901234567890123456789012345', ";
    }

    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT, "
                       + generatedColumns
                       + "v100 varchar(400) default '1234567890123456789012345678901234567890"
                       + "1234567890123456789012345678901234567890123456789012345')"
                       + " SPLIT INTO 1 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    startEngine(configBuilder, (success, message, error) -> {
      assertTrue(success);
    });

    awaitUntilConnectorIsReady();

    int recordsCount = 20000;

    String insertFormat = "INSERT INTO t1(id, name) VALUES (%d, 'value for split table');";

    int beginKey = 0;
    int endKey = beginKey + 100;
    for (int i = 0; i < 200; ++i) {
      TestHelper.executeBulkWithRange(insertFormat, beginKey, endKey);
      beginKey = endKey;
      endKey = beginKey + 100;
    }

    // Wait for splitting here
    TestHelper.waitFor(Duration.ofSeconds(15));

    CompletableFuture.runAsync(() -> verifyRecordCount(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
  }

  /**
   * Reproduces a server-side bug where after a Gen 2 split,
   * GetTabletListToPollForCDC returns {A, B} instead of just {A}.
   *
   * Scenario:
   *   Gen 0: A (single parent tablet)
   *   Gen 1: A splits -> B, C.  Connector polls B but NOT C.
   *   Gen 2: B splits -> D, E.  C splits -> F, G.
   *
   * After Gen 2 split:
   *   Correct behavior: GetTabletListToPollForCDC returns {A}
   *   Bug behavior:     returns {A, B}
   */
  @Order(3)
  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void shouldVerifyGetTabletListAfterGen2Split(
          boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 1 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    startEngine(configBuilder, (success, message, error) -> {
      assertTrue(success);
    });
    awaitUntilConnectorIsReady();

    int initialRecords = 50;
    String insertFormat = "INSERT INTO t1 VALUES (%d, 'value for split table');";
    for (int i = 0; i < initialRecords; ++i) {
      TestHelper.execute(String.format(insertFormat, i));
    }

    List<SourceRecord> preRecords = new ArrayList<>();
    waitAndFailIfCannotConsume(preRecords, initialRecords);
    assertEquals(initialRecords, preRecords.size());

    YBClient ybClient = TestHelper.getYbClient(masterAddresses);
    YBTable table = TestHelper.getYbTable(ybClient, "t1");
    Set<String> tablets = ybClient.getTabletUUIDs(table);
    assertEquals(1, tablets.size());
    String parentTabletId = tablets.iterator().next();

    // Gen 1 split: A -> B, C.  Poll B only, skip C.
    YugabyteDBStreamingChangeEventSource.TEST_PAUSE_GET_CHANGES_CALLS = true;

    ybClient.flushTable(table.getTableId());
    TestHelper.waitFor(Duration.ofSeconds(20));
    ybClient.splitTablet(tablets.iterator().next());
    TestHelper.waitForTablets(ybClient, table, 2);

    Set<String> gen1Tablets = ybClient.getTabletUUIDs(table);
    assertEquals(2, gen1Tablets.size());
    Iterator<String> it = gen1Tablets.iterator();
    String childToSkip = it.next();
    String childToAllow = it.next();
    LOGGER.info("Gen1 childToSkip (C): {}, childToAllow (B): {}", childToSkip, childToAllow);

    YugabyteDBStreamingChangeEventSource.TEST_TABLETS_TO_SKIP_POLLING = new HashSet<>();
    YugabyteDBStreamingChangeEventSource.TEST_TABLETS_TO_SKIP_POLLING.add(childToSkip);
    YugabyteDBStreamingChangeEventSource.TEST_PAUSE_GET_CHANGES_CALLS = false;

    int gen1MoreRecords = 100;
    for (int i = initialRecords; i < initialRecords + gen1MoreRecords; ++i) {
      TestHelper.execute(String.format(insertFormat, i));
    }

    TestHelper.waitFor(Duration.ofSeconds(30));

    Set<Integer> allKeys = new HashSet<>();
    for (SourceRecord r : preRecords) {
      allKeys.add(((Struct) r.key()).getStruct("id").getInt32("value"));
    }
    consumeAvailableRecords(record -> {
      Struct s = (Struct) record.key();
      allKeys.add(s.getStruct("id").getInt32("value"));
    });
    LOGGER.info("Unique keys after Gen1 partial polling: {}", allKeys.size());

    // Gen 2 split: B -> D,E and C -> F,G
    YugabyteDBStreamingChangeEventSource.TEST_PAUSE_GET_CHANGES_CALLS = true;

    int gen2MoreRecords = 200;
    for (int i = initialRecords + gen1MoreRecords;
         i < initialRecords + gen1MoreRecords + gen2MoreRecords; ++i) {
      TestHelper.execute(String.format(insertFormat, i));
    }

    ybClient.flushTable(table.getTableId());
    TestHelper.waitFor(Duration.ofSeconds(20));

    ybClient.splitTablet(childToAllow);
    TestHelper.waitForTablets(ybClient, table, 3);

    ybClient.splitTablet(childToSkip);
    TestHelper.waitForTablets(ybClient, table, 4);

    Set<String> gen2Tablets = ybClient.getTabletUUIDs(table);
    assertEquals(4, gen2Tablets.size());

    // Stop connector and check GetTabletListToPollForCDC.
    stopConnector();
    Awaitility.await()
        .pollDelay(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(60))
        .until(() -> engine == null);

    YugabyteDBStreamingChangeEventSource.TEST_TABLETS_TO_SKIP_POLLING = null;
    YugabyteDBStreamingChangeEventSource.TEST_PAUSE_GET_CHANGES_CALLS = false;

    GetTabletListToPollForCDCResponse resp = ybClient.getTabletListToPollForCdc(
        table, dbStreamId, table.getTableId());
    LOGGER.info("GetTabletListToPollForCDC after Gen2 split returned {} tablet(s)",
                resp.getTabletCheckpointPairListSize());

    List<String> returnedTabletIds = new ArrayList<>();
    for (int i = 0; i < resp.getTabletCheckpointPairListSize(); i++) {
      TabletCheckpointPair pair = resp.getTabletCheckpointPairList().get(i);
      String tid = pair.getTabletLocations().getTabletId().toStringUtf8();
      OpId cp = OpId.from(pair.getCdcSdkCheckpoint());
      String label;
      if (tid.equals(parentTabletId)) label = "PARENT(A)";
      else if (tid.equals(childToSkip)) label = "Gen1-C(skipped)";
      else if (tid.equals(childToAllow)) label = "Gen1-B(polled)";
      else if (gen2Tablets.contains(tid)) label = "Gen2-child";
      else label = "UNKNOWN";
      LOGGER.info("  [{}] tablet={} ({}), checkpoint={}", i, tid, label, cp);
      returnedTabletIds.add(tid);
    }

    // After Gen2 split with partial Gen1 polling (C was never polled),
    // the entire tablet tree should collapse to the original parent A.
    assertEquals(1, resp.getTabletCheckpointPairListSize(),
        "Expected only parent A; C was never polled so the tree should collapse to A. Got: " + returnedTabletIds);
    assertTrue(returnedTabletIds.contains(parentTabletId),
        "The single returned tablet should be parent A, got: " + returnedTabletIds);
  }

  /**
   * Verifies that after a partial-child-polling restart, the connector resumes
   * polling the previously-polled child (B) from its last acked checkpoint,
   * NOT from the split op.  Detects this by checking for duplicate records.
   *
   * Scenario:
   *   1. Connector polls parent A, acks up to some checkpoint
   *   2. A splits into {B, C}
   *   3. Connector polls B (acks beyond split op), never polls C
   *   4. Connector restarts
   *   5. GetTabletListToPoll returns A (C was not polled)
   *   6. Connector re-discovers split, adds B and C
   *   7. VERIFY: B resumes from its acked checkpoint (no duplicate records)
   */
  @Order(4)
  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
  public void shouldResumeChildFromAckedCheckpointNotSplitOp(
          boolean consistentSnapshot, boolean useSnapshot) throws Exception {
    TestHelper.dropAllSchemas();
    TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO 1 TABLETS;");

    String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);

    startEngine(configBuilder, (success, message, error) -> {
      assertTrue(success);
    });
    awaitUntilConnectorIsReady();

    // Insert enough initial records so the tablet has data for splitting.
    int initialRecords = 500;
    String insertFormat = "INSERT INTO t1 VALUES (%d, 'value for split table');";
    TestHelper.executeBulkWithRange(insertFormat, 0, initialRecords);

    List<SourceRecord> preRecords = new ArrayList<>();
    waitAndFailIfCannotConsume(preRecords, initialRecords);
    assertEquals(initialRecords, preRecords.size());

    Set<Integer> preRestartKeys = new HashSet<>();
    for (SourceRecord r : preRecords) {
      preRestartKeys.add(((Struct) r.key()).getStruct("id").getInt32("value"));
    }
    LOGGER.info("Pre-split records consumed from parent A: {}", preRestartKeys.size());

    YBClient ybClient = TestHelper.getYbClient(masterAddresses);
    YBTable table = TestHelper.getYbTable(ybClient, "t1");
    Set<String> tablets = ybClient.getTabletUUIDs(table);
    assertEquals(1, tablets.size());
    String parentTabletId = tablets.iterator().next();

    // Split A -> B, C.  Poll B only, skip C.
    YugabyteDBStreamingChangeEventSource.TEST_PAUSE_GET_CHANGES_CALLS = true;

    ybClient.flushTable(table.getTableId());
    TestHelper.waitFor(Duration.ofSeconds(20));
    ybClient.splitTablet(tablets.iterator().next());
    TestHelper.waitForTablets(ybClient, table, 2);

    Set<String> childTablets = ybClient.getTabletUUIDs(table);
    assertEquals(2, childTablets.size());
    Iterator<String> it = childTablets.iterator();
    String childToSkip = it.next();
    String childToAllow = it.next();
    LOGGER.info("childToSkip (C): {}, childToAllow (B): {}", childToSkip, childToAllow);

    YugabyteDBStreamingChangeEventSource.TEST_TABLETS_TO_SKIP_POLLING = new HashSet<>();
    YugabyteDBStreamingChangeEventSource.TEST_TABLETS_TO_SKIP_POLLING.add(childToSkip);
    YugabyteDBStreamingChangeEventSource.TEST_PAUSE_GET_CHANGES_CALLS = false;

    // Capture B's initial (split-op) checkpoint so we can verify it advances.
    GetTabletListToPollForCDCResponse childResp =
        ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId(), parentTabletId);
    long splitOpIndex = -1;
    for (TabletCheckpointPair pair : childResp.getTabletCheckpointPairList()) {
      String tid = pair.getTabletLocations().getTabletId().toStringUtf8();
      if (tid.equals(childToAllow)) {
        splitOpIndex = OpId.from(pair.getCdcSdkCheckpoint()).getIndex();
        break;
      }
    }
    assertTrue(splitOpIndex >= 0, "Could not find B's split-op checkpoint");
    final long splitOpIndexFinal = splitOpIndex;
    LOGGER.info("B's split-op checkpoint index: {}", splitOpIndex);

    // Insert a large number of post-split records so B gets substantial data.
    int moreRecords = 20000;
    int beginKey = initialRecords;
    int endKey = beginKey + 100;
    for (int i = 0; i < moreRecords / 100; ++i) {
      TestHelper.executeBulkWithRange(insertFormat, beginKey, endKey);
      beginKey = endKey;
      endKey = beginKey + 100;
    }

    // Wait until B's cdc_state checkpoint has advanced beyond the split-op value,
    // guaranteeing the explicit checkpoint has been flushed to the server.
    Awaitility.await()
        .atMost(Duration.ofSeconds(120))
        .pollInterval(Duration.ofSeconds(5))
        .until(() -> {
          consumeAvailableRecords(record -> {
            Struct s = (Struct) record.key();
            preRestartKeys.add(s.getStruct("id").getInt32("value"));
          });
          if (preRestartKeys.size() - initialRecords == 0) {
            return false;
          }
          GetTabletListToPollForCDCResponse cr =
              ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId(), parentTabletId);
          for (TabletCheckpointPair p : cr.getTabletCheckpointPairList()) {
            String tid = p.getTabletLocations().getTabletId().toStringUtf8();
            if (tid.equals(childToAllow)) {
              long idx = OpId.from(p.getCdcSdkCheckpoint()).getIndex();
              LOGGER.info("B cdc_state index: {} (split-op: {}, keys from B: {})",
                          idx, splitOpIndexFinal, preRestartKeys.size() - initialRecords);
              return idx > splitOpIndexFinal;
            }
          }
          return false;
        });

    // Drain any remaining buffered records.
    consumeAvailableRecords(record -> {
      Struct s = (Struct) record.key();
      preRestartKeys.add(s.getStruct("id").getInt32("value"));
    });
    int keysFromBBeforeRestart = preRestartKeys.size() - initialRecords;
    LOGGER.info("Unique keys consumed before restart: {} (initial: {}, from B: {})",
                preRestartKeys.size(), initialRecords, keysFromBBeforeRestart);
    assertTrue(keysFromBBeforeRestart > 0,
        "B should have consumed some records before restart to make the duplicate check meaningful");

    // Stop the connector.
    stopConnector();
    Awaitility.await()
        .pollDelay(Duration.ofSeconds(1))
        .atMost(Duration.ofSeconds(60))
        .until(() -> engine == null);

    YugabyteDBStreamingChangeEventSource.TEST_TABLETS_TO_SKIP_POLLING = null;
    YugabyteDBStreamingChangeEventSource.TEST_PAUSE_GET_CHANGES_CALLS = false;

    // Verify GetTabletListToPoll returns only parent A.
    GetTabletListToPollForCDCResponse resp =
      ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());
    assertEquals(1, resp.getTabletCheckpointPairListSize(),
        "Expected only parent tablet A because one child was never polled");
    String returnedTabletId = resp.getTabletCheckpointPairList().get(0)
        .getTabletLocations().getTabletId().toStringUtf8();
    assertEquals(parentTabletId, returnedTabletId,
        "The tablet returned should be the original parent, not a child");

    // Restart connector. Both children will be polled this time.
    startEngine(configBuilder, (success, message, error) -> {
      assertTrue(success);
    });
    awaitUntilConnectorIsReady();

    // Track post-restart records to detect duplicates.
    Set<Integer> allKeys = new HashSet<>(preRestartKeys);
    List<Integer> duplicateKeys = new ArrayList<>();
    int totalExpected = initialRecords + moreRecords;

    int failureCounter = 0;
    while (allKeys.size() < totalExpected) {
      int consumed = consumeAvailableRecords(record -> {
        Struct s = (Struct) record.key();
        int key = s.getStruct("id").getInt32("value");
        if (preRestartKeys.contains(key)) {
          duplicateKeys.add(key);
        }
        allKeys.add(key);
      });
      if (consumed > 0) {
        failureCounter = 0;
      } else {
        ++failureCounter;
        TestHelper.waitFor(Duration.ofSeconds(2));
      }
      if (failureCounter == 100) {
        LOGGER.error("Breaking: collected {} of {} keys, {} duplicates",
                     allKeys.size(), totalExpected, duplicateKeys.size());
        break;
      }
    }

    LOGGER.info("Total unique keys: {} (expected {}), Duplicate count: {}",
                allKeys.size(), totalExpected, duplicateKeys.size());
    assertEquals(totalExpected, allKeys.size(), "All unique records should be consumed");
    assertTrue(duplicateKeys.isEmpty(),
        "B should resume from acked checkpoint (no duplicates). Got " + duplicateKeys.size() + " duplicates");
    assertConnectorIsRunning();
  }

  private void verifyRecordCount(long recordsCount) {
    Set<Integer> recordKeySet = new HashSet<>();
    int failureCounter = 0;
    while (recordKeySet.size() < recordsCount) {
        int consumed = consumeAvailableRecords(record -> {
            Struct s = (Struct) record.key();
            int value = s.getStruct("id").getInt32("value");
            recordKeySet.add(value);
        });
        if (consumed > 0) {
            failureCounter = 0;
        } else {
          ++failureCounter;
          TestHelper.waitFor(Duration.ofSeconds(2));
        }

        if (failureCounter == 100) {
          LOGGER.error("Breaking becauase failure counter hit limit");
          break;
        }
    }

    LOGGER.debug("Record key set size: " + recordKeySet.size());
    List<Integer> rList = recordKeySet.stream().collect(Collectors.toList());
    Collections.sort(rList);

    assertEquals(recordsCount, recordKeySet.size());
  }
}
