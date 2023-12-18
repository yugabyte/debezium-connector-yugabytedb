package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.yb.client.CdcSdkCheckpoint;
import org.yb.client.GetCheckpointResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic unit tests to ensure proper working of snapshot resuming functionality - the connector
 * will resume the snapshot from the snapshot key returned. For more reference see
 * {@linkplain YugabyteDBSnapshotChangeEventSource#doExecute}
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBSnapshotResumeTest extends YugabyteDBContainerTestBase {
	private final String insertStmtFormat = "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 30);";
	private static final int snapshotBatchSize = 50;
	@BeforeAll
	public static void beforeClass() throws SQLException {
		initializeYBContainer(null, "cdc_snapshot_batch_size=" + snapshotBatchSize);
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

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForSnapshot")
  public void verifySnapshotIsResumedFromKey(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
		TestHelper.dropAllSchemas();
		TestHelper.executeDDL("yugabyte_create_tables.ddl");

		final int recordsCount = 5_000;

		// insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
		TestHelper.executeBulk(insertStmtFormat, recordsCount);

		YugabyteDBSnapshotChangeEventSource.TRACK_EXPLICIT_CHECKPOINTS = true;

		String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", consistentSnapshot, useSnapshot);
		Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
		configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());
		startEngine(configBuilder);
		awaitUntilConnectorIsReady();

		// Consume whatever records are available.
		int totalConsumedSoFar = consumeAllAvailableRecordsTill(1500);

		// Kill the connector after some seconds and consume whatever data is available.
		stopConnector();

		// There are changes that some records get published and are ready to consume while the
		// the connector was being stopped.
		totalConsumedSoFar += consumeAvailableRecords(record -> {});

		// Confirm whether there are no more records to consume.
		assertNoRecordsToConsume();

		// The last set explicit checkpoint can be obtained from the static variable.
		CdcSdkCheckpoint lastExplicitCheckpoint =
			YugabyteDBSnapshotChangeEventSource.LAST_EXPLICIT_CHECKPOINT;

		// The last explicit checkpoint should be the one stored in the state table, verify.
		YBClient ybClient = TestHelper.getYbClient(getMasterAddress());
		YBTable ybTable = TestHelper.getYbTable(ybClient, "t1");

		// Assuming that the table has only 1 tablet, get the first tablet and get its checkpoint.
		GetCheckpointResponse resp =
			ybClient.getCheckpoint(ybTable, dbStreamId, ybClient.getTabletUUIDs(ybTable).iterator().next());

		assertEquals(lastExplicitCheckpoint.getTerm(), resp.getTerm());
		assertEquals(lastExplicitCheckpoint.getIndex(), resp.getIndex());
		assertTrue(Arrays.equals(lastExplicitCheckpoint.getKey(), resp.getSnapshotKey()));

		// Start the connector again - this step will ensure that the connector is resuming the snapshot
		// and only starting the consumption from the point it left.
		startEngine(configBuilder);
		awaitUntilConnectorIsReady();

		// Only verifying the record count since the snapshot records are not ordered, so it may be
		// a little complex to verify them in the sorted order at the moment
		final int finalTotalConsumedSoFar = totalConsumedSoFar;
		List<SourceRecord> records = new ArrayList<>();
		waitAndFailIfCannotConsume(records, recordsCount - finalTotalConsumedSoFar);

		// Consume the remaining records as there can be duplicates records while resuming snapshot,
		// they should NOT be equal to what we have consumed before connector restart.
		LOGGER.info("Consuming remaining records (if available)");

		int remainingConsumedRecords = 0;
		for (int i = 0; i < 10; ++i) {
			remainingConsumedRecords += consumeAvailableRecords(record -> {});
			TestHelper.waitFor(Duration.ofSeconds(2));
		}

		LOGGER.info("Remaining consumed record count: {}", remainingConsumedRecords);
		assertNotEquals(finalTotalConsumedSoFar, remainingConsumedRecords);

		assertEquals(recordsCount - finalTotalConsumedSoFar, records.size());

		YugabyteDBSnapshotChangeEventSource.TRACK_EXPLICIT_CHECKPOINTS = false;
	}

  @ParameterizedTest
  @MethodSource("io.debezium.connector.yugabytedb.YugabyteDBSnapshotTest#streamTypeProviderForSnapshotWithColocation")
  public void verifyNoDataLossIfConnectorRestartDuringLastBatchConsumption (boolean consistentSnapshot, boolean useSnapshot, boolean colocation) throws Exception {
    /*
     * The objective of this test is to verify the connector starts in the snapshot phase and
     * consumes the all the snapshot records before switching to streaming phase in the
     * following scenario: Connector restarts after it has received the last snapshot batch but
     * kafka hasn't fully consumed the last batch.
     */
    TestHelper.dropAllSchemas();
    TestHelper.executeDDL("yugabyte_create_tables.ddl");
    createTablesInColocatedDB(colocation);

    final int recordsCount = 1000;
    final int totalRecordExpectedAfterPartialConsumption = recordsCount - snapshotBatchSize / 2;

    insertBulkRecordsInColocatedDB(recordsCount, "public.test_1");

    YugabyteDBSnapshotChangeEventSource.TRACK_EXPLICIT_CHECKPOINTS = true;

    String dbStreamId = TestHelper.getNewDbStreamId("colocated_database", "test_1", consistentSnapshot, useSnapshot);
    Configuration.Builder configBuilder = TestHelper.getConfigBuilder("colocated_database", "public.test_1", dbStreamId);
    configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());

    // Explicitly avoid sending half of the last batch to kafka
    startEngineWithPartialConsumptionOfLastBatch(configBuilder, totalRecordExpectedAfterPartialConsumption);
    awaitUntilConnectorIsReady();

    // Wait for kafka to receive all the expected records before consuming records from kafka topic.
    Awaitility.await()
        .atMost(Duration.ofSeconds(180))
        .until(() -> (getNonConsumedRecordCount() == totalRecordExpectedAfterPartialConsumption));

    // Consume 975 records (950 + 25 from last batch)
    int totalConsumedSoFar = consumeByTopic(totalRecordExpectedAfterPartialConsumption).recordsForTopic("test_server.public.test_1").size();
    assertNoRecordsToConsume();

    // Kill the connector
    stopConnector();

    // The last set explicit checkpoint can be obtained from the static variable.
    CdcSdkCheckpoint lastExplicitCheckpoint =
      YugabyteDBSnapshotChangeEventSource.LAST_EXPLICIT_CHECKPOINT;

    // The last explicit checkpoint should be the one stored in the state table, verify.
    YBClient ybClient = TestHelper.getYbClient(getMasterAddress());
    YBTable ybTable = TestHelper.getYbTable(ybClient, "test_1");

    // Assuming that the table has only 1 tablet, get the first tablet and get its checkpoint.
    GetCheckpointResponse resp =
      ybClient.getCheckpoint(ybTable, dbStreamId, ybClient.getTabletUUIDs(ybTable).iterator().next());

    assertEquals(lastExplicitCheckpoint.getTerm(), resp.getTerm());
    assertEquals(lastExplicitCheckpoint.getIndex(), resp.getIndex());
    assertArrayEquals(lastExplicitCheckpoint.getKey(), resp.getSnapshotKey());

    // Restart the connector
    YugabyteDBSnapshotChangeEventSource.TRACK_EXPLICIT_CHECKPOINTS = false;
    startEngine(configBuilder);
    awaitUntilConnectorIsReady();

    List<SourceRecord> recordsAfterRestart = new ArrayList<>();

    // We should receive the last 2 snapshot batch (500 records) again since we will be setting
    // from_op_id to the last explicit checkpoint (that points to start of the 2nd last batch) sent to the server
    // during the snapshot phase.
    waitAndFailIfCannotConsume(recordsAfterRestart, 2 * snapshotBatchSize);
    LOGGER.info("Remaining consumed record count: {}", recordsAfterRestart.size());
    assertNotEquals(totalConsumedSoFar, recordsAfterRestart.size());
    assertEquals(2 * snapshotBatchSize, recordsAfterRestart.size());
  }

	private int consumeAllAvailableRecordsTill(long minimumRecordsToConsume) {
		AtomicInteger totalConsumedSoFar = new AtomicInteger();
		Awaitility.await()
			.atMost(Duration.ofSeconds(60))
			.until(() -> {
				int consumed = consumeAvailableRecords(record -> {
					LOGGER.debug("The record being consumed is " + record);
				});
				if (consumed > 0) {
					totalConsumedSoFar.addAndGet(consumed);
					LOGGER.debug("Consumed " + totalConsumedSoFar + " records");
				}

				return totalConsumedSoFar.get() >= 1500;
			});

		return totalConsumedSoFar.get();
	}
}
