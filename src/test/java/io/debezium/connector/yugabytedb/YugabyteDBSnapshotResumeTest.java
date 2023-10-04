package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Basic unit tests to ensure proper working of snapshot resuming functionality - the connector
 * will resume the snapshot from the snapshot key returned. For more reference see
 * {@linkplain YugabyteDBSnapshotChangeEventSource#doExecute}
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBSnapshotResumeTest extends YugabytedTestBase {
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

	@Test
	public void verifySnapshotIsResumedFromKey() throws Exception {
		TestHelper.dropAllSchemas();
		TestHelper.executeDDL("yugabyte_create_tables.ddl");

		final int recordsCount = 5_000;

		// insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
		TestHelper.executeBulk(insertStmtFormat, recordsCount);

		YugabyteDBSnapshotChangeEventSource.TRACK_EXPLICIT_CHECKPOINTS = true;

		String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
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
