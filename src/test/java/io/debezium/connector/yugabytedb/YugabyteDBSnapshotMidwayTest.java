package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.*;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class YugabyteDBSnapshotMidwayTest extends YugabytedTestBase {
	@BeforeAll
	public static void beforeClass() throws SQLException {
		initializeYBContainer(null, "cdc_snapshot_batch_size=20");
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
	public void baseTestToAssertSnapshotResumeFeature() throws Exception {
		TestHelper.dropAllSchemas();
		TestHelper.executeDDL("yugabyte_create_tables.ddl");

		final int recordsCount = 5_000;
		// insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
		insertBulkRecords(recordsCount);

		String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
		Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
		configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.INITIAL.getValue());
		start(YugabyteDBConnector.class, configBuilder.build());

		awaitUntilConnectorIsReady();

		AtomicLong totalConsumedAfterKill = new AtomicLong();
		AtomicInteger timesNoRecordReceived = new AtomicInteger();
		Awaitility.await()
			.atMost(Duration.ofSeconds(60))
			.until(() -> {
				int consumed = super.consumeAvailableRecords(record -> {
					LOGGER.debug("The record being consumed is " + record);
				});
				if (consumed > 0) {
					totalConsumedAfterKill.addAndGet(consumed);
					LOGGER.info("Consumed " + totalConsumedAfterKill + " records");
				} else {
					LOGGER.info("Got 0 records upon consumption");
					timesNoRecordReceived.incrementAndGet();
				}

				return totalConsumedAfterKill.get() >= 1500;
			});
		LOGGER.info("Total consumed records after kill are {}", totalConsumedAfterKill.get());

		// Kill the connector after some seconds and consume whatever data is available.
		TestHelper.waitFor(Duration.ofSeconds(5));
		stopConnector();
		assertNoRecordsToConsume();

		TestHelper.waitFor(Duration.ofSeconds(15));

		// Start the connector again.
		start(YugabyteDBConnector.class, configBuilder.build());
		awaitUntilConnectorIsReady();

		// Only verifying the record count since the snapshot records are not ordered, so it may be
		// a little complex to verify them in the sorted order at the moment
		CompletableFuture.runAsync(() -> verifyRecordCount(recordsCount - totalConsumedAfterKill.get()))
			.exceptionally(throwable -> {
				throw new RuntimeException(throwable);
			}).get();
	}

	private void insertBulkRecords(int numRecords) throws Exception {
		String formatInsertString = "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 30);";
		TestHelper.executeBulk(formatInsertString, numRecords);
	}

	private void verifyRecordCount(long recordsCount) {
		waitAndFailIfCannotConsume(new ArrayList<>(), recordsCount);
	}

	private void waitAndFailIfCannotConsume(List<SourceRecord> records, long recordsCount) {
		waitAndFailIfCannotConsume(records, recordsCount, 300 * 1000 /* 5 minutes */);
	}

	/**
	 * Consume the records available and add them to a list for further assertion purposes.
	 * @param records list to which we need to add the records we consume, pass a
	 * {@code new ArrayList<>()} if you do not need assertions on the consumed values
	 * @param recordsCount total number of records which should be consumed
	 * @param milliSecondsToWait duration in milliseconds to wait for while consuming
	 */
	private void waitAndFailIfCannotConsume(List<SourceRecord> records, long recordsCount,
																					long milliSecondsToWait) {
		AtomicLong totalConsumedRecords = new AtomicLong();
		long seconds = milliSecondsToWait / 1000;
		try {
			Awaitility.await()
				.atMost(Duration.ofSeconds(seconds))
				.until(() -> {
					int consumed = super.consumeAvailableRecords(record -> {
						LOGGER.debug("The record being consumed is " + record);
						records.add(record);
					});
					if (consumed > 0) {
						totalConsumedRecords.addAndGet(consumed);
						LOGGER.info("Consumed " + totalConsumedRecords + " records");
					}

					return totalConsumedRecords.get() == recordsCount;
				});
		} catch (ConditionTimeoutException exception) {
			fail("Failed to consume " + recordsCount + " in " + seconds + " seconds", exception);
		}

		assertEquals(recordsCount, totalConsumedRecords.get());
	}
}
