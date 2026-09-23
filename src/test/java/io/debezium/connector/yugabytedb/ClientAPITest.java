package io.debezium.connector.yugabytedb;

import io.debezium.DebeziumException;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.yb.client.GetDBStreamInfoResponse;
import org.yb.client.GetTabletListToPollForCDCResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests to verify the behaviour of yb-client APIs we use to interact with YugabyteDB server.
 * Note that these tests are not exhaustive and are only meant to test and reproduce things
 * quickly.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class ClientAPITest extends YugabyteDBContainerTestBase {
	@BeforeAll
	public static void beforeClass() throws Exception {
		initializeYBContainer();
		TestHelper.dropAllSchemas();
		TestHelper.executeDDL("yugabyte_create_tables.ddl");
	}

	@BeforeEach
	public void before() throws Exception {
		initializeConnectorTestFramework();
		TestHelper.dropAllSchemas();
	}

	@AfterEach
	public void after() throws Exception {
		stopConnector();
	}

	@AfterAll
	public static void afterClass() throws Exception {
		shutdownYBContainer();
	}

	@ParameterizedTest(name = "Colocation: {0}")
	@ValueSource(booleans = {true, false})
	public void getTabletListToPollForCDC(boolean colocated) throws Exception {
		// Drop tables in case they already exist.
		TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_1;", DEFAULT_COLOCATED_DB_NAME);
		TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_2;", DEFAULT_COLOCATED_DB_NAME);

		final String createTable1 =
			String.format("CREATE TABLE test_1 (id INT PRIMARY KEY) WITH (COLOCATED = %s);", colocated);
		final String createTable2 =
			String.format("CREATE TABLE test_2 (id INT PRIMARY KEY) WITH (COLOCATED = %s);", colocated);

		// Create tables inside the colocated database.
		TestHelper.executeInDatabase(createTable1, DEFAULT_COLOCATED_DB_NAME);
		TestHelper.executeInDatabase(createTable2, DEFAULT_COLOCATED_DB_NAME);

		final String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");

		YBClient ybClient = TestHelper.getYbClient(getMasterAddress());

		List<YBTable> tables = new ArrayList<>();
		tables.add(TestHelper.getYbTable(ybClient, "test_1"));
		tables.add(TestHelper.getYbTable(ybClient, "test_2"));

		// Now get the tablet list for all the tables.
		for (YBTable table : tables) {
			assertNotNull(table);
			GetTabletListToPollForCDCResponse resp =
					ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());
			assertNotNull(resp);
		}
	}

	/**
	 * Verifies the {@link YBClientUtils#fetchTableList(YBClient, YugabyteDBConnectorConfig, GetDBStreamInfoResponse)}
	 * overload, which accepts a pre-fetched {@link GetDBStreamInfoResponse} so the caller can avoid
	 * one getDBStreamInfo RPC per table. It must (a) produce the same result as the original
	 * overload, and (b) actually use the response it is given for the stream-membership check rather
	 * than re-fetching it.
	 */
	@Test
	public void fetchTableListWithPrefetchedStreamInfo() throws Exception {
		TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_1;", DEFAULT_COLOCATED_DB_NAME);
		TestHelper.executeInDatabase("DROP TABLE IF EXISTS test_2;", DEFAULT_COLOCATED_DB_NAME);
		TestHelper.executeInDatabase(
			"CREATE TABLE test_1 (id INT PRIMARY KEY) WITH (COLOCATED = false);", DEFAULT_COLOCATED_DB_NAME);
		TestHelper.executeInDatabase(
			"CREATE TABLE test_2 (id INT PRIMARY KEY) WITH (COLOCATED = false);", DEFAULT_COLOCATED_DB_NAME);

		// The stream is namespace-level, so both test_1 and test_2 are part of it.
		final String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "test_1");

		try (YBClient ybClient = TestHelper.getYbClient(getMasterAddress())) {
			final String test1Uuid = TestHelper.getYbTable(ybClient, "test_1").getTableId();

			// Only include test_1; test_2 should be filtered out by the include list.
			final YugabyteDBConnectorConfig config = new YugabyteDBConnectorConfig(
				TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1", dbStreamId).build());

			final GetDBStreamInfoResponse streamInfo = ybClient.getDBStreamInfo(dbStreamId);

			// (a) The pre-fetched overload honours the include list and agrees with the original overload.
			Set<String> tableIds = YBClientUtils.fetchTableList(ybClient, config, streamInfo);
			assertEquals(Collections.singleton(test1Uuid), tableIds);
			assertEquals(YBClientUtils.fetchTableList(ybClient, config), tableIds);

			// (b) Hand the method a response that claims test_1 is NOT in the stream. If the method
			// silently re-fetched the stream info instead of using what it was given, test_1 would
			// still be returned and these assertions would fail.
			final GetDBStreamInfoResponse streamInfoWithoutTest1 = new GetDBStreamInfoResponse(
				streamInfo.getElapsedMillis(),
				streamInfo.getTsUUID(),
				streamInfo.getTableInfoList().stream()
					.filter(t -> !t.getTableId().toStringUtf8().equals(test1Uuid))
					.collect(Collectors.toList()),
				streamInfo.getNamespaceId());

			// An included table that is not part of the stream is an error by default...
			assertThrows(DebeziumException.class,
				() -> YBClientUtils.fetchTableList(ybClient, config, streamInfoWithoutTest1));

			// ...and is skipped with a warning when ignore.exceptions is set.
			final YugabyteDBConnectorConfig lenientConfig = new YugabyteDBConnectorConfig(
				TestHelper.getConfigBuilder(DEFAULT_COLOCATED_DB_NAME, "public.test_1", dbStreamId)
					.with(YugabyteDBConnectorConfig.IGNORE_EXCEPTIONS, true)
					.build());
			assertTrue(YBClientUtils.fetchTableList(ybClient, lenientConfig, streamInfoWithoutTest1).isEmpty());
		}
	}
}
