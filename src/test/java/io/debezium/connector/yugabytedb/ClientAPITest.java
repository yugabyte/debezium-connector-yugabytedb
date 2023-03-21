package io.debezium.connector.yugabytedb;

import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.yb.cdc.CdcService;
import org.yb.client.GetTabletListToPollForCDCResponse;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import java.util.ArrayList;
import java.util.List;

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

	@ParameterizedTest
	@ValueSource(booleans = {true, false})
	public void getTabletListToPollForCDC(boolean colocated) throws Exception {
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

		try {
			// Now get the tablet list for all the tables.
			for (YBTable table : tables) {
				assertNotNull(table);
				GetTabletListToPollForCDCResponse resp =
						ybClient.getTabletListToPollForCdc(table, dbStreamId, table.getTableId());
				assertNotNull(resp);
			}
		} catch (Exception e) {
			// The API call should not fail and should not throw exceptions.
			fail("Call to the API getTabletListToPollForCDC failed", e);
		}
	}
}
