package io.debezium.connector.yugabytedb;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import io.debezium.schema.SchemaTopicNamingStrategy;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;

import java.sql.SQLException;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class to verify that we are properly sending the transaction metadata to the topic.
 *
 * @author Vaibhav Kushwaha
 */
public class YugabyteDBTransactionMetadataTest extends YugabyteDBContainerTestBase {
	// By using generate_series(), we are ensuring that there are explicit transactions.
	private static final String INSERT_FORMAT =
		"INSERT INTO t1 VALUES (generate_series(%d,%d), 'fname', 'lname', 12.34);";

	@BeforeAll
	public static void beforeClass() throws SQLException {
		initializeYBContainer();
		TestHelper.dropAllSchemas();
	}

	@BeforeEach
	public void before() throws Exception {
		initializeConnectorTestFramework();
		TestHelper.executeDDL("yugabyte_create_tables.ddl");
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

	@ParameterizedTest(name = "Custom topic name: {0}")
	@ValueSource(booleans = {true, false})
	public void shouldPublishTransactionMetadata(boolean customTopicName) throws Exception {
		String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1");
		Configuration.Builder configBuilder = getConfigBuilderForMetadata(dbStreamId, "public.t1");

		String transactionTopicName = TestHelper.TEST_SERVER + ".transaction";
		if (customTopicName) {
			transactionTopicName = "custom.transaction.topic";
			configBuilder.with(SchemaTopicNamingStrategy.TOPIC_TRANSACTION, transactionTopicName);
		}

		startEngine(configBuilder);
		awaitUntilConnectorIsReady();

		TestHelper.execute(String.format(INSERT_FORMAT, 1, 5));

		// Wait for records to be available for consuming.
		waitForAvailableRecords(10000, TimeUnit.MILLISECONDS);

		// Consume records
		SourceRecords records = consumeByTopic(7 /* BEGIN + 5 INSERT + COMMIT */);

		// Assert for records in the transaction topic.
		List<SourceRecord> metadataRecords = records.recordsForTopic(transactionTopicName);
		assertEquals(2, records.recordsForTopic(transactionTopicName).size());

		Struct begin = (Struct) metadataRecords.get(0).value();
		String transactionId = assertBeginTransaction(metadataRecords.get(0));
		assertEndTransaction(metadataRecords.get(1), transactionId, 5, begin.getString("partition_id"));
	}

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void assertMultipleTransactions(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
		String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "t1", consistentSnapshot, useSnapshot);
		Configuration.Builder configBuilder = getConfigBuilderForMetadata(dbStreamId, "public.t1");

		String transactionTopicName = TestHelper.TEST_SERVER + ".transaction";

		startEngine(configBuilder);
		awaitUntilConnectorIsReady();

		TestHelper.execute(String.format(INSERT_FORMAT, 1, 5));

		TestHelper.execute(String.format(INSERT_FORMAT, 6, 20));

		// Wait for records to be available for consuming.
		waitForAvailableRecords(10000, TimeUnit.MILLISECONDS);

		// Consume records
		SourceRecords records = consumeByTopic(
			24 /* BEGIN + 5 INSERT + COMMIT + BEGIN + 15 INSERT + COMMIT*/);

		// Assert for records in the transaction topic.
		List<SourceRecord> metadataRecords = records.recordsForTopic(transactionTopicName);
		assertEquals(4, records.recordsForTopic(transactionTopicName).size());

		Struct begin1 = (Struct) metadataRecords.get(0).value();
		String transactionId1 = assertBeginTransaction(metadataRecords.get(0));
		assertEndTransaction(metadataRecords.get(1), transactionId1, 5,
												 begin1.getString("partition_id"));

		Struct begin2 = (Struct) metadataRecords.get(2).value();
		String transactionId2 = assertBeginTransaction(metadataRecords.get(2));
		assertEndTransaction(metadataRecords.get(3), transactionId2, 15,
												 begin2.getString("partition_id"));

		// Since there is just one tablet, the partition_id for begin1 and begin2 should be the same.
		assertEquals(begin1.getString("partition_id"), begin2.getString("partition_id"));
	}

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void verifyTransactionalDataAcrossMultipleTablets(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
		// This will lead to 2 tablets of range [lowest, 1000] and (1000, highest]
		final String createTable =
			"CREATE TABLE test_table (id INT, PRIMARY KEY(id ASC)) SPLIT AT VALUES ((1000));";
		TestHelper.execute("DROP TABLE IF EXISTS test_table;");
		TestHelper.execute(createTable);

		String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "test_table", consistentSnapshot, useSnapshot);
		Configuration.Builder configBuilder =
			getConfigBuilderForMetadata(dbStreamId, "public.test_table");

		String transactionTopicName = TestHelper.TEST_SERVER + ".transaction";

		startEngine(configBuilder);
		awaitUntilConnectorIsReady();

		final String statementBatch = "BEGIN; " 
			+ "INSERT INTO test_table VALUES (generate_series(1,10)); "
			+ "INSERT INTO test_table VALUES (generate_series(1011, 1020)); " + "COMMIT;";
		TestHelper.execute(statementBatch);

		waitForAvailableRecords(15000, TimeUnit.MILLISECONDS);

		// Consume records
		SourceRecords records = consumeByTopic(24 /* BEGIN + 20 INSERT + COMMIT*/);

		// Assert for records in the transaction topic.
		List<SourceRecord> metadataRecords = records.recordsForTopic(transactionTopicName);
		assertEquals(4, records.recordsForTopic(transactionTopicName).size());

		// Both the begin-commit block are the same and they both belong to the same transaction
		// thus they will have the same transaction_id.
		Struct begin1 = (Struct) metadataRecords.get(0).value();
		String transactionId1 = assertBeginTransaction(metadataRecords.get(0));
		assertEndTransaction(metadataRecords.get(1), transactionId1, 10,
			begin1.getString("partition_id"));

		Struct begin2 = (Struct) metadataRecords.get(2).value();
		String transactionId2 = assertBeginTransaction(metadataRecords.get(2));
		assertEndTransaction(metadataRecords.get(3), transactionId2, 10,
			begin2.getString("partition_id"));

		// Since the transaction is the same, the transaction_id should be the same.
		assertEquals(transactionId1, transactionId2);
	}

	private Configuration.Builder getConfigBuilderForMetadata(String dbStreamId, String tableName)
			throws Exception {
		Configuration.Builder configBuilder = TestHelper.getConfigBuilder(tableName, dbStreamId);
		configBuilder.with(YugabyteDBConnectorConfig.PROVIDE_TRANSACTION_METADATA, true);
		return configBuilder;
	}
}
