package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;

/**
 * Streams the same workloads with {@code cdc.poll.max.concurrent.tablets} at 1 (sequential) and above,
 * including a tablet split while requests for other tablets are in flight.
 */
public class YugabyteDBConcurrentGetChangesTest extends YugabyteDBContainerTestBase {

    private static final String INSERT = "INSERT INTO t1 VALUES (%d, 'concurrent getchanges');";

    @BeforeAll
    public static void beforeClass() throws SQLException {
        initializeYBContainer();
        TestHelper.dropAllSchemas();
    }

    @BeforeEach
    public void before() {
        initializeConnectorTestFramework();
    }

    @AfterEach
    public void after() throws Exception {
        stopConnector();
        TestHelper.execute("DROP TABLE IF EXISTS t1;");
    }

    @AfterAll
    public static void afterClass() {
        shutdownYBContainer();
    }

    private Configuration.Builder start(int tablets, int concurrency) throws Exception {
        TestHelper.execute("CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT) SPLIT INTO " + tablets + " TABLETS;");
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1", false /* consistentSnapshot */, false /* useSnapshot */);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId)
                .with(YugabyteDBConnectorConfig.CDC_POLL_MAX_CONCURRENT_TABLETS, concurrency);
        startEngine(configBuilder);
        awaitUntilConnectorIsReady();
        return configBuilder;
    }

    private static Set<Integer> ids(SourceRecords records) {
        return records.recordsForTopic("test_server.public.t1").stream()
                .map(SourceRecord::value)
                .map(v -> ((Struct) v).getStruct("after").getStruct("id").getInt32("value"))
                .collect(Collectors.toSet());
    }

    private static void insert(int fromInclusive, int toExclusive) {
        for (int i = fromInclusive; i < toExclusive; i++) {
            TestHelper.execute(String.format(INSERT, i));
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 4, 16 })
    public void streamsEveryRecordAcrossTablets(int concurrency) throws Exception {
        start(8, concurrency);

        insert(0, 200);
        SourceRecords records = consumeByTopic(200);

        assertEquals(200, ids(records).size());
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 8 })
    public void streamsEveryRecordAcrossATabletSplit(int concurrency) throws Exception {
        start(4, concurrency);
        insert(0, 50);

        YBClient ybClient = TestHelper.getYbClient(getMasterAddress());
        YBTable table = TestHelper.getYbTable(ybClient, "t1");
        ybClient.flushTable(table.getTableId());
        TestHelper.waitFor(Duration.ofSeconds(20));
        ybClient.splitTablet(ybClient.getTabletUUIDs(table).iterator().next());
        TestHelper.waitForTablets(ybClient, table, 5);

        insert(50, 100);
        SourceRecords records = consumeByTopic(100);

        assertEquals(100, ids(records).size());
        ybClient.close();
    }
}
