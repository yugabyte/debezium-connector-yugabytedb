package io.debezium.connector.yugabytedb;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.HelperBeforeImageModes.BeforeImageMode;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class YugabyteDBCQLTest extends YugabyteDBContainerTestBase {
    CqlSession session;

    @BeforeAll
    public static void beforeClass() throws SQLException {
        initializeYBContainer();
    }

    @BeforeEach
    public void before() {
        initializeConnectorTestFramework();
        session = CqlSession
                .builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .build();

    }

    @AfterEach
    public void after() throws Exception {
        stopConnector();
    }

    @AfterAll
    public static void afterClass() {
        shutdownYBContainer();
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void testRecordConsumption(boolean consistentSnapshot, boolean useSnapshot) throws Exception {

        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS cdctest;";
        session.execute(createKeyspace);

        session.execute("create table if not exists cdctest.test_cdc(a int primary key, b varchar, c text);");

        String dbStreamId = TestHelper.getNewDbStreamId("cdctest", "test_cdc", false, true, BeforeImageMode.CHANGE,
                true, consistentSnapshot, useSnapshot);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderForCQL("cdctest","test_cdc", dbStreamId);
        startEngine(configBuilder);

        final long recordsCount = 4;


        awaitUntilConnectorIsReady();

        session.execute("insert into cdctest.test_cdc(a,b,c) values (2,'abc','def');");
        session.execute("update cdctest.test_cdc set b = 'cde' where a = 2;");
        session.execute("delete from cdctest.test_cdc where a = 2;");
        
        verifyRecordCount(recordsCount);

    }

    @ParameterizedTest
    @MethodSource("streamTypeProviderForSnapshot")
    public void testSnapshotConsumption(boolean consistentSnapshot, boolean useSnapshot, String snapshotMode) throws Exception {
        setCommitCallbackDelay(10000);

        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS cdctest;";
        session.execute(createKeyspace);

        session.execute("create table if not exists cdctest.test_snapshot(a int primary key);");

        int recordsCount = 500;
        insertRecords(recordsCount, "cdctest", "test_snapshot");

        String dbStreamId = TestHelper.getNewDbStreamId("cdctest", "test_snapshot", false, true, BeforeImageMode.CHANGE,
                true, consistentSnapshot, useSnapshot);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderForCQL("cdctest","test_snapshot", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, snapshotMode);

        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        verifyRecordCount(recordsCount);
        resetCommitCallbackDelay();
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void testDatatypes(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS cdctest;";
        session.execute(createKeyspace);

        session.execute("create table if not exists cdctest.test_datatypes(a int primary key, b varchar, c text, d bigint, e boolean, f float, g date, h double, i smallint, j tinyint, k inet, l uuid, m timeuuid, n time, o timestamp, p decimal, q varint);");

        String dbStreamId = TestHelper.getNewDbStreamId("cdctest", "test_datatypes", false, true, BeforeImageMode.CHANGE, true, consistentSnapshot, useSnapshot);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderForCQL("cdctest","test_datatypes", dbStreamId);
        startEngine(configBuilder);

        final long recordsCount = 2;

        awaitUntilConnectorIsReady();

        session.execute("insert into cdctest.test_datatypes(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q) values (1, 'abc', 'def', 100000, false, 11.2, todate(now()), 17.8, 100, 8, '127.0.0.1', Uuid(), 123e4567-e89b-12d3-a456-426655440000, currenttime(), 1499171430000, 10.2, 45);");
        session.execute("insert into cdctest.test_datatypes(a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q) values (2, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);");

        verifyRecordCount(recordsCount);
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void testCounter(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS cdctest;";
        session.execute(createKeyspace);

        session.execute("create table if not exists cdctest.test_counter(id int primary key, b counter);");

        String dbStreamId = TestHelper.getNewDbStreamId("cdctest", "test_counter", false, true, BeforeImageMode.CHANGE,
                true, consistentSnapshot, useSnapshot);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderForCQL("cdctest","test_counter", dbStreamId);
        startEngine(configBuilder);

        final long recordsCount = 1;

        awaitUntilConnectorIsReady();

        session.execute("update cdctest.test_counter set b = b + 1 where id = 1;");

        verifyRecordCount(recordsCount);

    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void testBeforeImageWithCQL(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS cdctest;";
        session.execute(createKeyspace);

        session.execute("create table if not exists cdctest.t1(id INT PRIMARY KEY, first_name TEXT, last_name VARCHAR, hours int);");

        String dbStreamId = TestHelper.getNewDbStreamId("cdctest", "t1", true, true, BeforeImageMode.ALL, true, consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilderForCQL("cdctest", "t1", dbStreamId);
        
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        session.execute("INSERT INTO cdctest.t1(id,first_name,last_name,hours) values(1,'Vaibhav', 'Kushwaha', 40);");
        session.execute("UPDATE cdctest.t1 SET first_name='VKVK', hours=30 WHERE id = 1;");

        // Consume the records and verify that the records should have the relevant information.
        List<SourceRecord> records = new ArrayList<>();
        getRecords(records, 2, 20000);

        // The first record is an insert record with before image as null.
        SourceRecord insertRecord = records.get(0);
        assertValueField(insertRecord, "before", null);
        assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 40);

        // The second record will be an update record having a before image.
        SourceRecord updateRecord = records.get(1);
        assertBeforeImage(updateRecord, 1, "Vaibhav", "Kushwaha", 40);
        assertAfterImage(updateRecord, 1, "VKVK", "Kushwaha", 30);
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void testTransactions(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS cdctest;";
        session.execute(createKeyspace);

        session.execute("create table if not exists cdctest.test_transaction(a int primary key) WITH transactions = { 'enabled' : true };");

        String dbStreamId = TestHelper.getNewDbStreamId("cdctest", "test_transaction", false, true,
                BeforeImageMode.CHANGE, true, consistentSnapshot, useSnapshot);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderForCQL("cdctest","test_transaction", dbStreamId);
        startEngine(configBuilder);

        int recordsCount = 5;

        awaitUntilConnectorIsReady();
        executeTransaction(recordsCount, "cdctest", "test_transaction");

        verifyRecordCount(recordsCount);
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void testLargeStrings(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS cdctest;";
        session.execute(createKeyspace);

        session.execute("create table if not exists cdctest.test_big_string(a varchar primary key);");

        String dbStreamId = TestHelper.getNewDbStreamId("cdctest", "test_big_string", false, true,
                BeforeImageMode.CHANGE, true, consistentSnapshot, useSnapshot);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderForCQL("cdctest","test_big_string", dbStreamId);
        startEngine(configBuilder);

        final long recordsCount = 1;

        String bigInput = getBigInputString(5000);
        awaitUntilConnectorIsReady();

        session.execute("insert into cdctest.test_big_string(a) values ('" + bigInput + "');");
        
        verifyPrimaryKey(recordsCount, bigInput);
    }

    private void verifyRecordCount(long recordsCount) {
        waitAndFailIfCannotConsume(new ArrayList<>(), recordsCount);
    }

    private void insertRecords(int recordsCount, String keyspaceName, String tableName) {
        String formatString = "insert into " + keyspaceName + "." +tableName + "(a) values (%d);";
        for(int i=0; i<recordsCount; i++) {
            session.execute(String.format(formatString, i));
        }
    }

    private void executeTransaction (int recordsCount, String keyspaceName, String tableName) {
        session.execute("start transaction;");
        String formatString = "insert into " + keyspaceName + "." + tableName + "(a) values (%d);";
        for(int i=0; i<recordsCount; i++) {
            session.execute(String.format(formatString, i));
        }
        session.execute("commit;");
    }

    private void assertAfterImage(SourceRecord record, Integer id, String firstName, String lastName,
            int hours) {
        assertValueField(record, "after/id/value", id);
        assertValueField(record, "after/first_name/value", firstName);
        assertValueField(record, "after/last_name/value", lastName);
        assertValueField(record, "after/hours/value", hours);
    }

    private void assertBeforeImage(SourceRecord record, Integer id, String firstName, String lastName,
            int hours) {
        assertValueField(record, "before/id/value", id);
        assertValueField(record, "before/first_name/value", firstName);
        assertValueField(record, "before/last_name/value", lastName);
        assertValueField(record, "before/hours/value", hours);
    }

    private void getRecords(List<SourceRecord> records, long totalRecordsToConsume,
            long milliSecondsToWait) {
        AtomicLong totalConsumedRecords = new AtomicLong();
        long seconds = milliSecondsToWait / 1000;
        try {
            Awaitility.await()
                    .atMost(Duration.ofSeconds(seconds))
                    .until(() -> {
                        int consumed = consumeAvailableRecords(record -> {
                            LOGGER.debug("The record being consumed is " + record);
                            records.add(record);
                        });
                        if (consumed > 0) {
                            totalConsumedRecords.addAndGet(consumed);
                            LOGGER.debug("Consumed " + totalConsumedRecords + " records");
                        }

                        return totalConsumedRecords.get() == totalRecordsToConsume;
                    });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + totalRecordsToConsume + " records in " + seconds + " seconds",
                    exception);
        }

        assertEquals(totalRecordsToConsume, totalConsumedRecords.get());
    }

    private String getBigInputString(int length) {
        StringBuilder sb = new StringBuilder();
        Random r = new Random();
        for (int i = 0; i < length; i++) {
            sb.append((char) (r.nextInt(26) + 'A'));
        }
        return sb.toString();
    }

    private void verifyPrimaryKey(long recordsCount, String input) {
        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsCount);

        for (int i = 0; i < records.size(); ++i) {
            assertValueField(records.get(i), "after/a/value", input);
        }
    }
    
    static Stream<Arguments> streamTypeProviderForSnapshot(){
        return Stream.of(
                Arguments.of(false, false, "initial"), // Older stream 
                Arguments.of(false, false, "initial_only"), // Older stream 
                Arguments.of(true, true, "initial"), // USE_SNAPSHOT stream
                Arguments.of(true, true, "initial_only")); // USE_SNAPSHOT stream
    }
}
