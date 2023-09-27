package io.debezium.connector.yugabytedb;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.HelperBeforeImageModes.BeforeImageMode;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.*;

import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class YugabyteDBCQLTest extends YugabytedTestBase/*YugabyteDBContainerTestBase*/ {
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

    @Test
    public void testRecordConsumption() throws Exception {

        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS cdctest;";
        session.execute(createKeyspace);

        session.execute("create table if not exists cdctest.test_cdc(a int primary key, b varchar, c text);");

        String dbStreamId = TestHelper.getNewDbStreamId("cdctest", "test_cdc", false, false,BeforeImageMode.CHANGE, true);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderForCQL("cdctest","test_cdc", dbStreamId);
        startEngine(configBuilder);

        final long recordsCount = 4;


        awaitUntilConnectorIsReady();

        session.execute("insert into cdctest.test_cdc(a,b,c) values (2,'abc','def');");
        session.execute("update cdctest.test_cdc set b = 'cde' where a = 2;");
        session.execute("delete from cdctest.test_cdc where a = 2;");
        
        verifyRecordCount(recordsCount);

    }

    @Test
    public void testBeforeImageWithCQL() throws Exception {
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS cdctest;";
        session.execute(createKeyspace);

        session.execute("create table if not exists cdctest.t1(id INT PRIMARY KEY, first_name TEXT, last_name VARCHAR, hours int);");

        String dbStreamId = TestHelper.getNewDbStreamId("cdctest", "t1", true, true, BeforeImageMode.ALL, true);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilderForCQL("cdctest", "t1", dbStreamId);
        
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        session.execute("INSERT INTO cdctest.t1(id,first_name,last_name,hours) values(1,'Vaibhav', 'Kushwaha', 40);");
        session.execute("UPDATE cdctest.t1 SET first_name='VKVK', hours=30 WHERE id = 1;");

        // Consume the records and verify that the records should have the relevant information.
        List<SourceRecord> records = new ArrayList<>();
        CompletableFuture.runAsync(() -> getRecords(records, 2, 20000)).get();

        // The first record is an insert record with before image as null.
        SourceRecord insertRecord = records.get(0);
        assertValueField(insertRecord, "before", null);
        assertAfterImage(insertRecord, 1, "Vaibhav", "Kushwaha", 40);

        // The second record will be an update record having a before image.
        SourceRecord updateRecord = records.get(1);
        assertBeforeImage(updateRecord, 1, "Vaibhav", "Kushwaha", 40);
        assertAfterImage(updateRecord, 1, "VKVK", "Kushwaha", 30);
    }

    @Test
    public void testIncorrectQLType() throws Exception{
        String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS cdctest;";
        session.execute(createKeyspace);

        session.execute("create table if not exists cdctest.t1(id INT PRIMARY KEY, first_name TEXT, last_name VARCHAR, hours int);");

        String dbStreamId = TestHelper.getNewDbStreamId("cdctest", "t1", true, true, BeforeImageMode.ALL, true);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilderForCQL("cdctest", "t1", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.QL_TYPE, "CassandraQL");
        startEngine(configBuilder);
        assertThrows(Exception.class, () -> awaitUntilConnectorIsReady());
    }

    private void verifyRecordCount(long recordsCount) {
        waitAndFailIfCannotConsume(new ArrayList<>(), recordsCount);
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

}
