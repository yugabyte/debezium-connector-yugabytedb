package io.debezium.connector.yugabytedb;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Strings;

/**
 * Basic unit tests to check the behaviour with YugabyteDB datatypes
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */

public class YugabyteDBDatatypesTest extends AbstractConnectorTest {
    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
            "INSERT INTO s2.a (aa) VALUES (1);";
    private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "DROP SCHEMA IF EXISTS s2 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));";
    private static final String SETUP_TABLES_STMT = CREATE_TABLES_STMT + INSERT_STMT;
    private YugabyteDBConnector connector;

    private CompletableFuture<Void> insertRecords(long numOfRowsToBeInserted) {
        String formatInsertString = "INSERT INTO t1 VALUES (%d, 'Vaibhav', 'Kushwaha', 30);";
        return CompletableFuture.runAsync(() -> {
            for (int i = 0; i < numOfRowsToBeInserted; i++) {
                TestHelper.execute(String.format(formatInsertString, i));
            }

        }).exceptionally(throwable -> {
            throw new RuntimeException(throwable);
        });
    }

    private CompletableFuture<Void> insertRecordsInSchema(long numOfRowsToBeInserted) {
        String formatInsertString = "INSERT INTO test_schema.table_in_schema VALUES (%d, 'Vaibhav', " +
                "'Kushwaha', 30);";
        return CompletableFuture.runAsync(() -> {
            for (int i = 0; i < numOfRowsToBeInserted; i++) {
                TestHelper.execute(String.format(formatInsertString, i));
            }
        }).exceptionally(throwable -> {
            throw new RuntimeException(throwable);
        });
    }

    private void verifyPrimaryKeyOnly(long recordsCount) {
        System.out.println("verifyPrimaryKeyOnly ");
        int totalConsumedRecords = 0;
        long start = System.currentTimeMillis();
        List<SourceRecord> records = new ArrayList<>();
        while (totalConsumedRecords < recordsCount) {
            int consumed = super.consumeAvailableRecords(record -> {
                System.out.println("The record being consumed is " + record);
                records.add(record);
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
                System.out.println("Consumed " + totalConsumedRecords + " records");
            }
        }
        System.out.println("Total duration to ingest '" + recordsCount + "' records: " +
                Strings.duration(System.currentTimeMillis() - start));

        for (int i = 0; i < records.size(); ++i) {
            // verify the records
            assertInsert(records.get(i), "id", i);
        }

    }

    private void verifyValue(long recordsCount) {
        int totalConsumedRecords = 0;
        long start = System.currentTimeMillis();
        List<SourceRecord> records = new ArrayList<>();
        while (totalConsumedRecords < recordsCount) {
            int consumed = super.consumeAvailableRecords(record -> {
                System.out.println("The record being consumed is " + record);
                records.add(record);
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
            }
        }
        System.out.println("Total duration to ingest '" + recordsCount + "' records: " +
                Strings.duration(System.currentTimeMillis() - start));

        try {
            for (int i = 0; i < records.size(); ++i) {
                System.out.println(String.format("Verifying record values with pk: <id = %d>", i));
                assertValueField(records.get(i), "after/id", i);
                assertValueField(records.get(i), "after/first_name", "Vaibhav");
                assertValueField(records.get(i), "after/last_name", "Kushwaha");
            }
        }
        catch (Exception e) {
            System.out.println("Exception caught while parsing records: " + e);
            fail();
        }
    }

    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @Before
    public void before() {
        initializeConnectorTestFramework();
    }

    @After
    public void after() throws Exception {
        System.out.println("Stopping Connector");
        stopConnector();
        System.out.println("Stopped Connector");
        TestHelper.executeDDL("drop_postgres_tables.ddl");
    }

    @Test
    public void testRecordConsumption() throws Exception {
        System.out.println("testRecordConsumption");
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        Thread.sleep(1000);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "all_types");
        System.out.println("DB Stream Id is " + dbStreamId);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();
        final long recordsCount = 1;

        System.out.println("testRecordConsumption");
        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        insertRecords(recordsCount);
        System.out.println("testRecordConsumption");

        CompletableFuture.runAsync(() -> verifyPrimaryKeyOnly(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }

    @Test
    public void testSmallLoad() throws Exception {
        System.out.println("testSmallLoad");

        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        Thread.sleep(1000);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "all_types");
        System.out.println("DB Stream Id is " + dbStreamId);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        start(YugabyteDBConnector.class, configBuilder.build());

        assertConnectorIsRunning();
        final long recordsCount = 5;

        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        insertRecords(recordsCount);
        CompletableFuture.runAsync(() -> verifyPrimaryKeyOnly(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }

    @Test
    public void testVerifyValue() throws Exception {
        System.out.println("testVerifyValue");

        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        Thread.sleep(1000);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "all_types");
        System.out.println("DB Stream Id is " + dbStreamId);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        start(YugabyteDBConnector.class, configBuilder.build());

        assertConnectorIsRunning();
        final long recordsCount = 1;

        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        insertRecords(recordsCount);

        CompletableFuture.runAsync(() -> verifyValue(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }

    @Test
    public void testNonPublicSchema() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("tables_in_non_public_schema.ddl");
        Thread.sleep(1000);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "all_types");
        System.out.println("DB Stream Id is " + dbStreamId);

        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.t1", dbStreamId);
        start(YugabyteDBConnector.class, configBuilder.build());
        assertConnectorIsRunning();

        final long recordsCount = 1;

        // insert rows in the table t1 with values <some-pk, 'Vaibhav', 'Kushwaha', 30>
        insertRecordsInSchema(recordsCount);

        CompletableFuture.runAsync(() -> verifyValue(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }
}
