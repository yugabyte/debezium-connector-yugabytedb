package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.util.Strings;

public class YugabyteDBCompleteTypes extends AbstractConnectorTest {
    @BeforeClass
    public static void beforeClass() throws SQLException {
        TestHelper.dropAllSchemas();
    }

    @Before
    public void before() {
        initializeConnectorTestFramework();
    }

    @After
    public void after() {
        stopConnector();
    }

    protected Configuration.Builder getConfigBuilder(String fullTablenameWithSchema) throws Exception {
        return TestHelper.defaultConfig()
                .with(YugabyteDBConnectorConfig.HOSTNAME, "127.0.0.1") // this field is required as of now
                .with(YugabyteDBConnectorConfig.PORT, 5433)
                .with(YugabyteDBConnectorConfig.SNAPSHOT_MODE, YugabyteDBConnectorConfig.SnapshotMode.NEVER.getValue())
                .with(YugabyteDBConnectorConfig.DELETE_STREAM_ON_STOP, Boolean.TRUE)
                .with(YugabyteDBConnectorConfig.MASTER_ADDRESSES, "127.0.0.1:7100")
                .with(YugabyteDBConnectorConfig.TABLE_INCLUDE_LIST, fullTablenameWithSchema)
                .with(YugabyteDBConnectorConfig.AUTO_CREATE_STREAM, true);
    }

    private void consumeRecords(long recordsCount) {
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

        if (records.size() != 1) {
            throw new DebeziumException("Record count doesn't match");
        }

        // todo: make these assertions inside a for loop
        // At this point of time, it is assumed that the list has only one record, so it is safe to get the record at index 0.
        SourceRecord record = records.get(0);
        assertValueField(record, "after/id", 404);
        assertValueField(record, "after/bigintcol", 123456);
        assertValueField(record, "after/bitcol", "11011");
        assertValueField(record, "after/varbitcol", "10101");
        assertValueField(record, "after/booleanval", false);
        assertValueField(record, "after/byteaval", "\\x01");
        assertValueField(record, "after/ch", "five5");
        assertValueField(record, "after/vchar", "sample_text");
        assertValueField(record, "after/cidrval", "10.1.0.0/16");
        assertValueField(record, "after/dt", 19047);
        assertValueField(record, "after/dp", 12.345);
        assertValueField(record, "after/inetval", "127.0.0.1");
        assertValueField(record, "after/intervalval", 2505600000000L);
        assertValueField(record, "after/jsonval", "{\"a\":\"b\"}");
        assertValueField(record, "after/jsonbval", "{\"a\": \"b\"}");
        assertValueField(record, "after/mc", "2c:54:91:88:c9:e3");
        assertValueField(record, "after/mc8", "22:00:5c:03:55:08:01:02");
        assertValueField(record, "after/mn", 100.50);
        assertValueField(record, "after/nm", 12.34);
        assertValueField(record, "after/rl", 32.145);
        assertValueField(record, "after/si", 12);
        assertValueField(record, "after/i4r", "[2,10)");
        assertValueField(record, "after/i8r", "[101,200)");
        assertValueField(record, "after/nr", "(10.45,21.32)");
        assertValueField(record, "after/tsr", "(\"1970-01-01 00:00:00\",\"2000-01-01 12:00:00\")");
        assertValueField(record, "after/tstzr", "(\"2017-07-04 12:30:30+00\",\"2021-07-04 07:00:30+00\")");
        assertValueField(record, "after/dr", "[2019-10-08,2021-10-07)");
        assertValueField(record, "after/txt", "text to verify behaviour");
        assertValueField(record, "after/tm", 46052000);
        assertValueField(record, "after/tmtz", "06:30:00Z");
        assertValueField(record, "after/ts", 1637841600000L);
        assertValueField(record, "after/tstz", "2021-11-25T06:30:00Z");
        assertValueField(record, "after/uuidval", "ffffffff-ffff-ffff-ffff-ffffffffffff");

    }

    @Test
    public void verifyAllWorkingDataTypesInSingleTable() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("postgres_create_tables.ddl");
        Thread.sleep(1000);

        Configuration.Builder configBuilder = getConfigBuilder("public.all_types");
        start(YugabyteDBConnector.class, configBuilder.build());

        assertConnectorIsRunning();

        final long recordsCount = 1;

        // This insert statement will insert a row containing all types
        TestHelper.execute(HelperStrings.INSERT_ALL_TYPES);

        CompletableFuture.runAsync(() -> consumeRecords(recordsCount))
                .exceptionally(throwable -> {
                    throw new RuntimeException(throwable);
                }).get();
    }
}
