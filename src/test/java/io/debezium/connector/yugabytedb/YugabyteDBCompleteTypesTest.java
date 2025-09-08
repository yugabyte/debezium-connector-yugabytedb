package io.debezium.connector.yugabytedb;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig.DecimalHandlingMode;
import io.debezium.util.HexConverter;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.util.Strings;

import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

public class YugabyteDBCompleteTypesTest extends YugabytedTestBase {
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
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
    }

    @AfterAll
    public static void afterClass() throws Exception {
        shutdownYBContainer();
    }

    private void consumeRecords(List<SourceRecord> records, long recordsCount) {
        int totalConsumedRecords = 0;
        long start = System.currentTimeMillis();
        while (totalConsumedRecords < recordsCount) {
            int consumed = consumeAvailableRecords(record -> {
                LOGGER.debug("The record being consumed is " + record);
                records.add(record);
            });
            if (consumed > 0) {
                totalConsumedRecords += consumed;
                LOGGER.debug("Consumed " + totalConsumedRecords + " records");
            }
        }
        LOGGER.info("Total duration to consume " + recordsCount + " records: " + Strings.duration(System.currentTimeMillis() - start));

        if (records.size() != 1) {
            throw new DebeziumException("Record count doesn't match");
        }
    }

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void verifyAllWorkingDataTypesInSingleTable(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
        Thread.sleep(1000);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "all_types", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.all_types", dbStreamId);
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        final long recordsCount = 1;

        // This insert statement will insert a row containing all types
        TestHelper.execute(HelperStrings.INSERT_ALL_TYPES);

        List<SourceRecord> records = new ArrayList<>();
        consumeRecords(records, recordsCount);

        // At this point of time, it is assumed that the list has only one record, so it is safe to get the record at index 0.
        SourceRecord record = records.get(0);
        assertValueField(record, "after/bigintcol/value", 123456);
        assertValueField(record, "after/bitcol/value", "11011");
        assertValueField(record, "after/varbitcol/value", "10101");
        assertValueField(record, "after/booleanval/value", false);
        assertValueField(record, "after/byteaval/value", ByteBuffer.wrap(HexConverter.convertFromHex("01")));
        assertValueField(record, "after/ch/value", "five5");
        assertValueField(record, "after/vchar/value", "sample_text");
        assertValueField(record, "after/cidrval/value", "10.1.0.0/16");
        assertValueField(record, "after/dt/value", 19047);
        assertValueField(record, "after/dp/value", 12.345);
        assertValueField(record, "after/inetval/value", "127.0.0.1");
        assertValueField(record, "after/intervalval/value", 2505600000000L);
        assertValueField(record, "after/jsonval/value", "{\"a\":\"b\"}");
        assertValueField(record, "after/jsonbval/value", "{\"a\": \"b\"}");
        assertValueField(record, "after/mc/value", "2c:54:91:88:c9:e3");
        assertValueField(record, "after/mc8/value", "22:00:5c:03:55:08:01:02");
        assertValueField(record, "after/mn/value", 100.50);
        assertValueField(record, "after/nm/value", 12.34);
        assertValueField(record, "after/rl/value", 32.145);
        assertValueField(record, "after/si/value", 12);
        assertValueField(record, "after/i4r/value", "[2,10)");
        assertValueField(record, "after/i8r/value", "[101,200)");
        assertValueField(record, "after/nr/value", "(10.45,21.32)");
        assertValueField(record, "after/tsr/value", "(\"1970-01-01 00:00:00\",\"2000-01-01 12:00:00\")");
        assertValueField(record, "after/tstzr/value", "(\"2017-07-04 12:30:30+00\",\"2021-07-04 07:00:30+00\")");
        assertValueField(record, "after/dr/value", "[2019-10-08,2021-10-07)");
        assertValueField(record, "after/txt/value", "text to verify behaviour");
        assertValueField(record, "after/tm/value", 46052000);
        assertValueField(record, "after/tmtz/value", "06:30:00Z");
        assertValueField(record, "after/ts/value", 1637841600123456L);
        assertValueField(record, "after/tstz/value", "2021-11-25T06:30:00Z");
        assertValueField(record, "after/uuidval/value", "ffffffff-ffff-ffff-ffff-ffffffffffff");
    }

    @ParameterizedTest
    @ValueSource(strings = {"adaptive", "adaptive_time_microseconds", "connect"})
    public void shouldEmitTimestampValuesWithCorrectPrecision(String temporalPrecisionMode) throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
        Thread.sleep(1000);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "all_types");
        Configuration.Builder configBuilder =
          TestHelper.getConfigBuilder("public.all_types", dbStreamId);
        configBuilder.with("time.precision.mode", temporalPrecisionMode);
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        final long recordsCount = 1;

        // This insert statement will insert a row containing all types
        TestHelper.execute(HelperStrings.INSERT_ALL_TYPES);

        List<SourceRecord> records = new ArrayList<>();

        consumeRecords(records, recordsCount);

        assertEquals(1, records.size());

        // Get the only record from the list.
        SourceRecord record = records.get(0);

        TemporalPrecisionMode precisionMode = TemporalPrecisionMode.parse(temporalPrecisionMode);
        if (precisionMode == TemporalPrecisionMode.ADAPTIVE
              || precisionMode == TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS) {
            assertValueField(record, "after/ts/value", 1637841600123456L);
        } else if (precisionMode == TemporalPrecisionMode.CONNECT) {
            // Note that in 'connect' mode, we have a loss of precision.
            assertValueField(record, "after/ts/value", 1637841600123L);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"precise", "double", "string"})
    public void shouldWorkWithAllDecimalTypes(String decimalMode) throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
        Thread.sleep(1000);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "numeric_type");
        Configuration.Builder configBuilder =
          TestHelper.getConfigBuilder("public.numeric_type", dbStreamId);
        configBuilder.with("decimal.handling.mode", decimalMode);
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        final long recordsCount = 1;
        
        TestHelper.execute("INSERT INTO numeric_type VALUES (1, 987654321.12345678);");

        List<SourceRecord> records = new ArrayList<>();

        consumeRecords(records, recordsCount);

        assertEquals(1, records.size());

        // Get the only record from the list.
        SourceRecord record_0 = records.get(0);

        DecimalHandlingMode decimalHandlingMode =
          DecimalHandlingMode.parse(decimalMode);
        if (decimalHandlingMode == DecimalHandlingMode.PRECISE) {
            assertValueField(record_0, "after/col_val/value", 987654321.12345678);
        } else if (decimalHandlingMode == DecimalHandlingMode.DOUBLE) {
            assertValueField(record_0, "after/col_val/value", 9.876543211234568E8);
        } else if (decimalHandlingMode == DecimalHandlingMode.STRING) {
            assertValueField(record_0, "after/col_val/value", "987654321.12345678");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"precise", "double", "string"})
    public void shouldHaveLossOfPrecisionWithDecimalModeWithLargeValue(String decimalMode) throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
        Thread.sleep(1000);

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "numeric_type");
        Configuration.Builder configBuilder =
          TestHelper.getConfigBuilder("public.numeric_type", dbStreamId);
        configBuilder.with("decimal.handling.mode", decimalMode);
        startEngine(configBuilder);

        awaitUntilConnectorIsReady();

        final long recordsCount = 1;

        // Insert a value having a large value.
        BigDecimal val = new BigDecimal("100000000000000000000000000000000000000000000000000000000000000000000000000000.123456789123456789");
        TestHelper.execute("INSERT INTO numeric_type (id, col_val_2) VALUES (1, " + val + ");");

        List<SourceRecord> records = new ArrayList<>();

        consumeRecords(records, recordsCount);

        assertEquals(1, records.size());

        // Get the only record from the list.
        SourceRecord record_0 = records.get(0);
        LOGGER.info("schema: {}", record_0.valueSchema().toString());
        LOGGER.info(record_0.value().toString());

        DecimalHandlingMode decimalHandlingMode =
          DecimalHandlingMode.parse(decimalMode);
        if (decimalHandlingMode == DecimalHandlingMode.PRECISE) {
            assertValueField(record_0, "after/col_val_2/value", 100000000000000000000000000000000000000000000000000000000000000000000000000000.123456789123456789);
        } else if (decimalHandlingMode == DecimalHandlingMode.DOUBLE) {
            // This is a loss of precision as compared to the originally inserted value.
            assertValueField(record_0, "after/col_val_2/value", 1.0E77);
        } else if (decimalHandlingMode == DecimalHandlingMode.STRING) {
            assertValueField(record_0, "after/col_val_2/value", "100000000000000000000000000000000000000000000000000000000000000000000000000000.123456789123456789");
        }
    }
}
