package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

/**
 * Unit tests to verify the streaming of ENUM values.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBEnumValuesTest extends YugabytedTestBase {
    
    @BeforeAll
    public static void beforeClass() throws SQLException {
        initializeYBContainer();
        TestHelper.dropAllSchemas();
    }

    @BeforeEach
    public void before() throws Exception {
        initializeConnectorTestFramework();
        TestHelper.dropAllSchemas();
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

    @ParameterizedTest
    @MethodSource("io.debezium.connector.yugabytedb.TestHelper#streamTypeProviderForStreaming")
    public void testEnumValue(boolean consistentSnapshot, boolean useSnapshot) throws Exception {
        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "test_enum", consistentSnapshot, useSnapshot);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.test_enum", dbStreamId);
        startEngine(configBuilder);

        // 3 because there are 3 enum values in the enum type
        final long recordsCount = 3;

        awaitUntilConnectorIsReady();

        // 3 records will be inserted in the table test_enum
        insertEnumRecords();

        verifyEnumValue(recordsCount);
    }

    // This function will one row each of the specified enum labels
    private void insertEnumRecords() throws Exception {
        String[] enumLabels = {"ZERO", "ONE", "TWO"};
        String formatInsertString = "INSERT INTO test_enum VALUES (%d, '%s');";
        for (int i = 0; i < enumLabels.length; i++) {
            TestHelper.execute(String.format(formatInsertString, i, enumLabels[i]));
        }
    }

    private void verifyEnumValue(long recordsCount) {
        List<SourceRecord> records = new ArrayList<>();
        waitAndFailIfCannotConsume(records, recordsCount, 2 * 60 * 1000 /* 2 minutes */);
        
        String[] enum_val = {"ZERO", "ONE", "TWO"};

        try {
            for (int i = 0; i < records.size(); ++i) {
                assertValueField(records.get(i), "after/id/value", i);
                assertValueField(records.get(i), "after/enum_col/value", enum_val[i]);
            }
        } catch (Exception e) {
            LOGGER.error("Exception caught while parsing records: " + e);
            fail();
        }
    }
}
