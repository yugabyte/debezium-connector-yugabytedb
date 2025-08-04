package io.debezium.connector.yugabytedb;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.AbstractSourceInfoStructMaker;
import io.debezium.connector.yugabytedb.common.YugabyteDBContainerTestBase;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.data.VerifyRecord;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class SourceInfoTest extends YugabyteDBContainerTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(SourceInfoTest.class);
    private SourceInfo source;

    private final String DUMMY_TABLE_NAME = "dummy_table";
    private final String DUMMY_TABLE_ID = "tableId";
    private final String DUMMY_TABLET_ID = "tabletId";

    @BeforeAll
    public static void beforeClass() throws Exception {
        initializeYBContainer();
        TestHelper.executeDDL("yugabyte_create_tables.ddl");
    }

    @BeforeEach
    public void beforeEach() {
        try {
            String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "t1");
            source = new SourceInfo(new YugabyteDBConnectorConfig(
                    TestHelper.getConfigBuilder("public." + DUMMY_TABLE_NAME, dbStreamId).build()
            ));
        } catch (Exception e) {
            LOGGER.error("Exception while initializing the configuration builder", e);
            fail();
        }

        YBPartition partition = new YBPartition(DUMMY_TABLE_ID, DUMMY_TABLET_ID, false);

        source.update(partition, OpId.valueOf("1:2:keyStrValue:4:5"), 123L, "txId",
                      new TableId("yugabyte", "public", DUMMY_TABLE_NAME), 123L);
    }

    @AfterAll
    public static void afterClass() throws Exception {
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
        shutdownYBContainer();
    }

    @Test
    public void versionPresent() {
        assertEquals(Module.version(), source.struct().getString(SourceInfo.DEBEZIUM_VERSION_KEY));
    }

    @Test
    public void shouldHaveTableSchema() {
        assertEquals("public", source.struct().getString(SourceInfo.SCHEMA_NAME_KEY));
    }

    @Test
    public void shouldHaveTableName() {
        assertEquals(DUMMY_TABLE_NAME, source.struct().getString(SourceInfo.TABLE_NAME_KEY));
    }

    @Test
    public void shouldHaveCommitTime() {
        assertEquals(123L, source.struct().getInt64(SourceInfo.COMMIT_TIME));
    }

    @Test
    public void shouldHaveRecordTime() {
        assertEquals(123L, source.struct().getInt64(SourceInfo.RECORD_TIME));
    }

    @Test
    public void shouldHaveLsn() {
        assertEquals("1:2:keyStrValuc=:4:5", source.struct().getString(SourceInfo.LSN_KEY));
    }

    @Test
    public void isSchemaCorrect() {
        final Schema schema = SchemaBuilder.struct()
                .name("io.debezium.connector.yugabytedb.Source")
                .field(SourceInfo.DEBEZIUM_VERSION_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.DEBEZIUM_CONNECTOR_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.SERVER_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TIMESTAMP_KEY, Schema.INT64_SCHEMA)
                .field(SourceInfo.SNAPSHOT_KEY, AbstractSourceInfoStructMaker.SNAPSHOT_RECORD_SCHEMA)
                .field(SourceInfo.DATABASE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.SEQUENCE_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TXID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.COMMIT_TIME, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.RECORD_TIME, Schema.INT64_SCHEMA)
                .field(SourceInfo.TABLET_ID, Schema.STRING_SCHEMA)
                .field(SourceInfo.PARTITION_ID_KEY, Schema.STRING_SCHEMA)
                .build();

        VerifyRecord.assertConnectSchemasAreEqual(null, source.struct().schema(), schema);
    }
}
