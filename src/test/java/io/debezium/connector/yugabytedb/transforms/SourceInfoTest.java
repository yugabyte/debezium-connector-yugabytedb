package io.debezium.connector.yugabytedb.transforms;

import io.debezium.connector.yugabytedb.Module;
import io.debezium.connector.yugabytedb.SourceInfo;
import io.debezium.connector.yugabytedb.TestHelper;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;
import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class SourceInfoTest extends YugabyteDBTestBase {
    private SourceInfo source;

    private final String DUMMY_TABLE_NAME = "dummy_table";
    private final String DUMMY_TABLET_ID = "tabletId";

    @BeforeEach
    public void beforeEach() {
        source = new SourceInfo(new YugabyteDBConnectorConfig(
                TestHelper.getConfigBuilder("public." + DUMMY_TABLE_NAME, "dummyStreamId").build()
        ));
        source.update(DUMMY_TABLET_ID, OpId.valueOf("::::"), 123L, "txId",
                      new TableId("yugabyte", "public", DUMMY_TABLE_NAME), 123L, 123L);
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
        assertEquals("::::", source.struct().getString(SourceInfo.LSN_KEY));
    }
}
