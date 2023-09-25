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

        Configuration.Builder configBuilder = TestHelper.getConfigBuilderForCQL("cdctest","cdctest.test_cdc", dbStreamId);
        startEngine(configBuilder);

        final long recordsCount = 4;


        awaitUntilConnectorIsReady();

        session.execute("insert into cdctest.test_cdc(a,b,c) values (2,'abc','def');");
        session.execute("update cdctest.test_cdc set b = 'cde' where a = 2;");
        session.execute("delete from cdctest.test_cdc where a = 2;");
        
        verifyRecordCount(recordsCount);

    }

    private void verifyRecordCount(long recordsCount) {
        waitAndFailIfCannotConsume(new ArrayList<>(), recordsCount);
    }


}
