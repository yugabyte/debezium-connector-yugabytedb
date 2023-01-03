package io.debezium.connector.yugabytedb;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.YugabyteYSQLContainer;

import io.debezium.config.Configuration;
import io.debezium.connector.yugabytedb.common.YugabyteDBTestBase;

/**
 * Basic unit tests to check the behaviour with stream consistency.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */

public class YugabyteDBStreamConsistencyTest extends YugabyteDBTestBase {
    private final static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBStreamConsistencyTest.class);
    private static YugabyteYSQLContainer ybContainer;

    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
            "INSERT INTO s2.a (aa) VALUES (1);";
    private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "DROP SCHEMA IF EXISTS s2 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));";
    private static final String SETUP_TABLES_STMT = CREATE_TABLES_STMT + INSERT_STMT;
    @BeforeClass
    public static void beforeClass() throws SQLException {
//        ybContainer = TestHelper.getYbContainer();
//        ybContainer.start();

//        TestHelper.setContainerHostPort(ybContainer.getHost(), ybContainer.getMappedPort(5433));
//        TestHelper.setMasterAddress(ybContainer.getHost() + ":" + ybContainer.getMappedPort(7100));
        TestHelper.dropAllSchemas();
    }

    @Before
    public void before() {
        initializeConnectorTestFramework();

        TestHelper.execute("DROP TABLE IF EXISTS employee;");
        TestHelper.execute("DROP TABLE IF EXISTS department;");
    }

    @After
    public void after() throws Exception {
        stopConnector();
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
        TestHelper.execute("DROP TABLE employee;");
        TestHelper.execute("DROP TABLE department;");
    }

    @AfterClass
    public static void afterClass() throws Exception {
//        ybContainer.stop();
    }

    @Test
    public void recordsShouldStreamInConsistentOrderOnly() throws Exception {
        // Create 2 tables, refer first in the second one
        TestHelper.execute("CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT);");
        TestHelper.execute("CREATE TABLE employee (id INT PRIMARY KEY, emp_name TEXT, d_id INT, FOREIGN KEY (d_id) REFERENCES department(id));");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "department");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.department,public.employee", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.CONSISTENCY_MODE, "global");
        configBuilder.with("transforms", "Reroute");
        configBuilder.with("transforms.Reroute.type", "io.debezium.transforms.ByLogicalTableRouter");
        configBuilder.with("transforms.Reroute.topic.regex", "(.*)");
        configBuilder.with("transforms.Reroute.topic.replacement", "test_server_all_events");
        configBuilder.with("transforms.Reroute.key.field.regex", "test_server.public.(.*)");
        configBuilder.with("transforms.Reroute.key.field.replacement", "\\$1");

        start(YugabyteDBConnector.class, configBuilder.build());
        awaitUntilConnectorIsReady();

        TestHelper.waitFor(Duration.ofSeconds(25));

        final int iterations = 10;
        final int batchSize = 100;
        int departmentId = -1;
        long totalCount = 0;
        int beginKey = 1;
        int endKey = beginKey + batchSize - 1;
        List<Integer> indicesOfParentAdditions = new ArrayList<>();
        for (int i = 0; i < iterations; ++i) {
            // Insert records into the first table
            TestHelper.execute(String.format("INSERT INTO department VALUES (%d, 'my department no %d');", departmentId, departmentId));

            // Hack to add the indices of the required records
            indicesOfParentAdditions.add((int) totalCount);

            // Insert records into the second table
            TestHelper.execute(String.format("INSERT INTO employee VALUES (generate_series(%d,%d), 'gs emp name', %d);", beginKey, endKey, departmentId));

            // Change department ID for next iteration
            --departmentId;

            beginKey = endKey + 1;
            endKey = beginKey + batchSize - 1;

            // Every iteration will insert (batchSize + 1) records
            totalCount += batchSize /* batch to employee */ + 1 /* single insert to department */;
        }

        // Dummy wait
        TestHelper.waitFor(Duration.ofSeconds(25));

        List<SourceRecord> duplicateRecords = new ArrayList<>();
        List<SourceRecord> recordsToAssert = new ArrayList<>();

        Set<Integer> recordPkSet = new HashSet<>();

        final long total = totalCount;
        AtomicLong totalConsumedRecords = new AtomicLong();
        try {
            Awaitility.await()
                    .atMost(Duration.ofSeconds(600))
                    .until(() -> {
                        int consumed = super.consumeAvailableRecords(record -> {
                            LOGGER.debug("The record being consumed is " + record);
                            Struct s = (Struct) record.value();
                            int id = s.getStruct("after").getStruct("id").getInt32("value");

                            if (recordPkSet.contains(id)) {
                                duplicateRecords.add(record);
                            } else {
                                recordsToAssert.add(record);
                            }

                            recordPkSet.add(id);
                        });
                        if (consumed > 0) {
                            totalConsumedRecords.addAndGet(consumed);
                            LOGGER.info("Consumed " + totalConsumedRecords.get() + " records");
                        }

                        return recordPkSet.size() == total;
                    });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + totalCount + " records in 600 seconds, consumed " + totalConsumedRecords.get(), exception);
        }

        LOGGER.info("Found {} duplicate records while streaming", duplicateRecords.size());

        // This will print the indices of the records signifying to the department table which are also present in the list
        // indicesOfParentTabletAdditions - but having this log will help in debugging in case the test fails.
        for (int i = 0; i < recordsToAssert.size(); ++i) {
            SourceRecord record = recordsToAssert.get(i);
            Struct s = (Struct) record.value();
            if (s.getStruct("source").getString("table").equals("department")) {
                LOGGER.info("department table record found at index: {}", i);
            }
        }

        for (int index : indicesOfParentAdditions) {
            LOGGER.info("Asserting department table record at index {}", index);
            Struct s = (Struct) recordsToAssert.get(index).value();
            assertEquals("department", s.getStruct("source").getString("table"));
        }

        assertEquals(totalCount, recordPkSet.size());
    }
}
