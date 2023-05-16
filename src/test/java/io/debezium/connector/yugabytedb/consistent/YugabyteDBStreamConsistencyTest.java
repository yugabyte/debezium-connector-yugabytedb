package io.debezium.connector.yugabytedb.consistent;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import io.debezium.connector.yugabytedb.TestHelper;
import io.debezium.connector.yugabytedb.YugabyteDBConnector;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig;

import io.debezium.connector.yugabytedb.common.YugabytedTestBase;

import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Assertions.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;

/**
 * Basic unit tests to check the behaviour with stream consistency.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */

public class YugabyteDBStreamConsistencyTest extends YugabytedTestBase {
    private final static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBStreamConsistencyTest.class);

    private static final String INSERT_STMT = "INSERT INTO s1.a (aa) VALUES (1);" +
            "INSERT INTO s2.a (aa) VALUES (1);";
    private static final String CREATE_TABLES_STMT = "DROP SCHEMA IF EXISTS s1 CASCADE;" +
            "DROP SCHEMA IF EXISTS s2 CASCADE;" +
            "CREATE SCHEMA s1; " +
            "CREATE SCHEMA s2; " +
            "CREATE TABLE s1.a (pk SERIAL, aa integer, PRIMARY KEY(pk));" +
            "CREATE TABLE s2.a (pk SERIAL, aa integer, bb varchar(20), PRIMARY KEY(pk));";
    private static final String SETUP_TABLES_STMT = CREATE_TABLES_STMT + INSERT_STMT;
    
    @BeforeAll
    public static void beforeClass() throws SQLException {
        initializeYBContainer(null, "cdc_max_stream_intent_records=10,cdc_populate_safepoint_record=true");
        TestHelper.dropAllSchemas();
    }

    @BeforeEach
    public void before() {
        initializeConnectorTestFramework();

        TestHelper.execute("DROP TABLE IF EXISTS locality;");
        TestHelper.execute("DROP TABLE IF EXISTS address;");
        TestHelper.execute("DROP TABLE IF EXISTS contract;");
        TestHelper.execute("DROP TABLE IF EXISTS employee;");
        TestHelper.execute("DROP TABLE IF EXISTS department;");
    }

    @AfterEach
    public void after() throws Exception {
        stopConnector();
        TestHelper.executeDDL("drop_tables_and_databases.ddl");
        TestHelper.execute("DROP TABLE IF EXISTS locality;");
        TestHelper.execute("DROP TABLE IF EXISTS address;");
        TestHelper.execute("DROP TABLE IF EXISTS contract;");
        TestHelper.execute("DROP TABLE IF EXISTS employee;");
        TestHelper.execute("DROP TABLE IF EXISTS department;");
    }

    @AfterAll
    public static void afterClass() throws Exception {
        shutdownYBContainer();
    }

    @Test
    public void recordsShouldStreamInConsistentOrderOnly() throws Exception {
        // Create 2 tables, refer first in the second one
        TestHelper.execute("CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT);");
        TestHelper.execute("CREATE TABLE employee (id INT PRIMARY KEY, emp_name TEXT, d_id INT);"); // , FOREIGN KEY (d_id) REFERENCES department(id))

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "department");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.department,public.employee", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.CONSISTENCY_MODE, "global");
        configBuilder.with("transforms", "Reroute");
        configBuilder.with("transforms.Reroute.type", "io.debezium.transforms.ByLogicalTableRouter");
        configBuilder.with("transforms.Reroute.topic.regex", "(.*)");
        configBuilder.with("transforms.Reroute.topic.replacement", "test_server_all_events");
        configBuilder.with("transforms.Reroute.key.field.regex", "test_server(.*)");
        configBuilder.with("transforms.Reroute.key.field.replacement", "\\$1");
        configBuilder.with("provide.transaction.metadata", true);

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
            TestHelper.execute(String.format("BEGIN; INSERT INTO department VALUES (%d, 'my department no %d'); COMMIT;", departmentId, departmentId));

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
                            LOGGER.info("The record being consumed is " + record);
                            Struct s = (Struct) record.value();
                            if (s.schema().fields().stream().map(f -> f.name()).collect(Collectors.toSet()).contains("status")) {
                                LOGGER.info("Consumed txn record: {}", s);
                                return;
                            }
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

    @Test
    public void verifyRecordOrderWithHierarchicalTables() throws Exception {
        // Create multiple tables, each having a dependency on the former one so that we can form
        // a hierarchy of FK dependencies.
        TestHelper.execute("CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT);");
        TestHelper.execute("CREATE TABLE employee (id INT PRIMARY KEY, emp_name TEXT, d_id INT, FOREIGN KEY (d_id) REFERENCES department(id));");
        TestHelper.execute("CREATE TABLE contract (id INT PRIMARY KEY, contract_name TEXT, c_id INT, FOREIGN KEY (c_id) REFERENCES employee(id));");
        TestHelper.execute("CREATE TABLE address (id INT PRIMARY KEY, area_name TEXT, a_id INT, FOREIGN KEY (a_id) REFERENCES contract(id));");
        TestHelper.execute("CREATE TABLE locality (id INT PRIMARY KEY, loc_name TEXT, l_id INT, FOREIGN KEY (l_id) REFERENCES address(id));");
        // TestHelper.execute("CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT);");
        // TestHelper.execute("CREATE TABLE employee (id INT PRIMARY KEY, emp_name TEXT, d_id INT);");
        // TestHelper.execute("CREATE TABLE contract (id INT PRIMARY KEY, contract_name TEXT, c_id INT);");
        // TestHelper.execute("CREATE TABLE address (id INT PRIMARY KEY, area_name TEXT, a_id INT);");
        // TestHelper.execute("CREATE TABLE locality (id INT PRIMARY KEY, loc_name TEXT, l_id INT);");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "department");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.department,public.employee,public.contract,public.address", dbStreamId);
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

        // If this test needs to be run more for higher duration, this scale factor can be changed
        // accordingly.
        final int scaleFactor = 1;
        final int iterations = 5 * scaleFactor;
        int departmentId = 1;
        int employeeId = 1, employeeBatchSize = 5 * scaleFactor;
        int contractId = 1, contractBatchSize = 6 * scaleFactor;
        int addressId = 1, addressBatchSize = 7 * scaleFactor;
        int localityId = 1, localityBatchSize = 8 * scaleFactor;

        // This counter will also indicate the final index of the inserted record while streaming.
        long totalCount = 0;

        // Lists to store the expected indices of the elements of respective tables in the final
        // list of messages we will be receiving after streaming.
        List<Integer> departmentIndices = new ArrayList<>();
        List<Integer> employeeIndices = new ArrayList<>();
        List<Integer> contractIndices = new ArrayList<>();
        List<Integer> addressIndices = new ArrayList<>();
        List<Integer> localityIndices = new ArrayList<>();

        for (int i = 0; i < iterations; ++i) {
            TestHelper.execute(String.format("INSERT INTO department VALUES (%d, 'my department no %d');", departmentId, departmentId));

            // Inserting the index of the record for department table at its appropriate position.
            departmentIndices.add((int) totalCount);
            ++totalCount;

            for (int j = employeeId; j <= employeeId + employeeBatchSize - 1; ++j) {
                LOGGER.info("inserting into employee with id {}", j);
                TestHelper.execute(String.format("INSERT INTO employee VALUES (%d, 'emp no %d', %d);", j, j, departmentId));
                employeeIndices.add((int) totalCount);
                ++totalCount;
                for (int k = contractId; k <= contractId + contractBatchSize - 1; ++k) {
                    LOGGER.info("inserting into contract with id {}", k);
                    TestHelper.execute(String.format("INSERT INTO contract VALUES (%d, 'contract no %d', %d);", k, k, j /* employee fKey */));
                    contractIndices.add((int) totalCount);
                    ++totalCount;

                    for (int l = addressId; l <= addressId + addressBatchSize - 1; ++l) {
                        LOGGER.info("inserting into address with id {}", l);
                        TestHelper.execute(String.format("INSERT INTO address VALUES (%d, 'address no %d', %d);", l, l, k /* contract fKey */));
                        addressIndices.add((int) totalCount);
                        ++totalCount;

                        for (int m = localityId; m <= localityId + localityBatchSize - 1; ++m) {
                            LOGGER.info("inserting into locality with id {}", m);
                            TestHelper.execute(String.format("INSERT INTO locality VALUES (%d, 'locality no %d', %d);", m, m, l /* address fKey */));
                            localityIndices.add((int) totalCount);
                            ++totalCount;
                        }
                        // Increment localityId for next iteration.
                        localityId += localityBatchSize;
                    }
                    // Increment addressId for next iteration.
                    addressId += addressBatchSize;
                }
                // Increment contractId for next iteration.
                contractId += contractBatchSize;
            }

            // Increment employeeId for the next iteration
            employeeId += employeeBatchSize;

            // Increment department ID for more iterations
            ++departmentId;
        }

        // Dummy wait
        TestHelper.waitFor(Duration.ofSeconds(25));

        List<SourceRecord> recordsToAssert = new ArrayList<>();

        final long total = totalCount;
        AtomicLong totalConsumedRecords = new AtomicLong();
        try {
            Awaitility.await()
                    .atMost(Duration.ofSeconds(600))
                    .until(() -> {
                        int consumed = super.consumeAvailableRecords(record -> {
                            LOGGER.debug("The record being consumed is " + record);
                            Struct s = (Struct) record.value();
                            recordsToAssert.add(record);
                        });
                        if (consumed > 0) {
                            totalConsumedRecords.addAndGet(consumed);
                            LOGGER.info("Consumed " + totalConsumedRecords.get() + " records");
                        }

                        return recordsToAssert.size() == total;
                    });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + totalCount + " records in 600 seconds, consumed " + totalConsumedRecords.get(), exception);
        }

        assertEquals(total, recordsToAssert.size());
        LOGGER.info("department records: {}", departmentIndices.size());
        LOGGER.info("employee records: {}", employeeIndices.size());
        LOGGER.info("contract record: {}", contractIndices.size());
        LOGGER.info("address record: {}", addressIndices.size());
        LOGGER.info("total records: {},  total records added in list for assertions: {}", totalCount, recordsToAssert.size());

        assertTableNameInIndexList(recordsToAssert, departmentIndices, "department");
        assertTableNameInIndexList(recordsToAssert, employeeIndices, "employee");
        assertTableNameInIndexList(recordsToAssert, contractIndices, "contract");
        assertTableNameInIndexList(recordsToAssert, addressIndices, "address");
        assertTableNameInIndexList(recordsToAssert, localityIndices, "locality");
    }

    @Test
    public void singleTableSingleTablet() throws Exception {
        TestHelper.execute("CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT);");

        YugabyteDBConnection c = TestHelper.create();
        Connection conn = c.connection();
        conn.setAutoCommit(false);

        final String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "department");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.department", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.CONSISTENCY_MODE, "global");
        configBuilder.with("transforms", "Reroute");
        configBuilder.with("transforms.Reroute.type", "io.debezium.transforms.ByLogicalTableRouter");
        configBuilder.with("transforms.Reroute.topic.regex", "(.*)");
        configBuilder.with("transforms.Reroute.topic.replacement", "test_server_all_events");
        configBuilder.with("transforms.Reroute.key.field.regex", "test_server.public.(.*)");
        configBuilder.with("transforms.Reroute.key.field.replacement", "\\$1");

        start(YugabyteDBConnector.class, configBuilder.build());
        awaitUntilConnectorIsReady();
        TestHelper.waitFor(Duration.ofSeconds(10));

        long totalRecords = 1_00_000;
        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            try {
                LOGGER.info("Started inserting.");
                Statement st = conn.createStatement();
                for (long i = 0; i < totalRecords; ++i) {
                    st.execute(String.format("INSERT INTO department VALUES (%d, 'my department no %d');", i, i));
                    conn.commit();
                }
            } catch (Exception e) {
                LOGGER.error("Exception caught: ", e);
                throw new RuntimeException(e);
            }
        });

        final long total = totalRecords;
        List<SourceRecord> recordsToAssert = new ArrayList<>();
        AtomicLong totalConsumedRecords = new AtomicLong();
        final int seconds = 900;
        try {
            LOGGER.info("Started consuming");
            Awaitility.await()
              .atMost(Duration.ofSeconds(seconds))
              .until(() -> {
                  int consumed = super.consumeAvailableRecords(record -> {
                      LOGGER.debug("The record being consumed is " + record);
                      recordsToAssert.add(record);
                  });
                  if (consumed > 0) {
                      totalConsumedRecords.addAndGet(consumed);
                      LOGGER.info("Consumed " + totalConsumedRecords.get() + " records");
                  }

                  return recordsToAssert.size() == total;
              });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + totalRecords + " records in " + seconds + " seconds, consumed only " + totalConsumedRecords.get(), exception);
        }

        // Verify the consumed records now.
        long expectedId = 0;
        for (int i = 0; i < recordsToAssert.size(); ++i) {
            Struct value = (Struct) recordsToAssert.get(i).value();
            long id = value.getStruct("after").getStruct("id").getInt32("value");

            assertEquals("Expected id " + expectedId + " but got id " + id + " at index " + i, expectedId, id);
            ++expectedId;
        }
    }

    @Test
    public void singleTableTwoTablet() throws Exception {
        TestHelper.execute("CREATE TABLE department (id INT, dept_name TEXT, serial_no INT, PRIMARY KEY (id ASC)) SPLIT AT VALUES ((500000));");

        YugabyteDBConnection c = TestHelper.create();
        Connection conn = c.connection();
        conn.setAutoCommit(false);

        final String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "department");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.department", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.CONSISTENCY_MODE, "global");
        configBuilder.with("transforms", "Reroute");
        configBuilder.with("transforms.Reroute.type", "io.debezium.transforms.ByLogicalTableRouter");
        configBuilder.with("transforms.Reroute.topic.regex", "(.*)");
        configBuilder.with("transforms.Reroute.topic.replacement", "test_server_all_events");
        configBuilder.with("transforms.Reroute.key.field.regex", "test_server.public.(.*)");
        configBuilder.with("transforms.Reroute.key.field.replacement", "\\$1");

        start(YugabyteDBConnector.class, configBuilder.build());
        awaitUntilConnectorIsReady();
        TestHelper.waitFor(Duration.ofSeconds(10));

        long totalRecords = 1_00_000;
        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            try {
                LOGGER.info("Started inserting.");
                final long delta = 5_00_000;
                long serialNo = 0;
                Statement st = conn.createStatement();
                for (long i = 0; i < totalRecords / 2; ++i) {
                    st.execute(String.format("INSERT INTO department VALUES (%d, 'my department no %d', %d);", i, i, serialNo));
                    ++serialNo;
                    st.execute(String.format("INSERT INTO department VALUES (%d, 'my department no %d', %d);", i + delta, i + delta, serialNo));
                    ++serialNo;
                    conn.commit();
                }
            } catch (Exception e) {
                LOGGER.error("Exception caught: ", e);
                throw new RuntimeException(e);
            }
        });

        final long total = totalRecords;
        List<SourceRecord> recordsToAssert = new ArrayList<>();
        AtomicLong totalConsumedRecords = new AtomicLong();
        final int seconds = 900;
        try {
            LOGGER.info("Started consuming");
            Awaitility.await()
              .atMost(Duration.ofSeconds(seconds))
              .until(() -> {
                  int consumed = super.consumeAvailableRecords(record -> {
                      LOGGER.debug("The record being consumed is " + record);
                      recordsToAssert.add(record);
                  });
                  if (consumed > 0) {
                      totalConsumedRecords.addAndGet(consumed);
                      LOGGER.info("Consumed " + totalConsumedRecords.get() + " records");
                  }

                  return recordsToAssert.size() == total;
              });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + totalRecords + " records in " + seconds + " seconds, consumed only " + totalConsumedRecords.get(), exception);
        }

        // Verify the consumed records now.
        long expectedSerial = 0;
        for (int i = 0; i < recordsToAssert.size(); ++i) {
            Struct value = (Struct) recordsToAssert.get(i).value();
            long serialNo = value.getStruct("after").getStruct("serial_no").getInt32("value");

            assertEquals("Expected serial " + expectedSerial + " but got serial " + serialNo + " at index " + i, expectedSerial, serialNo);
            ++expectedSerial;
        }
    }

    @Test
    public void singleTableSingleTabletTwoRecord() throws Exception {
        TestHelper.execute("CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT);");

        YugabyteDBConnection c = TestHelper.create();
        Connection conn = c.connection();
        conn.setAutoCommit(false);

        final String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "department");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.department", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.CONSISTENCY_MODE, "global");
        configBuilder.with("transforms", "Reroute");
        configBuilder.with("transforms.Reroute.type", "io.debezium.transforms.ByLogicalTableRouter");
        configBuilder.with("transforms.Reroute.topic.regex", "(.*)");
        configBuilder.with("transforms.Reroute.topic.replacement", "test_server_all_events");
        configBuilder.with("transforms.Reroute.key.field.regex", "test_server.public.(.*)");
        configBuilder.with("transforms.Reroute.key.field.replacement", "\\$1");

        start(YugabyteDBConnector.class, configBuilder.build());
        awaitUntilConnectorIsReady();
        TestHelper.waitFor(Duration.ofSeconds(10));

        long totalRecords = 1_00_000;
        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            try {
                LOGGER.info("Started inserting.");
                Statement st = conn.createStatement();
                for (long i = 0; i < totalRecords; i += 2) {
                    st.execute(String.format("INSERT INTO department VALUES (%d, 'my department no %d');", i, i));
                    st.execute(String.format("INSERT INTO department VALUES (%d, 'my department no %d');", i+1, i+1));
                    conn.commit();
                }
            } catch (Exception e) {
                LOGGER.error("Exception caught: ", e);
                throw new RuntimeException(e);
            }
        });

        final long total = totalRecords;
        List<SourceRecord> recordsToAssert = new ArrayList<>();
        AtomicLong totalConsumedRecords = new AtomicLong();
        final int seconds = 900;
        try {
            LOGGER.info("Started consuming");
            Awaitility.await()
              .atMost(Duration.ofSeconds(seconds))
              .until(() -> {
                  int consumed = super.consumeAvailableRecords(record -> {
                      LOGGER.debug("The record being consumed is " + record);
                      recordsToAssert.add(record);
                  });
                  if (consumed > 0) {
                      totalConsumedRecords.addAndGet(consumed);
                      LOGGER.info("Consumed " + totalConsumedRecords.get() + " records");
                  }

                  return recordsToAssert.size() == total;
              });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + totalRecords + " records in " + seconds + " seconds, consumed only " + totalConsumedRecords.get(), exception);
        }

        // Verify the consumed records now.
        long expectedId = 0;
        for (int i = 0; i < recordsToAssert.size(); ++i) {
            Struct value = (Struct) recordsToAssert.get(i).value();
            long id = value.getStruct("after").getStruct("id").getInt32("value");

            assertEquals("Expected id " + expectedId + " but got id " + id + " at index " + i, expectedId, id);
            ++expectedId;
        }
    }

    @Test
    public void twoTableWithSingleTabletEach() throws Exception {
        TestHelper.execute("CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT);");
        TestHelper.execute("CREATE TABLE employee (id INT PRIMARY KEY, emp_name TEXT, d_id INT);");
//        TestHelper.execute("CREATE TABLE employee (id INT PRIMARY KEY, emp_name TEXT, d_id INT, FOREIGN KEY (d_id) REFERENCES department(id));");

        YugabyteDBConnection c = TestHelper.create();
        Connection conn = c.connection();

        final String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "department");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.department", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.CONSISTENCY_MODE, "global");
        configBuilder.with("transforms", "Reroute");
        configBuilder.with("transforms.Reroute.type", "io.debezium.transforms.ByLogicalTableRouter");
        configBuilder.with("transforms.Reroute.topic.regex", "(.*)");
        configBuilder.with("transforms.Reroute.topic.replacement", "test_server_all_events");
        configBuilder.with("transforms.Reroute.key.field.regex", "test_server.public.(.*)");
        configBuilder.with("transforms.Reroute.key.field.replacement", "\\$1");

        start(YugabyteDBConnector.class, configBuilder.build());
        awaitUntilConnectorIsReady();
        TestHelper.waitFor(Duration.ofSeconds(10));

        long totalRecords = 1_00_000;
        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            try {
                LOGGER.info("Started inserting.");
                Statement st = conn.createStatement();
                for (long i = 0; i < totalRecords; ++i) {
                    st.execute(String.format("BEGIN; INSERT INTO department VALUES (%d, 'my department no %d'); COMMIT;", i, i));
                    st.execute(String.format("BEGIN; INSERT INTO employee VALUES (%d, 'emp no %d', %d); COMMIT;", i, i, i));
                }
            } catch (Exception e) {
                LOGGER.error("Exception caught: ", e);
                throw new RuntimeException(e);
            }
        });

        final long total = 2 * totalRecords; // There are 2 tables each having totalRecords count.
        List<SourceRecord> recordsToAssert = new ArrayList<>();
        AtomicLong totalConsumedRecords = new AtomicLong();
        final int seconds = 900;
        try {
            LOGGER.info("Started consuming");
            Awaitility.await()
              .atMost(Duration.ofSeconds(seconds))
              .until(() -> {
                  int consumed = super.consumeAvailableRecords(record -> {
                      LOGGER.debug("The record being consumed is " + record);
                      recordsToAssert.add(record);
                  });
                  if (consumed > 0) {
                      totalConsumedRecords.addAndGet(consumed);
                      LOGGER.info("Consumed " + totalConsumedRecords.get() + " records");
                  }

                  return recordsToAssert.size() == total;
              });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + total + " records in " + seconds + " seconds, consumed only " + totalConsumedRecords.get(), exception);
        }

        // Verify the consumed records now.
        long expectedId = 0;
        for (int i = 0; i < recordsToAssert.size(); ++i) {
            Struct value = (Struct) recordsToAssert.get(i).value();
            long id = value.getStruct("after").getStruct("id").getInt32("value");

            assertEquals("Expected id " + expectedId + " but got id " + id + " at index " + i, expectedId, id);
            if (i % 2 != 0) {
                // There will be 2 records having the same ID, one belonging to the table department
                // and the other belonging to the table employee. The records for table employee should
                // be at the indices with odd numbers and that is where we should be incrementing
                // the expected value. Also verify at this stage that the table is employee.
                ++expectedId;

                final String recordTableName = value.getStruct("source").getString("table");
                assertEquals("Table name not matching at index " + i + "expected employee got " + recordTableName, "employee", recordTableName);
            }
        }
    }

    private void assertTableNameInIndexList(List<SourceRecord> sourceRecords, List<Integer> indicesList, String tableName) {
        for (int index : indicesList) {
            SourceRecord record = sourceRecords.get(index);
            Struct s = (Struct) record.value();
            assertEquals(tableName, s.getStruct("source").getString("table"));
        }
    }
}
