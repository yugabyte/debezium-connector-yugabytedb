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
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import io.debezium.connector.yugabytedb.TestHelper;
import io.debezium.connector.yugabytedb.YugabyteDBConnector;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig;
import io.debezium.connector.yugabytedb.common.YugabytedTestBase;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.*;
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
    
    @BeforeAll
    public static void beforeClass() throws SQLException {
        initializeYBContainer(null, "cdc_max_stream_intent_records=100,cdc_populate_safepoint_record=true");
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

        startEngine(configBuilder);
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
                        int consumed = consumeAvailableRecords(record -> {
                            LOGGER.info("The record being consumed is " + record);
                            Struct s = (Struct) record.value();
                            if (s.schema().fields().stream().map(Field::name).collect(Collectors.toSet()).contains("status")) {
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
    public void fiveTablesWithForeignKeys() throws Exception {
        // Create multiple tables, each having a dependency on the former one so that we can form
        // a hierarchy of FK dependencies.
        TestHelper.execute("CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT, serial_no INT) SPLIT INTO 1 TABLETS;");
        TestHelper.execute("CREATE TABLE employee (id INT PRIMARY KEY, emp_name TEXT, d_id INT, serial_no INT, FOREIGN KEY (d_id) REFERENCES department(id)) SPLIT INTO 1 TABLETS;");
        TestHelper.execute("CREATE TABLE contract (id INT PRIMARY KEY, contract_name TEXT, c_id INT, serial_no INT, FOREIGN KEY (c_id) REFERENCES employee(id)) SPLIT INTO 1 TABLETS;");
        TestHelper.execute("CREATE TABLE address (id INT PRIMARY KEY, area_name TEXT, a_id INT, serial_no INT, FOREIGN KEY (a_id) REFERENCES contract(id)) SPLIT INTO 1 TABLETS;");
        TestHelper.execute("CREATE TABLE locality (id INT PRIMARY KEY, loc_name TEXT, l_id INT, serial_no INT, FOREIGN KEY (l_id) REFERENCES address(id)) SPLIT INTO 1 TABLETS;");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "department", false, true);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.department,public.employee,public.contract,public.address,public.locality", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.CONSISTENCY_MODE, "global");
        configBuilder.with("transforms", "Reroute");
        configBuilder.with("transforms.Reroute.type", "io.debezium.transforms.ByLogicalTableRouter");
        configBuilder.with("transforms.Reroute.topic.regex", "(.*)");
        configBuilder.with("transforms.Reroute.topic.replacement", "test_server_all_events");
        configBuilder.with("transforms.Reroute.key.field.regex", "test_server.public.(.*)");
        configBuilder.with("transforms.Reroute.key.field.replacement", "\\$1");

        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        TestHelper.waitFor(Duration.ofSeconds(25));

        YugabyteDBConnection ybConn = TestHelper.create();
        Connection conn = ybConn.connection();
        conn.setAutoCommit(false);

        // If this test needs to be run more for higher duration, this scale factor can be changed
        // accordingly.
        final int scaleFactor = 1;
        final int iterations = 10 * scaleFactor;
        int employeeBatchSize = 5 * scaleFactor;
        int contractBatchSize = 6 * scaleFactor;
        int addressBatchSize = 7 * scaleFactor;
        int localityBatchSize = 8 * scaleFactor;

        // This counter will also indicate the final index of the inserted record while streaming.
        long totalCount = iterations /* department */
                            + (iterations * employeeBatchSize) /* employee */
                            + (iterations * employeeBatchSize * contractBatchSize) /* contract */
                            + (iterations * employeeBatchSize * contractBatchSize * addressBatchSize) /* address */
                            + (iterations * employeeBatchSize * contractBatchSize * addressBatchSize * localityBatchSize); /* locality */
        LOGGER.info("Total records to be inserted: {}", totalCount);

        ExecutorService exec = Executors.newFixedThreadPool(1);
        Future<?> future = exec.submit(() -> {
            int departmentId = 1;
            int employeeId = 1;
            int contractId = 1;
            int addressId = 1;
            int localityId = 1;
            long serial = 0;
            
            LOGGER.info("Started inserting");
            try {
                Statement st = conn.createStatement();
                for (int i = 0; i < iterations; ++i) {
                    st.execute(String.format("INSERT INTO department VALUES (%d, 'my department no %d', %d);", departmentId, departmentId, serial++));
        
                    for (int j = employeeId; j <= employeeId + employeeBatchSize - 1; ++j) {
                        LOGGER.info("inserting into employee with id {}", j);
                        st.execute(String.format("INSERT INTO employee VALUES (%d, 'emp no %d', %d, %d);", j, j, departmentId, serial++));
                        for (int k = contractId; k <= contractId + contractBatchSize - 1; ++k) {
                            LOGGER.info("inserting into contract with id {}", k);
                            st.execute(String.format("INSERT INTO contract VALUES (%d, 'contract no %d', %d, %d);", k, k, j /* employee fKey */, serial++));
        
                            for (int l = addressId; l <= addressId + addressBatchSize - 1; ++l) {
                                LOGGER.info("inserting into address with id {}", l);
                                st.execute(String.format("INSERT INTO address VALUES (%d, 'address no %d', %d, %d);", l, l, k /* contract fKey */, serial++));
        
                                for (int m = localityId; m <= localityId + localityBatchSize - 1; ++m) {
                                    LOGGER.info("inserting into locality with id {}", m);
                                    st.execute(String.format("INSERT INTO locality VALUES (%d, 'locality no %d', %d, %d);", m, m, l /* address fKey */, serial++));
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
                    conn.commit();
            }
            } catch (Exception e) {
                LOGGER.error("Exception caught: ", e);
                throw new RuntimeException(e);
            }
        });


        List<SourceRecord> recordsToAssert = new ArrayList<>();

        final long total = totalCount;
        AtomicLong totalConsumedRecords = new AtomicLong();
        try {
            LOGGER.info("Started consuming");
            Awaitility.await()
                    .atMost(Duration.ofSeconds(600))
                    .until(() -> {
                        int consumed = consumeAvailableRecords(record -> {
                            LOGGER.debug("The record being consumed is " + record);
                            Struct value = (Struct) record.value();
                            final int serialVal = value.getStruct("after").getStruct("serial_no").getInt32("value");
                            Assertions.assertEquals(recordsToAssert.size(), serialVal,
                                                    "Expected serial: " + recordsToAssert.size() + " but received " + serialVal + " at index " + recordsToAssert.size() + " with record " + record);
                            recordsToAssert.add(record);
                        });
                        if (consumed > 0) {
                            totalConsumedRecords.addAndGet(consumed);
                            LOGGER.info("Consumed " + totalConsumedRecords.get() + " records");
                        }

                        return recordsToAssert.size() == total && future.isDone();
                    });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + totalCount + " records in 600 seconds, consumed " + totalConsumedRecords.get(), exception);
        }

        assertEquals(total, recordsToAssert.size());
        
        // Verify the consumed records now.
        long expectedSerial = 0;
        for (int i = 0; i < recordsToAssert.size(); ++i) {
            Struct value = (Struct) recordsToAssert.get(i).value();
            long serial = value.getStruct("after").getStruct("serial_no").getInt32("value");
            assertEquals("Failed to verify serial number, expected: " + expectedSerial + " received: " + serial + " at index " + i + " with record " + recordsToAssert.get(i), expectedSerial, serial);

            ++expectedSerial;
        }
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

        startEngine(configBuilder);
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
                  int consumed = consumeAvailableRecords(record -> {
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

        startEngine(configBuilder);
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
                    conn.commit();
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
                  int consumed = consumeAvailableRecords(record -> {
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

        startEngine(configBuilder);
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
                  int consumed = consumeAvailableRecords(record -> {
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
        TestHelper.execute("CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT, serial_no INT);");
        TestHelper.execute("CREATE TABLE employee (id INT PRIMARY KEY, emp_name TEXT, d_id INT, serial_no INT);");

        YugabyteDBConnection c = TestHelper.create();
        Connection conn = c.connection();
        conn.setAutoCommit(false);

        final String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "department");
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.department,public.employee", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.CONSISTENCY_MODE, "global");
        configBuilder.with("transforms", "Reroute");
        configBuilder.with("transforms.Reroute.type", "io.debezium.transforms.ByLogicalTableRouter");
        configBuilder.with("transforms.Reroute.topic.regex", "(.*)");
        configBuilder.with("transforms.Reroute.topic.replacement", "test_server_all_events");
        configBuilder.with("transforms.Reroute.key.field.regex", "test_server.public.(.*)");
        configBuilder.with("transforms.Reroute.key.field.replacement", "\\$1");

        startEngine(configBuilder);
        awaitUntilConnectorIsReady();
        TestHelper.waitFor(Duration.ofSeconds(10));

        long totalRecords = 1_00_000;
        ExecutorService exec = Executors.newFixedThreadPool(1);
        exec.execute(() -> {
            try {
                LOGGER.info("Started inserting.");
                Statement st = conn.createStatement();
                long serial = 0;
                for (long i = 0; i < totalRecords; ++i) {
                    st.execute(String.format("INSERT INTO department VALUES (%d, 'my department no %d', %d);", i, i, serial++));
                    st.execute(String.format("INSERT INTO employee VALUES (%d, 'emp no %d', %d, %d);", i, i, i, serial++));
                    conn.commit();
                }
            } catch (Exception e) {
                LOGGER.error("Exception caught: ", e);
                throw new RuntimeException(e);
            }
        });

        final long total = 2 * totalRecords; // There are 2 tables each having totalRecords count.
        List<SourceRecord> recordsToAssert = new ArrayList<>();
        AtomicLong totalConsumedRecords = new AtomicLong();
        final int seconds = 1200;
        try {
            LOGGER.info("Started consuming");
            Awaitility.await()
              .atMost(Duration.ofSeconds(seconds))
              .until(() -> {
                  int consumed = consumeAvailableRecords(record -> {
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
        long expectedSerial = 0;
        for (int i = 0; i < recordsToAssert.size(); ++i) {
            Struct value = (Struct) recordsToAssert.get(i).value();
            long serial = value.getStruct("after").getStruct("serial_no").getInt32("value");
            assertEquals("Failed to verify serial number, expected: " + expectedSerial + " received: " + serial, expectedSerial, serial);

            ++expectedSerial;
        }
    }

    @Test
    public void fiveTablesSingleTabletEach() throws Exception {
        // NOTE: Run with cdc_max_stream_intent_records=10000
        TestHelper.execute("CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT, serial_no INT) SPLIT INTO 1 TABLETS;");
        TestHelper.execute("CREATE TABLE employee (id INT PRIMARY KEY, emp_name TEXT, d_id INT, serial_no INT) SPLIT INTO 1 TABLETS;");
        TestHelper.execute("CREATE TABLE contract (id INT PRIMARY KEY, contract_name TEXT, c_id INT, serial_no INT) SPLIT INTO 1 TABLETS;");
        TestHelper.execute("CREATE TABLE address (id INT PRIMARY KEY, area_name TEXT, a_id INT, serial_no INT) SPLIT INTO 1 TABLETS;");
        TestHelper.execute("CREATE TABLE locality (id INT PRIMARY KEY, loc_name TEXT, l_id INT, serial_no INT) SPLIT INTO 1 TABLETS;");

        String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "department", false, true);
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder("public.department,public.employee,public.contract,public.address,public.locality", dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.CONSISTENCY_MODE, "global");
        configBuilder.with("transforms", "Reroute");
        configBuilder.with("transforms.Reroute.type", "io.debezium.transforms.ByLogicalTableRouter");
        configBuilder.with("transforms.Reroute.topic.regex", "(.*)");
        configBuilder.with("transforms.Reroute.topic.replacement", "test_server_all_events");
        configBuilder.with("transforms.Reroute.key.field.regex", "test_server.public.(.*)");
        configBuilder.with("transforms.Reroute.key.field.replacement", "\\$1");

        startEngine(configBuilder);
        awaitUntilConnectorIsReady();

        TestHelper.waitFor(Duration.ofSeconds(10));

        // Set this flag to true if you want to keep running this test indefinitely.
        final boolean runIndefinitely = false;

        // If this test needs to be run more for higher duration, this scale factor can be changed
        // accordingly.
        final int scaleFactor = 1;
        final int iterations = runIndefinitely ? Integer.MAX_VALUE : 5 * scaleFactor;
        final int seconds = runIndefinitely ? Integer.MAX_VALUE : 2400;

        final int employeeBatchSize = 5 * scaleFactor;
        final int contractBatchSize = 6 * scaleFactor;
        final int addressBatchSize = 7 * scaleFactor;
        final int localityBatchSize = 8 * scaleFactor;

        YugabyteDBConnection ybConn = TestHelper.create();
        Connection conn = ybConn.connection();
        conn.setAutoCommit(false);

        final long totalRecords = runIndefinitely ? -1 : iterations /* department */
                                  + (iterations * employeeBatchSize) /* employee */
                                  + (iterations * employeeBatchSize * contractBatchSize) /* contract */
                                  + (iterations * employeeBatchSize * contractBatchSize * addressBatchSize) /* address */
                                  + (iterations * employeeBatchSize * contractBatchSize * addressBatchSize * localityBatchSize); /* locality */
        LOGGER.info("Total records to be inserted: {}", totalRecords);

        ExecutorService exec = Executors.newFixedThreadPool(1);
        Future<?> f = exec.submit(() -> {
            LOGGER.info("Started inserting");
            try {
                int serial = 0;
                int departmentId = 1;
                int employeeId = 1;
                int contractId = 1;
                int addressId = 1;
                int localityId = 1;

                Statement st = conn.createStatement();
                for (int i = 0; i < iterations; ++i) {
                    LOGGER.info("Inserting into department with {}", i);
                    executeWithRetry(st, String.format("INSERT INTO department VALUES (%d, 'my department no %d', %d);", i, i, serial));
                    ++serial;

                    for (int j = employeeId; j <= employeeId + employeeBatchSize - 1; ++j) {
                        LOGGER.info("Inserting into employee with {}", j);
                        executeWithRetry(st, String.format("INSERT INTO employee VALUES (%d, 'emp no %d', %d, %d);", j, j, i, serial));
                        ++serial;
                        for (int k = contractId; k <= contractId + contractBatchSize - 1; ++k) {
                            LOGGER.info("Inserting into contract with {}", k);
                            executeWithRetry(st, String.format("INSERT INTO contract VALUES (%d, 'contract no %d', %d, %d);", k, k, j /* employee fKey */, serial));
                            ++serial;

                            for (int l = addressId; l <= addressId + addressBatchSize - 1; ++l) {
                                LOGGER.info("Inserting into address with {}", l);
                                executeWithRetry(st, String.format("INSERT INTO address VALUES (%d, 'address no %d', %d, %d);", l, l, k /* contract fKey */, serial));
                                ++serial;

                                for (int m = localityId; m <= localityId + localityBatchSize - 1; ++m) {
                                    LOGGER.info("Inserting into locality with {}", m);
                                    executeWithRetry(st, String.format("INSERT INTO locality VALUES (%d, 'locality no %d', %d, %d);", m, m, l /* address fKey */, serial));
                                    ++serial;
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
                    conn.commit();
                }
            } catch (Exception e) {
                LOGGER.error("Exception caught: ", e);
                throw new RuntimeException(e);
            }
        });

        LOGGER.info("Done insertion");
        List<SourceRecord> recordsToAssert = new ArrayList<>();

        final long total = totalRecords;
        AtomicLong totalConsumedRecords = new AtomicLong();
        try {
            LOGGER.info("Started consuming");
            Awaitility.await()
              .atMost(Duration.ofSeconds(seconds))
              .until(() -> {
                  int consumed = consumeAvailableRecords(record -> {
                      LOGGER.debug("The record being consumed is " + record);
                      Struct value = (Struct) record.value();
                      final int serialVal = value.getStruct("after").getStruct("serial_no").getInt32("value");
                      Assertions.assertEquals(recordsToAssert.size(), serialVal,
                                              "Expected serial: " + recordsToAssert.size() + " but received " + serialVal + " at index " + recordsToAssert.size() + " with record " + record);
                      recordsToAssert.add(record);
                  });
                  if (consumed > 0) {
                      totalConsumedRecords.addAndGet(consumed);
                      LOGGER.info("Consumed " + totalConsumedRecords.get() + " records");
                  }

                  return recordsToAssert.size() == total && f.isDone();
              });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + total + " records in " + seconds + " seconds, consumed " + totalConsumedRecords.get(), exception);
        }

        // Verify the consumed records now.
        long expected = 0;
        for (int i = 0; i < recordsToAssert.size(); ++i) {
            Struct value = (Struct) recordsToAssert.get(i).value();
            long serialNo = value.getStruct("after").getStruct("serial_no").getInt32("value");

            assertEquals("Expected serial " + expected + " but got serial " + serialNo + " at index " + i, expected, serialNo);
            ++expected;
        }
    }

    @Test
    public void beforeImageWithConsistency() throws Exception {
        TestHelper.execute("CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT, serial_no INT) SPLIT INTO 1 TABLETS;");
        TestHelper.execute("CREATE TABLE employee (id INT PRIMARY KEY, emp_name TEXT, d_id INT, serial_no INT) SPLIT INTO 1 TABLETS;");

        YugabyteDBConnection c = TestHelper.create();
        Connection conn = c.connection();
        conn.setAutoCommit(false);

        final String dbStreamId = TestHelper.getNewDbStreamId("yugabyte", "department", true, true);
        Configuration.Builder configBuilder = getConsistentConfigurationBuilder("public.department,public.employee",dbStreamId);

        final boolean runIndefinitely = false;
        final int iterations = runIndefinitely ? Integer.MAX_VALUE : 10;
        final int seconds = 900;

        // Assuming there will be 2 records in every iteration - one for each update.
        // Plus there will be 2 more for the initial inserts.
        final long totalRecords = 2 + (2 * iterations);

        ExecutorService exec = Executors.newFixedThreadPool(1);
        Future<?> future = exec.submit(() -> {
            try (Statement st = conn.createStatement()) {
                long serial = 0;
                // Insert two records
                st.execute(String.format("INSERT INTO department VALUES (1, 'my department no 1', %d);", serial++));
                st.execute(String.format("INSERT INTO employee VALUES (1, 'emp no 1', 1, %d);", serial++));
                conn.commit();

                // Now simply keep updating all the values.
                for (int i = 0; i < iterations; ++i) {
                    st.execute(String.format("UPDATE department SET serial_no = %d WHERE id = 1;", serial++));
                    st.execute(String.format("UPDATE employee SET serial_no = %d WHERE id = 1;", serial++));

                    conn.commit();
                }
            } catch (Exception e) {
                LOGGER.error("Exception caught: ", e);
                throw new RuntimeException(e);
            }
        });

        List<SourceRecord> recordsToAssert = new ArrayList<>();

        AtomicLong totalConsumedRecords = new AtomicLong();
        try {
            LOGGER.info("Started consuming");
            Awaitility.await()
              .atMost(Duration.ofSeconds(seconds))
              .until(() -> {
                  int consumed = super.consumeAvailableRecords(record -> {
                      LOGGER.debug("The record being consumed is " + record);
                      Struct value = (Struct) record.value();
                      final int serialVal = value.getStruct("after").getStruct("serial_no").getInt32("value");
                      Assertions.assertEquals(recordsToAssert.size(), serialVal,
                            "Expected serial: " + recordsToAssert.size() + " but received "
                                + serialVal + " at index " + recordsToAssert.size()
                                + " with record " + record);

                      if (value.getStruct("before") != null) {
                          // Assert before image in this case - the before image would be one less
                          // then the current serial value received.
                          final int beforeSerial = value.getStruct("before").getStruct("serial_no").getInt32("value");
                          Assertions.assertEquals(serialVal - 1, beforeSerial,
                            "Wrong before image found at index " + recordsToAssert.size() + " expected serial: "
                            + (serialVal - 1) + " received serial: " + beforeSerial);
                      }

                      recordsToAssert.add(record);
                  });
                  if (consumed > 0) {
                      totalConsumedRecords.addAndGet(consumed);
                      LOGGER.info("Consumed " + totalConsumedRecords.get() + " records");
                  }

                  return recordsToAssert.size() == (long) totalRecords && future.isDone();
              });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + (long) totalRecords + " records in " + seconds + " seconds, consumed " + totalConsumedRecords.get(), exception);
        }
    }

    @Test
    public void consistencyWithColocatedTables() throws Exception {
        TestHelper.executeInDatabase("CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT, serial_no INT) WITH (colocated = true) SPLIT INTO 1 TABLETS;", DEFAULT_COLOCATED_DB_NAME);
        TestHelper.executeInDatabase("CREATE TABLE employee (id INT PRIMARY KEY, emp_name TEXT, d_id INT, serial_no INT) WITH (colocated = true) SPLIT INTO 1 TABLETS;", DEFAULT_COLOCATED_DB_NAME);

        YugabyteDBConnection c = TestHelper.createConnectionTo(DEFAULT_COLOCATED_DB_NAME);
        Connection conn = c.connection();
        conn.setAutoCommit(false);

        final String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_COLOCATED_DB_NAME, "department", false, true);
        Configuration.Builder configBuilder = getConsistentConfigurationBuilder(DEFAULT_COLOCATED_DB_NAME, "public.department,public.employee",dbStreamId);

        final boolean runIndefinitely = false;
        final int iterations = runIndefinitely ? Integer.MAX_VALUE : 10;
        final int seconds = 900;

        final int employeeBatchSize = 5;

        final long totalRecords = iterations /* department */
                                  + iterations * employeeBatchSize; /* employee */

        ExecutorService exec = Executors.newFixedThreadPool(1);
        Future<?> future = exec.submit(() -> {
            int employeeId = 1;
            try (Statement st = conn.createStatement()) {
                long serial = 0;

                for (int i = 0; i < iterations; ++i) {
                    st.execute(String.format("INSERT INTO department VALUES (%d, 'my department no %d', %d);", i, i, serial++));
                    for (int j = employeeId; j <= employeeId + employeeBatchSize - 1; ++j) {
                        st.execute(String.format("INSERT INTO employee VALUES (%d, 'emp no %d', %d, %d);", j, j, i, serial++));
                    }

                    conn.commit();

                    employeeId += employeeBatchSize;
                }
            } catch (Exception e) {
                LOGGER.error("Exception caught: ", e);
                throw new RuntimeException(e);
            }
        });

        List<SourceRecord> recordsToAssert = new ArrayList<>();

        AtomicLong totalConsumedRecords = new AtomicLong();
        try {
            LOGGER.info("Started consuming");
            Awaitility.await()
              .atMost(Duration.ofSeconds(seconds))
              .until(() -> {
                  int consumed = super.consumeAvailableRecords(record -> {
                      LOGGER.debug("The record being consumed is " + record);
                      Struct value = (Struct) record.value();
                      final int serialVal = value.getStruct("after").getStruct("serial_no").getInt32("value");
                      Assertions.assertEquals(recordsToAssert.size(), serialVal,
                        "Expected serial: " + recordsToAssert.size() + " but received "
                          + serialVal + " at index " + recordsToAssert.size()
                          + " with record " + record);

                      recordsToAssert.add(record);
                  });
                  if (consumed > 0) {
                      totalConsumedRecords.addAndGet(consumed);
                      LOGGER.info("Consumed " + totalConsumedRecords.get() + " records");
                  }

                  return recordsToAssert.size() == (long) totalRecords && future.isDone();
              });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + (long) totalRecords + " records in " + seconds + " seconds, consumed " + totalConsumedRecords.get(), exception);
        }
    }

    @Test
    public void consistencyWithColumnAdditions() throws Exception {
        TestHelper.execute("CREATE TABLE department (id INT PRIMARY KEY, dept_name TEXT, serial_no INT) SPLIT INTO 1 TABLETS;");
        TestHelper.execute("CREATE TABLE employee (id INT PRIMARY KEY, emp_name TEXT, d_id INT, serial_no INT) SPLIT INTO 1 TABLETS;");

        YugabyteDBConnection c = TestHelper.create();
        Connection conn = c.connection();
        conn.setAutoCommit(false);

        final String dbStreamId = TestHelper.getNewDbStreamId(DEFAULT_DB_NAME, "department", false, true);
        Configuration.Builder configBuilder = getConsistentConfigurationBuilder("public.department,public.employee",dbStreamId);

        final boolean runIndefinitely = false;
        final int iterations = runIndefinitely ? Integer.MAX_VALUE : 100;
        final int seconds = 900;

        final int employeeBatchSize = 5;

        final long totalRecords = iterations /* department */
                                    + iterations * employeeBatchSize; /* employee */

        final String newColumnName = "added_col";
        final int newColumnDefaultValue = 400;
        ExecutorService exec = Executors.newFixedThreadPool(1);
        Future<?> future = exec.submit(() -> {
            int employeeId = 1;
            boolean addColumn = false;

            try (Statement st = conn.createStatement()) {
                long serial = 0;

                for (int i = 0; i < iterations; ++i) {
                    if (serial != 0 && serial % 10 == 0) {
                        addColumn = !addColumn;
                        if (addColumn) {
                            st.execute("ALTER TABLE department ADD COLUMN " + newColumnName + i + " INT DEFAULT " + newColumnDefaultValue + ";");
                            st.execute("ALTER TABLE employee ADD COLUMN " + newColumnName + i + " INT DEFAULT " + newColumnDefaultValue + ";");
                        } else {
                            st.execute("ALTER TABLE department DROP COLUMN " + newColumnName + ";");
                            st.execute("ALTER TABLE employee DROP COLUMN " + newColumnName + ";");
                        }
                    }

                    st.execute(String.format("INSERT INTO department VALUES (%d, 'my department no %d', %d);", i, i, serial++));
                    for (int j = employeeId; j <= employeeId + employeeBatchSize - 1; ++j) {
                        st.execute(String.format("INSERT INTO employee VALUES (%d, 'emp no %d', %d, %d);", j, j, i, serial++));
                    }

                    conn.commit();

                    employeeId += employeeBatchSize;
                }
            } catch (Exception e) {
                LOGGER.error("Exception caught: ", e);
                throw new RuntimeException(e);
            }
        });

        List<SourceRecord> recordsToAssert = new ArrayList<>();

        AtomicLong totalConsumedRecords = new AtomicLong();
        AtomicBoolean shouldContainExtraColumn = new AtomicBoolean();
        try {
            LOGGER.info("Started consuming");
            Awaitility.await()
              .atMost(Duration.ofSeconds(seconds))
              .until(() -> {
                  int consumed = super.consumeAvailableRecords(record -> {
                      if (recordsToAssert.size() != 0 && recordsToAssert.size() % 10 == 0) {
                        shouldContainExtraColumn.set(!shouldContainExtraColumn.get());
                      }

                      LOGGER.debug("The record being consumed is " + record);
                      Struct value = (Struct) record.value();
                      final int serialVal = value.getStruct("after").getStruct("serial_no").getInt32("value");
                      Assertions.assertEquals(recordsToAssert.size(), serialVal,
                        "Expected serial: " + recordsToAssert.size() + " but received "
                          + serialVal + " at index " + recordsToAssert.size()
                          + " with record " + record);

                      if (shouldContainExtraColumn.get()) {
                          final int newColVal = value.getStruct("after").getStruct(newColumnName).getInt32("value");
                          Assertions.assertEquals(newColumnDefaultValue, newColVal);
                      } else {
                          boolean hasNewColumn = value.getStruct("after").schema().fields()
                                                   .stream().map(Field::name)
                                                   .collect(Collectors.toSet())
                                                   .contains(newColumnName);
                          Assertions.assertFalse(hasNewColumn);
                      }

                      recordsToAssert.add(record);
                  });
                  if (consumed > 0) {
                      totalConsumedRecords.addAndGet(consumed);
                      LOGGER.info("Consumed " + totalConsumedRecords.get() + " records");
                  }

                  return recordsToAssert.size() == (long) totalRecords && future.isDone();
              });
        } catch (ConditionTimeoutException exception) {
            fail("Failed to consume " + (long) totalRecords + " records in " + seconds + " seconds, consumed " + totalConsumedRecords.get(), exception);
        }
    }

    public void executeWithRetry(Statement st, String statement) throws SQLException {
        final int totalRetries = 3;

        int tryCount = 0;
        while (tryCount < totalRetries) {
            try {
                st.execute(statement);
                // Return execution if statement successful.
                return;
            } catch (SQLException e) {
                ++tryCount;
                if (tryCount >= totalRetries) {
                    LOGGER.error("Throwing error: ", e);
                    throw e;
                }

                // If need to be retried, wait for sometime before retrying.
                LOGGER.info("Error encountered: {}, retrying", e.getMessage());
                TestHelper.waitFor(Duration.ofSeconds(3));
            }
        }
    }

    private Configuration.Builder getConsistentConfigurationBuilder(String tableIncludeList, String dbStreamId) throws Exception {
        return getConsistentConfigurationBuilder(DEFAULT_DB_NAME, tableIncludeList, dbStreamId);
    }

    private Configuration.Builder getConsistentConfigurationBuilder(String databaseName, String tableIncludeList, String dbStreamId) throws Exception {
        Configuration.Builder configBuilder = TestHelper.getConfigBuilder(databaseName, tableIncludeList, dbStreamId);
        configBuilder.with(YugabyteDBConnectorConfig.CONSISTENCY_MODE, "global");
        configBuilder.with("transforms", "Reroute");
        configBuilder.with("transforms.Reroute.type", "io.debezium.transforms.ByLogicalTableRouter");
        configBuilder.with("transforms.Reroute.topic.regex", "(.*)");
        configBuilder.with("transforms.Reroute.topic.replacement", "test_server_all_events");
        configBuilder.with("transforms.Reroute.key.field.regex", "test_server.public.(.*)");
        configBuilder.with("transforms.Reroute.key.field.replacement", "\\$1");

        return configBuilder;
    }
}
