/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;

/**
 * Unit tests for the failure paths of the replication-slot LSN parsing in
 * {@link YugabyteDBConnection}. These paths only ever surface as an exception message in a
 * connector log, so the message is the contract: it has to say which column could not be read,
 * for which slot, and it has to keep the underlying {@link SQLException} as the cause.
 * <p>
 * No server is needed here - {@link YugabyteDBConnection} does not open a connection in its
 * constructor, and the parsing methods take a {@link ResultSet} that is stubbed below.
 *
 * @author Yaron Parasol
 */
public class YugabyteDBConnectionLsnMessageTest {

    private static final String SLOT = "test_slot";
    private static final String PLUGIN = "yboutput";
    private static final String DATABASE = "test_db";

    /**
     * A {@link ResultSet} stub that serves {@code getString(String)} from the supplied map. A null
     * map value means the column is present but null; a column missing from the map throws a
     * {@link SQLException}, which is how pgjdbc reports a column that is not in the result set.
     * Every other method is unsupported - the code under test must not need them.
     */
    private static ResultSet resultSetReturning(Map<String, String> columns) {
        InvocationHandler handler = (proxy, method, args) -> {
            if ("getString".equals(method.getName()) && args != null && args.length == 1) {
                String column = String.valueOf(args[0]);
                if (!columns.containsKey(column)) {
                    throw new SQLException("The column name " + column + " was not found in this ResultSet.");
                }
                return columns.get(column);
            }
            if ("toString".equals(method.getName())) {
                return "ResultSetStub" + columns;
            }
            throw new UnsupportedOperationException("Unexpected ResultSet call: " + method.getName());
        };

        return (ResultSet) Proxy.newProxyInstance(
                YugabyteDBConnectionLsnMessageTest.class.getClassLoader(),
                new Class<?>[]{ ResultSet.class },
                handler);
    }

    private static YugabyteDBConnection newConnection() {
        // Any host/port will do: no connection is opened by the constructor or by the methods
        // exercised below.
        JdbcConfiguration config = JdbcConfiguration.adapt(Configuration.create()
                .with(JdbcConfiguration.HOSTNAME, "127.0.0.1")
                .with(JdbcConfiguration.PORT, 5433)
                .with(JdbcConfiguration.USER, "yugabyte")
                .with(JdbcConfiguration.PASSWORD, "yugabyte")
                .with(JdbcConfiguration.DATABASE, DATABASE)
                .build());

        return new YugabyteDBConnection(config, YugabyteDBConnection.CONNECTION_GENERAL);
    }

    @Test
    public void restartLsnFailureShouldReportTheColumnTheSlotAndTheCause() {
        YugabyteDBConnection connection = newConnection();
        // restart_lsn absent from the result set, so tryParseLsn will throw a SQLException.
        ResultSet rs = resultSetReturning(Map.of());

        ConnectException ex = assertThrows(ConnectException.class,
                () -> connection.parseRestartLsn(SLOT, PLUGIN, DATABASE, rs));

        // Guards against the message regressing to "restart_lsn could be found", which stated the
        // opposite of the failure.
        assertTrue(ex.getMessage().contains("restart_lsn could not be read"),
                "Message should say the value could NOT be read, was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(SLOT), "Message should name the slot, was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(PLUGIN), "Message should name the plugin, was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(DATABASE), "Message should name the database, was: " + ex.getMessage());

        // The JDBC failure must remain reachable, not just be logged.
        assertTrue(ex.getCause() instanceof SQLException,
                "The SQLException should be attached as the cause");
    }

    @Test
    public void confirmedFlushLsnFailureShouldReportBothColumnsAndKeepBothCauses() {
        YugabyteDBConnection connection = newConnection();
        // Neither column is present, so both the primary read and the restart_lsn fallback fail.
        ResultSet rs = resultSetReturning(Map.of());

        ConnectException ex = assertThrows(ConnectException.class,
                () -> connection.parseConfirmedFlushLsn(SLOT, PLUGIN, DATABASE, rs));

        assertTrue(ex.getMessage().contains("confirmed_flush_lsn"),
                "Message should name confirmed_flush_lsn, was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("restart_lsn"),
                "Message should name restart_lsn, was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(SLOT), "Message should name the slot, was: " + ex.getMessage());

        // The fallback failure is the cause and the original failure is kept as suppressed, so no
        // information about either attempt is lost.
        assertTrue(ex.getCause() instanceof SQLException, "The fallback SQLException should be the cause");
        assertEquals(1, ex.getCause().getSuppressed().length,
                "The confirmed_flush_lsn failure should be retained as a suppressed exception");
    }

    @Test
    public void unparseableLsnShouldReportTheOffendingValueAndKeepTheCause() {
        YugabyteDBConnection connection = newConnection();
        // "zz/1" has the right shape but is not hexadecimal, so Lsn.valueOf throws.
        ResultSet rs = resultSetReturning(Map.of("restart_lsn", "zz/1"));

        ConnectException ex = assertThrows(ConnectException.class,
                () -> connection.tryParseLsn(SLOT, PLUGIN, DATABASE, rs, "restart_lsn"));

        assertTrue(ex.getMessage().contains("zz/1"),
                "Message should quote the offending value, was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("restart_lsn"),
                "Message should name the column, was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(SLOT), "Message should name the slot, was: " + ex.getMessage());

        // The parse failure must remain reachable rather than being dropped on the floor.
        assertNotNull(ex.getCause(), "The parse failure should be attached as the cause");
        assertTrue(ex.getCause() instanceof NumberFormatException,
                "Expected the NumberFormatException as the cause, got: " + ex.getCause());
    }

    @Test
    public void invalidLsnShouldReportTheValueTheColumnAndTheSlot() {
        YugabyteDBConnection connection = newConnection();
        // No slash, so Lsn.valueOf returns INVALID_LSN rather than throwing.
        ResultSet rs = resultSetReturning(Map.of("confirmed_flush_lsn", "not-an-lsn"));

        ConnectException ex = assertThrows(ConnectException.class,
                () -> connection.tryParseLsn(SLOT, PLUGIN, DATABASE, rs, "confirmed_flush_lsn"));

        // Guards against the message regressing to the bare "Invalid LSN returned from database".
        assertTrue(ex.getMessage().contains("not-an-lsn"),
                "Message should quote the invalid value, was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains("confirmed_flush_lsn"),
                "Message should name the column, was: " + ex.getMessage());
        assertTrue(ex.getMessage().contains(SLOT), "Message should name the slot, was: " + ex.getMessage());
    }

    @Test
    public void nullColumnShouldReturnNullWithoutThrowing() throws Exception {
        YugabyteDBConnection connection = newConnection();
        ResultSet rs = resultSetReturning(java.util.Collections.singletonMap("restart_lsn", null));

        assertNull(connection.tryParseLsn(SLOT, PLUGIN, DATABASE, rs, "restart_lsn"),
                "A null column value is not an error and should yield a null Lsn");
    }

    @Test
    public void validLsnShouldBeParsed() throws Exception {
        YugabyteDBConnection connection = newConnection();
        ResultSet rs = resultSetReturning(Map.of("restart_lsn", "0/15D68C50"));

        Lsn lsn = connection.tryParseLsn(SLOT, PLUGIN, DATABASE, rs, "restart_lsn");

        assertTrue(lsn.isValid(), "A well formed LSN should parse to a valid Lsn");
    }
}
