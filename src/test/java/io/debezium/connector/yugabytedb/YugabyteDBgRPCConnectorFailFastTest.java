/*
 * Copyright YugabyteDB Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.jupiter.api.Test;

import io.debezium.DebeziumException;

/**
 * Offline unit tests for how {@link YugabyteDBgRPCConnector} reports permanent errors to Kafka
 * Connect. They need no YugabyteDB container.
 * <p>
 * The E2E cases that go through {@code taskConfigs()} against a real cluster are in
 * {@link YugabyteDBCdcFailFastTest}.
 */
public class YugabyteDBgRPCConnectorFailFastTest {

    /**
     * A {@link ConnectorContext} that records what the connector reports instead of failing a worker.
     * A proxy keeps the test independent of the methods the Kafka Connect API version adds to the interface.
     */
    private static final class RecordingContext {
        final List<Exception> raised = new ArrayList<>();
        int reconfigurationRequests;

        final ConnectorContext context = (ConnectorContext) Proxy.newProxyInstance(
                ConnectorContext.class.getClassLoader(),
                new Class<?>[]{ ConnectorContext.class },
                (proxy, method, args) -> {
                    if ("raiseError".equals(method.getName())) {
                        raised.add((Exception) args[0]);
                    }
                    else if ("requestTaskReconfiguration".equals(method.getName())) {
                        reconfigurationRequests++;
                    }
                    return null;
                });
    }

    private static YugabyteDBgRPCConnector connectorWith(RecordingContext recorder) {
        YugabyteDBgRPCConnector connector = new YugabyteDBgRPCConnector();
        connector.initialize(recorder.context);
        return connector;
    }

    private static Exception invokeRaiseError(YugabyteDBgRPCConnector connector, Exception error) throws Exception {
        Method raiseError = YugabyteDBgRPCConnector.class.getDeclaredMethod("raiseError", Exception.class);
        raiseError.setAccessible(true);
        try {
            return (Exception) raiseError.invoke(connector, error);
        }
        catch (InvocationTargetException e) {
            throw (Exception) e.getCause();
        }
    }

    @Test
    public void raiseErrorShouldReportToConnectContextAndReturnTheSameException() throws Exception {
        RecordingContext recorder = new RecordingContext();
        YugabyteDBgRPCConnector connector = connectorWith(recorder);
        DebeziumException permanent = new DebeziumException("permanent config error");

        Exception returned = invokeRaiseError(connector, permanent);

        assertSame(permanent, returned, "raiseError must return the exception so callers can throw it.");
        assertEquals(1, recorder.raised.size(), "Connect context must be told about the failure exactly once.");
        assertSame(permanent, recorder.raised.get(0));
        assertEquals(0, recorder.reconfigurationRequests, "Failing must not request a task reconfiguration.");
    }
}
