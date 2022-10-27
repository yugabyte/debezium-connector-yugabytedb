/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import org.postgresql.util.PSQLException;
import org.yb.client.CDCErrorException;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for Postgres.
 *
 * @author Gunnar Morling
 */
public class YugabyteDBErrorHandler extends ErrorHandler {

    public YugabyteDBErrorHandler(YugabyteDBConnectorConfig connectorConfig, ChangeEventQueue<?> queue) {
        super(YugabyteDBConnector.class, connectorConfig, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        if (throwable instanceof PSQLException
                && (throwable.getMessage().contains("Database connection failed when writing to copy")
                        || throwable.getMessage().contains("Database connection failed when reading from copy"))
                || throwable.getMessage().contains("FATAL: terminating connection due to administrator command")) {
            return true;
        }

        return false;
    }
}
