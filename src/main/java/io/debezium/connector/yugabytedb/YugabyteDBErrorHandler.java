/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/**
 * Error handler for YugabyteDB.
 *
 * @author Suranjan Kumar, Vaibhav Kushwaha
 */
public class YugabyteDBErrorHandler extends ErrorHandler {
    private final static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBErrorHandler.class);

    public YugabyteDBErrorHandler(YugabyteDBConnectorConfig connectorConfig, ChangeEventQueue<?> queue) {
        super(YugabyteDBConnector.class, connectorConfig, queue);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        LOGGER.info("Checking if a retry is possible for retriable exception: {}", throwable);

        if (throwable.getMessage() == null) {
            LOGGER.warn("Exception message received is null");
        }

        if (throwable instanceof PSQLException
                && throwable.getMessage() != null
                && (throwable.getMessage().contains("Database connection failed when writing to copy")
                        || throwable.getMessage().contains("Database connection failed when reading from copy"))
                || throwable.getMessage().contains("FATAL: terminating connection due to administrator command")) {
            return true;
        }

        return false;
    }
}
