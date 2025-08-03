/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

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

    public YugabyteDBErrorHandler(YugabyteDBConnectorConfig connectorConfig, ChangeEventQueue<?> queue, ErrorHandler replacedErrorHandler) {
        super(YugabyteDBgRPCConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    @Override
    protected boolean isRetriable(Throwable throwable) {
        LOGGER.info("Received throwable to check for retry: {}", throwable);

        if (throwable.getMessage() == null) {
            LOGGER.warn("Exception message received in throwable is null");
        }

        // We do not need to retry errors at this stage since the connector itself retries
        // for a configurable number of times, if the flow reaches this point then it should simply
        // stop so that the user gets the exception and restarts the connector manually.
        LOGGER.warn("Returning false to indicate that the connector level task should not be retried");
        return false;
    }
}
