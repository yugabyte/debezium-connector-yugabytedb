/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.connection;

import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig;
import io.debezium.connector.yugabytedb.YugabyteDBSchema;

/**
 * Contextual data required by {@link MessageDecoder}s.
 *
 * @author Chris Cranford
 */
public class MessageDecoderContext {

    private final YugabyteDBConnectorConfig config;
    private final YugabyteDBSchema schema;

    public MessageDecoderContext(YugabyteDBConnectorConfig config, YugabyteDBSchema schema) {
        this.config = config;
        this.schema = schema;
    }

    public YugabyteDBConnectorConfig getConfig() {
        return config;
    }

    public YugabyteDBSchema getSchema() {
        return schema;
    }
}
