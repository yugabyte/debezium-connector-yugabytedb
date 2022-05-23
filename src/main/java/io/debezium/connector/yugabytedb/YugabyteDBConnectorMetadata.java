/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import org.apache.kafka.connect.connector.Connector;

import io.debezium.config.Field;
import io.debezium.metadata.AbstractConnectorMetadata;
import io.debezium.metadata.ConnectorDescriptor;

public class YugabyteDBConnectorMetadata extends AbstractConnectorMetadata {

    @Override
    public ConnectorDescriptor getConnectorDescriptor() {
        return new ConnectorDescriptor("postgres", "Debezium PostgreSQL Connector", getConnector().version());
    }

    @Override
    public Connector getConnector() {
        return new YugabyteDBConnector();
    }

    @Override
    public Field.Set getAllConnectorFields() {
        return YugabyteDBConnectorConfig.ALL_FIELDS;
    }

}
