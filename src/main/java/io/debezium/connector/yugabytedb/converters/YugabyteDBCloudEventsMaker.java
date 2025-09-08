/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.converters;

import java.util.Set;

import io.debezium.connector.AbstractSourceInfo;
import io.debezium.converters.recordandmetadata.RecordAndMetadata;
import io.debezium.converters.spi.CloudEventsMaker;
import io.debezium.converters.spi.SerializerType;
import io.debezium.data.Envelope;
import io.debezium.util.Collect;

/**
 * CloudEvents maker for records producer by the YugabyteDB connector.
 */
public class YugabyteDBCloudEventsMaker extends CloudEventsMaker {
    static final String TXID_KEY = "txId";
    static final String XMIN_KEY = "xmin";
    static final String LSN_KEY = "lsn";
    static final String SEQUENCE_KEY = "sequence";

    static final Set<String> YUGABYTE_SOURCE_FIELDS = Collect.unmodifiableSet(
            TXID_KEY,
            XMIN_KEY,
            LSN_KEY,
            SEQUENCE_KEY);

    public YugabyteDBCloudEventsMaker(RecordAndMetadata recordAndMetadata, SerializerType dataContentType, String dataSchemaUriBase,
                                      String cloudEventsSchemaName) {
        super(recordAndMetadata, dataContentType, dataSchemaUriBase, cloudEventsSchemaName, Envelope.FieldName.BEFORE, Envelope.FieldName.AFTER);
    }

    @Override
    public String ceId() {
        return "name:" + sourceField(AbstractSourceInfo.SERVER_NAME_KEY)
                + ";lsn:" + sourceField(LSN_KEY)
                + ";txId:" + sourceField(TXID_KEY)
                + ";sequence:" + sourceField(SEQUENCE_KEY);
    }

    @Override
    public Set<String> connectorSpecificSourceFields() {
        return YUGABYTE_SOURCE_FIELDS;
    }
}
