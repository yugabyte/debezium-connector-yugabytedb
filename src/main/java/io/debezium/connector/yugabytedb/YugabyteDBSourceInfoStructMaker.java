/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

/**
 * Helper class to create {@link Struct} values for the {@link SourceInfo} object for the records.
 *
 * @author Suranjan Kumar, Vaibhav Kushwaha
 */
public class YugabyteDBSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {
    private final Schema schema;

    public YugabyteDBSourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("io.debezium.connector.yugabytedb.Source")
                .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.TXID_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.LSN_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.COMMIT_TIME, Schema.OPTIONAL_INT64_SCHEMA)
                .field(SourceInfo.RECORD_TIME, Schema.INT64_SCHEMA)
                .field(SourceInfo.TABLET_ID, Schema.STRING_SCHEMA)
                .field(SourceInfo.PARTITION_ID_KEY, Schema.STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        assert sourceInfo.database() != null
                && sourceInfo.schemaName() != null
                && sourceInfo.tableName() != null;

        Struct result = super.commonStruct(sourceInfo);
        result.put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.schemaName());
        result.put(SourceInfo.TABLE_NAME_KEY, sourceInfo.tableName());
        result.put(SourceInfo.RECORD_TIME, sourceInfo.recordTime());
        result.put(SourceInfo.COMMIT_TIME, sourceInfo.commitTime());

        if (sourceInfo.tabletId() != null) {
            result.put(SourceInfo.TABLET_ID, sourceInfo.tabletId());
            result.put(SourceInfo.PARTITION_ID_KEY,
                       sourceInfo.tableUUID() + "." + sourceInfo.tabletId());
        }

        if (sourceInfo.txId() != null) {
            result.put(SourceInfo.TXID_KEY, sourceInfo.txId());
        }
        if (sourceInfo.lsn() != null) {
            result.put(SourceInfo.LSN_KEY, sourceInfo.lsn().toSerString());
        }
        return result;
    }
}
