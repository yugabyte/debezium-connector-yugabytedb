package io.debezium.connector.yugabytedb.transforms;

import java.util.*;

import io.debezium.connector.yugabytedb.transforms.SchemaUtil;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.util.Pair;

/**
 * A transformer to convert records in a format that is compatible with
 * Striim's PostgreSQLReader format i.e. WAEvent.
 * @param <R> Record
 */
public class StriimCompatible<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(StriimCompatible.class);

    @Override
    public R apply(final R record) {
        if (record == null || (record.value() != null && !(record.value() instanceof Struct))) {
            return record;
        }

        List<String> primaryKeys = getAllFieldsInOrder(record.keySchema());

        Schema updatedSchemaForValue = null;
        Struct updatedValueForValue = null;
        if (record.value() != null) {
            Pair<Schema, Struct> val = getUpdatedValueAndSchema(record.valueSchema(), (Struct) record.value(), primaryKeys);
            updatedSchemaForValue = val.getFirst();
            updatedValueForValue = val.getSecond();
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchemaForValue, updatedValueForValue, record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    private List<String> getAllFieldsInOrder(Schema schema) {
        List<String> fields = Arrays.asList(new String[schema.fields().size()]);
        for (Field field : schema.fields()) {
            fields.set(field.index(), field.name());
        }
        return fields;
    }

    private boolean isValueSetStruct(Field field) {
        return field.schema().fields().size() == 2
                && (Objects.equals(field.schema().fields().get(0).name(), "value")
                && Objects.equals(field.schema().fields().get(1).name(), "set"));
    }

    private Schema makeMetadataSchema() {
        final SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("LSN", Schema.STRING_SCHEMA);
        builder.field("OperationName", Schema.STRING_SCHEMA);
        builder.field("PK_UPDATE", Schema.OPTIONAL_BOOLEAN_SCHEMA);
        builder.field("Sequence", Schema.STRING_SCHEMA);
        builder.field("TableName", Schema.STRING_SCHEMA);
        builder.field("TxnID", Schema.STRING_SCHEMA);
        return builder.build();
    }

    private Struct makeMetadata(Struct value, Schema metadataSchema) {
        Struct metadata = new Struct(metadataSchema);
        Struct sourceValue = (Struct) value.get("source");
        metadata.put("LSN", sourceValue.getString("lsn"));
        metadata.put("Sequence", sourceValue.getString("sequence"));
        metadata.put("TxnID", sourceValue.getString("txId"));
        metadata.put("TableName", sourceValue.getString("schema") + "." + sourceValue.getString("table"));
        return metadata;
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder arrayBuilder = SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA);
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        builder.field("metadata", makeMetadataSchema());
        builder.field("data", arrayBuilder.build());
        builder.field("columns", arrayBuilder.build());
        builder.field("before", arrayBuilder.optional().build());
        return builder.build();
    }

    private Map<String, Object> extractData(Struct value) {
        Map<String, Object> values = new HashMap<>();
        if (value == null) {
            return values;
        }

        for (Field field : value.schema().fields()) {
            if (field.schema().type() == Type.STRUCT && isValueSetStruct(field)) {
                Struct fieldValue = (Struct) value.get(field);
                values.put(field.name(), fieldValue == null ? null : fieldValue.get("value").toString());
            } else {
                Object fieldValue = value.get(field);
                values.put(field.name(), fieldValue == null ? null : fieldValue.toString());
            }
        }
        return values;
    }

    private List<Object> convertToOrderedList(Map<String, Object> values, List<String> orderedKeys) {
        List<Object> valuesList = new ArrayList<>();
        for (String key : orderedKeys) {
            if (values.containsKey(key)) {
                valuesList.add(values.get(key));
            } else {
                LOGGER.debug("{} not found in values", key);
            }
        }
        return valuesList;
    }

    private void removeNonPrimaryKeyValues(Map<String, Object> values, List<String> primaryKeys) {
        values.forEach((fieldName, fieldValue) -> {
            if (!primaryKeys.contains(fieldName)) {
                values.put(fieldName, null);
            }
        });
    }

    private boolean comparePrimaryKeyValues(Map<String, Object> after, Map<String, Object> before, List<String> primaryKeys) {
        for (String key : primaryKeys) {
            if (!after.get(key).equals(before.get(key))) {
                return false;
            }
        }
        return true;
    }

    public Pair<Schema, Struct> getUpdatedValueAndSchema(Schema schema, Struct value, List<String> primaryKeys) {
        LOGGER.debug("Original Schema as json: " + io.debezium.data.SchemaUtil.asString(schema));
        Schema updatedSchema = makeUpdatedSchema(schema);
        LOGGER.debug("Updated schema as json: " + io.debezium.data.SchemaUtil.asString(updatedSchema));

        List<String> allFields = getAllFieldsInOrder(schema.field("after").schema());

        LOGGER.debug("Original value as json: {}", io.debezium.data.SchemaUtil.asDetailedString(value));
        Struct newVal = new Struct(updatedSchema);
        Struct metadata = makeMetadata(value, updatedSchema.field("metadata").schema());
        newVal.put("columns", allFields);

        switch (value.getString("op")) {
            case "c": {
               Map<String, Object> newValues = extractData((Struct) value.get("after"));

                metadata.put("OperationName", "INSERT");
                newVal.put("metadata", metadata);
                newVal.put("data", convertToOrderedList(newValues, allFields));

               LOGGER.debug("Update value as json: {}", io.debezium.data.SchemaUtil.asDetailedString(newVal));
               return new org.yb.util.Pair<>(updatedSchema, newVal);
            }
            case "u": {
                Map<String, Object> newValues = extractData((Struct) value.get("after"));
                Map<String, Object> oldValues = extractData((Struct) value.get("before"));
                removeNonPrimaryKeyValues(oldValues, primaryKeys);

                metadata.put("OperationName", "UPDATE");
                if (!comparePrimaryKeyValues(newValues, oldValues, primaryKeys)) {
                    metadata.put("PK_UPDATE", true);
                }
                newVal.put("metadata", metadata);
                newVal.put("data", convertToOrderedList(newValues, allFields));
                newVal.put("before", convertToOrderedList(oldValues, allFields));

                LOGGER.debug("Update value as json: {}", io.debezium.data.SchemaUtil.asDetailedString(newVal));
                return new org.yb.util.Pair<>(updatedSchema, newVal);
            }
            case "d": {
                Map<String, Object> oldValues = extractData((Struct) value.get("before"));
                removeNonPrimaryKeyValues(oldValues, primaryKeys);

                metadata.put("OperationName", "DELETE");
                newVal.put("metadata", metadata);
                newVal.put("data", convertToOrderedList(oldValues, allFields));

                LOGGER.debug("Update value as json: {}", io.debezium.data.SchemaUtil.asDetailedString(newVal));
                return new org.yb.util.Pair<>(updatedSchema, newVal);
            }
            default: {
                return new org.yb.util.Pair<>(schema, value);
            }
        }
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
