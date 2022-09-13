package io.debezium.connector.yugabytedb.transforms;

import java.util.Map;
import java.util.Objects;

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

public class PGCompatible<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PGCompatible.class);

    @Override
    public R apply(final R record) {
        if (record == null || (record.value() != null && !(record.value() instanceof Struct))) {
            return record;
        }

        Pair<Schema, Struct> p = getUpdatedValueAndSchema((Struct) record.key());
        Schema updatedSchemaForKey = p.getFirst();
        Struct updatedValueForKey = p.getSecond();

        Schema updatedSchemaForValue = null;
        Struct updatedValueForValue = null;
        if (record.value() != null) {
            Pair<Schema, Struct> val = getUpdatedValueAndSchema((Struct) record.value());
            updatedSchemaForValue = val.getFirst();
            updatedValueForValue = val.getSecond();
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchemaForKey, updatedValueForKey, updatedSchemaForValue, updatedValueForValue, record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    private boolean isValueSetStruct(Field field) {
        return field.schema().fields().size() == 2
                && (Objects.equals(field.schema().fields().get(0).name(), "value")
                && Objects.equals(field.schema().fields().get(1).name(), "set"));
    }

    private Schema makeUpdatedSchema(Schema schema, Struct value) {
        if (value == null) {
            return schema;
        }

        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            if (field.schema().type() == Type.STRUCT) {
                if (isValueSetStruct(field) && value.get(field.name()) != null) {
                    builder.field(field.name(), field.schema().field("value").schema());
                } else {
                    builder.field(field.name(), makeUpdatedSchema(field.schema(), (Struct) value.get(field.name())));
                }
            } else {
                builder.field(field.name(), field.schema());
            }
        }

        return builder.build();
    }

    private Struct makeUpdatedValue(Schema updatedSchema, Struct value) {
        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            if (field.schema().type() == Type.STRUCT) {
                Struct fieldValue = (Struct) value.get(field);
                if (isValueSetStruct(field) && fieldValue != null) {
                    updatedValue.put(field.name(), fieldValue.get("value"));
                } else if (fieldValue != null) {
                    updatedValue.put(field.name(), makeUpdatedValue(updatedSchema.field(field.name()).schema(), fieldValue));
                }
            } else {
                updatedValue.put(field.name(), value.get(field));
            }
        }

        return updatedValue;
    }

    public Pair<Schema, Struct> getUpdatedValueAndSchema(Struct value) {
        Schema updatedSchema = makeUpdatedSchema(value.schema(), value);

        LOGGER.debug("Updated schema as json: " + io.debezium.data.SchemaUtil.asString(value.schema()));

        return new org.yb.util.Pair<>(updatedSchema, makeUpdatedValue(updatedSchema, value));
    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
