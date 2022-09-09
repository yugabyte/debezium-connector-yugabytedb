package io.debezium.connector.yugabytedb.transforms;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.util.Pair;

import io.debezium.transforms.ExtractNewRecordState;

public class PGCompatible<R extends ConnectRecord<R>> extends ExtractNewRecordState<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PGCompatible.class);

    @Override
    public R apply(final R record) {
        final R ret = super.apply(record);
        if (ret == null || (ret.value() != null && !(ret.value() instanceof Struct))) {
            return ret;
        }

        Pair p = getUpdatedValueAndSchema((Struct) ret.key());
        Schema updatedSchemaForKey = (Schema) p.getFirst();
        Struct updatedValueForKey = (Struct) p.getSecond();

        Schema updatedSchemaForValue = null;
        Struct updatedValueForValue = null;
        if (ret.value() != null) {
            Pair val = getUpdatedValueAndSchema((Struct) ret.value());
            updatedSchemaForValue = (Schema) val.getFirst();
            updatedValueForValue = (Struct) val.getSecond();
        }

        return ret.newRecord(ret.topic(), ret.kafkaPartition(), updatedSchemaForKey, updatedValueForKey, updatedSchemaForValue, updatedValueForValue, ret.timestamp());
    }

    @Override
    public void close() {
        super.close();
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

    public Pair<Schema, Struct> getUpdatedValueAndSchema(Struct obj) {
        final Struct value = obj;
        Schema updatedSchema = null;
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema(), value);
        }

        LOGGER.debug("Updated schema as json: " + io.debezium.data.SchemaUtil.asString(value.schema()));

        return new org.yb.util.Pair<Schema, Struct>(updatedSchema, makeUpdatedValue(updatedSchema, value));
    }
}
