package io.debezium.connector.yugabytedb.transforms;

import io.debezium.data.Envelope;
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.yb.util.Pair;

import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.ADD_HEADERS;
import static io.debezium.transforms.ExtractNewRecordStateConfigDefinition.HANDLE_DELETES;
import static org.hamcrest.MatcherAssert.assertThat;


public class PGCompatibleTest {
    final Schema idSchema =  SchemaBuilder.struct()
            .field("value", Schema.INT64_SCHEMA)
            .field("set", Schema.BOOLEAN_SCHEMA);

    final Schema nameSchema =  SchemaBuilder.struct()
            .field("value", Schema.STRING_SCHEMA)
            .field("set", Schema.BOOLEAN_SCHEMA)
            .optional();

    final Schema keySchema = SchemaBuilder.struct()
            .field("id", idSchema)
            .build();

    final Schema valueSchema = SchemaBuilder.struct()
            .field("id", idSchema)
            .field("name", nameSchema)
            .field("location", nameSchema)
            .build();

    final Schema sourceSchema = SchemaBuilder.struct()
            .field("lsn", Schema.INT32_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA)
            .field("op", Schema.STRING_SCHEMA)
            .build();

    final Envelope envelope = Envelope.defineSchema()
            .withName("dummy.Envelope")
            .withRecord(valueSchema)
            .withSource(sourceSchema)
            .build();

    private Struct createIdStruct() {
        final Struct id = new Struct(idSchema);
        id.put("value", (long) 1L);
        id.put("set", true);

        return id;
    }

    private Struct createNameStruct() {
        final Struct name = new Struct(nameSchema);
        name.put("value", "yb");
        name.put("set", true);
        return name;
    }

    private Struct createValue() {
        final Struct value = new Struct(valueSchema);
        value.put("id", createIdStruct());
        value.put("name", createNameStruct());
        value.put("location", null);

        return value;
    }

    @Test
    public void testSingleLevelStruct() {
        try (final PGCompatible<SourceRecord> transform = new PGCompatible<>()) {
            final Pair<Schema, Struct> unwrapped = transform.getUpdatedValueAndSchema(valueSchema, createValue());
            assert(((Struct) unwrapped.getSecond()).getInt64("id") == 1);
            assert(((Struct) unwrapped.getSecond()).getString("name").equals("yb"));
            assert(((Struct) unwrapped.getSecond()).getString("location") == null);

        }
    }

    private Struct createPayload() {
        final Struct source = new Struct(sourceSchema);
        source.put("lsn", 1234);
        source.put("ts_ms", 12836);
        source.put("op", "c");
        return envelope.create(createValue(), source, Instant.now());
    }

    @Test
    public void testPayload() {
        try (final PGCompatible<SourceRecord> transform = new PGCompatible<>()) {
            final Pair<Schema, Struct> unwrapped = transform.getUpdatedValueAndSchema(sourceSchema, createPayload());
            Schema valueSchema = unwrapped.getFirst();

            assert(valueSchema.type() == Schema.Type.STRUCT);
            assert(valueSchema.fields().size() == 6);
            assert(valueSchema.field("op").schema().type() == Schema.Type.STRING);

            Schema afterSchema = valueSchema.field("after").schema();
            assert (afterSchema.type() == Schema.Type.STRUCT);
            assert (afterSchema.fields().size() == 2);
            assert (afterSchema.field("id").schema().type() == Schema.Type.INT64);
            assert (afterSchema.field("name").schema().type() == Schema.Type.STRING);

            Struct after = ((Struct) unwrapped.getSecond()).getStruct("after");
            assert(after.getInt64("id") == 1);
            assert(after.getString("name").equals("yb"));
        }
    }

    private SourceRecord createCreateRecord() {
        final Struct key = new Struct(keySchema);
        key.put("id", createIdStruct());

        final Struct payload = createPayload();
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", keySchema, key, envelope.schema(), payload);
    }

    private String getSourceRecordHeaderByKey(SourceRecord record, String headerKey) {
        Iterator<Header> operationHeader = record.headers().allWithName(headerKey);
        if (!operationHeader.hasNext()) {
            return null;
        }

        Object value = operationHeader.next().value();

        return value != null ? value.toString() : null;
    }


    @Test
    public void testHandleCreateRewrite() {
        try (final PGCompatible<SourceRecord> transform = new PGCompatible<>()) {
            final Map<String, String> props = new HashMap<>();
            props.put(HANDLE_DELETES.name(), "rewrite");
            props.put(ADD_HEADERS.name(), "op");
            transform.configure(props);

            final SourceRecord createRecord = createCreateRecord();
            final SourceRecord unwrapped = transform.apply(createRecord);
            assert(((Struct) unwrapped.value()).getString("__deleted").equals("false"));
            assert(((Struct) unwrapped.value()).getInt64("id") == 1);
            assert(((Struct) unwrapped.value()).getString("name").equals("yb"));
            assert(unwrapped.headers().size() == 1);
            String headerValue = getSourceRecordHeaderByKey(unwrapped, ExtractNewRecordStateConfigDefinition.DEBEZIUM_OPERATION_HEADER_KEY);
            assert(headerValue.equals(Envelope.Operation.CREATE.code()));
        }
    }
}
