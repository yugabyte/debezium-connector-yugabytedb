package io.debezium.connector.yugabytedb.transforms;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class StriimCompatibleTest {
    final Schema idSchema =  SchemaBuilder.struct()
            .field("value", Schema.INT64_SCHEMA)
            .field("set", Schema.BOOLEAN_SCHEMA);

    final Schema nameSchema =  SchemaBuilder.struct()
            .field("value", Schema.OPTIONAL_STRING_SCHEMA)
            .field("set", Schema.BOOLEAN_SCHEMA)
            .optional();

    final Schema keySchema = SchemaBuilder.struct()
            .field("id", idSchema)
            .build();

    final Schema valueSchema = SchemaBuilder.struct()
            .field("id", idSchema)
            .field("name", nameSchema)
            .build();

    final Schema sourceSchema = SchemaBuilder.struct()
            .field("lsn", Schema.STRING_SCHEMA)
            .field("sequence", Schema.STRING_SCHEMA)
            .field("txId", Schema.STRING_SCHEMA)
            .field("schema", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .build();

    final List<String> columns = Arrays.asList("id", "name");

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

    private Struct createUpdatedIdStruct() {
        final Struct id = new Struct(idSchema);
        id.put("value", (long) 2L);
        id.put("set", true);
        return id;
    }

    private Struct createNameStruct() {
        final Struct name = new Struct(nameSchema);
        name.put("value", "yb");
        name.put("set", true);
        return name;
    }

    private Struct createUpdatedNameStruct() {
        final Struct name = new Struct(nameSchema);
        name.put("value", "yb2");
        name.put("set", true);
        return name;
    }

    private Struct createValue() {
        final Struct value = new Struct(valueSchema);
        value.put("id", createIdStruct());
        value.put("name", createNameStruct());
        return value;
    }

    private Struct createUpdatedValue(boolean updateId) {
        final Struct value = new Struct(valueSchema);
        value.put("id", updateId ? createUpdatedIdStruct() : createIdStruct());
        value.put("name", createUpdatedNameStruct());
        return value;
    }

    private Struct createSourceStruct() {
        final Struct source = new Struct(sourceSchema);
        source.put("lsn", "1:3::0:0");
        source.put("sequence", "[\"454::89\"]");
        source.put("schema", "public");
        source.put("table", "store");
        source.put("txId", "");
        return source;
    }

    private SourceRecord createCreateRecord() {
        final Struct key = new Struct(keySchema);
        key.put("id", createIdStruct());

        final Struct createPayload = envelope.create(createValue(), createSourceStruct(), Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", keySchema, key, envelope.schema(), createPayload);
    }

    private SourceRecord createUpdateRecord(boolean updateId) {
        final Struct key = new Struct(keySchema);
        key.put("id", updateId ? createUpdatedIdStruct() : createIdStruct());

        final Struct updatePayload = envelope.update(createValue(), createUpdatedValue(updateId), createSourceStruct(), Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", keySchema, key, envelope.schema(), updatePayload);
    }

    private SourceRecord createDeleteRecord() {
        final Struct key = new Struct(keySchema);
        key.put("id", createIdStruct());

        final Struct deletePayload = envelope.delete(createValue(), createSourceStruct(), Instant.now());
        return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", keySchema, key, envelope.schema(), deletePayload);
    }

    @Test
    public void testCreateRecord() {
        try (final StriimCompatible<SourceRecord> transform = new StriimCompatible<>()) {
            final SourceRecord createRecord = createCreateRecord();

            List<String> expectedData = new ArrayList<>();
            expectedData.add("1");
            expectedData.add("yb");

            final SourceRecord unwrapped = transform.apply(createRecord);
            Struct unwrappedKey = (Struct) unwrapped.key();
            Struct unwrappedValue = (Struct) unwrapped.value();

            assert(((Struct)unwrappedKey.get("id")).getInt64("value") == 1);
            assert(unwrappedValue.get("before") == null);
            assert(unwrappedValue.getArray("data").equals(expectedData));
            assert(unwrappedValue.getArray("columns").equals(columns));

            assert(((Struct)unwrappedValue.get("metadata")).getString("LSN").equals("1:3::0:0"));
            assert(((Struct)unwrappedValue.get("metadata")).getString("Sequence").equals("[\"454::89\"]"));
            assert(((Struct)unwrappedValue.get("metadata")).getString("TxnID").equals(""));
            assert(((Struct)unwrappedValue.get("metadata")).getString("TableName").equals("public.store"));
            assert(((Struct)unwrappedValue.get("metadata")).getString("OperationName").equals("INSERT"));
            assert(((Struct)unwrappedValue.get("metadata")).get("PK_UPDATE") == null);
        }
    }

    @Test
    public void testUpdateRecord() {
        try (final StriimCompatible<SourceRecord> transform = new StriimCompatible<>()) {
            final SourceRecord updateRecord = createUpdateRecord(true);

            List<String> expectedData = new ArrayList<>();
            expectedData.add("2");
            expectedData.add("yb2");

            List<String> expectedBeforeData = new ArrayList<>();
            expectedBeforeData.add("1");
            expectedBeforeData.add(null);

            final SourceRecord unwrapped = transform.apply(updateRecord);
            Struct unwrappedKey = (Struct) unwrapped.key();
            Struct unwrappedValue = (Struct) unwrapped.value();

            assert(((Struct)unwrappedKey.get("id")).getInt64("value") == 2);
            assert(unwrappedValue.getArray("before").equals(expectedBeforeData));
            assert(unwrappedValue.getArray("data").equals(expectedData));
            assert(unwrappedValue.getArray("columns").equals(columns));

            assert(((Struct)unwrappedValue.get("metadata")).getString("LSN").equals("1:3::0:0"));
            assert(((Struct)unwrappedValue.get("metadata")).getString("Sequence").equals("[\"454::89\"]"));
            assert(((Struct)unwrappedValue.get("metadata")).getString("TxnID").equals(""));
            assert(((Struct)unwrappedValue.get("metadata")).getString("TableName").equals("public.store"));
            assert(((Struct)unwrappedValue.get("metadata")).getString("OperationName").equals("UPDATE"));
            assert(((Struct)unwrappedValue.get("metadata")).getBoolean("PK_UPDATE") == true);
        }
    }

    @Test
    public void testUpdateRecordWithoutPrimaryKeyChange() {
        try (final StriimCompatible<SourceRecord> transform = new StriimCompatible<>()) {
            final SourceRecord updateRecord = createUpdateRecord(false);

            List<String> expectedData = new ArrayList<>();
            expectedData.add("1");
            expectedData.add("yb2");

            List<String> expectedBeforeData = new ArrayList<>();
            expectedBeforeData.add("1");
            expectedBeforeData.add(null);

            final SourceRecord unwrapped = transform.apply(updateRecord);
            Struct unwrappedKey = (Struct) unwrapped.key();
            Struct unwrappedValue = (Struct) unwrapped.value();

            assert(((Struct)unwrappedKey.get("id")).getInt64("value") == 1);
            assert(unwrappedValue.getArray("before").equals(expectedBeforeData));
            assert(unwrappedValue.getArray("data").equals(expectedData));
            assert(unwrappedValue.getArray("columns").equals(columns));

            assert(((Struct)unwrappedValue.get("metadata")).getString("LSN").equals("1:3::0:0"));
            assert(((Struct)unwrappedValue.get("metadata")).getString("Sequence").equals("[\"454::89\"]"));
            assert(((Struct)unwrappedValue.get("metadata")).getString("TxnID").equals(""));
            assert(((Struct)unwrappedValue.get("metadata")).getString("TableName").equals("public.store"));
            assert(((Struct)unwrappedValue.get("metadata")).getString("OperationName").equals("UPDATE"));
            assert(((Struct)unwrappedValue.get("metadata")).get("PK_UPDATE") == null);
        }
    }


    @Test
    public void testDeleteRecord() {
        try (final StriimCompatible<SourceRecord> transform = new StriimCompatible<>()) {
            final SourceRecord deleteRecord = createDeleteRecord();

            List<String> expectedData = new ArrayList<>();
            expectedData.add("1");
            expectedData.add(null);

            final SourceRecord unwrapped = transform.apply(deleteRecord);
            Struct unwrappedKey = (Struct) unwrapped.key();
            Struct unwrappedValue = (Struct) unwrapped.value();

            assert(((Struct)unwrappedKey.get("id")).getInt64("value") == 1);
            assert(unwrappedValue.get("before") == null);
            assert(unwrappedValue.getArray("data").equals(expectedData));
            assert(unwrappedValue.getArray("columns").equals(columns));

            assert(((Struct)unwrappedValue.get("metadata")).getString("LSN").equals("1:3::0:0"));
            assert(((Struct)unwrappedValue.get("metadata")).getString("Sequence").equals("[\"454::89\"]"));
            assert(((Struct)unwrappedValue.get("metadata")).getString("TxnID").equals(""));
            assert(((Struct)unwrappedValue.get("metadata")).getString("TableName").equals("public.store"));
            assert(((Struct)unwrappedValue.get("metadata")).getString("OperationName").equals("DELETE"));
            assert(((Struct)unwrappedValue.get("metadata")).get("PK_UPDATE") == null);
        }
    }


}
