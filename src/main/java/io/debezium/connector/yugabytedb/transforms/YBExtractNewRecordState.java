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
import io.debezium.transforms.ExtractNewRecordStateConfigDefinition;

/**
 * This is a SMT used to flatten the records published by the YugabyteDB gRPC Connector. The connector
 * publishes records which represent each operation that happen on a database - these records have a
 * complex structure representing the details of the original database event.
 *
 * For example, a record representing an insert operation would have structure similar to the following:
 * 
 * <br/><br/>
 * 
 * <code>
 *  "id": {"value": 123, "set": true},<br/>
 *  "name": {"value": "John Doe", "set": true},<br/>
 *  "email": {"value": "johndoe@email.com", "set": true}<br/>
 * </code>
 * 
 * <br/>
 * The complex structure published by the connector is not compatible with many downstream consumers
 * and they expect a record in a simpler flattened format. This SMT extends the event flattening
 * capabilities of the base {@link ExtractNewRecordState} SMT to facilitate event flattening.
 *
 * After flattening, the above record would look like:
 * 
 * <br/><br/>
 * <code> {"id": 123, "name": "John Doe", "email": "johndoe@email.com"} </code>
 * <br/><br/>
 * 
 * Now, the records published by the YugabyteDB gRPC Connector by default only contain the columns
 * which were changed in the original database event. Similarly, there can be a case where a column
 * can be explicitly set to null in the original database event. So in addition to the above
 * flattening, this SMT also handles the case where it distinguishes between a column which was 
 * explicitly set to null and a column which was not changed at all in the original database event
 * and generates a flattened record accordingly.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YBExtractNewRecordState<R extends ConnectRecord<R>> extends ExtractNewRecordState<R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(YBExtractNewRecordState.class);

    // This is a connector property, which, when set, will read the delete records and convert them
    // all to tombstone records and subsequently drop all the tombstone records explicitly received
    // from the source connector. This is useful when the downstream consumers expect
    // tombstone records to process deletes.
    public static final String DELETE_TO_TOMBSTONE = "delete.to.tombstone";

    private Cache<Schema, Schema> schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(256));
    private boolean convertDeleteToTombstone = false;

    @Override
    public void configure(Map<String, ?> configs) {
        // Explicitly set default value to false when config is not provided.
        String deleteToTombstoneConfig = (String) configs.get(DELETE_TO_TOMBSTONE);
        convertDeleteToTombstone = deleteToTombstoneConfig != null ?
                                    Boolean.parseBoolean(deleteToTombstoneConfig) : false;

        // Create a mutable copy of configs to allow modifications
        Map<String, Object> mutableConfigs = new java.util.HashMap<>(configs);

        if (convertDeleteToTombstone) {
            // If we need to convert delete records to tombstones, we do not want
            // the base SMT to drop delete records, set delete.handling.mode to 'none'.
            mutableConfigs.put(ExtractNewRecordStateConfigDefinition.HANDLE_DELETES.name(), "none");
        }

        super.configure(mutableConfigs);
    }

    @Override
    public R apply(final R record) {
        final R ret = super.apply(record);
        if (ret == null || (ret.value() != null && !(ret.value() instanceof Struct))) {
            // If ret == null, it means that the base SMT has dropped the record.
            // The other condition is to check if the record is a valid envelope record
            // (DML record) and not a metadata record like a transactional or heartbeat
            // record, as the latter ones need to be passed through as is.
            LOGGER.trace("Returning the value as returned by the base transform");
            return ret;
        }

        // If the config drop.tombstone is set to true, they will be dropped by the base SMT
        // which is handled in the previous block itself, but if it is set to false and our
        // config delete.to.tombstone is set to true, we need to drop it here.
        if (convertDeleteToTombstone && isTombstoneRecord(record)) {
            LOGGER.trace("Dropping tombstone record as per configuration");
            return null;
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

        if (convertDeleteToTombstone && isDeleteRecord(record)) {
            LOGGER.trace("Converting delete record to tombstone as per configuration");
            return ret.newRecord(ret.topic(), ret.kafkaPartition(), updatedSchemaForKey, updatedValueForKey, null, null, record.timestamp());
        }

        return ret.newRecord(ret.topic(), ret.kafkaPartition(), updatedSchemaForKey, updatedValueForKey, updatedSchemaForValue, updatedValueForValue, ret.timestamp());
    }

    @Override
    public void close() {
        super.close();
        schemaUpdateCache = null;
    }

    private boolean isSimplifiableField(Field field) {
        if (field.schema().type() != Type.STRUCT) {
            return false;
        }

        if (field.schema().fields().size() != 2
                || (!Objects.equals(field.schema().fields().get(0).name(), "value")
                        || !Objects.equals(field.schema().fields().get(1).name(), "set"))) {
            return false;
        }
        return true;
    }

    /**
     * @param record
     * @return true if the record is a tombstone record, false otherwise.
     */
    protected boolean isTombstoneRecord(R record) {
        return record.valueSchema() == null && record.value() == null;
    }

    /**
     * @param record
     * @return true if the record is a delete record, false otherwise.
     */
    protected boolean isDeleteRecord(R record) {
        return record.valueSchema() != null && ((Struct) record.value()).getStruct("after") == null;
    }

    // todo: this function can be removed
    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            if (isSimplifiableField(field)) {
                builder.field(field.name(), field.schema().field("value").schema());
            }
            else {
                builder.field(field.name(), field.schema());
            }
        }

        return builder.build();
    }

    private Schema makeUpdatedSchema(Schema schema, Struct value) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            if (isSimplifiableField(field)) {
                if (value.get(field.name()) != null) {
                    builder.field(field.name(), field.schema().field("value").schema());
                }
            }
            else {
                builder.field(field.name(), field.schema());
            }
        }

        return builder.build();
    }

    private Pair<Schema, Struct> getUpdatedValueAndSchema(Struct obj) {
        final Struct value = obj;
        Schema updatedSchema = null;
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema(), value);
        }

        LOGGER.debug("Updated schema as json: " + io.debezium.data.SchemaUtil.asString(value.schema()));

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            if (isSimplifiableField(field)) {
                Struct fieldValue = (Struct) value.get(field);
                if (fieldValue != null) {
                    updatedValue.put(field.name(), fieldValue.get("value"));
                }
            }
            else {
                updatedValue.put(field.name(), value.get(field));
            }
        }

        return new org.yb.util.Pair<Schema, Struct>(updatedSchema, updatedValue);
    }
}

class SchemaUtil {

    public static SchemaBuilder copySchemaBasics(Schema source) {
        return copySchemaBasics(source, new SchemaBuilder(source.type()));
    }

    public static SchemaBuilder copySchemaBasics(Schema source, SchemaBuilder builder) {
        builder.name(source.name());
        builder.version(source.version());
        builder.doc(source.doc());

        final Map<String, String> params = source.parameters();
        if (params != null) {
            builder.parameters(params);
        }

        return builder;
    }

}
