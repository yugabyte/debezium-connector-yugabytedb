/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A standalone producer for writing schema history to a Kafka topic.
 * <p>
 * Instantiated in {@link YugabyteDBStreamingChangeEventSource} to produce schema history events:
 * <ul>
 *   <li><b>SCHEMA_SNAPSHOT:</b> On connector startup with no existing offsets, publishes
 *       the initial schema state for a schema history-configured table. /li>
 *   <li><b>SCHEMA_CHANGE:</b> When table schema differs from cached schema, indicating
 *       a DDL operation has occurred</li>
 * </ul>
 * </p>
 * <p>
 * This producer operates asynchronously and independently of the main change event capture
 * pipeline. Producer failures do not impact connector health or change event processing.
 * </p>
 * <p>
 * Schema history events include YugabyteDB-specific metadata such as primary key structure,
 * hash key indicators, and PostgreSQL OID type information.
 * </p>
 *
 * @author Michael Terranova
 * @see YugabyteDBStreamingChangeEventSource
 */

public class YugabyteDBSchemaHistoryProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBSchemaHistoryProducer.class);

    // Singleton instances - one producer per topic (shared across all tasks)
    private static final ConcurrentHashMap<String, YugabyteDBSchemaHistoryProducer> INSTANCES = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, AtomicInteger> REFERENCE_COUNTS = new ConcurrentHashMap<>();

    private final String topicName;
    private final String bootstrapServers;
    private final String connectorName;
    private final String securityProtocol;
    private final String sslKeystoreLocation;
    private final String sslKeystorePassword;
    private final String sslKeystoreType;
    private final String sslTruststoreLocation;
    private final String sslTruststorePassword;
    private final String sslTruststoreType;

    private KafkaProducer<String, String> producer;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean disabled = new AtomicBoolean(false);

    /**
     * Gets or creates a schema history producer for the given topic.
     * Uses singleton pattern to ensure one producer per topic across all tasks.
     *
     * @param topicName the Kafka topic to write schema history to (used as singleton key)
     * @param bootstrapServers the Kafka bootstrap servers
     * @param connectorName the connector name (used as key prefix)
     * @param securityProtocol the security protocol (e.g., SSL, PLAINTEXT)
     * @param sslKeystoreLocation the SSL keystore location
     * @param sslKeystorePassword the SSL keystore password
     * @param sslKeystoreType the SSL keystore type
     * @param sslTruststoreLocation the SSL truststore location
     * @param sslTruststorePassword the SSL truststore password
     * @param sslTruststoreType the SSL truststore type
     * @return the singleton producer instance for this topic
     */
    public static YugabyteDBSchemaHistoryProducer getInstance(
            String topicName,
            String bootstrapServers,
            String connectorName,
            String securityProtocol,
            String sslKeystoreLocation,
            String sslKeystorePassword,
            String sslKeystoreType,
            String sslTruststoreLocation,
            String sslTruststorePassword,
            String sslTruststoreType) {

        return INSTANCES.computeIfAbsent(topicName, key -> {
            LOGGER.info("Creating singleton schema history producer for topic: {}", topicName);
            REFERENCE_COUNTS.put(topicName, new AtomicInteger(0));
            return new YugabyteDBSchemaHistoryProducer(
                    topicName, bootstrapServers, connectorName, securityProtocol,
                    sslKeystoreLocation, sslKeystorePassword, sslKeystoreType,
                    sslTruststoreLocation, sslTruststorePassword, sslTruststoreType);
        });
    }

    /**
     * Increments the reference count for this producer instance.
     * Should be called by each task that uses the producer.
     */
    public void acquire() {
        AtomicInteger refCount = REFERENCE_COUNTS.get(topicName);
        if (refCount != null) {
            int count = refCount.incrementAndGet();
            LOGGER.debug("Schema history producer for topic {} acquired, ref count: {}", topicName, count);
        }
    }

    /**
     * Decrements the reference count and closes the producer if no longer used.
     * Should be called by each task when it's done using the producer.
     */
    public void release() {
        AtomicInteger refCount = REFERENCE_COUNTS.get(topicName);
        if (refCount != null) {
            int count = refCount.decrementAndGet();
            LOGGER.debug("Schema history producer for topic {} released, ref count: {}", topicName, count);

            if (count <= 0) {
                LOGGER.info("Schema history producer for topic {} has no more references, closing", topicName);
                YugabyteDBSchemaHistoryProducer instance = INSTANCES.remove(topicName);
                REFERENCE_COUNTS.remove(topicName);
                if (instance != null) {
                    instance.closeInternal();
                }
            }
        }
    }

    /**
     * Private constructor - use getInstance() instead.
     */
    private YugabyteDBSchemaHistoryProducer(
            String topicName,
            String bootstrapServers,
            String connectorName,
            String securityProtocol,
            String sslKeystoreLocation,
            String sslKeystorePassword,
            String sslKeystoreType,
            String sslTruststoreLocation,
            String sslTruststorePassword,
            String sslTruststoreType) {
        this.topicName = topicName;
        this.bootstrapServers = bootstrapServers;
        this.connectorName = connectorName;
        this.securityProtocol = securityProtocol;
        this.sslKeystoreLocation = sslKeystoreLocation;
        this.sslKeystorePassword = sslKeystorePassword;
        this.sslKeystoreType = sslKeystoreType;
        this.sslTruststoreLocation = sslTruststoreLocation;
        this.sslTruststorePassword = sslTruststorePassword;
        this.sslTruststoreType = sslTruststoreType;
        LOGGER.info("Schema history producer configured for topic: {}", topicName);
    }

    /**
     * Lazily initializes the Kafka producer. If initialization fails, the producer
     * is disabled and all subsequent operations become no-ops.
     *
     */
    private synchronized void ensureInitialized() {
        if (disabled.get() || initialized.get()) {
            return;
        }
        
        try {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, connectorName + "-schema-history");
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
            props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);

            // Add SSL configuration if security protocol is SSL
            if (securityProtocol != null && securityProtocol.equalsIgnoreCase("SSL")) {
                // Check if SSL certificate files exist
                java.io.File keystoreFile = new java.io.File(sslKeystoreLocation);
                java.io.File truststoreFile = new java.io.File(sslTruststoreLocation);

                if (!keystoreFile.exists() || !truststoreFile.exists()) {
                    LOGGER.warn("SSL certificate files not found (keystore: {}, truststore: {}), " +
                            "falling back to PLAINTEXT, will silently fail if Kafka requires SSL",
                            keystoreFile.exists(), truststoreFile.exists());
                } else {
                    props.put("security.protocol", "SSL");
                    props.put("ssl.keystore.location", sslKeystoreLocation);
                    props.put("ssl.keystore.type", sslKeystoreType);
                    props.put("ssl.truststore.location", sslTruststoreLocation);
                    props.put("ssl.truststore.type", sslTruststoreType);

                    if (sslKeystorePassword != null) {
                        props.put("ssl.keystore.password", sslKeystorePassword);
                    }
                    if (sslTruststorePassword != null) {
                        props.put("ssl.truststore.password", sslTruststorePassword);
                    }

                    LOGGER.info("Schema history producer configured with SSL security");
                }
            }

            StringSerializer keySerializer = new StringSerializer();
            StringSerializer valueSerializer = new StringSerializer();
            
            Map<String, Object> configMap = new HashMap<>();
            for (String name : props.stringPropertyNames()) {
                configMap.put(name, props.getProperty(name));
            }
            keySerializer.configure(configMap, true);
            valueSerializer.configure(configMap, false);
            
            this.producer = new KafkaProducer<>(props, keySerializer, valueSerializer);
            initialized.set(true);
            LOGGER.info("Schema history producer initialized successfully for topic: {}", topicName);
        } catch (Throwable t) {
            LOGGER.error("Failed to initialize schema history producer, disabling feature: {}", t.getMessage(), t);
            disabled.set(true);
        }
    }

    /**
     * Records a schema change event (DDL).
     * 
     * @param tableId the table identifier
     * @param tabletId the tablet identifier
     * @param schema the schema protobuf
     * @param eventType the type of event (e.g., "DDL", "INITIAL")
     */
    public void recordSchemaChange(String tableId, String tabletId, CdcService.CDCSDKSchemaPB schema, String eventType) {
        if (disabled.get()) {
            return;
        }
        
        try {
            ensureInitialized();
            if (disabled.get() || producer == null) {
                return;
            }
            
            String key = connectorName + ":" + tableId;
            String value = buildSchemaJson(tableId, tabletId, schema, eventType);
            
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
            
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOGGER.warn("Failed to send schema history record for table {}: {}", tableId, exception.getMessage());
                } else {
                    LOGGER.debug("Schema history record sent for table {} to partition {} offset {}", 
                            tableId, metadata.partition(), metadata.offset());
                }
            });
            
            LOGGER.info("Schema history {} event queued for table: {}", eventType, tableId);
        } catch (Throwable t) {
            LOGGER.warn("Error recording schema change for table {}: {}", tableId, t.getMessage());
        }
    }

    /**
     * Builds a simple JSON representation of the schema change.
     */
    private String buildSchemaJson(String tableId, String tabletId, CdcService.CDCSDKSchemaPB schema, String eventType) {
        String checksum = calculateSchemaChecksum(schema);

        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"connector\":\"").append(escapeJson(connectorName)).append("\",");
        sb.append("\"table\":\"").append(escapeJson(tableId)).append("\",");
        sb.append("\"tablet\":\"").append(escapeJson(tabletId)).append("\",");
        sb.append("\"eventType\":\"").append(escapeJson(eventType)).append("\",");
        sb.append("\"timestamp\":\"").append(Instant.now().toString()).append("\",");
        sb.append("\"schemaChecksum\":\"").append(checksum).append("\",");
        sb.append("\"schema\":{");

        sb.append("\"columns\":[");
        for (int i = 0; i < schema.getColumnInfoCount(); i++) {
            if (i > 0) sb.append(",");
            CdcService.CDCSDKColumnInfoPB col = schema.getColumnInfo(i);
            sb.append("{");
            sb.append("\"name\":\"").append(escapeJson(col.getName())).append("\",");
            sb.append("\"type\":\"").append(col.getType().getMain().name()).append("\",");
            sb.append("\"isKey\":").append(col.getIsKey()).append(",");
            sb.append("\"isHashKey\":").append(col.getIsHashKey()).append(",");
            sb.append("\"isNullable\":").append(col.getIsNullable()).append(",");
            sb.append("\"oid\":").append(col.getOid());
            sb.append("}");
        }
        sb.append("]");

        sb.append("}");
        sb.append("}");

        return sb.toString();
    }

    /**
     * Simple JSON string escaping.
     */
    private String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private String cleanTypeName(String typeName) {
        if (typeName == null) return "";
        String cleaned = typeName.replace("main: ", "").replace("\n", "").trim();
        return cleaned;
    }

    /**
     * Calculates a deterministic checksum for a schema from protobuf representation.
     * The checksum is based on column names, types, keys, and nullability to detect schema changes.
     *
     * @param schema the CDC schema protobuf
     * @return hex string representation of SHA-256 hash
     */
    private String calculateSchemaChecksum(CdcService.CDCSDKSchemaPB schema) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            StringBuilder schemaString = new StringBuilder();

            // Build deterministic string representation of schema
            for (int i = 0; i < schema.getColumnInfoCount(); i++) {
                CdcService.CDCSDKColumnInfoPB col = schema.getColumnInfo(i);
                schemaString.append(col.getName()).append(":");
                schemaString.append(col.getType().getMain().name()).append(":");
                schemaString.append(col.getIsKey()).append(":");
                schemaString.append(col.getIsHashKey()).append(":");
                schemaString.append(col.getIsNullable()).append(":");
                schemaString.append(col.getOid()).append(";");
            }

            byte[] hashBytes = digest.digest(schemaString.toString().getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hashBytes);
        } catch (Exception e) {
            LOGGER.warn("Failed to calculate schema checksum: {}", e.getMessage());
            return "unknown";
        }
    }

    /**
     * Calculates a deterministic checksum for a schema from Debezium Table representation.
     *
     * @param table the Debezium table object
     * @return hex string representation of SHA-256 hash
     */
    private String calculateSchemaChecksum(Table table) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            StringBuilder schemaString = new StringBuilder();

            // Build deterministic string representation of schema
            List<Column> columns = table.columns();
            for (Column col : columns) {
                schemaString.append(col.name()).append(":");
                schemaString.append(col.typeName()).append(":");
                schemaString.append(col.isOptional()).append(":");
                schemaString.append(col.length()).append(":");
                schemaString.append(col.scale().orElse(null)).append(";");
            }

            // Include primary key information
            List<String> pkColumns = table.primaryKeyColumnNames();
            schemaString.append("PK:");
            for (String pkCol : pkColumns) {
                schemaString.append(pkCol).append(",");
            }

            byte[] hashBytes = digest.digest(schemaString.toString().getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hashBytes);
        } catch (Exception e) {
            LOGGER.warn("Failed to calculate schema checksum: {}", e.getMessage());
            return "unknown";
        }
    }

    /**
     * Converts byte array to hex string.
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    /**
     * Records schema change from Debezium's internal schema representation.
     * This provides proper SQL types instead of protobuf types.
     *
     * @param tableId the table identifier
     * @param tabletId the tablet identifier
     * @param schema the YugabyteDB schema object
     * @param eventType the type of event (e.g., "DDL", "INITIAL")
     */
    public void recordSchemaFromDebeziumSchema(
            TableId tableId,
            String tabletId,
            YugabyteDBSchema schema,
            String eventType) {
        if (disabled.get()) {
            return;
        }

        try {
            ensureInitialized();
            if (disabled.get() || producer == null) {
                return;
            }

            Table table = schema.tableForTablet(tableId, tabletId);
            if (table == null) {
                LOGGER.warn("No schema found for table {} tablet {}", tableId, tabletId);
                return;
            }

            String key = connectorName + ":" + tableId;
            String value = buildSchemaJsonFromDebeziumTable(tableId.toString(), tabletId, table, eventType);

            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOGGER.warn("Failed to send schema history record for table {}: {}", tableId, exception.getMessage());
                } else {
                    LOGGER.debug("Schema history record sent for table {} to partition {} offset {}",
                            tableId, metadata.partition(), metadata.offset());
                }
            });

            LOGGER.info("Schema history {} event queued for table: {}", eventType, tableId);
        } catch (Throwable t) {
            LOGGER.warn("Error recording schema change from Debezium schema for table {}: {}", tableId, t.getMessage());
        }
    }

    /**
     * Builds JSON from Debezium's Table object (proper SQL types).
     */
    private String buildSchemaJsonFromDebeziumTable(
            String tableId,
            String tabletId,
            Table table,
            String eventType) {
        String checksum = calculateSchemaChecksum(table);

        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append("\"connector\":\"").append(escapeJson(connectorName)).append("\",");
        sb.append("\"table\":\"").append(escapeJson(tableId)).append("\",");
        sb.append("\"tablet\":\"").append(escapeJson(tabletId)).append("\",");
        sb.append("\"eventType\":\"").append(escapeJson(eventType)).append("\",");
        sb.append("\"timestamp\":\"").append(Instant.now().toString()).append("\",");
        sb.append("\"schemaChecksum\":\"").append(checksum).append("\",");
        sb.append("\"schema\":{");

        sb.append("\"columns\":[");
        List<Column> columns = table.columns();
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) sb.append(",");
            Column col = columns.get(i);
            sb.append("{");
            sb.append("\"name\":\"").append(escapeJson(col.name())).append("\",");
            sb.append("\"type\":\"").append(cleanTypeName(col.typeName())).append("\",");
            sb.append("\"isNullable\":").append(col.isOptional());
            if (col.length() > 0) {
                sb.append(",\"length\":").append(col.length());
            }
            if (col.scale().isPresent()) {
                sb.append(",\"scale\":").append(col.scale().get());
            }
            sb.append("}");
        }
        sb.append("]");

        List<String> pkColumns = table.primaryKeyColumnNames();
        if (!pkColumns.isEmpty()) {
            sb.append(",\"primaryKey\":[");
            for (int i = 0; i < pkColumns.size(); i++) {
                if (i > 0) sb.append(",");
                sb.append("\"").append(escapeJson(pkColumns.get(i))).append("\"");
            }
            sb.append("]");
        }

        sb.append("}");
        sb.append("}");

        return sb.toString();
    }

    /**
     * Calculates checksum for a schema (public method for external use).
     * Allows the streaming source to compare schemas before publishing.
     *
     * @param schema the CDC schema protobuf
     * @return hex string representation of SHA-256 hash
     */
    public String getSchemaChecksum(CdcService.CDCSDKSchemaPB schema) {
        return calculateSchemaChecksum(schema);
    }

    /**
     * Calculates checksum for a Debezium table (public method for external use).
     * Allows the streaming source to compare schemas before publishing.
     *
     * @param table the Debezium table object
     * @return hex string representation of SHA-256 hash
     */
    public String getSchemaChecksum(Table table) {
        return calculateSchemaChecksum(table);
    }

    /**
     * Closes the producer (delegates to release() for reference counting).
     * This should be called by tasks when they're done using the producer.
     */
    public void close() {
        release();
    }

    /**
     * Internal method to actually close the Kafka producer.
     * Only called when reference count reaches zero.
     */
    private void closeInternal() {
        if (producer != null) {
            try {
                producer.flush();
                producer.close();
                LOGGER.info("Schema history producer closed for connector: {}", connectorName);
            } catch (Throwable t) {
                LOGGER.warn("Error closing schema history producer for {}: {}", connectorName, t.getMessage());
            }
        }
    }

    /**
     * Checks if the producer is disabled due to init failure.
     */
    public boolean isDisabled() {
        return disabled.get();
    }
}

