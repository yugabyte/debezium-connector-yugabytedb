/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.connector.yugabytedb.metrics.YugabyteDBSchemaHistoryMetricsMXBean;
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

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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

public class YugabyteDBSchemaHistoryProducer implements YugabyteDBSchemaHistoryMetricsMXBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBSchemaHistoryProducer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

    // Metrics
    private final AtomicLong schemaHistoryEventsSent = new AtomicLong(0);
    private final AtomicLong schemaHistoryEventsFailed = new AtomicLong(0);
    private final AtomicLong schemaHistoryEventsQueued = new AtomicLong(0);
    private final AtomicLong lastSuccessfulSendTimestamp = new AtomicLong(0);
    private final AtomicLong lastFailedSendTimestamp = new AtomicLong(0);
    private volatile String lastErrorMessage = null;
    private ObjectName mbeanName;

    /**
     * Creates a schema history producer for the given topic.
     * Each task creates its own producer instance following MySQL connector pattern.
     * @param topicName the Kafka topic to write schema history to
     * @param connectorName the connector name (used as client ID prefix)
     * @param bootstrapServers Kafka bootstrap servers
     * @param securityProtocol Security protocol (SSL, PLAINTEXT, etc.)
     * @param sslKeystoreLocation SSL keystore location (optional)
     * @param sslKeystorePassword SSL keystore password (optional)
     * @param sslKeystoreType SSL keystore type (PKCS12, JKS, etc.)
     * @param sslTruststoreLocation SSL truststore location (optional)
     * @param sslTruststorePassword SSL truststore password (optional)
     * @param sslTruststoreType SSL truststore type (PKCS12, JKS, etc.)
     */
    public YugabyteDBSchemaHistoryProducer(
            String topicName,
            String connectorName,
            String bootstrapServers,
            String securityProtocol,
            String sslKeystoreLocation,
            String sslKeystorePassword,
            String sslKeystoreType,
            String sslTruststoreLocation,
            String sslTruststorePassword,
            String sslTruststoreType) {
        this.topicName = topicName;
        this.connectorName = connectorName;
        this.bootstrapServers = bootstrapServers;
        this.securityProtocol = securityProtocol;
        this.sslKeystoreLocation = sslKeystoreLocation;
        this.sslKeystorePassword = sslKeystorePassword;
        this.sslKeystoreType = sslKeystoreType;
        this.sslTruststoreLocation = sslTruststoreLocation;
        this.sslTruststorePassword = sslTruststorePassword;
        this.sslTruststoreType = sslTruststoreType;
        LOGGER.info("Schema history producer configured for topic: {}", topicName);

        try {
            String metricName = "debezium.yugabytedb:type=schema-history-producer,topic=" +
                                org.apache.kafka.common.utils.Sanitizer.jmxSanitize(topicName);
            this.mbeanName = new ObjectName(metricName);
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            if (mBeanServer != null && !mBeanServer.isRegistered(mbeanName)) {
                mBeanServer.registerMBean(this, mbeanName);
                LOGGER.info("Registered schema history metrics MBean: {}", mbeanName);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to register schema history metrics MBean: {}", e.getMessage());
        }
    }

    /**
     * Lazily initializes the Kafka producer. If initialization fails, throws an exception
     * to fail the connector task.
     */
    private synchronized void ensureInitialized() {
        if (initialized.get()) {
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
            LOGGER.error("Failed to initialize schema history producer: {}", t.getMessage(), t);
            throw new RuntimeException("Schema history producer failed to initialize - Kafka unreachable or misconfigured", t);
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
        try {
            ensureInitialized();
            if (producer == null) {
                throw new IllegalStateException("Producer not initialized");
            }
            
            String key = connectorName + ":" + tableId;
            String value = buildSchemaJson(tableId, tabletId, schema, eventType);
            
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
            
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    schemaHistoryEventsFailed.incrementAndGet();
                    lastFailedSendTimestamp.set(System.currentTimeMillis());
                    lastErrorMessage = exception.getMessage();
                    LOGGER.warn("Failed to send schema history record for table {}: {}", tableId, exception.getMessage());
                } else {
                    schemaHistoryEventsSent.incrementAndGet();
                    lastSuccessfulSendTimestamp.set(System.currentTimeMillis());
                    LOGGER.debug("Schema history record sent for table {} to partition {} offset {}",
                            tableId, metadata.partition(), metadata.offset());
                }
            });

            schemaHistoryEventsQueued.incrementAndGet();
            LOGGER.info("Schema history {} event queued for table: {}", eventType, tableId);
        } catch (Throwable t) {
            LOGGER.warn("Error recording schema change for table {}: {}", tableId, t.getMessage());
        }
    }

    private String buildSchemaJson(String tableId, String tabletId, CdcService.CDCSDKSchemaPB schema, String eventType) {
        try {
            YugabyteDBSchemaHistoryEvent event = new YugabyteDBSchemaHistoryEvent();
            event.setConnector(connectorName);
            event.setTable(tableId);
            event.setTablet(tabletId);
            event.setEventType(eventType);
            event.setSchemaChecksum(calculateSchemaChecksum(schema));

            YugabyteDBSchemaHistoryEvent.SchemaInfo schemaInfo = new YugabyteDBSchemaHistoryEvent.SchemaInfo();
            List<YugabyteDBSchemaHistoryEvent.ColumnInfo> columns = new ArrayList<>();

            for (int i = 0; i < schema.getColumnInfoCount(); i++) {
                CdcService.CDCSDKColumnInfoPB col = schema.getColumnInfo(i);
                YugabyteDBSchemaHistoryEvent.ColumnInfo colInfo = new YugabyteDBSchemaHistoryEvent.ColumnInfo();
                colInfo.setName(col.getName());
                colInfo.setType(col.getType().getMain().name());
                colInfo.setIsKey(col.getIsKey());
                colInfo.setIsHashKey(col.getIsHashKey());
                colInfo.setIsNullable(col.getIsNullable());
                colInfo.setOid(col.getOid());
                columns.add(colInfo);
            }

            schemaInfo.setColumns(columns);
            event.setSchema(schemaInfo);

            return OBJECT_MAPPER.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            LOGGER.warn("Failed to serialize schema history event: {}", e.getMessage());
            return "{}";
        }
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

            List<Column> columns = table.columns();
            for (Column col : columns) {
                schemaString.append(col.name()).append(":");
                schemaString.append(col.typeName()).append(":");
                schemaString.append(col.isOptional()).append(":");
                schemaString.append(col.length()).append(":");
                schemaString.append(col.scale().orElse(null)).append(";");
            }

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
        try {
            ensureInitialized();
            if (producer == null) {
                throw new IllegalStateException("Producer not initialized");
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
                    schemaHistoryEventsFailed.incrementAndGet();
                    lastFailedSendTimestamp.set(System.currentTimeMillis());
                    lastErrorMessage = exception.getMessage();
                    LOGGER.warn("Failed to send schema history record for table {}: {}", tableId, exception.getMessage());
                } else {
                    schemaHistoryEventsSent.incrementAndGet();
                    lastSuccessfulSendTimestamp.set(System.currentTimeMillis());
                    LOGGER.debug("Schema history record sent for table {} to partition {} offset {}",
                            tableId, metadata.partition(), metadata.offset());
                }
            });

            schemaHistoryEventsQueued.incrementAndGet();
            LOGGER.info("Schema history {} event queued for table: {}", eventType, tableId);
        } catch (Throwable t) {
            LOGGER.warn("Error recording schema change from Debezium schema for table {}: {}", tableId, t.getMessage());
        }
    }

    private String buildSchemaJsonFromDebeziumTable(
            String tableId,
            String tabletId,
            Table table,
            String eventType) {
        try {
            YugabyteDBSchemaHistoryEvent event = new YugabyteDBSchemaHistoryEvent();
            event.setConnector(connectorName);
            event.setTable(tableId);
            event.setTablet(tabletId);
            event.setEventType(eventType);
            event.setSchemaChecksum(calculateSchemaChecksum(table));

            YugabyteDBSchemaHistoryEvent.SchemaInfo schemaInfo = new YugabyteDBSchemaHistoryEvent.SchemaInfo();
            List<YugabyteDBSchemaHistoryEvent.ColumnInfo> columns = new ArrayList<>();

            for (Column col : table.columns()) {
                YugabyteDBSchemaHistoryEvent.ColumnInfo colInfo = new YugabyteDBSchemaHistoryEvent.ColumnInfo();
                colInfo.setName(col.name());
                colInfo.setType(col.typeName());
                colInfo.setIsNullable(col.isOptional());
                if (col.length() > 0) {
                    colInfo.setLength(col.length());
                }
                if (col.scale().isPresent()) {
                    colInfo.setScale(col.scale().get());
                }
                columns.add(colInfo);
            }

            schemaInfo.setColumns(columns);
            schemaInfo.setPrimaryKey(table.primaryKeyColumnNames());
            event.setSchema(schemaInfo);

            return OBJECT_MAPPER.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            LOGGER.warn("Failed to serialize schema history event: {}", e.getMessage());
            return "{}";
        }
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
     * Closes this schema history producer and releases its resources.
     * This should be called by tasks when they're done using the producer.
     */
    public void close() {
        if (mbeanName != null) {
            try {
                MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                if (mBeanServer != null && mBeanServer.isRegistered(mbeanName)) {
                    mBeanServer.unregisterMBean(mbeanName);
                    LOGGER.info("Unregistered schema history metrics MBean: {}", mbeanName);
                }
            } catch (Exception e) {
                LOGGER.warn("Failed to unregister schema history metrics MBean: {}", e.getMessage());
            }
        }

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

    @Override
    public String getTopicName() {
        return topicName;
    }

    @Override
    public boolean isInitialized() {
        return initialized.get();
    }

    @Override
    public long getSchemaHistoryEventsSent() {
        return schemaHistoryEventsSent.get();
    }

    @Override
    public long getSchemaHistoryEventsFailed() {
        return schemaHistoryEventsFailed.get();
    }

    @Override
    public long getSchemaHistoryEventsQueued() {
        return schemaHistoryEventsQueued.get();
    }

    @Override
    public long getLastSuccessfulSendTimestamp() {
        return lastSuccessfulSendTimestamp.get();
    }

    @Override
    public long getLastFailedSendTimestamp() {
        return lastFailedSendTimestamp.get();
    }

    @Override
    public String getLastErrorMessage() {
        return lastErrorMessage;
    }

    @Override
    public void reset() {
        schemaHistoryEventsSent.set(0);
        schemaHistoryEventsFailed.set(0);
        schemaHistoryEventsQueued.set(0);
        lastSuccessfulSendTimestamp.set(0);
        lastFailedSendTimestamp.set(0);
        lastErrorMessage = null;
    }
}

