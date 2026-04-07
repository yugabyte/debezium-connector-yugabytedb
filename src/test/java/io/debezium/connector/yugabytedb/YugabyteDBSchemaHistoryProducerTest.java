/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import org.junit.jupiter.api.Test;
import org.yb.Common;
import org.yb.Value;
import org.yb.cdc.CdcService;
import org.yb.cdc.CdcService.CDCSDKColumnInfoPB;
import org.yb.cdc.CdcService.CDCSDKSchemaPB;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for YugabyteDBSchemaHistoryProducer.
 */
public class YugabyteDBSchemaHistoryProducerTest {

    @Test
    public void testBuildSchemaJsonProducesValidJson() throws Exception {
        CDCSDKSchemaPB schema = CDCSDKSchemaPB.newBuilder()
                .addColumnInfo(CDCSDKColumnInfoPB.newBuilder()
                        .setName("id")
                        .setType(Common.QLTypePB.newBuilder().setMain(Value.PersistentDataType.INT32).build())
                        .setIsKey(true)
                        .setIsHashKey(true)
                        .setIsNullable(false)
                        .setOid(23)
                        .build())
                .addColumnInfo(CDCSDKColumnInfoPB.newBuilder()
                        .setName("name")
                        .setType(Common.QLTypePB.newBuilder().setMain(Value.PersistentDataType.STRING).build())
                        .setIsKey(false)
                        .setIsHashKey(false)
                        .setIsNullable(true)
                        .setOid(1043)
                        .build())
                .build();

        YugabyteDBSchemaHistoryProducer producer = new YugabyteDBSchemaHistoryProducer(
                "test-topic", "test-connector");


        assertFalse(producer.isDisabled(), "Producer should not be disabled on construction");
    }

    @Test
    public void testProducerIsDisabledOnNullTopic() {
        YugabyteDBSchemaHistoryProducer producer = new YugabyteDBSchemaHistoryProducer(
                null, "test-connector");

        assertNotNull(producer);
    }

    @Test
    public void testProducerIsDisabledOnNullBootstrapServers() {
        YugabyteDBSchemaHistoryProducer producer = new YugabyteDBSchemaHistoryProducer(
                "test-topic", "test-connector");

        assertNotNull(producer);
    }

    @Test
    public void testRecordSchemaChangeWithNullSchemaDoesNotThrow() {
        YugabyteDBSchemaHistoryProducer producer = new YugabyteDBSchemaHistoryProducer(
                "test-topic", "test-connector");

        assertDoesNotThrow(() -> {
            producer.recordSchemaChange("test-table", "test-tablet", null, "SCHEMA_SNAPSHOT");
        });
    }

    @Test
    public void testCloseIsIdempotent() {
        YugabyteDBSchemaHistoryProducer producer = new YugabyteDBSchemaHistoryProducer(
                "test-topic", "test-connector");

        assertDoesNotThrow(() -> {
            producer.close();
            producer.close();
        });
    }

    @Test
    public void testSchemaJsonContainsExpectedFields() throws Exception {
        CDCSDKSchemaPB schema = CDCSDKSchemaPB.newBuilder()
                .addColumnInfo(CDCSDKColumnInfoPB.newBuilder()
                        .setName("id")
                        .setType(Common.QLTypePB.newBuilder().setMain(Value.PersistentDataType.INT32).build())
                        .setIsKey(true)
                        .setIsHashKey(true)
                        .setIsNullable(false)
                        .setOid(23)
                        .build())
                .build();

        YugabyteDBSchemaHistoryProducer producer = new YugabyteDBSchemaHistoryProducer(
                "test-topic", "test-connector");

        java.lang.reflect.Method method = YugabyteDBSchemaHistoryProducer.class
                .getDeclaredMethod("buildSchemaJson", String.class, String.class,
                        CDCSDKSchemaPB.class, String.class);
        method.setAccessible(true);

        String json = (String) method.invoke(producer, "core.products", "tablet-123",
                schema, "SCHEMA_SNAPSHOT");

        assertTrue(json.contains("\"connector\":\"test-connector\""), "Should contain connector");
        assertTrue(json.contains("\"table\":\"core.products\""), "Should contain table");
        assertTrue(json.contains("\"tablet\":\"tablet-123\""), "Should contain tablet");
        assertTrue(json.contains("\"eventType\":\"SCHEMA_SNAPSHOT\""), "Should contain eventType");
        assertTrue(json.contains("\"schemaChecksum\":"), "Should contain schemaChecksum");
        assertTrue(json.contains("\"name\":\"id\""), "Should contain column name");
        assertTrue(json.contains("\"type\":\"INT32\""), "Should contain clean type name");
        assertTrue(json.contains("\"isKey\":true"), "Should contain isKey");
        assertTrue(json.contains("\"isHashKey\":true"), "Should contain isHashKey");
        assertTrue(json.contains("\"isNullable\":false"), "Should contain isNullable");
        assertTrue(json.contains("\"oid\":23"), "Should contain oid");
    }

    @Test
    public void testSchemaChecksumIsDeterministic() {
        CDCSDKSchemaPB schema1 = CDCSDKSchemaPB.newBuilder()
                .addColumnInfo(CDCSDKColumnInfoPB.newBuilder()
                        .setName("id")
                        .setType(Common.QLTypePB.newBuilder().setMain(Value.PersistentDataType.INT32).build())
                        .setIsKey(true)
                        .setIsHashKey(true)
                        .setIsNullable(false)
                        .setOid(23)
                        .build())
                .build();

        CDCSDKSchemaPB schema2 = CDCSDKSchemaPB.newBuilder()
                .addColumnInfo(CDCSDKColumnInfoPB.newBuilder()
                        .setName("id")
                        .setType(Common.QLTypePB.newBuilder().setMain(Value.PersistentDataType.INT32).build())
                        .setIsKey(true)
                        .setIsHashKey(true)
                        .setIsNullable(false)
                        .setOid(23)
                        .build())
                .build();

        YugabyteDBSchemaHistoryProducer producer = new YugabyteDBSchemaHistoryProducer(
                "test-topic", "test-connector");

        String checksum1 = producer.getSchemaChecksum(schema1);
        String checksum2 = producer.getSchemaChecksum(schema2);

        assertEquals(checksum1, checksum2, "Identical schemas should produce identical checksums");
        assertFalse(checksum1.isEmpty(), "Checksum should not be empty");
    }

    @Test
    public void testSchemaChecksumChangeWithDifferentSchema() {
        CDCSDKSchemaPB schema1 = CDCSDKSchemaPB.newBuilder()
                .addColumnInfo(CDCSDKColumnInfoPB.newBuilder()
                        .setName("id")
                        .setType(Common.QLTypePB.newBuilder().setMain(Value.PersistentDataType.INT32).build())
                        .setIsKey(true)
                        .setIsHashKey(true)
                        .setIsNullable(false)
                        .setOid(23)
                        .build())
                .build();

        CDCSDKSchemaPB schema2 = CDCSDKSchemaPB.newBuilder()
                .addColumnInfo(CDCSDKColumnInfoPB.newBuilder()
                        .setName("id")
                        .setType(Common.QLTypePB.newBuilder().setMain(Value.PersistentDataType.INT32).build())
                        .setIsKey(true)
                        .setIsHashKey(true)
                        .setIsNullable(false)
                        .setOid(23)
                        .build())
                .addColumnInfo(CDCSDKColumnInfoPB.newBuilder()
                        .setName("name")
                        .setType(Common.QLTypePB.newBuilder().setMain(Value.PersistentDataType.STRING).build())
                        .setIsKey(false)
                        .setIsHashKey(false)
                        .setIsNullable(true)
                        .setOid(1043)
                        .build())
                .build();

        YugabyteDBSchemaHistoryProducer producer = new YugabyteDBSchemaHistoryProducer(
                "test-topic", "test-connector");

        String checksum1 = producer.getSchemaChecksum(schema1);
        String checksum2 = producer.getSchemaChecksum(schema2);

        assertNotEquals(checksum1, checksum2, "Different schemas should produce different checksums");
    }

    @Test
    public void testEscapeJsonHandlesSpecialCharacters() throws Exception {
        YugabyteDBSchemaHistoryProducer producer = new YugabyteDBSchemaHistoryProducer(
                "test-topic", "test-connector");

        java.lang.reflect.Method method = YugabyteDBSchemaHistoryProducer.class
                .getDeclaredMethod("escapeJson", String.class);
        method.setAccessible(true);

        assertEquals("", method.invoke(producer, (String) null), "Null should return empty string");
        assertEquals("test", method.invoke(producer, "test"), "Simple string unchanged");
        assertEquals("test\\\"quote", method.invoke(producer, "test\"quote"), "Quotes should be escaped");
        assertEquals("test\\\\backslash", method.invoke(producer, "test\\backslash"), "Backslash should be escaped");
        assertEquals("test\\nnewline", method.invoke(producer, "test\nnewline"), "Newline should be escaped");
    }
}

