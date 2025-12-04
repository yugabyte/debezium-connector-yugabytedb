/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;

/**
 * POJO representing a schema history event for JSON serialization.
 *
 * @author Michael Terranova
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class YugabyteDBSchemaHistoryEvent {

    @JsonProperty("connector")
    private String connector;

    @JsonProperty("table")
    private String table;

    @JsonProperty("tablet")
    private String tablet;

    @JsonProperty("eventType")
    private String eventType;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("schemaChecksum")
    private String schemaChecksum;

    @JsonProperty("schema")
    private SchemaInfo schema;

    public YugabyteDBSchemaHistoryEvent() {
        this.timestamp = Instant.now().toString();
    }

    public String getConnector() { return connector; }
    public void setConnector(String connector) { this.connector = connector; }

    public String getTable() { return table; }
    public void setTable(String table) { this.table = table; }

    public String getTablet() { return tablet; }
    public void setTablet(String tablet) { this.tablet = tablet; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public String getSchemaChecksum() { return schemaChecksum; }
    public void setSchemaChecksum(String schemaChecksum) { this.schemaChecksum = schemaChecksum; }

    public SchemaInfo getSchema() { return schema; }
    public void setSchema(SchemaInfo schema) { this.schema = schema; }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class SchemaInfo {
        @JsonProperty("columns")
        private List<ColumnInfo> columns;

        @JsonProperty("primaryKey")
        private List<String> primaryKey;

        public List<ColumnInfo> getColumns() { return columns; }
        public void setColumns(List<ColumnInfo> columns) { this.columns = columns; }

        public List<String> getPrimaryKey() { return primaryKey; }
        public void setPrimaryKey(List<String> primaryKey) { this.primaryKey = primaryKey; }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ColumnInfo {
        @JsonProperty("name")
        private String name;

        @JsonProperty("type")
        private String type;

        @JsonProperty("isNullable")
        private Boolean isNullable;

        @JsonProperty("isKey")
        private Boolean isKey;

        @JsonProperty("isHashKey")
        private Boolean isHashKey;

        @JsonProperty("oid")
        private Integer oid;

        @JsonProperty("length")
        private Integer length;

        @JsonProperty("scale")
        private Integer scale;

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getType() { return type; }
        public void setType(String type) { this.type = type; }

        public Boolean getIsNullable() { return isNullable; }
        public void setIsNullable(Boolean isNullable) { this.isNullable = isNullable; }

        public Boolean getIsKey() { return isKey; }
        public void setIsKey(Boolean isKey) { this.isKey = isKey; }

        public Boolean getIsHashKey() { return isHashKey; }
        public void setIsHashKey(Boolean isHashKey) { this.isHashKey = isHashKey; }

        public Integer getOid() { return oid; }
        public void setOid(Integer oid) { this.oid = oid; }

        public Integer getLength() { return length; }
        public void setLength(Integer length) { this.length = length; }

        public Integer getScale() { return scale; }
        public void setScale(Integer scale) { this.scale = scale; }
    }
}
