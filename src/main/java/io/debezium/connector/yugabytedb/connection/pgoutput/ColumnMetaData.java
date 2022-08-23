/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.connection.pgoutput;

import io.debezium.annotation.Immutable;
import io.debezium.connector.yugabytedb.YugabyteDBType;
import io.debezium.connector.yugabytedb.YugabyteDBTypeRegistry;

/**
 * Defines the relational column mapping for a table.
 *
 * @author Gunnar Morling
 * @author Chris Cranford
 */
@Immutable
public class ColumnMetaData {
    private final String columnName;
    private final YugabyteDBType yugabyteDBType;
    private final boolean key;
    private final boolean optional;
    private final boolean hasDefaultValue;
    private final String defaultValueExpression;
    private final int length;
    private final int scale;
    private final String typeName;

    /**
     * Create a metadata structure representing a column.
     *
     * @param columnName name of the column; must not be null
     * @param yugabyteDBType postgres database type; must not be null
     * @param key {@code true} if column is part of the primary key, {@code false} otherwise
     * @param optional {@code true} if the column is considered optional, {@code false} otherwise
     * @param hasDefaultValue {@code true} if the column has a default value specified, {@code false} otherwise
     * @param defaultValueExpression the parsed default value for the column
     * @param typeModifier the attribute type modifier
     */
    ColumnMetaData(String columnName, YugabyteDBType yugabyteDBType, boolean key, boolean optional, boolean hasDefaultValue, String defaultValueExpression, int typeModifier) {
        this.columnName = columnName;
        this.yugabyteDBType = yugabyteDBType;
        this.key = key;
        this.optional = optional;
        this.hasDefaultValue = hasDefaultValue;
        this.defaultValueExpression = defaultValueExpression;

        // todo: investigate whether this can be removed and PostgresType updated to always delegate
        // Currently PostgresType only delegates calls to length and scale with an attribute modifier
        // for specific types and ideally for PgOutput, we should always delegate if a modifier
        // is provided. For now, I've allowed PostgresType to expose the TypeInfo object where
        // I will use it here for now until further research can be done.
        if (YugabyteDBTypeRegistry.NO_TYPE_MODIFIER != typeModifier && yugabyteDBType.getTypeInfo() != null) {
            length = yugabyteDBType.getTypeInfo().getPrecision(yugabyteDBType.getOid(), typeModifier);
            scale = yugabyteDBType.getTypeInfo().getScale(yugabyteDBType.getOid(), typeModifier);
        }
        else {
            length = yugabyteDBType.getDefaultLength();
            scale = yugabyteDBType.getDefaultScale();
        }

        // Constructs a fully qualified type name, including dimensions if applicable
        String type = yugabyteDBType.getName();
        if (!(length == yugabyteDBType.getDefaultLength() && scale == 0)) {
            type += "(" + length + "," + scale + ")";
        }
        this.typeName = type;
    }

    public String getColumnName() {
        return columnName;
    }

    public YugabyteDBType getPostgresType() {
        return yugabyteDBType;
    }

    public boolean isKey() {
        return key;
    }

    public boolean isOptional() {
        return optional;
    }

    public boolean hasDefaultValue() {
        return hasDefaultValue;
    }

    public String getDefaultValueExpression() {
        return defaultValueExpression;
    }

    public int getLength() {
        return length;
    }

    public int getScale() {
        return scale;
    }

    public String getTypeName() {
        return typeName;
    }
}
