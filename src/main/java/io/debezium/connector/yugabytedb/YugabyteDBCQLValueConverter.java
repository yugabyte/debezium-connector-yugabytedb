/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import static java.time.ZoneId.systemDefault;

import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Optional;
import java.util.stream.Collectors;

import io.debezium.util.HexConverter;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.postgresql.PGStatement;
import org.postgresql.geometric.PGpoint;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.HStoreConverter;
import org.postgresql.util.PGInterval;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig.HStoreHandlingMode;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig.IntervalHandlingMode;
import static io.debezium.util.NumberConversions.SHORT_FALSE;
import io.debezium.connector.yugabytedb.data.Ltree;
import io.debezium.data.Json;
import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.Uuid;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.data.geometry.Geography;
import io.debezium.data.geometry.Geometry;
import io.debezium.data.geometry.Point;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.JdbcValueConverters.BigIntUnsignedMode;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.ResultReceiver;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.jdbc.ValueConversionCallback;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.time.*;
import io.debezium.util.NumberConversions;
import io.debezium.util.Strings;

/**
 * A provider of {@link ValueConverter}s and {@link SchemaBuilder}s for various YugabyteDB (ycql) 
 * specific column types.
 *
 * It handles data type conversion for values coming from CDC stream.
 *
 * @author Sumukh Phalgaonkar (sumukh.phalgaonkar@yugabyte.com)
 */
public class YugabyteDBCQLValueConverter implements ValueConverterProvider {
    public static final Timestamp POSITIVE_INFINITY_TIMESTAMP = new Timestamp(PGStatement.DATE_POSITIVE_INFINITY);
    public static final Instant POSITIVE_INFINITY_INSTANT = Conversions.toInstantFromMicros(PGStatement.DATE_POSITIVE_INFINITY);
    public static final LocalDateTime POSITIVE_INFINITY_LOCAL_DATE_TIME = LocalDateTime.ofInstant(POSITIVE_INFINITY_INSTANT, ZoneOffset.UTC);
    public static final OffsetDateTime POSITIVE_INFINITY_OFFSET_DATE_TIME = OffsetDateTime.ofInstant(Conversions.toInstantFromMillis(PGStatement.DATE_POSITIVE_INFINITY),
            ZoneOffset.UTC);

    public static final Timestamp NEGATIVE_INFINITY_TIMESTAMP = new Timestamp(PGStatement.DATE_NEGATIVE_INFINITY);
    public static final Instant NEGATIVE_INFINITY_INSTANT = Conversions.toInstantFromMicros(PGStatement.DATE_NEGATIVE_INFINITY);
    public static final LocalDateTime NEGATIVE_INFINITY_LOCAL_DATE_TIME = LocalDateTime.ofInstant(NEGATIVE_INFINITY_INSTANT, ZoneOffset.UTC);
    public static final OffsetDateTime NEGATIVE_INFINITY_OFFSET_DATE_TIME = OffsetDateTime.ofInstant(Conversions.toInstantFromMillis(PGStatement.DATE_NEGATIVE_INFINITY),
            ZoneOffset.UTC);

    protected final DecimalMode decimalMode;
   
    /**
     * Variable scale decimal/numeric is defined by metadata
     * scale - 0
     * length - 131089
     */
    private static final int VARIABLE_SCALE_DECIMAL_LENGTH = 131089;

    /**
     * A string denoting not-a- number for FP and Numeric types
     */
    public static final String N_A_N = "NaN";

    /**
     * A string denoting positive infinity for FP and Numeric types
     */
    public static final String POSITIVE_INFINITY = "Infinity";

    /**
     * A string denoting negative infinity for FP and Numeric types
     */
    public static final String NEGATIVE_INFINITY = "-Infinity";

    private static final BigDecimal MICROSECONDS_PER_SECOND = new BigDecimal(1_000_000);

    /**
     * A formatter used to parse TIMETZ columns when provided as strings.
     */
    private static final DateTimeFormatter TIME_WITH_TIMEZONE_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("HH:mm:ss")
            .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
            .appendPattern("[XXX][XX][X]")
            .toFormatter();

    /**
     * {@code true} if fields of data type not know should be handle as opaque binary;
     * {@code false} if they should be omitted
     */
    private final boolean includeUnknownDatatypes;

    private final HStoreHandlingMode hStoreMode;
    private final IntervalHandlingMode intervalMode;

    /**
     * The current database's character encoding.
     */
    private final Charset databaseCharset;

    private final JsonFactory jsonFactory;

    private static final Logger logger = LoggerFactory.getLogger(YugabyteDBConnector.class);


    public static YugabyteDBCQLValueConverter of(YugabyteDBConnectorConfig connectorConfig,
                                              Charset databaseCharset) {
        return new YugabyteDBCQLValueConverter(
                databaseCharset,
                connectorConfig.getDecimalMode(),
                connectorConfig.includeUnknownDatatypes(),
                connectorConfig.hStoreHandlingMode(),
                connectorConfig.intervalHandlingMode());
    }

    protected YugabyteDBCQLValueConverter(Charset databaseCharset, DecimalMode decimalMode, boolean includeUnknownDatatypes,
                                       HStoreHandlingMode hStoreMode, IntervalHandlingMode intervalMode) {
        // super(decimalMode, temporalPrecisionMode, defaultOffset, null, bigIntUnsignedMode, binaryMode);
        this.databaseCharset = databaseCharset;
        this.decimalMode = decimalMode;
        this.jsonFactory = new JsonFactory();
        this.includeUnknownDatatypes = includeUnknownDatatypes;
        this.hStoreMode = hStoreMode;
        this.intervalMode = intervalMode;
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        int type = column.nativeType();
        switch (type) {
        case Types.TINYINT:
            // values are an 8-bit unsigned integer value between 0 and 255
            return SchemaBuilder.int8();
        case Types.SMALLINT:
            // values are a 16-bit signed integer value between -32768 and 32767
            return SchemaBuilder.int16();
        case Types.INTEGER:
            // values are a 32-bit signed integer value between - 2147483648 and 2147483647
            return SchemaBuilder.int32();
        case Types.BIGINT:
            // values are a 64-bit signed integer value between -9223372036854775808 and 9223372036854775807
            return SchemaBuilder.int64();

        // Numeric decimal numbers
        case Types.REAL:
            // values are single precision floating point number which supports 7 digits of mantissa.
            return SchemaBuilder.float32();
        case Types.FLOAT:
        case Types.DOUBLE:
            // values are double precision floating point number which supports 15 digits of mantissa.
            return SchemaBuilder.float64();
        case Types.VARCHAR:
            return SchemaBuilder.string();
        default :
            logger.error("Required type not found in YugabyteDBCQLValueConverter SchemaBuilder ");
            return null;
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        int type = column.nativeType();
        switch (type) {
            case Types.TINYINT:
                return ((data) -> convertTinyInt(column, fieldDefn, data));
            case Types.SMALLINT:
                return (data) -> convertSmallInt(column, fieldDefn, data);
            case Types.INTEGER:
                return (data) -> convertInteger(column, fieldDefn, data);
            case Types.BIGINT:
                return (data) -> convertBigInt(column, fieldDefn, data);

            // Numeric decimal numbers
            case Types.FLOAT:
                return (data) -> convertFloat(column, fieldDefn, data);
            case Types.DOUBLE:
                return (data) -> convertDouble(column, fieldDefn, data);
            case Types.REAL:
                return (data) -> convertReal(column, fieldDefn, data);
            case Types.NUMERIC:
                return (data) -> convertNumeric(column, fieldDefn, data);
            case Types.DECIMAL:
                return (data) -> convertDecimal(column, fieldDefn, data);
            case Types.VARCHAR:
                return (data) -> convertString(column, fieldDefn, data);
            default:
                logger.error("Required type not found in YugabyteDBCQLValueConverter Converter ");
                return null;
        }

    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#TINYINT}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertTinyInt(Column column, Field fieldDefn, Object data) {
        return convertSmallInt(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#SMALLINT}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertSmallInt(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, SHORT_FALSE, (r) -> {
            if (data instanceof Short) {
                r.deliver(data);
            }
            else if (data instanceof Number) {
                Number value = (Number) data;
                r.deliver(Short.valueOf(value.shortValue()));
            }
            else if (data instanceof Boolean) {
                r.deliver(NumberConversions.getShort((Boolean) data));
            }
            else if (data instanceof String) {
                r.deliver(Short.valueOf((String) data));
            }
        });
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#INTEGER}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertInteger(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0, (r) -> {
            if (data instanceof Integer) {
                r.deliver(data);
            }
            else if (data instanceof Number) {
                Number value = (Number) data;
                r.deliver(Integer.valueOf(value.intValue()));
            }
            else if (data instanceof Boolean) {
                r.deliver(NumberConversions.getInteger((Boolean) data));
            }
            else if (data instanceof String) {
                r.deliver(Integer.valueOf((String) data));
            }
        });
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#INTEGER}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertBigInt(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            if (data instanceof Long) {
                r.deliver(data);
            }
            else if (data instanceof Number) {
                Number value = (Number) data;
                r.deliver(Long.valueOf(value.longValue()));
            }
            else if (data instanceof Boolean) {
                r.deliver(NumberConversions.getLong((Boolean) data));
            }
            else if (data instanceof String) {
                r.deliver(Long.valueOf((String) data));
            }
        });
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#FLOAT}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertFloat(Column column, Field fieldDefn, Object data) {
        return convertDouble(column, fieldDefn, data);
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#DOUBLE}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertDouble(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0.0d, (r) -> {
            if (data instanceof Double) {
                r.deliver(data);
            }
            else if (data instanceof Number) {
                // Includes BigDecimal and other numeric values ...
                Number value = (Number) data;
                r.deliver(Double.valueOf(value.doubleValue()));
            }
            else if (data instanceof SpecialValueDecimal) {
                r.deliver(((SpecialValueDecimal) data).toDouble());
            }
            else if (data instanceof Boolean) {
                r.deliver(NumberConversions.getDouble((Boolean) data));
            }
        });
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#REAL}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertReal(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0.0f, (r) -> {
            if (data instanceof Float) {
                r.deliver(data);
            }
            else if (data instanceof Number) {
                // Includes BigDecimal and other numeric values ...
                Number value = (Number) data;
                r.deliver(Float.valueOf(value.floatValue()));
            }
            else if (data instanceof Boolean) {
                r.deliver(NumberConversions.getFloat((Boolean) data));
            }
        });
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#NUMERIC}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertNumeric(Column column, Field fieldDefn, Object data) {
        return convertDecimal(column, fieldDefn, data);
    }
    /**
     * Converts a value object for an expected JDBC type of {@link Types#NUMERIC}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertDecimal(Column column, Field fieldDefn, Object data) {
        if (data instanceof SpecialValueDecimal) {
            return SpecialValueDecimal.fromLogical((SpecialValueDecimal) data, decimalMode, column.name());
        }
        Object decimal = toBigDecimal(column, fieldDefn, data);
        if (decimal instanceof BigDecimal) {
            return SpecialValueDecimal.fromLogical(new SpecialValueDecimal((BigDecimal) decimal), decimalMode, column.name());
        }
        return decimal;
    }

    protected Object toBigDecimal(Column column, Field fieldDefn, Object data) {
        BigDecimal fallback = withScaleAdjustedIfNeeded(column, BigDecimal.ZERO);
        return convertValue(column, fieldDefn, data, fallback, (r) -> {
            if (data instanceof BigDecimal) {
                r.deliver(data);
            }
            else if (data instanceof Boolean) {
                r.deliver(NumberConversions.getBigDecimal((Boolean) data));
            }
            else if (data instanceof Short) {
                r.deliver(new BigDecimal(((Short) data).intValue()));
            }
            else if (data instanceof Integer) {
                r.deliver(new BigDecimal(((Integer) data).intValue()));
            }
            else if (data instanceof Long) {
                r.deliver(BigDecimal.valueOf(((Long) data).longValue()));
            }
            else if (data instanceof Float) {
                r.deliver(BigDecimal.valueOf(((Float) data).doubleValue()));
            }
            else if (data instanceof Double) {
                r.deliver(BigDecimal.valueOf(((Double) data).doubleValue()));
            }
            else if (data instanceof String) {
                r.deliver(new BigDecimal((String) data));
            }
        });
    }

    protected BigDecimal withScaleAdjustedIfNeeded(Column column, BigDecimal data) {
        if (column.scale().isPresent() && column.scale().get() > data.scale()) {
            data = data.setScale(column.scale().get());
        }

        return data;
    }
    protected Object convertValue(Column column, Field fieldDefn, Object data, Object fallback, ValueConversionCallback callback) {
        if (data == null) {
            if (column.isOptional()) {
                return null;
            }
            final Object schemaDefault = fieldDefn.schema().defaultValue();
            return schemaDefault != null ? schemaDefault : fallback;
        }
        logger.trace("Value from data object: *** {} ***", data);

        final ResultReceiver r = ResultReceiver.create();
        callback.convert(r);
        logger.trace("Callback is: {}", callback);
        logger.trace("Value from ResultReceiver: {}", r);
        return r.hasReceived() ? r.get() : handleUnknownData(column, fieldDefn, data);
    }

    /**
     * Convert an unknown data value.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object handleUnknownData(Column column, Field fieldDefn, Object data) {
        if (column.isOptional() || fieldDefn.schema().isOptional()) {
            Class<?> dataClass = data.getClass();
            if (logger.isWarnEnabled()) {
                logger.warn("Unexpected value for JDBC type {} and column {}: class={}", column.jdbcType(), column,
                        dataClass.isArray() ? dataClass.getSimpleName() : dataClass.getName()); // don't include value in case its
                                                                                                // sensitive
            }
            return null;
        }
        throw new IllegalArgumentException("Unexpected value for JDBC type " + column.jdbcType() + " and column " + column +
                ": class=" + data.getClass()); // don't include value in case its sensitive
    }

    /**
     * Converts a value object for an expected JDBC type of {@link Types#CHAR}, {@link Types#VARCHAR},
     * {@link Types#LONGVARCHAR}, {@link Types#CLOB}, {@link Types#NCHAR}, {@link Types#NVARCHAR}, {@link Types#LONGNVARCHAR},
     * {@link Types#NCLOB}, {@link Types#DATALINK}, and {@link Types#SQLXML}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        String dataString = data.toString();
        Pattern pattern = Pattern.compile("contents=\"(.*?)\"");
        Matcher matcher = pattern.matcher(dataString);
        Object data_;
        if(matcher.find()) {
            String contentString = matcher.group(1);
            data_ = contentString;
        } else {
            data_ = data;
        }
        return convertValue(column, fieldDefn, data_, "", (r) -> {
            if (data_ instanceof SQLXML) {
                try {
                    r.deliver(((SQLXML) data_).getString());
                }
                catch (SQLException e) {
                    throw new RuntimeException("Error processing data from " + column.typeName() + " and column " + column +
                            ": class=" + data_.getClass(), e);
                }
            }
            else {
                r.deliver(data_.toString());
            }
        });
    }


}
