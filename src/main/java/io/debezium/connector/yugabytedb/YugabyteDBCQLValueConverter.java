/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAdjuster;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.Base64.Encoder;
import java.util.Base64;

import io.debezium.util.HexConverter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.postgresql.PGStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonFactory;
import com.google.protobuf.ByteString;

import io.debezium.config.CommonConnectorConfig.BinaryHandlingMode;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig.HStoreHandlingMode;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig.IntervalHandlingMode;

import static io.debezium.util.NumberConversions.SHORT_FALSE;
import static io.debezium.util.NumberConversions.BYTE_ZERO;

import io.debezium.data.SpecialValueDecimal;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.ResultReceiver;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.jdbc.ValueConversionCallback;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.relational.ValueConverterProvider;
import io.debezium.time.*;
import io.debezium.util.NumberConversions;

/**
 * A provider of {@link ValueConverter}s and {@link SchemaBuilder}s for various YugabyteDB YCQL 
 * specific column types.
 *
 * It handles data type conversion for values coming from CDC stream.
 *
 * @author Sumukh Phalgaonkar (sumukh.phalgaonkar@yugabyte.com)
 */
public class YugabyteDBCQLValueConverter implements ValueConverterProvider {
    public static final java.sql.Timestamp POSITIVE_INFINITY_TIMESTAMP = new java.sql.Timestamp(PGStatement.DATE_POSITIVE_INFINITY);
    public static final Instant POSITIVE_INFINITY_INSTANT = Conversions.toInstantFromMicros(PGStatement.DATE_POSITIVE_INFINITY);
    public static final LocalDateTime POSITIVE_INFINITY_LOCAL_DATE_TIME = LocalDateTime.ofInstant(POSITIVE_INFINITY_INSTANT, ZoneOffset.UTC);
    public static final OffsetDateTime POSITIVE_INFINITY_OFFSET_DATE_TIME = OffsetDateTime.ofInstant(Conversions.toInstantFromMillis(PGStatement.DATE_POSITIVE_INFINITY),
            ZoneOffset.UTC);

    public static final java.sql.Timestamp NEGATIVE_INFINITY_TIMESTAMP = new java.sql.Timestamp(PGStatement.DATE_NEGATIVE_INFINITY);
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

    protected final BinaryHandlingMode binaryMode;

    protected final boolean adaptiveTimePrecisionMode;
    protected final boolean adaptiveTimeMicrosecondsPrecisionMode;

    private final TemporalAdjuster adjuster;

    private static final Logger logger = LoggerFactory.getLogger(YugabyteDBConnector.class);


    public static YugabyteDBCQLValueConverter of(YugabyteDBConnectorConfig connectorConfig, Charset databaseCharset) {
        return new YugabyteDBCQLValueConverter(
                databaseCharset,
                connectorConfig.getDecimalMode(),
                connectorConfig.includeUnknownDatatypes(),
                connectorConfig.hStoreHandlingMode(),
                connectorConfig.intervalHandlingMode(),
                connectorConfig.binaryHandlingMode(),
                connectorConfig.getTemporalPrecisionMode());
    }

    protected YugabyteDBCQLValueConverter(Charset databaseCharset, DecimalMode decimalMode, boolean includeUnknownDatatypes,
                                       HStoreHandlingMode hStoreMode, IntervalHandlingMode intervalMode, BinaryHandlingMode binaryMode,
                                       TemporalPrecisionMode temporalPrecisionMode) {
        this.databaseCharset = databaseCharset;
        this.decimalMode = decimalMode;
        this.jsonFactory = new JsonFactory();
        this.includeUnknownDatatypes = includeUnknownDatatypes;
        this.hStoreMode = hStoreMode;
        this.intervalMode = intervalMode;
        this.binaryMode = binaryMode;
        this.adaptiveTimePrecisionMode = temporalPrecisionMode.equals(TemporalPrecisionMode.ADAPTIVE);
        this.adaptiveTimeMicrosecondsPrecisionMode = temporalPrecisionMode.equals(TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS);
        this.adjuster = null;
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
        case PgOid.VARINT:
            return SchemaBuilder.int64();


        // Numeric decimal numbers
        case Types.REAL:
            // values are single precision floating point number which supports 7 digits of mantissa.
            return SchemaBuilder.float32();
        case Types.FLOAT:
        case Types.DOUBLE:
            // values are double precision floating point number which supports 15 digits of mantissa.
            return SchemaBuilder.float64();
        case Types.DECIMAL:
            return SpecialValueDecimal.builder(decimalMode, column.length(), column.scale().get());
        case PgOid.INET:
        case PgOid.UUID:
        case Types.VARCHAR:
            return SchemaBuilder.string();
        case Types.BINARY:
            return binaryMode.getSchema();
        case Types.BOOLEAN:
            return SchemaBuilder.bool();
        case Types.DATE:
            if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                return Date.builder();
            }
            return org.apache.kafka.connect.data.Date.builder();
        case PgOid.TIME:
            if (adaptiveTimeMicrosecondsPrecisionMode) {
                return MicroTime.builder();
            }
            if (adaptiveTimePrecisionMode) {
                if (getTimePrecision(column) <= 3) {
                    return NanoTime.builder();
                }
                if (getTimePrecision(column) <= 6) {
                    return MicroTime.builder();
                }
                return NanoTime.builder();
            }
            return org.apache.kafka.connect.data.Time.builder();
        case PgOid.TIMESTAMP:
            if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                if (getTimePrecision(column) <= 3) {
                    return Timestamp.builder();
                }
                if (getTimePrecision(column) <= 6) {
                    return MicroTimestamp.builder();
                }
                return NanoTimestamp.builder();
            }
            return org.apache.kafka.connect.data.Timestamp.builder();
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
            case PgOid.VARINT:
                return (data) -> convertVarInt(column, fieldDefn, data);

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
            case PgOid.INET:
                return (data) -> convertInet(column, fieldDefn, data);
            case PgOid.UUID:
                return (data) -> convertUUID(column, fieldDefn, data);
            case Types.VARCHAR:
                return (data) -> convertString(column, fieldDefn, data);
            case Types.BINARY:
                return (data) -> convertBinary(column, fieldDefn, data, binaryMode);
            case Types.BOOLEAN:
                return (data) -> convertBoolean(column, fieldDefn, data);
            case Types.DATE:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    return (data) -> convertDateToEpochDays(column, fieldDefn, data);
                }
                return (data) -> convertDateToEpochDaysAsDate(column, fieldDefn, data);
            case PgOid.TIME:
                return (data) -> convertTime(column, fieldDefn, data);
            case PgOid.TIMESTAMP:
                if (adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode) {
                    if (getTimePrecision(column) <= 3) {
                        return data -> convertTimestampToEpochMillis(column, fieldDefn, data);
                    }
                    if (getTimePrecision(column) <= 6) {
                        return data -> convertTimestampToEpochMicros(column, fieldDefn, data);
                    }
                    return (data) -> convertTimestampToEpochNanos(column, fieldDefn, data);
                }
                return (data) -> convertTimestampToEpochMillisAsDate(column, fieldDefn, data);
            default:
                logger.error("Required type not found in YugabyteDBCQLValueConverter Converter ");
                return null;
        }

    }

    /**
     * Converts a value object for an expected type of {@link Types#TINYINT}.
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
     * Converts a value object for an expected type of {@link Types#SMALLINT}.
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
                Short value = (Short) data;
                r.deliver(Short.valueOf(value.shortValue()));
            } else if (data instanceof Byte) {
                Byte value  = (Byte) data;
                r.deliver(Byte.valueOf(value.byteValue()));
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
     * Converts a value object for an expected type of {@link Types#INTEGER}.
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
     * Converts a value object for an expected type of {@link Types#INTEGER}.
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

    protected Object convertVarInt(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            if (data instanceof ByteString) {
                String dataString = ((ByteString) data).toStringUtf8();
                if(dataString.equals("")) {
                    r.deliver(Long.valueOf(0));
                } else {
                    r.deliver(Long.valueOf(dataString));
                }
            }
        });    
    }

    /**
     * Converts a value object for an expected type of {@link Types#FLOAT}.
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
     * Converts a value object for an expected type of {@link Types#DOUBLE}.
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
     * Converts a value object for an expected type of {@link Types#REAL}.
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
     * Converts a value object for an expected type of {@link Types#NUMERIC}.
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
     * Converts a value object for an expected type of {@link Types#NUMERIC}.
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
            else if (data instanceof ByteString) {
                String dataString = ((ByteString)data).toStringUtf8();
                if (dataString.equals("")) {
                    r.deliver(Double.valueOf(0));
                } else {
                    r.deliver(Double.valueOf(dataString));
                }
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
                logger.warn("Unexpected value for type {} and column {}: class={}", column.nativeType(), column,
                        dataClass.isArray() ? dataClass.getSimpleName() : dataClass.getName()); 
            }
            return null;
        }
        throw new IllegalArgumentException("Unexpected value for type " + column.nativeType() + " and column " + column +
                ": class=" + data.getClass());
    }

    /**
     * Converts a value object for an expected type of TEXT and {@link Types#VARCHAR},
     * 
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        Object data_;
        if (data instanceof ByteString) {
            String dataString = ((ByteString) data).toStringUtf8();
            data_ = dataString;
        } else {
            data_ = data;
        }
        return convertValue(column, fieldDefn, data_, "", (r) -> {
            r.deliver(data_.toString());
        });
    }

    protected Object convertInet(Column column, Field fieldDefn, Object data) {
        Object data_;
        if (data instanceof ByteString) {
            ByteString inetBytes = (ByteString) data;
            String ipAddress = convertInetToIPAddress(inetBytes);
            data_ = ipAddress;
        } else {
            return convertString(column, fieldDefn, data);
        }
        return convertValue(column, fieldDefn, data_, "", (r) -> {
            r.deliver(data_.toString());            
        });
    }

    public static String convertInetToIPAddress(ByteString inetBytes) {
        byte[] inetBytesArray = inetBytes.toByteArray();
        if (inetBytesArray.length == 4) {
            int[] octets = new int[4];
            for (int i = 0; i < 4; i++) {
                octets[i] = inetBytesArray[i] & 0xFF;
            }
            return String.format("%d.%d.%d.%d", octets[0], octets[1], octets[2], octets[3]);
        } else {
            logger.warn("Invalid INET representation or null value");
            return "";
        }
    }

    protected Object convertUUID(Column column, Field fieldDefn, Object data) {
        Object data_;
        if (data instanceof ByteString) {
            ByteString uuidBytes = (ByteString) data;
            UUID uuid = convertByteStringToUUID(uuidBytes);
            String uuidString;
            if (uuid == null) {
                uuidString = "";
            } else {
                uuidString = uuid.toString();
            }
            data_ = uuidString;
        } else {
            return convertString(column, fieldDefn, data);
        }
        return convertValue(column, fieldDefn, data_, "", (r) -> {
            r.deliver(data_.toString());
            });
    }

    public static UUID convertByteStringToUUID(ByteString byteString) {
        byte[] byteArray = byteString.toByteArray();
        if (byteArray.length == 0) {
            return null;
        }
        long msb = 0;
        long lsb = 0;

        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (byteArray[i] & 0xff);
        }

        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (byteArray[i] & 0xff);
        }

        return new UUID(msb, lsb);
    }

    protected Object convertBinary(Column column, Field fieldDefn, Object data, BinaryHandlingMode mode) {
        switch (mode) {
            case BASE64:
                return convertBinaryToBase64(column, fieldDefn, data);
            case HEX:
                return convertBinaryToHex(column, fieldDefn, data);
            case BYTES:
            default:
                return convertBinaryToBytes(column, fieldDefn, data);
        }
    }

    /**
     * Converts a value object for an expected type of {@link Types#BLOB}, {@link Types#BINARY},
     * {@link Types#VARBINARY}, {@link Types#LONGVARBINARY}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertBinaryToBase64(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", (r) -> {
            Encoder base64Encoder = Base64.getEncoder();

            if (data instanceof String) {
                r.deliver(new String(base64Encoder.encode(((String) data).getBytes(StandardCharsets.UTF_8))));
            }
            else if (data instanceof char[]) {
                r.deliver(new String(base64Encoder.encode(toByteArray((char[]) data)), StandardCharsets.UTF_8));
            }
            else if (data instanceof byte[]) {
                r.deliver(new String(base64Encoder.encode(normalizeBinaryData(column, (byte[]) data)), StandardCharsets.UTF_8));
            }
            else {
                // An unexpected value
                r.deliver(unexpectedBinary(data, fieldDefn));
            }
        });
    }

    /**
     * Converts a value object for an expected type of {@link Types#BLOB}, {@link Types#BINARY},
     * {@link Types#VARBINARY}, {@link Types#LONGVARBINARY}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertBinaryToHex(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, "", (r) -> {

            if (data instanceof String) {
                r.deliver(HexConverter.convertToHexString(((String) data).getBytes(StandardCharsets.UTF_8)));
            }
            else if (data instanceof char[]) {
                r.deliver(HexConverter.convertToHexString(toByteArray((char[]) data)));
            }
            else if (data instanceof byte[]) {
                r.deliver(HexConverter.convertToHexString(normalizeBinaryData(column, (byte[]) data)));
            }
            else {
                // An unexpected value
                r.deliver(unexpectedBinary(data, fieldDefn));
            }
        });
    }

    /**
     * Converts a value object for an expected type of {@link Types#BLOB}, {@link Types#BINARY},
     * {@link Types#VARBINARY}, {@link Types#LONGVARBINARY}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertBinaryToBytes(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, BYTE_ZERO, (r) -> {
            if (data instanceof String) {
                r.deliver(toByteBuffer(((String) data)));
            }
            else if (data instanceof char[]) {
                r.deliver(toByteBuffer((char[]) data));
            }
            else if (data instanceof byte[]) {
                r.deliver(toByteBuffer(column, (byte[]) data));
            }
            else {
                // An unexpected value
                r.deliver(unexpectedBinary(data, fieldDefn));
            }
        });
    }

    private byte[] toByteArray(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        ByteBuffer byteBuffer = Charset.forName("UTF-8").encode(charBuffer);
        return byteBuffer.array();
    }

    /**
     * Converts the given byte array value into a normalized byte array. Specific connectors
     * can perform value adjustments based on the column definition, e.g. right-pad with 0x00 bytes in case of
     * fixed length BINARY in MySQL.
     */
    protected byte[] normalizeBinaryData(Column column, byte[] data) {
        return data;
    }

    /**
     * Handle the unexpected value from a row with a column type of {@link Types#BLOB}, {@link Types#BINARY},
     * {@link Types#VARBINARY}, {@link Types#LONGVARBINARY}.
     *
     * @param value the binary value for which no conversion was found; never null
     * @param fieldDefn the field definition in the Kafka Connect schema; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     * @see #convertBinaryToBytes(Column, Field, Object)
     */
    protected byte[] unexpectedBinary(Object value, Field fieldDefn) {
        logger.warn("Unexpected BINARY value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                fieldDefn.schema(), value.getClass(), value);
        return null;
    }

    private ByteBuffer toByteBuffer(String string) {
        return ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8));
    }

    private ByteBuffer toByteBuffer(char[] chars) {
        CharBuffer charBuffer = CharBuffer.wrap(chars);
        return Charset.forName("UTF-8").encode(charBuffer);
    }

    /**
     * Converts the given byte array value into a byte buffer as preferred by Kafka Connect. Specific connectors
     * can perform value adjustments based on the column definition, e.g. right-pad with 0x00 bytes in case of
     * fixed length BINARY in MySQL.
     */
    protected ByteBuffer toByteBuffer(Column column, byte[] data) {
        // Kafka Connect would support raw byte arrays, too, but byte buffers are recommended
        return ByteBuffer.wrap(normalizeBinaryData(column, data));
    }

    /**
     * Converts a value object for an expected type of {@link Types#BOOLEAN}.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertBoolean(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, false, (r) -> {
            if (data instanceof Boolean) {
                r.deliver(data);
            }
            else if (data instanceof Short) {
                r.deliver(((Short) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE);
            }
            else if (data instanceof Integer) {
                r.deliver(((Integer) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE);
            }
            else if (data instanceof Long) {
                r.deliver(((Long) data).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE);
            }
        });
    }

    /**
     * Converts a value object for an expected type of {@link Types#DATE} to the number of days past epoch.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertDateToEpochDays(Column column, Field fieldDefn, Object data) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0, (r) -> {
            try {
                r.deliver(Date.toEpochDay(data, adjuster));
            }
            catch (IllegalArgumentException e) {
                logger.warn("Unexpected  DATE value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                        fieldDefn.schema(), data.getClass(), data);
            }
        });
    }

    /**
     * Converts a value object for an expected type of {@link Types#DATE} to the number of days past epoch, but represented
     * as a {@link java.util.Date} value at midnight on the date.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertDateToEpochDaysAsDate(Column column, Field fieldDefn, Object data) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, new java.util.Date(0L), (r) -> {
            try {
                int epochDay = Date.toEpochDay(data, adjuster);
                long epochMillis = TimeUnit.DAYS.toMillis(epochDay);
                r.deliver(new java.util.Date(epochMillis));
            }
            catch (IllegalArgumentException e) {
                logger.warn("Unexpected DATE value for field {} with schema {}: class={}, value={}", fieldDefn.name(),
                        fieldDefn.schema(), data.getClass(), data);
            }
        });
    }

    protected int getTimePrecision(Column column) {
        return column.length();
    }

    protected Object convertTime(Column column, Field fieldDefn, Object data) {
        if (adaptiveTimeMicrosecondsPrecisionMode) {
            return convertTimeToMicrosPastMidnight(column, fieldDefn, data);
        }
        if (adaptiveTimePrecisionMode) {
            if (getTimePrecision(column) <= 3) {
                return convertTimeToMillisPastMidnight(column, fieldDefn, data);
            }
            if (getTimePrecision(column) <= 6) {
                return convertTimeToMicrosPastMidnight(column, fieldDefn, data);
            }
            return convertTimeToNanosPastMidnight(column, fieldDefn, data);
        }
        else {
            return convertTimeToMillisPastMidnightAsDate(column, fieldDefn, data);
        }
    }

    /**
     * Converts a value object for an expected type of {@link Types#TIME} to {@link MicroTime} values, or microseconds past
     * midnight.
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn the field definition; never null
     * @param data the data object to be converted into a {@link Date Kafka Connect date} type; never null
     * @return the converted value, or null if the conversion could not be made and the column allows nulls
     * @throws IllegalArgumentException if the value could not be converted but the column does not allow nulls
     */
    protected Object convertTimeToMicrosPastMidnight(Column column, Field fieldDefn, Object data) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            try {
                r.deliver(MicroTime.toMicroOfDay(data, supportsLargeTimeValues()));
            }
            catch (IllegalArgumentException e) {
                logger.error("Exception encountered while converting time", e);
            }
        });
    }

    private boolean supportsLargeTimeValues() {
        return adaptiveTimePrecisionMode || adaptiveTimeMicrosecondsPrecisionMode;
    }

    protected Object convertTimeToMillisPastMidnight(Column column, Field fieldDefn, Object data) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0, (r) -> {
            try {
                java.time.Duration d = java.time.Duration.ofNanos((Long) data);
                r.deliver(NanoTime.toNanoOfDay(d, supportsLargeTimeValues()));
            }
            catch (IllegalArgumentException e) {
                logger.error("Exception encountered while converting time", e);
            }
        });
    }

    protected Object convertTimeToNanosPastMidnight(Column column, Field fieldDefn, Object data) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            try {
                r.deliver(NanoTime.toNanoOfDay(data, supportsLargeTimeValues()));
            }
            catch (IllegalArgumentException e) {
                logger.error("Exception encountered while converting time", e);
            }
        });
    }

    protected Object convertTimeToMillisPastMidnightAsDate(Column column, Field fieldDefn, Object data) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, new java.util.Date(0L), (r) -> {
            try {
                r.deliver(new java.util.Date(Time.toMilliOfDay(data, supportsLargeTimeValues())));
            }
            catch (IllegalArgumentException e) {
                logger.error("Exception encountered while converting time", e);
            }
        });
    }

    protected Object convertTimestampToEpochMillis(Column column, Field fieldDefn, Object data) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            try {
                r.deliver(Timestamp.toEpochMillis(data, adjuster));
            }
            catch (IllegalArgumentException e) {
                logger.error("Exception encountered while converting timestamp", e);
            }
        });
    }

    protected Object convertTimestampToEpochMicros(Column column, Field fieldDefn, Object data) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            try {
                r.deliver(MicroTimestamp.toEpochMicros(data, adjuster));
            }
            catch (IllegalArgumentException e) {
                logger.error("Exception encountered while converting timestamp", e);
            }
        });
    }

    protected Object convertTimestampToEpochNanos(Column column, Field fieldDefn, Object data) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, 0L, (r) -> {
            try {
                r.deliver(NanoTimestamp.toEpochNanos(data, adjuster));
            }
            catch (IllegalArgumentException e) {
                logger.error("Exception encountered while converting timestamp", e);
            }
        });
    }

    protected Object convertTimestampToEpochMillisAsDate(Column column, Field fieldDefn, Object data) {
        // epoch is the fallback value
        return convertValue(column, fieldDefn, data, new java.util.Date(0L), (r) -> {
            try {
                r.deliver(new java.util.Date(Timestamp.toEpochMillis(data, adjuster)));
            }
            catch (IllegalArgumentException e) {
                logger.error("Exception encountered while converting timestamp", e);
            }
        });
    }

}
