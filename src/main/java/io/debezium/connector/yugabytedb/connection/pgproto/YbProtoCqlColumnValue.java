package io.debezium.connector.yugabytedb.connection.pgproto;

import io.debezium.connector.yugabytedb.YugabyteDBValueConverter;
import io.debezium.connector.yugabytedb.connection.wal2json.DateTimeFormat;
import io.debezium.data.SpecialValueDecimal;
import org.yb.Value;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class YbProtoCqlColumnValue {

    private final Value.QLValuePB valuepb;
    public YbProtoCqlColumnValue(Value.QLValuePB value) {
        this.valuepb = value;
    }

    public Value.QLValuePB getValuepb() {
        return valuepb;
    }
}
