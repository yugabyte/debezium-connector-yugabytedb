package io.debezium.connector.yugabytedb;

public class HelperStrings {
    public static String INSERT_ALL_TYPES = "INSERT INTO all_types (id, bigintcol, bitcol, varbitcol, booleanval, byteaval, ch, vchar, cidrval, dt, dp, inetval, "
            + "intervalval, jsonval, jsonbval, mc, mc8, mn, nm, rl, si, i4r, i8r, nr, tsr, tstzr, dr, txt, tm, tmtz, ts, tstz, uuidval) VALUES "
            + "(404, 123456, '11011', '10101', FALSE, E'\\\\001', 'five5', 'sample_text', '10.1.0.0/16', '2022-02-24', 12.345, '127.0.0.1', "
            + "'2020-03-10 00:00:00'::timestamp-'2020-02-10 00:00:00'::timestamp, '{\"a\":\"b\"}', '{\"a\":\"b\"}', '2C:54:91:88:C9:E3', '22:00:5c:03:55:08:01:02', '$100.5', "
            + "12.34, 32.145, 12, '(1, 10)', '(100, 200)', '(10.45, 21.32)', '(1970-01-01 00:00:00, 2000-01-01 12:00:00)', "
            + "'(2017-07-04 12:30:30 UTC, 2021-07-04 12:30:30+05:30)', '(2019-10-07, 2021-10-07)', 'text to verify behaviour', '12:47:32', '12:00:00+05:30', "
            + "'2021-11-25 12:00:00', '2021-11-25 12:00:00+05:30', 'ffffffff-ffff-ffff-ffff-ffffffffffff');";

    public static Object[] EXPECTED_VALUES = { 404, 123456, "11011", "10101", false, "\\x01", "five5", "sample_text", "10.1.0.0/16", 19047, 12.345, "127.0.0.1",
            2505600000000L, "{\"a\":\"b\"}", "{\"a\": \"b\"}", "2c:54:91:88:c9:e3", "22:00:5c:03:55:08:01:02", 100.50, 12.34, 32.145, 12, "[2,10)", "[101,200)",
            "(10.45,21.32)", "(\"1970-01-01 00:00:00\",\"2000-01-01 12:00:00\")", "(\"2017-07-04 12:30:30+00\",\"2021-07-04 07:00:30+00\")", "[2019-10-08,2021-10-07)",
            "text to verify behaviour", 46052000, "06:30:00Z", 1637841600000L, "2021-11-25T06:30:00Z", "ffffffff-ffff-ffff-ffff-ffffffffffff" };

}
