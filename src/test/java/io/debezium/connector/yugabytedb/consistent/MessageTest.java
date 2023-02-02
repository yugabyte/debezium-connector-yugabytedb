package io.debezium.connector.yugabytedb.consistent;

import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

class MessageTest {
    @Test
    void comparatorTest() {
        Message begin = new Message(null, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        Message insert = new Message(null, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296477519872L),
                BigInteger.valueOf(6822178296477519872L),
                BigInteger.ZERO, 35);

        Message commit = new Message(null, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                36);

        assertEquals(-1, begin.compareTo(commit));
        assertEquals(-1, begin.compareTo(insert));
        assertEquals(1, commit.compareTo(insert));
    }

    @Test
    void valueBasedTest() {
        Message first = new Message(null, "6940b1af2c764cbb9743c9c1dee04a68",
                "5e5af632-75ca-41d1-92c5-5525a4e1b73e",
                BigInteger.valueOf(6861762200086036480L),
                BigInteger.valueOf(6861762200086036480L),
                BigInteger.ZERO, 8268);

        Message second = new Message(null, "ee06f1ee103f4ceeb0ee3e100ba2f805",
                "5e5af632-75ca-41d1-92c5-5525a4e1b73e",
                BigInteger.valueOf(6861762200090394624L),
                BigInteger.valueOf(6861762200090394624L),
                BigInteger.ZERO, 2468);

        assertEquals(-1, first.compareTo(second));
    }

}