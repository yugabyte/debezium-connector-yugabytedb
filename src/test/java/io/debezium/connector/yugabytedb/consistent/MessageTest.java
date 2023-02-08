package io.debezium.connector.yugabytedb.consistent;

import org.junit.jupiter.api.Test;
import org.yb.cdc.CdcService;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

class MessageTest {
    @Test
    public void comparatorTest() {
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
    public void messageInequality() {
        CdcService.CDCSDKProtoRecordPB dummyRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder()
                        .setOp(CdcService.RowMessage.Op.INSERT).build()).build();

        Message m1 = new Message(dummyRecord, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        // Differs in tablet ID.
        Message m2 = new Message(dummyRecord, "3fe122ffe3f24ad39c2cf8a57f54321f",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296477519872L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        // Differs in transaction.
        Message m3 = new Message(dummyRecord, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa12345",
                BigInteger.valueOf(6822178296477519872L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        // Differs in commit time.
        Message m4 = new Message(dummyRecord, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(682217829L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        // Differs in record time.
        Message m5 = new Message(dummyRecord, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.valueOf(123456789L),
                BigInteger.ZERO,
                34);

        // Differs in snapshot time.
        Message m6 = new Message(dummyRecord, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.valueOf(987654321L),
                34);

        // Differs by Op.
        CdcService.CDCSDKProtoRecordPB record = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder()
                        .setOp(CdcService.RowMessage.Op.UPDATE).build()).build();
        Message m7 = new Message(record, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        assertFalse(m1.equals(m2)); // Comparison by tablet ID.
        assertFalse(m1.equals(m3)); // Comparison by transaction.
        assertFalse(m1.equals(m4)); // Comparison by commit time.
        assertFalse(m1.equals(m5)); // Comparison by record time.
        assertFalse(m1.equals(m6)); // Comparison by snapshot time.
        assertFalse(m1.equals(m7)); // Comparison by op.
    }

    @Test
    public void messageEquality() {
        CdcService.CDCSDKProtoRecordPB dummyRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder()
                        .setOp(CdcService.RowMessage.Op.INSERT).build()).build();

        Message m1 = new Message(dummyRecord, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        Message m2 = new Message(dummyRecord, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        assertTrue(m1.equals(m2));
    }

}
