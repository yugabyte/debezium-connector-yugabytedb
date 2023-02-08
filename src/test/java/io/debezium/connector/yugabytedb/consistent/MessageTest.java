package io.debezium.connector.yugabytedb.consistent;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.yb.cdc.CdcService;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Rajat Venkatesh, Vaibhav Kushwaha
 */
class MessageTest {
    @Test
    public void comparatorTest() {
        CdcService.CDCSDKProtoRecordPB beginRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder()
                        .setOp(CdcService.RowMessage.Op.BEGIN).build()).build();
        Message begin = new Message(beginRecord, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        CdcService.CDCSDKProtoRecordPB insertRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder()
                        .setOp(CdcService.RowMessage.Op.INSERT).build()).build();
        Message insert = new Message(insertRecord, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.valueOf(6822178296477519872L),
                BigInteger.ZERO, 35);

        CdcService.CDCSDKProtoRecordPB commitRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder()
                        .setOp(CdcService.RowMessage.Op.COMMIT).build()).build();
        Message commit = new Message(commitRecord, "3fe122ffe3f24ad39c2cf8a57fa124b3",
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
    public void checkForBeginMessages() {
        // Building a dummy row message
        CdcService.RowMessage.Builder rowMessageBuilder = CdcService.RowMessage.newBuilder()
                .setOp(CdcService.RowMessage.Op.BEGIN)
                .setCommitTime(10L)
                .setTransactionId(ByteString.EMPTY)
                .setRecordTime(5L);
        CdcService.RowMessage rowMessage = rowMessageBuilder.build();
        CdcService.CDCSDKProtoRecordPB record = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(rowMessage).build();

        Message m = new Message.Builder().setRecord(record).setTabletId("dummyTabletId")
                .setSnapshotTime(0).build();

        assertTrue(Message.isBegin(m));
    }

    @Test
    public void checkForCommitMessages() {
        // Building a dummy row message
        CdcService.RowMessage.Builder rowMessageBuilder = CdcService.RowMessage.newBuilder()
                .setOp(CdcService.RowMessage.Op.COMMIT)
                .setCommitTime(10L)
                .setTransactionId(ByteString.EMPTY)
                .setRecordTime(5L);
        CdcService.RowMessage rowMessage = rowMessageBuilder.build();
        CdcService.CDCSDKProtoRecordPB record = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(rowMessage).build();

        Message m = new Message.Builder().setRecord(record).setTabletId("dummyTabletId")
                .setSnapshotTime(0).build();

        assertTrue(Message.isCommit(m));
    }

    @Test
    public void checkNeitherBeginNorCommit() {
        // Building a dummy row message
        CdcService.RowMessage.Builder rowMessageBuilder = CdcService.RowMessage.newBuilder()
                .setOp(CdcService.RowMessage.Op.INSERT)
                .setCommitTime(10L)
                .setTransactionId(ByteString.EMPTY)
                .setRecordTime(5L);
        CdcService.RowMessage rowMessage = rowMessageBuilder.build();
        CdcService.CDCSDKProtoRecordPB record = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(rowMessage).build();

        Message m1 = new Message.Builder().setRecord(record).setTabletId("dummyTabletId")
                .setSnapshotTime(0).build();
        Message m2 = new Message.Builder().setRecord(record).setTabletId("anotherDummyTablet")
                .setSnapshotTime(0).build();

        assertTrue(Message.notBeginCommit(m1, m2));
    }
}