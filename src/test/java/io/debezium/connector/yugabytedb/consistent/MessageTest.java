package io.debezium.connector.yugabytedb.consistent;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;
import org.yb.cdc.CdcService.RowMessage.Op;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Rajat Venkatesh, Vaibhav Kushwaha
 */
public class MessageTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageTest.class);
    private final long lowCommitTime = 12345L;
    private final long highCommitTime = 123456L;
    private final long lowRecordTime = 12345L;
    private final long highRecordTime = 23456L;
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

    @Test
    public void verifyCommitTimeRecordTimeComparison() {
        List<Params> parameterList = new ArrayList<>();

        /**
         * Comparison based on CommitTime i.e. M1, M2, M1.commitTime >,<,= M2.commitTime
         * Begin, Normal, Normal < Begin.
         * Begin, Normal, Normal > Begin
         * Begin, Normal, Normal = Begin
         * Normal, Begin, Normal < Begin.
         * Normal, Begin, Normal > Begin
         * Normal, Begin, Normal = Begin
         *
         * Note that record time does not matter here.
         */
        parameterList.add(getParameter(Op.BEGIN, Op.INSERT, lowCommitTime, highCommitTime, 0, lowRecordTime, -1)); // 0
        parameterList.add(getParameter(Op.BEGIN, Op.INSERT, highCommitTime, lowCommitTime, 0, lowRecordTime, 1)); // 1
        parameterList.add(getParameter(Op.BEGIN, Op.INSERT, lowCommitTime, lowCommitTime, 0, lowRecordTime, -1)); // 2
        parameterList.add(getParameter(Op.INSERT, Op.BEGIN, lowCommitTime, highCommitTime, lowRecordTime, 0, -1)); // 3
        parameterList.add(getParameter(Op.INSERT, Op.BEGIN, highCommitTime, lowCommitTime, lowRecordTime, 0, 1)); // 4
        parameterList.add(getParameter(Op.INSERT, Op.BEGIN, lowCommitTime, lowCommitTime, lowRecordTime, 0, 1)); // 5

        /**
         * Comparison based on CommitTime i.e. M1, M2, M1.commitTime >,<,= M2.commitTime
         * Commit, Normal, Normal < Commit.
         * Commit, Normal, Normal > Commit
         * Commit, Normal, Normal = Commit
         * Normal, Commit, Normal < Commit.
         * Normal, Commit, Normal > Commit
         * Normal, Commit, Normal = Commit
         *
         * Note that record time does not matter here.
         */
        parameterList.add(getParameter(Op.COMMIT, Op.INSERT, highCommitTime, lowCommitTime, 0, lowRecordTime, 1)); // 6
        parameterList.add(getParameter(Op.COMMIT, Op.INSERT, lowCommitTime, highCommitTime, 0, lowRecordTime, -1)); // 7
        parameterList.add(getParameter(Op.COMMIT, Op.INSERT, lowCommitTime, lowCommitTime, 0, lowRecordTime, 1)); // 8
        parameterList.add(getParameter(Op.INSERT, Op.COMMIT, lowCommitTime, highCommitTime, lowRecordTime, 0, -1)); // 9
        parameterList.add(getParameter(Op.INSERT, Op.COMMIT, highCommitTime, lowRecordTime, lowRecordTime, 0, 1)); // 10
        parameterList.add(getParameter(Op.INSERT, Op.COMMIT, lowCommitTime, lowCommitTime, lowRecordTime, 0, -1)); // 11

        /**
         * Comparison based on commitTime, recordTime
         * M1 = M2, M1 > M2
         * M1 = M2, M1 < M2
         * M1 > M2
         * M1 < M2
         *
         * Note that here if commit time is different then record time does not matter.
         */
        parameterList.add(getParameter(Op.INSERT, Op.INSERT, lowCommitTime, lowCommitTime, highRecordTime, lowRecordTime, 1)); // 12
        parameterList.add(getParameter(Op.INSERT, Op.INSERT, lowCommitTime, highCommitTime, lowCommitTime, highRecordTime, -1)); // 13
        parameterList.add(getParameter(Op.INSERT, Op.INSERT, highCommitTime, lowCommitTime, highCommitTime, lowRecordTime, 1)); // 14
        parameterList.add(getParameter(Op.INSERT, Op.INSERT, lowCommitTime, highCommitTime, lowRecordTime, highCommitTime, -1)); // 15

        for (int i = 0; i < parameterList.size(); ++i) {
            LOGGER.info("Verifying record at index {}", i);
            parameterList.get(i).verify();
        }
    }

    public static class Params {
        public Message m1;
        public Message m2;
        public int expectedResult;

        public Params(Message m1, Message m2, int expectedResult) {
            this.m1 = m1;
            this.m2 = m2;
            this.expectedResult = expectedResult;
        }

        public void verify() {
            assertEquals(this.m1.compareTo(this.m2), this.expectedResult);
        }
    }

    public Params getParameter(CdcService.RowMessage.Op op1, CdcService.RowMessage.Op op2, long commitTime1, long commitTime2, long recordTime1, long recordTime2, long expectedResult) {
        Message m1 = new Message.Builder()
                .setRecord(CdcService.CDCSDKProtoRecordPB.newBuilder()
                        .setRowMessage(CdcService.RowMessage.newBuilder()
                                .setOp(op1)
                                .setRecordTime(recordTime1)
                                .setCommitTime(commitTime1)
                                .build()).build()).setTabletId("tablet1").build();
        Message m2 = new Message.Builder()
                .setRecord(CdcService.CDCSDKProtoRecordPB.newBuilder()
                        .setRowMessage(CdcService.RowMessage.newBuilder()
                                .setOp(op2)
                                .setRecordTime(recordTime2)
                                .setCommitTime(commitTime2)
                                .build()).build()).setTabletId("tablet2").build();
        return new Params(m1, m2, (int) expectedResult);
    }
}