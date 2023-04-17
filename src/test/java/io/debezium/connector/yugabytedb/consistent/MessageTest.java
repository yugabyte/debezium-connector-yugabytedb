package io.debezium.connector.yugabytedb.consistent;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.yb.cdc.CdcService;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService.RowMessage.Op;

import java.math.BigInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Rajat Venkatesh, Vaibhav Kushwaha
 */
public class MessageTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageTest.class);
    private final String DUMMY_TABLE_ID = "dummy_table_id";
    private final static long lowCommitTime = 12345L;
    private final static long highCommitTime = 123456L;
    private final static long lowRecordTime = 12345L;
    private final static long highRecordTime = 23456L;

    @DisplayName("Basic working of compareTo()")
    @Test
    public void comparatorTest() {
        CdcService.CDCSDKProtoRecordPB beginRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder()
                        .setOp(CdcService.RowMessage.Op.BEGIN).build()).build();
        Message begin = new Message(beginRecord, DUMMY_TABLE_ID, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        CdcService.CDCSDKProtoRecordPB insertRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder()
                        .setOp(CdcService.RowMessage.Op.INSERT).build()).build();
        Message insert = new Message(insertRecord, DUMMY_TABLE_ID, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.valueOf(6822178296477519872L),
                BigInteger.ZERO, 35);

        CdcService.CDCSDKProtoRecordPB commitRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder()
                        .setOp(CdcService.RowMessage.Op.COMMIT).build()).build();
        Message commit = new Message(commitRecord, DUMMY_TABLE_ID, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                36);

        assertEquals(-1, begin.compareTo(commit));
        assertEquals(-1, begin.compareTo(insert));
        assertEquals(1, commit.compareTo(insert));
    }

    @DisplayName("Two unequal messages are detected")
    @ParameterizedTest(name = "{index}. {0}")
    @MethodSource("parameterSourceForInequality")
    public void messageInequality(String testName, String tabletId, String txn,
                                  BigInteger commitTime, BigInteger recordTime,
                                  BigInteger snapshotTime, Op op) {
        CdcService.CDCSDKProtoRecordPB dummyRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder()
                        .setOp(CdcService.RowMessage.Op.INSERT).build()).build();

        Message m1 = new Message(dummyRecord, DUMMY_TABLE_ID, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        CdcService.CDCSDKProtoRecordPB record = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder()
                        .setOp(op).build()).build();
        Message m2 = new Message(record, DUMMY_TABLE_ID, tabletId, txn, commitTime, recordTime, snapshotTime, 35);

        assertFalse(m1.equals(m2));
    }


    @DisplayName("Verify when two messages are equal")
    @Test
    public void messageEquality() {
        CdcService.CDCSDKProtoRecordPB dummyRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder()
                        .setOp(CdcService.RowMessage.Op.INSERT).build()).build();

        Message m1 = new Message(dummyRecord, DUMMY_TABLE_ID, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        Message m2 = new Message(dummyRecord, DUMMY_TABLE_ID, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        assertTrue(m1.equals(m2));
    }

    @DisplayName("Verify BEGIN messages")
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

    @DisplayName("Verify COMMIT messages")
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
                .setTableId(DUMMY_TABLE_ID).setSnapshotTime(0).build();

        assertTrue(Message.isCommit(m));
    }

    @DisplayName("Verify message is neither BEGIN nor COMMIT")
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
                .setTableId(DUMMY_TABLE_ID).setSnapshotTime(0).build();
        Message m2 = new Message.Builder().setRecord(record).setTabletId("anotherDummyTablet")
                .setTableId(DUMMY_TABLE_ID).setSnapshotTime(0).build();

        assertTrue(Message.notBeginCommit(m1, m2));
    }

    @DisplayName("Different combinations of compareTo")
    @ParameterizedTest(name = "{index}. Checking {0}")
    @MethodSource("parameterSourceForCompareTo")
    public void compareToTest(String testName, CdcService.RowMessage.Op op1,
                              CdcService.RowMessage.Op op2, long commitTime1, long commitTime2,
                              long recordTime1, long recordTime2, long expectedResult) {
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
                                .build()).build()).setTableId(DUMMY_TABLE_ID).setTabletId("tablet2").build();

        assertEquals(m1.compareTo(m2), expectedResult);
    }

    private static Stream<Arguments> parameterSourceForCompareTo() {
        return Stream.of(
                /*
                 * Comparison based on CommitTime i.e. M1, M2, M1.commitTime >,<,= M2.commitTime
                 * Begin, Normal, Normal < Begin
                 * Begin, Normal, Normal > Begin
                 * Begin, Normal, Normal = Begin
                 * Normal, Begin, Normal < Begin
                 * Normal, Begin, Normal > Begin
                 * Normal, Begin, Normal = Begin
                 *
                 * Note that record time does not matter here.
                 */
                Arguments.of("Begin, Normal, Normal < Begin", Op.BEGIN, Op.INSERT, lowCommitTime, highCommitTime, 0, lowRecordTime, -1),
                Arguments.of("Begin, Normal, Normal > Begin", Op.BEGIN, Op.INSERT, highCommitTime, lowCommitTime, 0, lowRecordTime, 1),
                Arguments.of("Begin, Normal, Normal = Begin", Op.BEGIN, Op.INSERT, lowCommitTime, lowCommitTime, 0, lowRecordTime, -1),
                Arguments.of("Normal, Begin, Normal < Begin", Op.INSERT, Op.BEGIN, lowCommitTime, highCommitTime, lowRecordTime, 0, -1),
                Arguments.of("Normal, Begin, Normal > Begin", Op.INSERT, Op.BEGIN, highCommitTime, lowCommitTime, lowRecordTime, 0, 1),
                Arguments.of("Normal, Begin, Normal = Begin", Op.INSERT, Op.BEGIN, lowCommitTime, lowCommitTime, lowRecordTime, 0, 1),

                /*
                 * Comparison based on CommitTime i.e. M1, M2, M1.commitTime >,<,= M2.commitTime
                 * Commit, Normal, Normal < Commit
                 * Commit, Normal, Normal > Commit
                 * Commit, Normal, Normal = Commit
                 * Normal, Commit, Normal < Commit
                 * Normal, Commit, Normal > Commit
                 * Normal, Commit, Normal = Commit
                 *
                 * Note that record time does not matter here.
                 */
                Arguments.of("Commit, Normal, Normal < Commit", Op.COMMIT, Op.INSERT, highCommitTime, lowCommitTime, 0, lowRecordTime, 1),
                Arguments.of("Commit, Normal, Normal > Commit", Op.COMMIT, Op.INSERT, lowCommitTime, highCommitTime, 0, lowRecordTime, -1),
                Arguments.of("Commit, Normal, Normal = Commit", Op.COMMIT, Op.INSERT, lowCommitTime, lowCommitTime, 0, lowRecordTime, 1),
                Arguments.of("Normal, Commit, Normal < Commit", Op.INSERT, Op.COMMIT, lowCommitTime, highCommitTime, lowRecordTime, 0, -1),
                Arguments.of("Normal, Commit, Normal > Commit", Op.INSERT, Op.COMMIT, highCommitTime, lowRecordTime, lowRecordTime, 0, 1),
                Arguments.of("Normal, Commit, Normal = Commit", Op.INSERT, Op.COMMIT, lowCommitTime, lowCommitTime, lowRecordTime, 0, -1),

                /*
                 * Comparison based on commitTime, recordTime
                 * M1 = M2, M1 > M2
                 * M1 = M2, M1 < M2
                 * M1 > M2
                 * M1 < M2
                 * <p>
                 * Note that here if commit time is different then record time does not matter.
                 */
                Arguments.of("M1 = M2, M1 > M2", Op.INSERT, Op.INSERT, lowCommitTime, lowCommitTime, highRecordTime, lowRecordTime, 1),
                Arguments.of("M1 = M2, M1 < M2", Op.INSERT, Op.INSERT, lowCommitTime, highCommitTime, lowRecordTime, highRecordTime, -1),
                Arguments.of("M1 > M2", Op.INSERT, Op.INSERT, highCommitTime, lowCommitTime, highRecordTime, lowRecordTime, 1),
                Arguments.of("M1 < M2", Op.INSERT, Op.INSERT, lowCommitTime, highCommitTime, lowRecordTime, highRecordTime, -1)
        );
    }

    private static Stream<Arguments> parameterSourceForInequality() {
        return Stream.of(
                Arguments.of("Difference of tablet ID", "3fe122ffe3f24ad39c2cf8a57f54321f", "57b8705f-69cd-4709-ac9b-b6c57fa995ce", BigInteger.valueOf(6822178296477519872L), BigInteger.ZERO, BigInteger.ZERO, Op.INSERT),
                Arguments.of("Difference of transaction", "3fe122ffe3f24ad39c2cf8a57fa124b3", "57b8705f-69cd-4709-ac9b-b6c57fa12345", BigInteger.valueOf(6822178296477519872L), BigInteger.ZERO, BigInteger.ZERO, Op.INSERT),
                Arguments.of("Difference of commit time", "3fe122ffe3f24ad39c2cf8a57fa124b3", "57b8705f-69cd-4709-ac9b-b6c57fa995ce", BigInteger.valueOf(682217829L), BigInteger.ZERO, BigInteger.ZERO, Op.INSERT),
                Arguments.of("Difference of record time", "3fe122ffe3f24ad39c2cf8a57fa124b3", "57b8705f-69cd-4709-ac9b-b6c57fa995ce", BigInteger.valueOf(6822178296477519872L), BigInteger.valueOf(123456789L), BigInteger.ZERO, Op.INSERT),
                Arguments.of("Difference of snapshot time", "3fe122ffe3f24ad39c2cf8a57fa124b3", "57b8705f-69cd-4709-ac9b-b6c57fa995ce", BigInteger.valueOf(6822178296477519872L), BigInteger.ZERO, BigInteger.valueOf(987654321L), Op.INSERT),
                Arguments.of("Difference of Op", "3fe122ffe3f24ad39c2cf8a57fa124b3", "57b8705f-69cd-4709-ac9b-b6c57fa995ce", BigInteger.valueOf(6822178296477519872L), BigInteger.ZERO, BigInteger.ZERO, Op.UPDATE)
        );
    }
}
