package io.debezium.connector.yugabytedb.consistent;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@Disabled("Disabled in lieu of transaction ordering with logical replication")
class MergerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MergerTest.class);
    private final String DUMMY_TABLE_ID = "dummy_table_id";
    @Test
    public void addAndPollTest() {
        Merger merger = new Merger(List.of("3fe122ffe3f24ad39c2cf8a57fa124b3"));

        CdcService.CDCSDKProtoRecordPB beginProtoRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.BEGIN).build())
                .build();
        Message begin = new Message(beginProtoRecord, DUMMY_TABLE_ID, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        CdcService.CDCSDKProtoRecordPB insertProtoRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.INSERT).build())
                .build();
        Message insert = new Message(insertProtoRecord, DUMMY_TABLE_ID, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.valueOf(6822178296477519872L),
                BigInteger.ZERO, 35);

        CdcService.CDCSDKProtoRecordPB commitProtoRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.COMMIT).build())
                .build();
        Message commit = new Message(commitProtoRecord, DUMMY_TABLE_ID, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                36);

        merger.addMessage(begin);
        merger.addMessage(insert);
        merger.addMessage(commit);

        assertEquals(begin, merger.poll().get());
        assertEquals(insert, merger.poll().get());
        assertEquals(commit, merger.poll().get());
    }

    // This test fails currently because the compareTo function modification is not there in this PR.
    @Test
    public void addMessagesToTheQueue() {
        final String tabletOne = "11244bf18c8847d1bf195f417056d423";
        final String tabletTwo = "99b31e3a72ea419daaf740f1cba47ec4";

        Merger merger = new Merger(List.of(tabletOne, tabletTwo));

        CdcService.CDCSDKProtoRecordPB parentProtoRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.INSERT).build())
                .build();
        Message parent = new Message(parentProtoRecord, DUMMY_TABLE_ID, tabletOne, "9820053e-1597-42cb-a1aa-6f0ebe8237ca",
                BigInteger.valueOf(6863526294757593088L),
                BigInteger.valueOf(6863526294428749824L),
                BigInteger.ZERO,
                605244);

        CdcService.CDCSDKProtoRecordPB childProtoRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.INSERT).build())
                .build();
        Message child = new Message(childProtoRecord, DUMMY_TABLE_ID, tabletTwo, "be309af9-0a7d-40c2-b855-79e2e73b2daa",
                BigInteger.valueOf(6863526294757593088L),
                BigInteger.valueOf(6863526294462816256L),
                BigInteger.ZERO,
                605228);

        // Purposely insert the child message first and verify the sorting after parent is inserted.
        merger.addMessage(child);
        merger.addMessage(parent);

        // Upon polling, the first message should be parent and second should be child.
        Optional<Message> firstPoll = merger.poll();
        assertFalse(firstPoll.isEmpty());
        assertTrue(firstPoll.get().equals(parent));

        Optional<Message> secondPoll = merger.poll();
        assertFalse(secondPoll.isEmpty());
        assertTrue(secondPoll.get().equals(child));
    }

    @DisplayName("Verify for exception when adding a record of lower commit time to merge slot")
    @Test
    public void throwExceptionIfRecordOutOfOrder() {
        final String dummyTablet = "99b31e3a72ea419daaf740f1cba47ec4";
        CdcService.CDCSDKProtoRecordPB proto1 = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.INSERT).build())
                .build();
        Message m1 = new Message(proto1, DUMMY_TABLE_ID, dummyTablet, "9820053e-1597-42cb-a1aa-6f0ebe8237ca",
                BigInteger.valueOf(6863526294757593088L),
                BigInteger.valueOf(6863526294428749824L),
                BigInteger.ZERO,
                605244);

        CdcService.CDCSDKProtoRecordPB proto2 = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.INSERT).build())
                .build();
        Message m2 = new Message(proto2, DUMMY_TABLE_ID, dummyTablet, "be309af9-0a7d-40c2-b855-79e2e73b2daa",
                BigInteger.valueOf(68635262947L), // Lower commit time than previous record.
                BigInteger.valueOf(6863526294462816256L),
                BigInteger.ZERO,
                605228);

        Merger merger = new Merger(List.of(dummyTablet));

        merger.addMessage(m1);

        try {
            merger.addMessage(m2);
        } catch (AssertionError ae) {
            // An assertion error will be thrown saying that commit time of incoming message is less
            // than that of the last record in the merge slot.
            assertTrue(ae.getMessage().contains("Merger tried to set tablet safetime to a lower value"));
        }
    }

    @Test
    public void orderingOfMultipleMessages() throws Exception {
        final String txn = "be309af9-0a7d-40c2-b855-79e2e73b2daa";

        final String tablet1 = "tablet_1";
        final String tablet2 = "tablet_2";

        final long commitTime1 = 12345L;
        final long commitTime2 = 23456L;

        CdcService.CDCSDKProtoRecordPB begin1 = CdcService.CDCSDKProtoRecordPB.newBuilder()
                                                   .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.BEGIN).build())
                                                   .build();
        CdcService.CDCSDKProtoRecordPB insert1 = CdcService.CDCSDKProtoRecordPB.newBuilder()
                                                  .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.INSERT).build())
                                                  .build();
        CdcService.CDCSDKProtoRecordPB commit1 = CdcService.CDCSDKProtoRecordPB.newBuilder()
                                                   .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.COMMIT).build())
                                                   .build();

        CdcService.CDCSDKProtoRecordPB begin2 = CdcService.CDCSDKProtoRecordPB.newBuilder()
                                                  .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.BEGIN).build())
                                                  .build();
        CdcService.CDCSDKProtoRecordPB insert2 = CdcService.CDCSDKProtoRecordPB.newBuilder()
                                                  .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.INSERT).build())
                                                  .build();
        CdcService.CDCSDKProtoRecordPB commit2 = CdcService.CDCSDKProtoRecordPB.newBuilder()
                                                   .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.COMMIT).build())
                                                   .build();

        CdcService.CDCSDKProtoRecordPB begin3 = CdcService.CDCSDKProtoRecordPB.newBuilder()
                                                  .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.BEGIN).build())
                                                  .build();
        CdcService.CDCSDKProtoRecordPB insert3 = CdcService.CDCSDKProtoRecordPB.newBuilder()
                                                  .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.INSERT).build())
                                                  .build();
        CdcService.CDCSDKProtoRecordPB commit3 = CdcService.CDCSDKProtoRecordPB.newBuilder()
                                                   .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.COMMIT).build())
                                                   .build();

        Merger merger = new Merger(List.of(tablet1, tablet2));

        Message m1 = new Message(begin1, DUMMY_TABLE_ID, tablet1, txn,
          BigInteger.valueOf(commitTime1),
          BigInteger.ZERO,
          BigInteger.ZERO,
          1);
        Message m2 = new Message(insert1, DUMMY_TABLE_ID, tablet1, txn,
          BigInteger.valueOf(commitTime1),
          BigInteger.ZERO,
          BigInteger.ZERO,
          2);
        Message m3 = new Message(commit1, DUMMY_TABLE_ID, tablet1, txn,
          BigInteger.valueOf(commitTime1),
          BigInteger.ZERO,
          BigInteger.ZERO,
          3);

        Message m4 = new Message(begin2, DUMMY_TABLE_ID, tablet1, txn,
          BigInteger.valueOf(commitTime2),
          BigInteger.ZERO,
          BigInteger.ZERO,
          4);
        Message m5 = new Message(insert2, DUMMY_TABLE_ID, tablet1, txn,
          BigInteger.valueOf(commitTime2),
          BigInteger.ZERO,
          BigInteger.ZERO,
          5);
        Message m6 = new Message(commit2, DUMMY_TABLE_ID, tablet1, txn,
          BigInteger.valueOf(commitTime2),
          BigInteger.ZERO,
          BigInteger.ZERO,
          6);

        Message m7 = new Message(begin3, DUMMY_TABLE_ID, tablet2, txn,
          BigInteger.valueOf(commitTime1),
          BigInteger.ZERO,
          BigInteger.ZERO,
          7);
        Message m8 = new Message(insert3, DUMMY_TABLE_ID, tablet2, txn,
          BigInteger.valueOf(commitTime1),
          BigInteger.ZERO,
          BigInteger.ZERO,
          8);
        Message m9 = new Message(commit3, DUMMY_TABLE_ID, tablet2, txn,
          BigInteger.valueOf(commitTime1),
          BigInteger.ZERO,
          BigInteger.ZERO,
          9);

        CdcService.CDCSDKProtoRecordPB sp = CdcService.CDCSDKProtoRecordPB.newBuilder()
                                                .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.SAFEPOINT)
                                                   .setCommitTime(commitTime2).build()).build();
        Message safepoint = new Message(sp, DUMMY_TABLE_ID, tablet2, txn, BigInteger.valueOf(commitTime2), BigInteger.ZERO, BigInteger.ZERO, 10);

        merger.addMessage(m1);
        merger.addMessage(m2);
        merger.addMessage(m3);
        merger.addMessage(m4);
        merger.addMessage(m5);
        merger.addMessage(m6);
        merger.addMessage(m7);
        merger.addMessage(m8);
        merger.addMessage(m9);
        merger.addMessage(safepoint);

        LOGGER.info("Done putting all the messages");

        List<Message> listOfMessages = List.of(m1, m7, m2, m8, m3, m9, m4, m5, m6);

        int ind = 0;
        while (!merger.isEmpty()) {
            Optional<Message> opt = merger.poll();
            Message expectedMessage = listOfMessages.get(ind++);
            opt.ifPresent(message -> assertEquals(expectedMessage, message));
        }
    }
}
