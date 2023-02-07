package io.debezium.connector.yugabytedb.consistent;

import org.junit.jupiter.api.Test;
import org.yb.cdc.CdcService;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class MergerTest {
    @Test
    public void addAndPollTest() {
        Merger merger = new Merger(List.of("3fe122ffe3f24ad39c2cf8a57fa124b3"));

        CdcService.CDCSDKProtoRecordPB beginProtoRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.BEGIN).build())
                .build();
        Message begin = new Message(beginProtoRecord, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                34);

        CdcService.CDCSDKProtoRecordPB insertProtoRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.INSERT).build())
                .build();
        Message insert = new Message(insertProtoRecord, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296477519872L),
                BigInteger.valueOf(6822178296477519872L),
                BigInteger.ZERO, 35);

        CdcService.CDCSDKProtoRecordPB commitProtoRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.COMMIT).build())
                .build();
        Message commit = new Message(commitProtoRecord, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                BigInteger.valueOf(6822178296495259648L),
                BigInteger.ZERO,
                BigInteger.ZERO,
                36);

        merger.addMessage(begin);
        merger.addMessage(insert);
        merger.addMessage(commit);

        assertEquals(insert, merger.poll().get());
        assertEquals(begin, merger.poll().get());
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
        Message parent = new Message(parentProtoRecord, tabletOne, "9820053e-1597-42cb-a1aa-6f0ebe8237ca",
                BigInteger.valueOf(6863526294757593088L),
                BigInteger.valueOf(6863526294428749824L),
                BigInteger.ZERO,
                605244);

        CdcService.CDCSDKProtoRecordPB childProtoRecord = CdcService.CDCSDKProtoRecordPB.newBuilder()
                .setRowMessage(CdcService.RowMessage.newBuilder().setOp(CdcService.RowMessage.Op.INSERT).build())
                .build();
        Message child = new Message(childProtoRecord, tabletTwo, "be309af9-0a7d-40c2-b855-79e2e73b2daa",
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
}
