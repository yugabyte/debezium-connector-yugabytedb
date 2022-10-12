package io.debezium.connector.yugabytedb.consistent;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MergerTest {
    @Test
    void addAndPollTest() {
        Merger merger = new Merger(List.of("3fe122ffe3f24ad39c2cf8a57fa124b3"));
        Message begin = new Message(null, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                6822178296495259648L,
                0,
                0,
                34);

        Message insert = new Message(null, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                6822178296477519872L,
                6822178296477519872L,
                0, 35);

        Message commit = new Message(null, "3fe122ffe3f24ad39c2cf8a57fa124b3",
                "57b8705f-69cd-4709-ac9b-b6c57fa995ce",
                6822178296495259648L,
                0,
                0,
                36);

        merger.addMessage(begin);
        merger.addMessage(insert);
        merger.addMessage(commit);

        assertEquals(insert, merger.poll());
        assertEquals(begin, merger.poll());
        assertEquals(commit, merger.poll());
    }

}