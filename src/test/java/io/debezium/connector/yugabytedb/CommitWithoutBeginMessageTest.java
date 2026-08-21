/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import io.debezium.connector.yugabytedb.connection.OpId;

/**
 * Unit tests for {@link YugabyteDBStreamingChangeEventSource#commitWithoutBeginMessage}.
 * <p>
 * A COMMIT without a preceding BEGIN means the transaction boundaries coming off the stream are
 * malformed, and the connector fails on it. The message is the only thing an operator gets, so it
 * has to identify the tablet and the transaction - otherwise there is nothing to correlate against
 * the server logs. This message is raised from four call sites (transaction metadata enabled and
 * disabled, in both the streaming and the consistent-streaming source), which is why they all share
 * one helper.
 *
 * @author Yaron Parasol
 */
public class CommitWithoutBeginMessageTest {

    private static final String PARTITION_ID = "3fe122ffe3f24ad39c2cf8a57fa124b3.111111ffe3f24ad39c2cf8a57fa124b3";
    private static final String TRANSACTION_ID = "6f3d8a1e-0000-0000-0000-000000000001";

    private static OpId opId() {
        return new OpId(3L, 42L, new byte[]{ 55, -6 }, 7, 1234567890L);
    }

    @Test
    public void shouldIdentifyThePartitionTransactionAndPosition() {
        String message = YugabyteDBStreamingChangeEventSource.commitWithoutBeginMessage(
                PARTITION_ID, TRANSACTION_ID, opId(), 987654321L, false);

        assertTrue(message.contains("COMMIT record encountered without a preceding BEGIN record"),
                "Message should keep the recognisable summary, was: " + message);
        assertTrue(message.contains(PARTITION_ID),
                "Message should name the partition, was: " + message);
        assertTrue(message.contains(TRANSACTION_ID),
                "Message should name the transaction, was: " + message);
        assertTrue(message.contains("index=42"),
                "Message should carry the checkpoint of the offending record, was: " + message);
        assertTrue(message.contains("987654321"),
                "Message should carry the commit time, was: " + message);
    }

    @Test
    public void shouldDistinguishTheTransactionMetadataDisabledCallSite() {
        String message = YugabyteDBStreamingChangeEventSource.commitWithoutBeginMessage(
                PARTITION_ID, TRANSACTION_ID, opId(), 987654321L, false);

        assertTrue(message.contains("transaction metadata is disabled"),
                "Message should say which of the two code paths raised it, was: " + message);
    }

    @Test
    public void shouldDistinguishTheTransactionMetadataEnabledCallSite() {
        String message = YugabyteDBStreamingChangeEventSource.commitWithoutBeginMessage(
                PARTITION_ID, TRANSACTION_ID, opId(), 987654321L, true);

        assertTrue(message.contains("transaction metadata is enabled"),
                "Message should say which of the two code paths raised it, was: " + message);
    }

    @Test
    public void shouldNotBreakOnAMissingTransactionId() {
        // getTransactionId() can be null for records that carry no transaction, and building the
        // message must not be the thing that fails while reporting a failure.
        String message = YugabyteDBStreamingChangeEventSource.commitWithoutBeginMessage(
                PARTITION_ID, null, opId(), 0L, true);

        assertTrue(message.contains(PARTITION_ID),
                "Message should still name the partition, was: " + message);
        assertTrue(message.contains("null"),
                "A missing transaction ID should be rendered rather than throwing, was: " + message);
    }
}
