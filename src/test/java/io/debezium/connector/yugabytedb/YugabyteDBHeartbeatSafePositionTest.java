package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.yb.client.CdcSdkCheckpoint;

import io.debezium.connector.yugabytedb.connection.OpId;

public class YugabyteDBHeartbeatSafePositionTest {

    private static OpId op(long index, long time) {
        return new OpId(1, index, new byte[0], 0, time);
    }

    private static CdcSdkCheckpoint checkpoint(long index, long time) {
        return op(index, time).toCdcSdkCheckpoint();
    }

    @Test
    public void commitsTheExplicitCheckpointOnceTheLastRecordIsCommitted() {
        OpId safe = YugabyteDBStreamingChangeEventSource.heartbeatSafePosition(op(10, 100), op(10, 100), checkpoint(40, 900));

        assertNotNull(safe);
        assertEquals(40, safe.getIndex());
        assertEquals(900, safe.getTime());
    }

    @Test
    public void commitsTheExplicitCheckpointOnATabletThatNeverHadARecord() {
        OpId safe = YugabyteDBStreamingChangeEventSource.heartbeatSafePosition(op(5, 0), null, checkpoint(40, 900));

        assertNotNull(safe);
        assertEquals(40, safe.getIndex());
    }

    @Test
    public void keepsTheLastRecordWhileARecordIsInFlight() {
        // The last record dispatched is ahead of what Kafka Connect has committed.
        assertNull(YugabyteDBStreamingChangeEventSource.heartbeatSafePosition(op(50, 950), op(50, 950), checkpoint(40, 900)));
    }

    @Test
    public void keepsTheLastRecordWhenItsPositionIsAheadOfTheCheckpoint() {
        assertNull(YugabyteDBStreamingChangeEventSource.heartbeatSafePosition(op(50, 950), op(10, 100), checkpoint(40, 900)));
    }

    @Test
    public void keepsTheLastRecordWithoutAnExplicitCheckpoint() {
        assertNull(YugabyteDBStreamingChangeEventSource.heartbeatSafePosition(op(10, 100), op(10, 100), null));
    }
}
