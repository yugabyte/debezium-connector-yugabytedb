package io.debezium.connector.yugabytedb.autoscale;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import io.debezium.connector.yugabytedb.YugabyteDBgRPCConnectorTestAccess;

public class EffectiveTabletCapTest {

    @Test
    public void theTabletCapFollowsTheGetChangesConcurrency() {
        assertEquals(8, YugabyteDBgRPCConnectorTestAccess.effectiveMaxTabletsPerTask(32, 8));
        assertEquals(16, YugabyteDBgRPCConnectorTestAccess.effectiveMaxTabletsPerTask(16, 32));
        assertEquals(32, YugabyteDBgRPCConnectorTestAccess.effectiveMaxTabletsPerTask(32, 1), "no pipelining: keep the configured cap");
    }

    @Test
    public void taskCountFollowsThePlanWithinTheTabletCap() {
        assertEquals(3, YugabyteDBgRPCConnectorTestAccess.autoscaleTaskCount(80, 32, 16, 1, null), "no plan: tablets ÷ cap, not tasks.max");
        assertEquals(4, YugabyteDBgRPCConnectorTestAccess.autoscaleTaskCount(80, 32, 16, 4, null), "no plan: at least tasks.autoscale.min");
        assertEquals(7, YugabyteDBgRPCConnectorTestAccess.autoscaleTaskCount(80, 32, 16, 1, 7));
        assertEquals(4, YugabyteDBgRPCConnectorTestAccess.autoscaleTaskCount(100, 32, 16, 1, 3), "tablets added since the plan");
        assertEquals(2, YugabyteDBgRPCConnectorTestAccess.autoscaleTaskCount(2, 32, 2, 1, 9), "never more than the partitions allow");
        assertEquals(1, YugabyteDBgRPCConnectorTestAccess.autoscaleTaskCount(0, 32, 1, 1, null));
    }
}
