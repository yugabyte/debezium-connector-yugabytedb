package io.debezium.connector.yugabytedb.autoscale;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.connector.yugabytedb.connection.HashPartition;

public class TaskAutoscalePolicyTest {

    private static final long MIN = 60_000;

    private static TaskAutoscalePolicy policy(int maxTasks) {
        return policy(maxTasks, 1.0);
    }

    private static TaskAutoscalePolicy policy(int maxTasks, double downHeadroom) {
        return new TaskAutoscalePolicy(new TaskAutoscalePolicy.Settings(1, maxTasks, 100, 8, 60, 2, 3, 5 * MIN, 10 * MIN, downHeadroom));
    }

    /** n tablets whose WAL index grows by {@code perTabletOpsPerMin} between the two samples. */
    private static List<List<TaskAutoscalePolicy.TabletSample>> step(int n, long start, long perTabletOpsPerMin) {
        return List.of(sample(n, start, start), sample(n, start + perTabletOpsPerMin, start + perTabletOpsPerMin));
    }

    /** Tablets t00..t(n-1), all at the same WAL index and checkpoint. */
    private static List<TaskAutoscalePolicy.TabletSample> sample(int tablets, long index, long checkpoint) {
        List<TaskAutoscalePolicy.TabletSample> s = new ArrayList<>();
        for (int i = 0; i < tablets; i++) {
            s.add(new TaskAutoscalePolicy.TabletSample(String.format("t%02d", i), index, checkpoint));
        }
        return s;
    }

    @Test
    public void initialPlanFollowsTabletsWhenIdle() {
        TaskAutoscalePolicy p = policy(16);
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(20, 100, 100), sample(20, 100, 100), MIN, 0);
        assertNotNull(e.apply);
        assertEquals(3, e.apply.taskCount, "20 tablets at 8 per task");
    }

    @Test
    public void initialPlanFollowsWriteRate() {
        TaskAutoscalePolicy p = policy(16);
        // 20 tablets x 3000 ops in 60 s = 1000 ops/s -> 10 tasks at 100 ops/s per task
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(20, 0, 0), sample(20, 3000, 3000), MIN, 0);
        assertEquals(10, e.apply.taskCount);
    }

    @Test
    public void neverExceedsTasksMaxOrTablets() {
        TaskAutoscalePolicy p = policy(4);
        assertEquals(4, p.evaluate(sample(20, 0, 0), sample(20, 30000, 30000), MIN, 0).apply.taskCount);
        TaskAutoscalePolicy q = policy(16);
        assertEquals(2, q.evaluate(sample(2, 0, 0), sample(2, 30000, 30000), MIN, 0).apply.taskCount);
    }

    @Test
    public void scalesUpAfterTwoEvaluationsAndTheCooldown() {
        TaskAutoscalePolicy p = policy(16);
        p.evaluate(sample(8, 0, 0), sample(8, 0, 0), MIN, 0); // 1 task
        // 8 tablets x 1500 ops per minute = 200 ops/s -> 2 tasks wanted
        assertNull(p.evaluate(sample(8, 0, 0), sample(8, 1500, 1500), MIN, 6 * MIN).apply, "one evaluation is not enough");
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(8, 1500, 1500), sample(8, 3000, 3000), MIN, 7 * MIN);
        assertEquals(2, e.apply.taskCount);
    }

    @Test
    public void respectsTheScaleUpCooldown() {
        TaskAutoscalePolicy p = policy(16);
        p.evaluate(sample(8, 0, 0), sample(8, 0, 0), MIN, 0);
        p.evaluate(sample(8, 0, 0), sample(8, 1500, 1500), MIN, MIN);
        assertNull(p.evaluate(sample(8, 1500, 1500), sample(8, 3000, 3000), MIN, 2 * MIN).apply);
    }

    @Test
    public void laggingScalesUpAtOnce() {
        TaskAutoscalePolicy p = policy(16);
        p.evaluate(sample(8, 0, 0), sample(8, 0, 0), MIN, 0);
        // little write rate but a growing backlog of 8 x 2000 ops, more than 60 s at 100 ops/s
        p.evaluate(sample(8, 0, 0), sample(8, 1900, 0), MIN, 2 * MIN);
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(8, 1900, 0), sample(8, 2010, 0), MIN, 6 * MIN);
        assertTrue(e.lagging);
        assertEquals(2, e.apply.taskCount);
    }

    @Test
    public void aStuckCheckpointOnIdleTabletsIsNotLag() {
        TaskAutoscalePolicy p = policy(16);
        p.evaluate(sample(8, 0, 0), sample(8, 4500, 0), MIN, 0);
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(8, 4500, 0), sample(8, 4500, 0), MIN, 11 * MIN);
        assertFalse(e.lagging, "nothing new is written and the checkpoint just hasn't been committed");
    }

    @Test
    public void scalesDownStraightToTargetAfterSustainedLowLoad() {
        TaskAutoscalePolicy p = policy(16);
        p.evaluate(sample(8, 0, 0), sample(8, 4500, 4500), MIN, 0); // 600 ops/s -> 6 tasks
        long t = 11 * MIN;
        assertNull(p.evaluate(sample(8, 4500, 4500), sample(8, 4500, 4500), MIN, t).apply);
        assertNull(p.evaluate(sample(8, 4500, 4500), sample(8, 4500, 4500), MIN, t + MIN).apply);
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(8, 4500, 4500), sample(8, 4500, 4500), MIN, t + 2 * MIN);
        assertEquals(1, e.apply.taskCount, "straight to the target, one restart");
    }

    @Test
    public void aSplitPastTheTabletCapAddsATaskWithoutCooldown() {
        TaskAutoscalePolicy p = policy(16);
        p.evaluate(sample(8, 0, 0), sample(8, 0, 0), MIN, 0);
        assertNull(p.evaluate(sample(8, 0, 0), sample(9, 0, 0), MIN, MIN).apply, "wait for a second sample to agree");
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(9, 0, 0), sample(9, 0, 0), MIN, 2 * MIN);
        assertNotNull(e.apply);
        assertEquals(2, e.apply.taskCount, "9 tablets need a second task at 8 per task");
        assertEquals(9, e.apply.tablets.size());
    }

    @Test
    public void aSplitParentDisappearingIsIgnored() {
        TaskAutoscalePolicy p = policy(16);
        p.evaluate(sample(8, 0, 0), sample(8, 0, 0), MIN, 0);
        assertNull(p.evaluate(sample(7, 0, 0), sample(7, 0, 0), MIN, MIN).apply);
    }

    @Test
    public void aSampleWhereTabletsChangeDoesNotVoteOnRate() {
        TaskAutoscalePolicy p = policy(16);
        p.evaluate(sample(8, 0, 0), sample(8, 4500, 4500), MIN, 0); // 6 tasks
        // parent gone, children new: rate looks like 0 for one sample
        for (int i = 1; i <= 5; i++) {
            assertNull(p.evaluate(sample(8, 4500, 4500), sample(7, 4500, 4500), MIN, 20 * MIN + i * MIN).apply);
        }
    }

    @Test
    public void aRateHoveringAtABoundaryDoesNotScaleDownWithHeadroom() {
        TaskAutoscalePolicy p = policy(16, 0.6);
        // 8 tablets x 3000 ops/min = 400 ops/s -> 4 tasks
        assertEquals(4, p.evaluate(sample(8, 0, 0), sample(8, 3000, 3000), MIN, 0).apply.taskCount);
        // falls to 290 ops/s (3 tasks at target) and stays: 290 > 3 x 100 x 0.6, so no scale-down
        long t = 20 * MIN;
        for (int i = 0; i < 10; i++) {
            List<List<TaskAutoscalePolicy.TabletSample>> s = step(8, 100_000 + i * 3000L, 2175);
            assertNull(p.evaluate(s.get(0), s.get(1), MIN, t + i * MIN).apply);
        }
    }

    @Test
    public void scaleDownIsSizedForThePeakOfTheLowStretch() {
        TaskAutoscalePolicy p = policy(16, 0.6);
        assertEquals(8, p.evaluate(sample(8, 0, 0), sample(8, 7500, 7500), MIN, 0).apply.taskCount); // 1000 ops/s, capped at 8 tablets
        long t = 20 * MIN;
        long[] perTablet = {300, 900, 300}; // 40, 120, 40 ops/s
        TaskAutoscalePolicy.Evaluation e = null;
        long base = 100_000;
        for (int i = 0; i < 3; i++) {
            List<List<TaskAutoscalePolicy.TabletSample>> s = step(8, base, perTablet[i]);
            base += perTablet[i];
            e = p.evaluate(s.get(0), s.get(1), MIN, t + i * MIN);
        }
        assertNotNull(e.apply);
        assertEquals(2, e.apply.taskCount, "120 ops/s peak / (100 x 0.6) -> 2, not the 1 the last sample alone needs");
    }

    @Test
    public void weightedGroupingKeepsEachTaskWithinTheTabletCap() {
        List<HashPartition> parts = new ArrayList<>();
        Map<String, Double> w = new HashMap<>();
        for (int i = 0; i < 40; i++) {
            String id = String.format("t%02d", i);
            parts.add(new HashPartition("table", id, new byte[0], new byte[0]));
            w.put(id, i == 0 ? 10_000.0 : 0.0); // one hot tablet, 39 idle ones
        }
        List<List<HashPartition>> groups = WeightedTabletGrouping.group(parts, 5, w, 8);
        assertEquals(5, groups.size());
        for (List<HashPartition> g : groups) {
            assertTrue(g.size() <= 8, "a task got " + g.size() + " tablets with a cap of 8");
        }
    }

    @Test
    public void neverScalesDownWhileLagging() {
        TaskAutoscalePolicy p = policy(2);
        assertEquals(2, p.evaluate(sample(8, 0, 0), sample(8, 1500, 1500), MIN, 0).apply.taskCount);
        // At tasks.max, writing little (66 ops/s fits one task) but the backlog keeps growing.
        long t = 20 * MIN;
        for (int i = 0; i < 6; i++) {
            long head = 10_000 + i * 500L;
            TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(8, head - 500, 0), sample(8, head, 0), MIN, t + i * MIN);
            assertTrue(e.lagging);
            assertNull(e.apply);
        }
    }

    @Test
    public void theSampleStraddlingADropDoesNotSetTheScaleDownTarget() {
        TaskAutoscalePolicy p = policy(16);
        assertEquals(8, p.evaluate(sample(8, 0, 0), sample(8, 6000, 6000), MIN, 0).apply.taskCount); // 800 ops/s
        // 520 ops/s (half the minute at the old rate), then 80 ops/s: one step to 1 task, not 6 then 1
        assertNull(p.evaluate(sample(8, 6000, 6000), sample(8, 9900, 9900), MIN, 11 * MIN).apply);
        assertNull(p.evaluate(sample(8, 9900, 9900), sample(8, 10500, 10500), MIN, 12 * MIN).apply);
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(8, 10500, 10500), sample(8, 11100, 11100), MIN, 13 * MIN);
        assertEquals(1, e.apply.taskCount);
    }

    @Test
    public void aCheckpointTrailingABurstIsNotLag() {
        TaskAutoscalePolicy p = policy(16);
        // 800 ops/s over 8 tablets (8 tasks), the checkpoint 30 s behind
        assertEquals(8, p.evaluate(sample(8, 0, 0), sample(8, 6000, 3000), MIN, 0).apply.taskCount);
        // The load drops to 80 ops/s. Until the next commit the checkpoint still trails by the burst's 30 s,
        // more than 60 s of the new rate, and the backlog grows: not lag.
        assertFalse(p.evaluate(sample(8, 6000, 3000), sample(8, 6600, 3000), MIN, MIN).lagging);
        assertFalse(p.evaluate(sample(8, 6600, 3000), sample(8, 7200, 6600), MIN, 2 * MIN).lagging);
    }

    @Test
    public void aSplitIsPlacedAgainWhileAScaleUpIsPending() {
        // 16 tablets per task keeps the structure term at 1, so only the rate asks for a second task.
        TaskAutoscalePolicy p = new TaskAutoscalePolicy(new TaskAutoscalePolicy.Settings(1, 16, 100, 16, 60, 2, 3, 5 * MIN, 10 * MIN, 1.0));
        p.evaluate(sample(8, 0, 0), sample(8, 0, 0), MIN, 0);
        assertNull(p.evaluate(sample(8, 0, 0), sample(9, 1500, 1500), MIN, MIN).apply);
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(9, 1500, 1500), sample(9, 3000, 3000), MIN, 2 * MIN);
        assertNotNull(e.apply);
        assertEquals(1, e.apply.taskCount, "re-placed at the current count; the scale-up still waits for its evaluations");
        assertEquals(9, e.apply.tablets.size());
    }

    @Test
    public void aSplitCarriesAScaleUpWhoseEvaluationsAreComplete() {
        TaskAutoscalePolicy p = new TaskAutoscalePolicy(new TaskAutoscalePolicy.Settings(1, 16, 100, 16, 60, 2, 3, 5 * MIN, 10 * MIN, 1.0));
        p.evaluate(sample(8, 0, 0), sample(8, 0, 0), MIN, 0);
        // two evaluations want 2 tasks, but the cooldown hasn't passed
        assertNull(p.evaluate(sample(8, 0, 0), sample(8, 1500, 1500), MIN, MIN).apply);
        assertNull(p.evaluate(sample(8, 1500, 1500), sample(8, 3000, 3000), MIN, 2 * MIN).apply);
        assertNull(p.evaluate(sample(8, 3000, 3000), sample(9, 4500, 4500), MIN, 3 * MIN).apply);
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(9, 4500, 4500), sample(9, 6000, 6000), MIN, 4 * MIN);
        assertEquals(3, e.apply.taskCount, "the restart for the split also applies the scale-up (225 ops/s)");
    }

    @Test
    public void aSplitCarriesAScaleDownWhoseEvaluationsAreComplete() {
        TaskAutoscalePolicy p = new TaskAutoscalePolicy(new TaskAutoscalePolicy.Settings(1, 16, 100, 16, 60, 2, 3, 5 * MIN, 10 * MIN, 1.0));
        assertEquals(3, p.evaluate(sample(8, 0, 0), sample(8, 2250, 2250), MIN, 0).apply.taskCount);
        // three low evaluations (80, 120, 80 ops/s) inside the 10 min down cooldown, then a split
        assertNull(p.evaluate(sample(8, 2250, 2250), sample(8, 2850, 2850), MIN, MIN).apply);
        assertNull(p.evaluate(sample(8, 2850, 2850), sample(8, 3750, 3750), MIN, 2 * MIN).apply);
        assertNull(p.evaluate(sample(8, 3750, 3750), sample(8, 4350, 4350), MIN, 3 * MIN).apply);
        assertNull(p.evaluate(sample(8, 4350, 4350), sample(9, 4350, 4350), MIN, 4 * MIN).apply);
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(9, 4350, 4350), sample(9, 4350, 4350), MIN, 5 * MIN);
        assertEquals(2, e.apply.taskCount, "sized for the stretch's 120 ops/s peak, not the idle last sample");
    }

    @Test
    public void aRePlaceDoesNotRestartThePendingCooldown() {
        TaskAutoscalePolicy p = new TaskAutoscalePolicy(new TaskAutoscalePolicy.Settings(1, 16, 100, 16, 60, 5, 3, 5 * MIN, 10 * MIN, 1.0));
        p.evaluate(sample(8, 0, 0), sample(8, 0, 0), MIN, 0);
        long head = 0;
        TaskAutoscalePolicy.Evaluation last = null;
        int tablets = 8;
        for (int m = 1; m <= 6; m++) {
            if (m == 3) {
                tablets = 9;
            }
            int prev = (m == 3) ? 8 : tablets;
            last = p.evaluate(sample(prev, head, head), sample(tablets, head + 1500, head + 1500), MIN, m * MIN);
            head += 1500;
        }
        assertNotNull(last.apply);
        assertEquals(3, last.apply.taskCount, "scale-up fires at minute 6 after its own cooldown, not 5 min after the re-place");
    }

    @Test
    public void weightedGroupingSpreadsHotTablets() {
        List<HashPartition> parts = new ArrayList<>();
        Map<String, Double> w = new HashMap<>();
        for (int i = 0; i < 8; i++) {
            String id = String.format("t%02d", i);
            parts.add(new HashPartition("table", id, new byte[0], new byte[0]));
            w.put(id, i < 2 ? 100.0 : 1.0);
        }
        List<List<HashPartition>> groups = WeightedTabletGrouping.group(parts, 2, w);
        assertEquals(2, groups.size());
        for (List<HashPartition> g : groups) {
            assertEquals(1, g.stream().filter(p -> w.get(p.getTabletId()) > 50).count(), "one hot tablet per task");
        }
        assertEquals(groups, WeightedTabletGrouping.group(parts, 2, w), "deterministic");
    }
}
