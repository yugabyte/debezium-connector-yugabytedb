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
    public void aSplitPastTheTabletCapStartsOnlyTheTasksTheTabletsNeedDuringABurst() {
        TaskAutoscalePolicy p = new TaskAutoscalePolicy(new TaskAutoscalePolicy.Settings(1, 16, 100, 8, 60, 20, 3, 5 * MIN, 10 * MIN, 1.0));
        assertEquals(2, p.evaluate(sample(16, 0, 0), sample(16, 0, 0), MIN, 0).apply.taskCount);
        assertNull(p.evaluate(sample(16, 0, 0), sample(17, 0, 0), MIN, MIN).apply);
        // 17 tablets x 3,000 ops in 60 s = 850 ops/s, one evaluation of the 20 a rate scale-up needs.
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(17, 0, 0), sample(17, 3000, 3000), MIN, 2 * MIN);
        assertEquals(3, e.apply.taskCount, "17 tablets at 8 per task; the burst's 9 waits for its evaluations");
    }

    @Test
    public void aSplitPastTheTabletCapCarriesAScaleUpThatIsDue() {
        TaskAutoscalePolicy p = policy(16);
        p.evaluate(sample(8, 0, 0), sample(8, 0, 0), MIN, 0);
        assertNull(p.evaluate(sample(8, 0, 0), sample(8, 1500, 1500), MIN, 6 * MIN).apply, "200 ops/s, one evaluation of two");
        assertNull(p.evaluate(sample(8, 1500, 1500), sample(9, 3000, 3000), MIN, 7 * MIN).apply, "wait for a second sample to agree");
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(9, 3000, 3000), sample(9, 4500, 4500), MIN, 8 * MIN);
        assertEquals(3, e.apply.taskCount, "225 ops/s after two evaluations and the cooldown, above the 2 the tablets need");
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

    private static TaskAutoscalePolicy backlogPolicy() {
        // 8 tablets per task, target 100 ops/s, lag after 60 s of rate; up after 2 evaluations and a 30 min
        // cooldown, but a backlog only waits 1 min and is sized to clear within 600 s.
        return new TaskAutoscalePolicy(new TaskAutoscalePolicy.Settings(1, 16, 100, 8, 60, 2, 3, 30 * MIN, 60 * MIN, 1.0, MIN, 600));
    }

    @Test
    public void aGrowingBacklogWaitsForTheLagCooldownThenIsSizedToClear() {
        // At least 2 tasks; 800,000 ops behind and still growing slowly, with almost no new writes.
        TaskAutoscalePolicy p = new TaskAutoscalePolicy(new TaskAutoscalePolicy.Settings(2, 16, 100, 8, 60, 2, 3, 30 * MIN, 60 * MIN, 1.0, MIN, 600));
        assertEquals(2, p.evaluate(sample(8, 100_000, 0), sample(8, 100_080, 0), MIN, 0).apply.taskCount);
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(8, 100_080, 0), sample(8, 100_120, 0), MIN / 2, MIN / 2);
        assertTrue(e.lagging);
        assertNull(e.apply, "within the 1 min lag cooldown");
        e = p.evaluate(sample(8, 100_120, 0), sample(8, 100_140, 0), MIN / 2, MIN);
        assertTrue(e.lagging);
        assertEquals(4, e.apply.taskCount, "(5 + 801,120 / 600) / 100 = 14 to clear the backlog, capped at double the 2 running");
    }

    @Test
    public void aBacklogThatClearsTooSlowlyAddsTasks() {
        TaskAutoscalePolicy p = backlogPolicy();
        assertEquals(1, p.evaluate(sample(8, 100_000, 0), sample(8, 100_000, 0), MIN, 0).apply.taskCount); // after an outage
        // No new writes; the backlog (800,000 ops) shrinks by 80 ops/s: 10,000 s to clear, more than 600 s.
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(8, 100_000, 0), sample(8, 100_000, 600), MIN, MIN);
        assertTrue(e.lagging, "catching up");
        assertEquals(2, e.apply.taskCount, "(0 + 795,200 / 600) / 100 = 14, but at most double the current count");
    }

    @Test
    public void aBacklogThatClearsTooSlowlyCountsWhenSamplesAreFurtherApartThanTheLagWindow() {
        TaskAutoscalePolicy p = backlogPolicy();
        assertEquals(1, p.evaluate(sample(8, 100_000, 0), sample(8, 100_000, 0), MIN, 0).apply.taskCount);
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(8, 100_000, 0), sample(8, 100_000, 600), MIN + 1, MIN + 1);
        assertTrue(e.lagging, "catching up: the drain rate spans the two samples, 1 ms more than the 60 s window");
        assertEquals(2, e.apply.taskCount);
    }

    @Test
    public void aStaleCheckpointClearingTabletByTabletIsNotCatchingUp() {
        TaskAutoscalePolicy p = backlogPolicy();
        assertEquals(12, p.evaluate(sample(16, 0, 0), sample(16, 4500, 0), MIN, 0).apply.taskCount); // 1,200 ops/s
        // No writes; one stale tablet's checkpoint catches up each minute (72,000 behind, shrinking by 75 ops/s:
        // up to 15 minutes to clear, longer than the 600 s catch-up time).
        long[] checkpoints = new long[16];
        for (int m = 1; m <= 12; m++) {
            List<TaskAutoscalePolicy.TabletSample> prev = new ArrayList<>();
            List<TaskAutoscalePolicy.TabletSample> latest = new ArrayList<>();
            for (int i = 0; i < 16; i++) {
                prev.add(new TaskAutoscalePolicy.TabletSample(String.format("t%02d", i), 4500, checkpoints[i]));
            }
            checkpoints[m - 1] = 4500;
            for (int i = 0; i < 16; i++) {
                latest.add(new TaskAutoscalePolicy.TabletSample(String.format("t%02d", i), 4500, checkpoints[i]));
            }
            TaskAutoscalePolicy.Evaluation e = p.evaluate(prev, latest, MIN, m * MIN);
            assertFalse(e.lagging, "minute " + m + ": the tasks read almost nothing, so more of them can't help");
        }
    }

    @Test
    public void aSplitPastTheTabletFloorWhileBehindWaitsForTheLagCooldownToSize() {
        TaskAutoscalePolicy p = new TaskAutoscalePolicy(new TaskAutoscalePolicy.Settings(2, 16, 100, 8, 60, 2, 3, 30 * MIN, 60 * MIN, 1.0, MIN, 600));
        assertEquals(2, p.evaluate(sample(16, 100_000, 0), sample(16, 100_080, 0), MIN, 0).apply.taskCount);
        assertNull(p.evaluate(sample(16, 100_080, 0), sample(17, 100_100, 0), MIN / 3, MIN / 3).apply);
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(17, 100_100, 0), sample(17, 100_120, 0), MIN / 3, 2 * MIN / 3);
        assertTrue(e.lagging);
        assertEquals(3, e.apply.taskCount, "the split needs 3 (17 tablets at 8 per task) at once; the sized step (4) waits");
    }

    @Test
    public void withCatchUpOffAGrowingBacklogWaitsForTheScaleUpCooldown() {
        // Catch-up off (0): the 1 min lag cooldown doesn't apply, the 30 min scale-up cooldown does.
        TaskAutoscalePolicy p = new TaskAutoscalePolicy(new TaskAutoscalePolicy.Settings(2, 16, 100, 8, 60, 2, 3, 30 * MIN, 60 * MIN, 1.0, MIN, 0));
        assertEquals(2, p.evaluate(sample(8, 100_000, 0), sample(8, 100_080, 0), MIN, 0).apply.taskCount);
        long head = 100_080;
        for (int m = 1; m < 30; m++) {
            TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(8, head, 0), sample(8, head + 20, 0), MIN, m * MIN);
            head += 20;
            assertTrue(e.lagging);
            assertNull(e.apply, "minute " + m);
        }
        assertEquals(3, p.evaluate(sample(8, head, 0), sample(8, head + 20, 0), MIN, 30 * MIN).apply.taskCount);
    }

    @Test
    public void aBacklogThatClearsInTimeAddsNothing() {
        TaskAutoscalePolicy p = backlogPolicy();
        assertEquals(1, p.evaluate(sample(8, 10_000, 0), sample(8, 10_000, 0), MIN, 0).apply.taskCount);
        // Still 8,000 ops behind (above the 6,000 that counts as large), but it shrank by 1,200 ops/s:
        // about 7 s to clear, well within 600 s.
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(8, 10_000, 0), sample(8, 10_000, 9000), MIN, MIN);
        assertFalse(e.lagging);
        assertNull(e.apply);
    }

    @Test
    public void aBacklogIsSizedByWhatTheTasksActuallyRead() {
        // 16 tablets, 1 per task at most 16; the target claims 1,000 ops/s per task.
        TaskAutoscalePolicy p = new TaskAutoscalePolicy(new TaskAutoscalePolicy.Settings(4, 16, 1000, 16, 180, 2, 3, 30 * MIN, 60 * MIN, 1.0, 5 * MIN, 600));
        assertEquals(4, p.evaluate(sample(16, 0, 0), sample(16, 0, 0), MIN, 0).apply.taskCount);
        // 3,200 ops/s written (200 per tablet), but 4 tasks read only 1,600 (100 per tablet): 400 per task, not 1,000.
        long head = 0, read = 0, t = 0;
        TaskAutoscalePolicy.Evaluation e = null;
        for (int i = 1; i <= 12; i++) {
            List<TaskAutoscalePolicy.TabletSample> prev = sample(16, head, read);
            head += 12_000; read += 6000; t += MIN;
            e = p.evaluate(prev, sample(16, head, read), MIN, t);
            if (e.apply != null) {
                break;
            }
        }
        assertNotNull(e.apply);
        assertTrue(e.lagging);
        assertEquals(8, e.apply.taskCount, "the measured 400 ops/s per task calls for more than double; capped at 8");
    }

    @Test
    public void aStuckCheckpointOnIdleTabletsIsNotLagWithCatchUpOn() {
        TaskAutoscalePolicy p = backlogPolicy();
        assertEquals(6, p.evaluate(sample(8, 0, 0), sample(8, 4500, 0), MIN, 0).apply.taskCount); // 600 ops/s
        // Idle from now on; the checkpoint stays where it was, 36,000 ops behind, so the backlog is flat.
        TaskAutoscalePolicy.Evaluation e = null;
        for (int m = 1; m <= 70 && (e == null || e.apply == null); m++) {
            e = p.evaluate(sample(8, 4500, 0), sample(8, 4500, 0), MIN, m * MIN);
            assertFalse(e.lagging, "minute " + m);
        }
        assertEquals(1, e.apply.taskCount, "scales down once the down cooldown has passed");
    }

    private static List<TaskAutoscalePolicy.TabletSample> oneHotTablet(long hotIndex, long others) {
        List<TaskAutoscalePolicy.TabletSample> s = new ArrayList<>();
        s.add(new TaskAutoscalePolicy.TabletSample("t00", hotIndex, 0));
        for (int i = 1; i < 8; i++) {
            s.add(new TaskAutoscalePolicy.TabletSample(String.format("t%02d", i), others, others));
        }
        return s;
    }

    @Test
    public void aBacklogOnFewerTabletsThanTasksAddsOneTaskPerScaleUpCooldown() {
        TaskAutoscalePolicy p = new TaskAutoscalePolicy(new TaskAutoscalePolicy.Settings(2, 16, 100, 8, 60, 2, 3, 30 * MIN, 60 * MIN, 1.0, MIN, 600));
        assertEquals(2, p.evaluate(oneHotTablet(300_000, 0), oneHotTablet(300_000, 0), MIN, 0).apply.taskCount);
        // Only t00 is behind (and slowly growing); the other tablets are read as fast as they are written.
        long hot = 300_000, other = 0, t = 0;
        TaskAutoscalePolicy.Evaluation e = null;
        List<Integer> applied = new ArrayList<>();
        List<Long> at = new ArrayList<>();
        for (int i = 1; i <= 140; i++) {
            List<TaskAutoscalePolicy.TabletSample> prev = oneHotTablet(hot, other);
            hot += 30; other += 30; t += MIN / 2;
            e = p.evaluate(prev, oneHotTablet(hot, other), MIN / 2, t);
            assertTrue(e.lagging);
            if (e.apply != null) {
                applied.add(e.apply.taskCount);
                at.add(t / MIN);
            }
        }
        // Sizing would say 6 within a minute, but a backlog on one tablet can only be read by one task:
        // no backlog-sized step, and one task per 30 min scale-up cooldown, not per 1 min lag cooldown.
        assertEquals(List.of(3, 4), applied);
        assertEquals(List.of(30L, 60L), at);
    }


    @Test
    public void aSplitWhileBehindAddsOneTaskWithinTheLagCooldown() {
        TaskAutoscalePolicy p = new TaskAutoscalePolicy(new TaskAutoscalePolicy.Settings(2, 16, 100, 8, 60, 2, 3, 30 * MIN, 60 * MIN, 1.0, MIN, 600));
        assertEquals(2, p.evaluate(sample(8, 100_000, 0), sample(8, 100_080, 0), MIN, 0).apply.taskCount);
        // A ninth tablet appears while 800,000 ops behind; the set is stable on the next sample.
        assertNull(p.evaluate(sample(8, 100_080, 0), sample(9, 100_100, 0), MIN / 3, MIN / 3).apply);
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(9, 100_100, 0), sample(9, 100_120, 0), MIN / 3, 2 * MIN / 3);
        assertTrue(e.lagging);
        assertEquals(3, e.apply.taskCount, "the split re-places at once, but the backlog-sized step (4) waits for the lag cooldown");
    }

    private static List<TaskAutoscalePolicy.TabletSample> eightOfTenBehind(long index) {
        List<TaskAutoscalePolicy.TabletSample> s = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            s.add(new TaskAutoscalePolicy.TabletSample(String.format("t%02d", i), index, i < 8 ? 0 : index));
        }
        return s;
    }

    @Test
    public void aSizedStepGoesAtMostToTheTabletsHoldingTheBacklog() {
        TaskAutoscalePolicy p = new TaskAutoscalePolicy(new TaskAutoscalePolicy.Settings(4, 16, 100, 8, 60, 2, 3, 30 * MIN, 60 * MIN, 1.0, MIN, 600));
        assertEquals(4, p.evaluate(eightOfTenBehind(100_000), eightOfTenBehind(100_080), MIN, 0).apply.taskCount);
        p.evaluate(eightOfTenBehind(100_080), eightOfTenBehind(100_120), MIN / 2, MIN / 2);
        TaskAutoscalePolicy.Evaluation e = p.evaluate(eightOfTenBehind(100_120), eightOfTenBehind(100_140), MIN / 2, MIN);
        assertTrue(e.lagging);
        assertEquals(7, e.apply.taskCount, "sizing says 14 (8 at double), but 7 tablets hold 80% of the backlog");
    }

    @Test
    public void aResumedPolicyMeasuresTheBacklogFromItsOwnSamples() {
        TaskAutoscalePolicy p = backlogPolicy();
        assertEquals(1, p.evaluate(sample(8, 0, 0), sample(8, 60, 60), MIN, 0).apply.taskCount);
        p.evaluate(sample(8, 60, 60), sample(8, 120, 120), MIN, MIN);
        // Stopped for 4 minutes while writes went on: 8 × 930 ops behind on resume, above the 6,000 that counts as large.
        p.resumed(5 * MIN);
        TaskAutoscalePolicy.Evaluation e = p.evaluate(sample(8, 1120, 120), sample(8, 1130, 200), MIN, 6 * MIN);
        assertFalse(e.lagging, "the backlog built up while stopped is not growth");
        assertNull(e.apply);
    }

    @Test
    public void settingsCompareByValue() {
        assertEquals(new TaskAutoscalePolicy.Settings(1, 16, 100, 8, 60, 2, 3, MIN, MIN, 1.0, MIN, 600),
                new TaskAutoscalePolicy.Settings(1, 16, 100, 8, 60, 2, 3, MIN, MIN, 1.0, MIN, 600));
        assertNotEquals(new TaskAutoscalePolicy.Settings(1, 16, 100, 8, 60, 2, 3, MIN, MIN, 1.0, MIN, 600),
                new TaskAutoscalePolicy.Settings(1, 16, 200, 8, 60, 2, 3, MIN, MIN, 1.0, MIN, 600));
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
