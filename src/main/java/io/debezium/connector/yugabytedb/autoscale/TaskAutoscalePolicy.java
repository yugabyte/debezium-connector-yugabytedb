/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.autoscale;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Decides how many tasks a connector should run and how its tablets are weighted, from two samples
 * of the stream's tablets taken {@code intervalMs} apart. Pure: no I/O, no clock, so it can be
 * tested and replayed.
 */
public class TaskAutoscalePolicy {

    /** Server-side state of one tablet of the stream at one instant. */
    public static final class TabletSample {
        final String tabletId;
        final long latestIndex;
        final long checkpointIndex;

        public TabletSample(String tabletId, long latestIndex, long checkpointIndex) {
            this.tabletId = tabletId;
            this.latestIndex = latestIndex;
            this.checkpointIndex = checkpointIndex;
        }
    }

    /** What the connector runs: the task count and the per-tablet weights used to place tablets. */
    public static final class Plan {
        public final int taskCount;
        public final Map<String, Double> weights;
        public final Set<String> tablets;
        public final String reason;

        Plan(int taskCount, Map<String, Double> weights, Set<String> tablets, String reason) {
            this.taskCount = taskCount;
            this.weights = Collections.unmodifiableMap(weights);
            this.tablets = Collections.unmodifiableSet(tablets);
            this.reason = reason;
        }

        @Override
        public String toString() {
            return taskCount + " task(s) over " + tablets.size() + " tablet(s): " + reason;
        }
    }

    /** Settings, all from the connector config. */
    public static final class Settings {
        final int minTasks;
        final int maxTasks;
        final double targetOpsPerTask;
        final int maxTabletsPerTask;
        final double lagSeconds;
        final int scaleUpEvaluations;
        final int scaleDownEvaluations;
        final long scaleUpCooldownMs;
        final long scaleDownCooldownMs;
        final double scaleDownHeadroom;
        final long lagCooldownMs;
        final double catchUpSeconds;

        /** Without catch-up: a growing backlog adds one task at a time, after the scale-up cooldown. */
        public Settings(int minTasks, int maxTasks, double targetOpsPerTask, int maxTabletsPerTask, double lagSeconds,
                        int scaleUpEvaluations, int scaleDownEvaluations, long scaleUpCooldownMs, long scaleDownCooldownMs,
                        double scaleDownHeadroom) {
            this(minTasks, maxTasks, targetOpsPerTask, maxTabletsPerTask, lagSeconds, scaleUpEvaluations, scaleDownEvaluations,
                    scaleUpCooldownMs, scaleDownCooldownMs, scaleDownHeadroom, scaleUpCooldownMs, Double.POSITIVE_INFINITY);
        }

        public Settings(int minTasks, int maxTasks, double targetOpsPerTask, int maxTabletsPerTask, double lagSeconds,
                        int scaleUpEvaluations, int scaleDownEvaluations, long scaleUpCooldownMs, long scaleDownCooldownMs,
                        double scaleDownHeadroom, long lagCooldownMs, double catchUpSeconds) {
            this.minTasks = Math.max(1, minTasks);
            this.maxTasks = Math.max(this.minTasks, maxTasks);
            this.targetOpsPerTask = targetOpsPerTask;
            this.maxTabletsPerTask = Math.max(1, maxTabletsPerTask);
            this.lagSeconds = lagSeconds;
            this.scaleUpEvaluations = Math.max(1, scaleUpEvaluations);
            this.scaleDownEvaluations = Math.max(1, scaleDownEvaluations);
            this.scaleUpCooldownMs = scaleUpCooldownMs;
            this.scaleDownCooldownMs = scaleDownCooldownMs;
            this.scaleDownHeadroom = Math.min(1.0, Math.max(0.1, scaleDownHeadroom));
            // With catch-up off, a growing backlog waits for the scale-up cooldown, as documented.
            this.lagCooldownMs = catchUpSeconds > 0 ? lagCooldownMs : scaleUpCooldownMs;
            this.catchUpSeconds = catchUpSeconds > 0 ? catchUpSeconds : Double.POSITIVE_INFINITY;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Settings)) {
                return false;
            }
            Settings s = (Settings) o;
            return minTasks == s.minTasks && maxTasks == s.maxTasks && targetOpsPerTask == s.targetOpsPerTask
                    && maxTabletsPerTask == s.maxTabletsPerTask && lagSeconds == s.lagSeconds
                    && scaleUpEvaluations == s.scaleUpEvaluations && scaleDownEvaluations == s.scaleDownEvaluations
                    && scaleUpCooldownMs == s.scaleUpCooldownMs && scaleDownCooldownMs == s.scaleDownCooldownMs
                    && scaleDownHeadroom == s.scaleDownHeadroom && lagCooldownMs == s.lagCooldownMs
                    && catchUpSeconds == s.catchUpSeconds;
        }

        @Override
        public int hashCode() {
            return Objects.hash(minTasks, maxTasks, targetOpsPerTask, maxTabletsPerTask, lagSeconds, scaleUpEvaluations,
                    scaleDownEvaluations, scaleUpCooldownMs, scaleDownCooldownMs, scaleDownHeadroom, lagCooldownMs, catchUpSeconds);
        }
    }

    /** The outcome of one evaluation: the plan to apply, or null to keep the current one. */
    public static final class Evaluation {
        public final int desiredTasks;
        public final double opsPerSecond;
        public final long backlogOps;
        public final boolean lagging;
        public final Plan apply;

        Evaluation(int desiredTasks, double opsPerSecond, long backlogOps, boolean lagging, Plan apply) {
            this.desiredTasks = desiredTasks;
            this.opsPerSecond = opsPerSecond;
            this.backlogOps = backlogOps;
            this.lagging = lagging;
            this.apply = apply;
        }
    }

    private static final long SETTLE_MS = 120_000;

    private final Settings settings;
    private Plan current;
    private long lastChangeMs = Long.MIN_VALUE / 2;
    private long lastRestartMs = Long.MIN_VALUE / 2;
    private int wantUp;
    private int wantDown;
    private long previousBacklog = Long.MAX_VALUE;
    private final Deque<double[]> recentRates = new ArrayDeque<>();
    private final Deque<double[]> recentBacklog = new ArrayDeque<>();
    // (time, ops read per second) since the last change settled: what the current tasks actually carry.
    private final Deque<double[]> recentReadRates = new ArrayDeque<>();
    private double downPeakRate;

    public TaskAutoscalePolicy(Settings settings) {
        this.settings = settings;
    }

    public Plan current() {
        return current;
    }

    /**
     * Called when a new connector instance takes over this policy: growth, drain and read rates are measured
     * from the new instance's samples only, and the tasks may have restarted. The plan and cooldowns stay.
     */
    public void resumed(long nowMs) {
        previousBacklog = Long.MAX_VALUE;
        recentBacklog.clear();
        recentReadRates.clear();
        lastRestartMs = nowMs;
    }

    /**
     * Evaluates two samples taken {@code elapsedMs} apart, at time {@code nowMs}. Returns the plan to
     * apply when the task count or the tablet set should change, else a null {@code apply}.
     */
    public Evaluation evaluate(List<TabletSample> previous, List<TabletSample> latest, long elapsedMs, long nowMs) {
        Map<String, Long> before = new HashMap<>();
        Map<String, Long> checkpointBefore = new HashMap<>();
        Set<String> previousTablets = new TreeSet<>();
        for (TabletSample s : previous) {
            before.put(s.tabletId, s.latestIndex);
            checkpointBefore.put(s.tabletId, s.checkpointIndex);
            previousTablets.add(s.tabletId);
        }
        double readRate = 0;
        List<Long> tabletBacklogs = new ArrayList<>();
        Map<String, Double> rates = new HashMap<>();
        Set<String> tablets = new TreeSet<>();
        double totalRate = 0;
        long backlog = 0;
        double seconds = Math.max(1, elapsedMs) / 1000.0;
        for (TabletSample s : latest) {
            tablets.add(s.tabletId);
            Long b = before.get(s.tabletId);
            // A new tablet (split child) has no earlier sample; it gets the mean rate when placed.
            double rate = b == null ? 0 : Math.max(0, s.latestIndex - b) / seconds;
            if (b != null) {
                rates.put(s.tabletId, rate);
            }
            totalRate += rate;
            backlog += Math.max(0, s.latestIndex - s.checkpointIndex);
            tabletBacklogs.add(Math.max(0, s.latestIndex - s.checkpointIndex));
            Long cb = checkpointBefore.get(s.tabletId);
            if (cb != null) {
                readRate += Math.max(0, s.checkpointIndex - cb) / seconds;
            }
        }

        int demand = (int) Math.ceil(totalRate / settings.targetOpsPerTask);
        int structure = (int) Math.ceil(tablets.size() / (double) settings.maxTabletsPerTask);
        // The stored checkpoint trails by up to an offset-flush interval of the rate at the time, and stops
        // moving on idle tablets until something is committed, so a backlog only counts when it is worth
        // lagSeconds of the highest recent rate (not the current one, which drops right after a burst) and
        // is still growing, or shrinking too slowly (below).
        recentRates.addLast(new double[]{ nowMs, totalRate });
        pruneBefore(recentRates, nowMs - settings.lagSeconds * 1000);
        double recentPeak = recentRates.stream().mapToDouble(r -> r[1]).max().orElse(totalRate);
        boolean largeBacklog = backlog > Math.max(settings.targetOpsPerTask, recentPeak) * settings.lagSeconds;
        boolean growing = backlog > previousBacklog;
        previousBacklog = backlog;
        // A backlog that shrinks, but too slowly to clear within catchUpSeconds (after an outage, or once
        // a scale-up stopped the growth), also counts. The drain rate is taken over the last lagSeconds,
        // because the stored checkpoint moves in steps of an offset flush.
        recentBacklog.addLast(new double[]{ nowMs, backlog });
        pruneBefore(recentBacklog, nowMs - settings.lagSeconds * 1000);
        double[] oldest = recentBacklog.peekFirst();
        double drainSeconds = Math.max(1, nowMs - oldest[0]) / 1000.0;
        double drainRate = (oldest[1] - backlog) / drainSeconds;
        boolean catchUp = !Double.isInfinite(settings.catchUpSeconds);
        // A flat backlog is not draining slowly: on idle tablets the checkpoint just stops until a commit. Nor
        // is one the tasks barely read (a stale checkpoint clearing as stray writes commit): only tasks reading
        // at half the target or more are catching up, and only then can more tasks help.
        boolean busy = current != null && totalRate + drainRate >= 0.5 * settings.targetOpsPerTask * current.taskCount;
        boolean slowDrain = catchUp && !growing && busy && oldest[0] < nowMs
                && drainRate > 0 && backlog / drainRate > settings.catchUpSeconds;
        boolean lagging = largeBacklog && (growing || slowDrain);
        int desired = Math.max(Math.max(demand, structure), settings.minTasks);
        int fromDemand = desired;
        // What the running tasks read per second, measured once the last change has settled (a restart
        // stalls reading for a minute or two). When the backlog is spread across the tasks they are all
        // saturated, so this is their capacity.
        if (current != null && nowMs - lastRestartMs >= SETTLE_MS && tablets.equals(previousTablets)) {
            recentReadRates.addLast(new double[]{ nowMs, readRate });
        }
        while (!recentReadRates.isEmpty() && recentReadRates.peekFirst()[0] < nowMs - settings.lagSeconds * 1000) {
            recentReadRates.removeFirst();
        }
        // A tablet is read by one task. When the backlog sits on fewer tablets than there are tasks, more
        // tasks can't read it faster: a growing backlog then adds one task at a time after the scale-up
        // cooldown, as without catch-up, and a backlog-sized step goes at most one task past those tablets.
        int holding = backlog > 0 ? tabletsHolding(tabletBacklogs, 0.8 * backlog) : 0;
        boolean spread = current != null && backlog > 0 && holding >= current.taskCount;
        if (lagging && current != null && catchUp && spread) {
            // Size for the new writes plus clearing the backlog within catchUpSeconds, in one step. Per task,
            // use what the tasks actually read when it is measured (the target can overstate it), and grow
            // at most to double the current count.
            double perTask = settings.targetOpsPerTask;
            if (recentReadRates.size() >= 3) {
                double measured = recentReadRates.stream().mapToDouble(r -> r[1]).average().orElse(0) / current.taskCount;
                // A rate far below the target means the tasks are stalled or idle, not their capacity.
                if (measured >= 0.25 * settings.targetOpsPerTask) {
                    perTask = Math.min(perTask, measured * 0.8);
                }
            }
            int toDrain = (int) Math.ceil((totalRate + backlog / settings.catchUpSeconds) / perTask);
            toDrain = Math.min(toDrain, Math.max(current.taskCount + 1, Math.min(2 * current.taskCount, holding)));
            desired = Math.max(desired, Math.max(toDrain, growing ? current.taskCount + 1 : current.taskCount));
        }
        else if (lagging && current != null && growing) {
            desired = Math.max(desired, current.taskCount + 1);
        }
        desired = clamp(desired, settings.minTasks, Math.min(settings.maxTasks, Math.max(1, tablets.size())));

        String reason = String.format("%.0f ops/s (demand %d), %d tablets (structure %d), backlog %d ops%s",
                totalRate, demand, tablets.size(), structure, backlog, lagging ? (growing ? ", lagging" : ", catching up") : "");

        if (current == null) {
            return accept(desired, rates, tablets, nowMs, "initial: " + reason, totalRate, backlog, lagging);
        }

        // A split shows up as the parent disappearing before its children appear; act only on new
        // tablets, once two samples in a row agree on the set.
        boolean tabletsChanged = !current.tablets.containsAll(tablets) && tablets.equals(previousTablets);
        // While tablets change, rates are incomplete (a new child has no earlier sample), so this
        // evaluation neither scales up nor down on rate.
        boolean ratesComplete = tablets.equals(previousTablets);
        if (tabletsChanged && structure > current.taskCount) {
            // Splits pushed a task past the tablets one pass can keep in flight: those tasks start without a
            // cooldown. More, for the rate or a backlog, only if the scale-up would apply now anyway.
            int to = clamp(Math.max(structure, settings.minTasks), settings.minTasks,
                    Math.min(settings.maxTasks, Math.max(1, tablets.size())));
            boolean upDue = (wantUp + 1 >= settings.scaleUpEvaluations || lagging)
                    && nowMs - lastChangeMs >= (lagging && spread ? settings.lagCooldownMs : settings.scaleUpCooldownMs);
            if (upDue) {
                to = Math.max(to, desired);
            }
            return accept(to, rates, tablets, nowMs, "tablets changed: " + reason, totalRate, backlog, lagging);
        }
        if (lagging) {
            wantDown = 0;
            downPeakRate = 0;
        }
        if (!ratesComplete && !lagging) {
            return new Evaluation(desired, totalRate, backlog, lagging, null);
        }
        if (desired > current.taskCount) {
            wantUp++;
            wantDown = 0;
            downPeakRate = 0;
            boolean cooled = nowMs - lastChangeMs >= (lagging && spread ? settings.lagCooldownMs : settings.scaleUpCooldownMs);
            if ((wantUp >= settings.scaleUpEvaluations || lagging) && cooled) {
                return accept(desired, rates, tablets, nowMs, "scale up: " + reason, totalRate, backlog, lagging);
            }
        }
        else if (!lagging && downTarget(Math.max(downPeakRate, totalRate), structure, tablets.size()) < current.taskCount) {
            // Scale down only to a count that the peak of the whole low stretch fits with headroom, so a
            // rate hovering at a task boundary doesn't flip the count back and forth. The stretch's first
            // sample can straddle the drop (part of it at the old rate), so it doesn't set the peak.
            if (wantDown > 0) {
                downPeakRate = Math.max(downPeakRate, totalRate);
            }
            wantDown++;
            wantUp = 0;
            boolean cooled = nowMs - lastChangeMs >= settings.scaleDownCooldownMs;
            if (wantDown >= settings.scaleDownEvaluations && cooled) {
                // Straight to the target: every reconfiguration restarts all tasks and replays since the last offset commit.
                int to = downTarget(stretchPeak(totalRate), structure, tablets.size());
                return accept(to, rates, tablets, nowMs, "scale down: " + reason, totalRate, backlog, lagging);
            }
        }
        else {
            wantUp = 0;
            wantDown = 0;
            downPeakRate = 0;
        }
        if (tabletsChanged) {
            // Split children start in their parent's task, so the tasks restart anyway; a scale whose
            // evaluations are complete rides along.
            int to = current.taskCount;
            if (desired > current.taskCount && (wantUp >= settings.scaleUpEvaluations || lagging)) {
                to = desired;
                // A backlog-sized step still waits for its cooldown (had it passed, the scale-up above would have
                // applied it), so splits under load can't double the count again before the last step is measured.
                if (lagging) {
                    to = clamp(Math.max(fromDemand, Math.min(desired, current.taskCount + 1)), settings.minTasks,
                            Math.min(settings.maxTasks, Math.max(1, tablets.size())));
                }
            }
            else if (wantDown >= settings.scaleDownEvaluations) {
                to = downTarget(stretchPeak(totalRate), structure, tablets.size());
            }
            if (to != current.taskCount) {
                return accept(to, rates, tablets, nowMs, "tablets changed: " + reason, totalRate, backlog, lagging);
            }
            int up = wantUp;
            int down = wantDown;
            double peak = downPeakRate;
            long last = lastChangeMs;
            Evaluation e = accept(to, rates, tablets, nowMs, "tablets changed: " + reason, totalRate, backlog, lagging);
            wantUp = up;
            wantDown = down;
            downPeakRate = peak;
            // A re-place at the same count doesn't restart the cooldown of the pending scale.
            lastChangeMs = last;
            return e;
        }
        return new Evaluation(desired, totalRate, backlog, lagging, null);
    }

    private Evaluation accept(int tasks, Map<String, Double> rates, Set<String> tablets, long nowMs, String reason,
                              double totalRate, long backlog, boolean lagging) {
        // Every accepted plan restarts the tasks, including a re-place at the same count.
        recentReadRates.clear();
        lastRestartMs = nowMs;
        current = new Plan(tasks, rates, tablets, reason);
        lastChangeMs = nowMs;
        wantUp = 0;
        wantDown = 0;
        downPeakRate = 0;
        return new Evaluation(tasks, totalRate, backlog, lagging, current);
    }

    private static int tabletsHolding(List<Long> backlogs, double share) {
        List<Long> sorted = new ArrayList<>(backlogs);
        sorted.sort(Collections.reverseOrder());
        long sum = 0;
        int n = 0;
        for (long b : sorted) {
            if (sum >= share) {
                break;
            }
            sum += b;
            n++;
        }
        return Math.max(1, n);
    }

    private double stretchPeak(double latestRate) {
        return wantDown <= 1 ? latestRate : downPeakRate;
    }

    private int downTarget(double peakRate, int structure, int tablets) {
        int demand = (int) Math.ceil(peakRate / (settings.targetOpsPerTask * settings.scaleDownHeadroom));
        return clamp(Math.max(Math.max(demand, structure), settings.minTasks), settings.minTasks,
                Math.min(settings.maxTasks, Math.max(1, tablets)));
    }

    /** Drops samples before {@code cutoffMs} but the newest of them, so the window spans at least to the cutoff. */
    private static void pruneBefore(Deque<double[]> samples, double cutoffMs) {
        while (samples.size() > 1) {
            Iterator<double[]> it = samples.iterator();
            it.next();
            if (it.next()[0] > cutoffMs) {
                break;
            }
            samples.removeFirst();
        }
    }

    private static int clamp(int v, int lo, int hi) {
        return Math.max(lo, Math.min(hi, v));
    }
}
