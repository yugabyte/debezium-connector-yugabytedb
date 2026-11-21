/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.autoscale;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

        public Settings(int minTasks, int maxTasks, double targetOpsPerTask, int maxTabletsPerTask, double lagSeconds,
                        int scaleUpEvaluations, int scaleDownEvaluations, long scaleUpCooldownMs, long scaleDownCooldownMs,
                        double scaleDownHeadroom) {
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

    private final Settings settings;
    private Plan current;
    private long lastChangeMs = Long.MIN_VALUE / 2;
    private int wantUp;
    private int wantDown;
    private long previousBacklog = Long.MAX_VALUE;
    private final Deque<double[]> recentRates = new ArrayDeque<>();
    private double downPeakRate;

    public TaskAutoscalePolicy(Settings settings) {
        this.settings = settings;
    }

    public Plan current() {
        return current;
    }

    /**
     * Evaluates two samples taken {@code elapsedMs} apart, at time {@code nowMs}. Returns the plan to
     * apply when the task count or the tablet set should change, else a null {@code apply}.
     */
    public Evaluation evaluate(List<TabletSample> previous, List<TabletSample> latest, long elapsedMs, long nowMs) {
        Map<String, Long> before = new HashMap<>();
        Set<String> previousTablets = new TreeSet<>();
        for (TabletSample s : previous) {
            before.put(s.tabletId, s.latestIndex);
            previousTablets.add(s.tabletId);
        }
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
        }

        int demand = (int) Math.ceil(totalRate / settings.targetOpsPerTask);
        int structure = (int) Math.ceil(tablets.size() / (double) settings.maxTabletsPerTask);
        // The stored checkpoint trails by up to an offset-flush interval of the rate at the time, and stops
        // moving on idle tablets until something is committed, so a backlog only counts when it is worth
        // lagSeconds of the highest recent rate (not the current one, which drops right after a burst) and
        // is still growing.
        recentRates.addLast(new double[]{ nowMs, totalRate });
        while (recentRates.peekFirst()[0] < nowMs - settings.lagSeconds * 1000) {
            recentRates.removeFirst();
        }
        double recentPeak = recentRates.stream().mapToDouble(r -> r[1]).max().orElse(totalRate);
        boolean lagging = backlog > Math.max(settings.targetOpsPerTask, recentPeak) * settings.lagSeconds
                && backlog > previousBacklog;
        previousBacklog = backlog;
        int desired = Math.max(Math.max(demand, structure), settings.minTasks);
        if (lagging && current != null) {
            desired = Math.max(desired, current.taskCount + 1);
        }
        desired = clamp(desired, settings.minTasks, Math.min(settings.maxTasks, Math.max(1, tablets.size())));

        String reason = String.format("%.0f ops/s (demand %d), %d tablets (structure %d), backlog %d ops%s",
                totalRate, demand, tablets.size(), structure, backlog, lagging ? ", lagging" : "");

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
            // Splits pushed a task past the tablets one pass can keep in flight; don't wait for a cooldown.
            return accept(desired, rates, tablets, nowMs, "tablets changed: " + reason, totalRate, backlog, lagging);
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
            boolean cooled = nowMs - lastChangeMs >= settings.scaleUpCooldownMs;
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
        current = new Plan(tasks, rates, tablets, reason);
        lastChangeMs = nowMs;
        wantUp = 0;
        wantDown = 0;
        downPeakRate = 0;
        return new Evaluation(tasks, totalRate, backlog, lagging, current);
    }

    private double stretchPeak(double latestRate) {
        return wantDown <= 1 ? latestRate : downPeakRate;
    }

    private int downTarget(double peakRate, int structure, int tablets) {
        int demand = (int) Math.ceil(peakRate / (settings.targetOpsPerTask * settings.scaleDownHeadroom));
        return clamp(Math.max(Math.max(demand, structure), settings.minTasks), settings.minTasks,
                Math.min(settings.maxTasks, Math.max(1, tablets)));
    }

    private static int clamp(int v, int lo, int hi) {
        return Math.max(lo, Math.min(hi, v));
    }
}
