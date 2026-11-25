/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.autoscale;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;

import io.debezium.connector.yugabytedb.connection.HashPartition;

/**
 * Places tablets on tasks by weight (longest-processing-time first), so busy tablets are spread
 * instead of sitting next to each other in hash order. Deterministic for the same input, so asking
 * Kafka Connect for task configs again does not restart tasks.
 */
public final class WeightedTabletGrouping {

    private WeightedTabletGrouping() {
    }

    public static List<List<HashPartition>> group(List<HashPartition> partitions, int groups, Map<String, Double> weights) {
        return group(partitions, groups, weights, Integer.MAX_VALUE);
    }

    /** As above, but no task gets more than {@code maxTabletsPerTask} tablets while the others can take them. */
    public static List<List<HashPartition>> group(List<HashPartition> partitions, int groups, Map<String, Double> weights,
                                                  int maxTabletsPerTask) {
        // Partitions of one tablet (colocated tables) always go to the same task.
        Map<String, List<HashPartition>> byTablet = new TreeMap<>();
        for (HashPartition p : partitions) {
            byTablet.computeIfAbsent(p.getTabletId(), k -> new ArrayList<>()).add(p);
        }
        int n = Math.max(1, Math.min(groups, byTablet.size()));
        double mean = weights.values().stream().mapToDouble(Double::doubleValue).average().orElse(0);
        // Unknown tablets (new split children) weigh the mean; a small floor keeps idle tablets spread by count.
        Map<String, Double> w = new LinkedHashMap<>();
        for (String tablet : byTablet.keySet()) {
            w.put(tablet, weights.getOrDefault(tablet, mean) + 1e-3);
        }
        List<String> order = new ArrayList<>(byTablet.keySet());
        order.sort(Comparator.<String>comparingDouble(w::get).reversed().thenComparing(Comparator.naturalOrder()));

        List<List<HashPartition>> result = new ArrayList<>(n);
        double[] load = new double[n];
        int[] tabletCount = new int[n];
        // With too few groups for the cap (tasks.max too low), spread evenly instead of failing.
        int cap = Math.max(maxTabletsPerTask, (int) Math.ceil(byTablet.size() / (double) n));
        PriorityQueue<Integer> lightest = new PriorityQueue<>(
                Comparator.<Integer>comparingDouble(i -> load[i]).thenComparingInt(i -> result.get(i).size()).thenComparingInt(i -> i));
        for (int i = 0; i < n; i++) {
            result.add(new ArrayList<>());
            lightest.add(i);
        }
        for (String tablet : order) {
            int g = lightest.poll();
            while (tabletCount[g] >= cap) {
                g = lightest.poll();
            }
            result.get(g).addAll(byTablet.get(tablet));
            load[g] += w.get(tablet);
            tabletCount[g]++;
            lightest.add(g);
        }
        return result;
    }
}
