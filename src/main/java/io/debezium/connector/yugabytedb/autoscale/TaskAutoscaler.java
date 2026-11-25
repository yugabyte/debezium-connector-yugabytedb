/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.autoscale;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.Opid.OpIdPB;
import org.yb.cdc.CdcService.TabletCheckpointPair;
import org.yb.client.GetLatestEntryOpIdResponse;
import org.yb.client.GetTabletListToPollForCDCResponse;
import org.yb.client.LocatedTablet;
import org.yb.client.YBClient;
import org.yb.client.YBTable;

import com.google.common.net.HostAndPort;

import io.debezium.connector.yugabytedb.YBClientUtils;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig;

/**
 * Runs in the connector (not the tasks). Every interval it reads, from the master and tservers, the
 * tablets of the stream with their latest WAL index and stored CDC checkpoint, asks
 * {@link TaskAutoscalePolicy} whether the task count or tablet placement should change, and if so
 * asks Kafka Connect to reconfigure the tasks. Tasks are restarted by Kafka Connect exactly as for a
 * {@code tasks.max} change.
 */
public class TaskAutoscaler extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskAutoscaler.class);

    private final YugabyteDBConnectorConfig config;
    private final ConnectorContext context;
    private volatile List<String> tableIds;
    private final TaskAutoscalePolicy policy;
    private final long intervalMs;
    private final long startupSampleMs;
    private final CountDownLatch initialized = new CountDownLatch(1);
    private final CountDownLatch shutdown = new CountDownLatch(1);
    private static final int STARTUP_ATTEMPTS = 6;
    // Long enough for a restart or a short pause; short enough that a connector returning from another worker,
    // where it may have rescaled, rebuilds from current rates instead of resuming an old plan.
    private static final long RETAIN_NANOS = TimeUnit.MINUTES.toNanos(5);
    // A connector instance restarted on the same worker (REST restart, pause and resume, a rebalance that keeps
    // it here) resumes the previous instance's plan, so its task configs come out unchanged and Kafka Connect
    // doesn't restart the tasks. On another worker the plan is rebuilt from samples.
    private static final Map<String, Retained> RETAINED = new ConcurrentHashMap<>();
    // The newest instance per connector. Connect may start the next instance before an old thread has exited;
    // only the newest stores, so a slow old thread can't overwrite a newer instance's state.
    private static final Map<String, TaskAutoscaler> LATEST = new ConcurrentHashMap<>();

    private static final class Retained {
        final TaskAutoscalePolicy.Settings settings;
        final String source;
        final TaskAutoscalePolicy policy;
        final TaskAutoscalePolicy.Plan plan;
        final long storedAtNanos = System.nanoTime();

        Retained(TaskAutoscalePolicy.Settings settings, String source, TaskAutoscalePolicy policy, TaskAutoscalePolicy.Plan plan) {
            this.settings = settings;
            this.source = source;
            this.policy = policy;
            this.plan = plan;
        }
    }

    private volatile TaskAutoscalePolicy.Plan plan;
    private volatile TaskAutoscalePolicy.Plan consumed;
    private volatile boolean taskConfigsWaiting;
    private TaskAutoscalePolicy.Plan requested;
    private volatile boolean unavailable;
    private int attemptsWithoutPlan;

    private final String connectorName;
    private final TaskAutoscalePolicy.Settings settings;

    public TaskAutoscaler(String connectorName, YugabyteDBConnectorConfig config, ConnectorContext context, List<String> tableIds,
                          TaskAutoscalePolicy.Settings settings, long intervalMs, long startupSampleMs) {
        super("yb-task-autoscaler-" + connectorName);
        this.connectorName = connectorName;
        setDaemon(true);
        this.config = config;
        this.context = context;
        this.tableIds = new ArrayList<>(tableIds);
        this.settings = settings;
        LATEST.put(connectorName, this);
        Retained r = RETAINED.remove(connectorName);
        if (r != null && r.settings.equals(settings) && r.source.equals(source(tableIds))
                && System.nanoTime() - r.storedAtNanos < RETAIN_NANOS) {
            this.policy = r.policy;
            this.policy.resumed(TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
            this.plan = r.plan;
            initialized.countDown();
            LOGGER.info("Task autoscaler [{}]: resuming the previous instance's plan, {}", connectorName, r.plan);
        }
        else {
            this.policy = new TaskAutoscalePolicy(settings);
        }
        this.intervalMs = intervalMs;
        this.startupSampleMs = startupSampleMs;
    }

    /**
     * The plan to build task configs from. Only the first call waits (up to {@code timeoutMs}) for a first plan;
     * later calls return at once, since taskConfigs runs on the herder thread. A plan that arrives later
     * requests a reconfiguration itself.
     */
    public TaskAutoscalePolicy.Plan plan(long timeoutMs) {
        taskConfigsWaiting = true;
        try {
            initialized.await(timeoutMs, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            initialized.countDown();
            consumed = plan;
            taskConfigsWaiting = false;
        }
        return consumed;
    }

    /** Tables to sample; the connector updates it when task configs are rebuilt (new tables in the stream). */
    public void setTableIds(Collection<String> tableIds) {
        this.tableIds = new ArrayList<>(tableIds);
    }

    /** True once sampling has failed to produce a plan for a while, or the thread has stopped. */
    public boolean unavailable() {
        return unavailable;
    }

    public void shutdown() {
        shutdown.countDown();
    }

    // The tasks already run the count taskConfigs uses without a plan, so nothing is reconfigured.
    private void markUnavailable(String why) {
        if (unavailable) {
            return;
        }
        unavailable = true;
        LOGGER.warn("Task autoscaler [{}]: {}; keeping the tablet cap's minimum task count until a plan arrives", connectorName, why);
    }

    @Override
    public void run() {
        try (YBClient client = YBClientUtils.getYbClient(config)) {
            List<TaskAutoscalePolicy.TabletSample> previous = null;
            long previousAt = 0;
            long wait = 0;
            while (!shutdown.await(wait, TimeUnit.MILLISECONDS)) {
                try {
                    List<TaskAutoscalePolicy.TabletSample> latest = sample(client);
                    // Timestamp after sample(): the WAL indexes it reads last are what the rate is measured from.
                    long now = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
                    if (previous != null) {
                        TaskAutoscalePolicy.Evaluation e = policy.evaluate(previous, latest, now - previousAt, now);
                        LOGGER.info("Task autoscaler [{}]: desired {} task(s), {} ops/s, backlog {} ops{}; current {}",
                                connectorName, e.desiredTasks, Math.round(e.opsPerSecond), e.backlogOps, e.lagging ? " (behind)" : "",
                                policy.current());
                        if (e.apply != null) {
                            plan = e.apply;
                            unavailable = false;
                            initialized.countDown();
                        }
                        // Ask again for any plan task configs haven't been built from, including a first plan
                        // that arrived after taskConfigs stopped waiting for it.
                        TaskAutoscalePolicy.Plan p = plan;
                        if (p != null && p != consumed && p != requested && !taskConfigsWaiting && shutdown.getCount() > 0) {
                            LOGGER.info("Task autoscaler [{}]: reconfiguring tasks, {}", connectorName, p);
                            requested = p;
                            context.requestTaskReconfiguration();
                        }
                    }
                    previous = latest;
                    previousAt = now;
                }
                catch (Exception e) {
                    LOGGER.warn("Task autoscaler [{}]: evaluation failed; keeping the current plan", connectorName, e);
                }
                if (plan == null && ++attemptsWithoutPlan >= STARTUP_ATTEMPTS) {
                    markUnavailable("no plan after " + attemptsWithoutPlan + " samples");
                }
                wait = plan == null ? startupSampleMs : intervalMs;
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            LOGGER.error("Task autoscaler [{}] stopped", connectorName, e);
        }
        finally {
            if (plan == null && shutdown.getCount() > 0) {
                markUnavailable("stopped");
            }
            // Atomic with the newest-instance check: a new instance's LATEST.put waits for this, then finds the state.
            LATEST.computeIfPresent(connectorName, (name, newest) -> {
                if (newest != this) {
                    return newest;
                }
                if (shutdown.getCount() == 0 && plan != null && plan == policy.current()) {
                    long now = System.nanoTime();
                    RETAINED.values().removeIf(r -> now - r.storedAtNanos >= RETAIN_NANOS);
                    RETAINED.put(name, new Retained(settings, source(tableIds), policy, plan));
                }
                return null;
            });
            initialized.countDown();
        }
    }

    /** The stream and its current tables: a resumed policy's backlog and rate history only fit the same source. */
    private String source(Collection<String> tables) {
        return config.streamId() + "|" + String.join(",", new TreeSet<>(tables));
    }

    private List<TaskAutoscalePolicy.TabletSample> sample(YBClient client) throws Exception {
        Map<String, Long> checkpoints = new HashMap<>();
        // A colocated tablet belongs to every table on it; sample it once.
        Map<HostAndPort, Set<String>> byLeader = new LinkedHashMap<>();
        Set<String> seen = new HashSet<>();
        for (String tableId : tableIds) {
            YBTable table = client.openTableByUUID(tableId);
            GetTabletListToPollForCDCResponse resp = YBClientUtils.getTabletListToPollForCDCWithRetry(client, table, tableId, config);
            for (TabletCheckpointPair pair : resp.getTabletCheckpointPairList()) {
                checkpoints.put(pair.getTabletLocations().getTabletId().toStringUtf8(), pair.getCdcSdkCheckpoint().getIndex());
            }
            for (LocatedTablet t : table.getTabletsLocations(config.adminOperationTimeoutMs())) {
                String tabletId = new String(t.getTabletId());
                if (!checkpoints.containsKey(tabletId) || !seen.add(tabletId)) {
                    continue;
                }
                LocatedTablet.Replica leader = t.getLeaderReplica() != null ? t.getLeaderReplica() : t.getReplicas().get(0);
                byLeader.computeIfAbsent(HostAndPort.fromParts(leader.getRpcHost(), leader.getRpcPort()), k -> new LinkedHashSet<>())
                        .add(tabletId);
            }
        }
        List<TaskAutoscalePolicy.TabletSample> samples = new ArrayList<>();
        for (Map.Entry<HostAndPort, Set<String>> e : byLeader.entrySet()) {
            List<String> tablets = new ArrayList<>(e.getValue());
            GetLatestEntryOpIdResponse resp = client.getLatestEntryOpIds(e.getKey(), tablets);
            List<OpIdPB> ops = resp.getOpIds();
            // A partial sample would read as a backlog jump on the next one, so drop the whole evaluation.
            if (resp.hasError() || ops.size() != tablets.size()) {
                throw new IllegalStateException("GetLatestEntryOpIds from " + e.getKey() + " returned " + ops.size() + " of "
                        + tablets.size() + " tablets" + (resp.hasError() ? ": " + resp.getError() : ""));
            }
            for (int i = 0; i < tablets.size(); i++) {
                String tabletId = tablets.get(i);
                samples.add(new TaskAutoscalePolicy.TabletSample(tabletId, ops.get(i).getIndex(), checkpoints.get(tabletId)));
            }
        }
        return samples;
    }
}
