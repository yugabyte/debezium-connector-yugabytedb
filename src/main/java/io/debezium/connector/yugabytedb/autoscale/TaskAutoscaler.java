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

    private volatile TaskAutoscalePolicy.Plan plan;
    private volatile TaskAutoscalePolicy.Plan consumed;
    private volatile boolean taskConfigsWaiting;
    private TaskAutoscalePolicy.Plan requested;
    private volatile boolean unavailable;
    private int attemptsWithoutPlan;

    private final String connectorName;

    public TaskAutoscaler(String connectorName, YugabyteDBConnectorConfig config, ConnectorContext context, List<String> tableIds,
                          TaskAutoscalePolicy.Settings settings, long intervalMs, long startupSampleMs) {
        super("yb-task-autoscaler-" + connectorName);
        this.connectorName = connectorName;
        setDaemon(true);
        this.config = config;
        this.context = context;
        this.tableIds = new ArrayList<>(tableIds);
        this.policy = new TaskAutoscalePolicy(settings);
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

    private void markUnavailable(String why) {
        if (unavailable) {
            return;
        }
        unavailable = true;
        LOGGER.warn("Task autoscaler [{}]: {}; running tasks.max tasks as without autoscaling", connectorName, why);
        if (shutdown.getCount() > 0) {
            context.requestTaskReconfiguration();
        }
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
                                connectorName, e.desiredTasks, Math.round(e.opsPerSecond), e.backlogOps, e.lagging ? " (lagging)" : "",
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
            initialized.countDown();
        }
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
