/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.client.CDCErrorException;
import org.yb.client.CdcSdkCheckpoint;
import org.yb.client.GetChangesResponse;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * Keeps up to {@code maxConcurrentTablets} GetChanges requests of one streaming task in flight, at most
 * one per tablet, and hands responses out in the task's tablet order. A request left over from an
 * earlier pass is reused because it was built from checkpoints that only that tablet's own response
 * advances, so it is still the request the loop would send.
 */
class GetChangesPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetChangesPipeline.class);

    /** Issues a GetChanges request for a tablet from its current checkpoints. */
    @FunctionalInterface
    interface Requester {
        Deferred<GetChangesResponse> request(YBPartition partition, CdcSdkCheckpoint explicitCheckpoint);
    }

    /** A GetChanges request in flight and the explicit checkpoint it was sent with. */
    static final class Pending {
        final Deferred<GetChangesResponse> response;
        final CdcSdkCheckpoint explicitCheckpoint;
        final long issuedAtNanos;
        volatile long completedAtNanos;
        volatile Exception failure;

        Pending(Deferred<GetChangesResponse> response, CdcSdkCheckpoint explicitCheckpoint, long issuedAtNanos) {
            this.response = response;
            this.explicitCheckpoint = explicitCheckpoint;
            this.issuedAtNanos = issuedAtNanos;
        }
    }

    private final int maxConcurrentTablets;
    private final long maxRequestAgeNanos;
    private final Requester requester;
    private final Function<YBPartition, CdcSdkCheckpoint> explicitCheckpointFor;
    private final Predicate<YBPartition> willPoll;
    private final GetChangesPipelineStats metrics;
    private final Map<String, Pending> inFlight = new HashMap<>();

    GetChangesPipeline(int maxConcurrentTablets, long maxRequestAgeMillis, Requester requester,
                       Function<YBPartition, CdcSdkCheckpoint> explicitCheckpointFor,
                       Predicate<YBPartition> willPoll, GetChangesPipelineStats metrics) {
        this.maxConcurrentTablets = Math.max(1, maxConcurrentTablets);
        this.maxRequestAgeNanos = TimeUnit.MILLISECONDS.toNanos(maxRequestAgeMillis);
        this.requester = requester;
        this.explicitCheckpointFor = explicitCheckpointFor;
        this.willPoll = willPoll;
        this.metrics = metrics;
    }

    boolean isPipelined() {
        return maxConcurrentTablets > 1;
    }

    /**
     * Returns the request for {@code tablets.get(index)}, issuing it if none is outstanding, and makes
     * sure the next tablets of the pass, up to the concurrency limit, have theirs on the wire.
     */
    Pending take(List<Pair<String, String>> tablets, int index) {
        YBPartition current = partition(tablets.get(index));
        Pending pending = inFlight.remove(current.getId());
        if (pending != null && pending.failure != null) {
            // Resent because a stale failure would cost a connector retry the sequential loop never spends.
            // Server errors (mostly tablet splits) come back on the new request and are handled there.
            if (pending.failure instanceof CDCErrorException) {
                LOGGER.debug("GetChanges for tablet {} failed before it was processed ({}); sending it again",
                        current.getTabletId(), pending.failure.toString());
            }
            else {
                LOGGER.warn("GetChanges for tablet {} failed before it was processed ({}); sending it again",
                        current.getTabletId(), pending.failure.toString());
            }
            abandoned(1);
            pending = null;
        }
        if (pending == null) {
            pending = issue(current);
        }
        else {
            metrics.reused();
        }
        for (int ahead = index + 1; ahead < tablets.size() && inFlight.size() < maxConcurrentTablets - 1
                && ahead < index + maxConcurrentTablets; ahead++) {
            YBPartition next = partition(tablets.get(ahead));
            if (!inFlight.containsKey(next.getId()) && willPoll.test(next)) {
                inFlight.put(next.getId(), issue(next));
            }
        }
        metrics.inFlight(inFlight.size() + 1);
        return pending;
    }

    /**
     * Called when the join on a request timed out. When pipelined, keeps the request for the loop to wait
     * on again until it is older than the client's operation deadline: until then yb-client may still be
     * retrying it, and a second request for the tablet could land after it and regress the checkpoint.
     *
     * @return whether the request was kept
     */
    boolean timedOut(YBPartition partition, Pending pending) {
        if (!isPipelined() || System.nanoTime() - pending.issuedAtNanos >= maxRequestAgeNanos) {
            abandoned(1);
            return false;
        }
        inFlight.put(partition.getId(), pending);
        metrics.inFlight(inFlight.size());
        return true;
    }

    /** Forgets a tablet's outstanding request; used when the loop will not poll the tablet. */
    void discard(YBPartition partition) {
        if (inFlight.remove(partition.getId()) != null) {
            abandoned(1);
        }
    }

    /** Drops requests for tablets that are no longer in the task's list, e.g. a split parent. */
    void retainOnly(Set<String> partitionIds) {
        int dropped = 0;
        for (Iterator<String> it = inFlight.keySet().iterator(); it.hasNext();) {
            if (!partitionIds.contains(it.next())) {
                it.remove();
                dropped++;
            }
        }
        abandoned(dropped);
    }

    /**
     * Drops every outstanding request; called when the streaming loop exits. Waits up to
     * {@code drainMillis} in total for them first, because a response that arrives after the client
     * shuts down is logged by yb-client as an "Invalid rpcid" error.
     */
    void close(long drainMillis) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(drainMillis);
        for (Pending pending : inFlight.values()) {
            long remainingMillis = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remainingMillis <= 0) {
                break;
            }
            try {
                pending.response.join(remainingMillis);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            catch (Exception e) {
                // Only waiting for the request to finish; its outcome no longer matters.
            }
        }
        abandoned(inFlight.size());
        inFlight.clear();
        metrics.inFlight(0);
    }

    int inFlightCount() {
        return inFlight.size();
    }

    private void abandoned(int count) {
        if (count > 0) {
            LOGGER.debug("Dropping {} GetChanges request(s) that will not be processed", count);
            metrics.abandoned(count);
            metrics.inFlight(inFlight.size());
        }
    }

    private Pending issue(YBPartition partition) {
        CdcSdkCheckpoint explicitCheckpoint = explicitCheckpointFor.apply(partition);
        Pending pending = new Pending(requester.request(partition, explicitCheckpoint), explicitCheckpoint, System.nanoTime());
        // Typed as Object because an errback receives the exception in place of the response.
        @SuppressWarnings("unchecked")
        Deferred<Object> untyped = (Deferred<Object>) (Deferred<?>) pending.response;
        untyped.addBoth(new Callback<Object, Object>() {
            @Override
            public Object call(Object result) {
                if (result instanceof Exception) {
                    pending.failure = (Exception) result;
                }
                pending.completedAtNanos = System.nanoTime();
                return result;
            }
        });
        metrics.issued();
        return pending;
    }

    static YBPartition partition(Pair<String, String> tableAndTablet) {
        return YBPartition.forStreaming(tableAndTablet.getKey(), tableAndTablet.getValue());
    }
}
