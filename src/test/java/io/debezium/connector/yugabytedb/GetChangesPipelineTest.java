package io.debezium.connector.yugabytedb;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.yb.client.CdcSdkCheckpoint;
import org.yb.client.GetChangesResponse;

import com.stumbleupon.async.Deferred;

/**
 * Unit tests for {@link GetChangesPipeline}: which requests it issues, from which checkpoints, and when.
 * Requests are recorded instead of sent; a tablet's checkpoint is a counter that only its own
 * processing advances, as in the streaming loop.
 */
public class GetChangesPipelineTest {

    private static final String TABLE = "table";

    /** A recorded request: the tablet and the checkpoint it was built from. */
    private static final class Request {
        final String tabletId;
        final long checkpoint;
        final Deferred<GetChangesResponse> response = new Deferred<>();

        Request(String tabletId, long checkpoint) {
            this.tabletId = tabletId;
            this.checkpoint = checkpoint;
        }
    }

    private final Map<String, Long> checkpoints = new HashMap<>();
    private final Map<String, Long> explicitCheckpoints = new HashMap<>();
    private final List<Request> sent = new ArrayList<>();
    private final Set<String> skipped = new HashSet<>();

    private GetChangesPipeline pipeline(int concurrency) {
        return pipeline(concurrency, 60_000);
    }

    private GetChangesPipeline pipeline(int concurrency, long maxRequestAgeMillis) {
        return new GetChangesPipeline(concurrency, maxRequestAgeMillis,
                (part, explicit) -> {
                    Request request = new Request(part.getTabletId(), checkpoints.getOrDefault(part.getTabletId(), 0L));
                    sent.add(request);
                    return request.response;
                },
                part -> new CdcSdkCheckpoint(1, explicitCheckpoints.getOrDefault(part.getTabletId(), 0L), new byte[0], 0, 0),
                part -> !skipped.contains(part.getTabletId()),
                GetChangesPipelineStats.NONE);
    }

    private static List<Pair<String, String>> tablets(int count) {
        List<Pair<String, String>> tablets = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            tablets.add(new ImmutablePair<>(TABLE, "tablet-" + i));
        }
        return tablets;
    }

    /** Runs one pass the way the streaming loop does, processing each tablet's response in order. */
    private List<Request> pass(GetChangesPipeline pipeline, List<Pair<String, String>> tablets, int stopBefore) {
        List<Request> processed = new ArrayList<>();
        for (int i = 0; i < tablets.size() && i < stopBefore; i++) {
            String tabletId = tablets.get(i).getValue();
            if (skipped.contains(tabletId)) {
                continue;
            }
            GetChangesPipeline.Pending pending = pipeline.take(tablets, i);
            Request request = requestFor(pending);
            processed.add(request);
            checkpoints.merge(tabletId, 1L, Long::sum);
        }
        return processed;
    }

    private Request requestFor(GetChangesPipeline.Pending pending) {
        return sent.stream().filter(r -> r.response == pending.response).findFirst().orElseThrow();
    }

    @Test
    public void sequentialIssuesOnlyTheCurrentTablet() {
        GetChangesPipeline pipeline = pipeline(1);
        List<Pair<String, String>> tablets = tablets(5);

        pipeline.take(tablets, 0);

        assertEquals(1, sent.size());
        assertEquals("tablet-0", sent.get(0).tabletId);
        assertEquals(0, pipeline.inFlightCount());
    }

    @Test
    public void keepsTheNextTabletsInFlightUpToTheLimit() {
        GetChangesPipeline pipeline = pipeline(4);
        List<Pair<String, String>> tablets = tablets(10);

        pipeline.take(tablets, 0);
        assertEquals(List.of("tablet-0", "tablet-1", "tablet-2", "tablet-3"), sentTablets());

        pipeline.take(tablets, 1);
        assertEquals(List.of("tablet-0", "tablet-1", "tablet-2", "tablet-3", "tablet-4"), sentTablets());
        assertEquals(3, pipeline.inFlightCount(), "three ahead of the one being processed");
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2, 4, 8, 32 })
    public void sendsTheSameRequestsAsTheSequentialLoop(int concurrency) {
        List<Pair<String, String>> tablets = tablets(10);
        List<String> expected = new ArrayList<>();
        for (int round = 0; round < 3; round++) {
            for (Pair<String, String> tablet : tablets) {
                expected.add(tablet.getValue() + "@" + round);
            }
        }

        GetChangesPipeline pipeline = pipeline(concurrency);
        List<String> processed = new ArrayList<>();
        for (int round = 0; round < 3; round++) {
            for (Request r : pass(pipeline, tablets, Integer.MAX_VALUE)) {
                processed.add(r.tabletId + "@" + r.checkpoint);
            }
        }

        assertEquals(expected, processed);
    }

    @Test
    public void neverHasTwoRequestsOutstandingForOneTablet() {
        GetChangesPipeline pipeline = pipeline(4);
        List<Pair<String, String>> tablets = tablets(10);

        pass(pipeline, tablets, 3);
        pass(pipeline, tablets, Integer.MAX_VALUE);

        Map<String, Long> perTabletCheckpoint = new HashMap<>();
        for (Request r : sent) {
            Long previous = perTabletCheckpoint.put(r.tabletId, r.checkpoint);
            assertTrue(previous == null || previous < r.checkpoint,
                    "second request for " + r.tabletId + " from the same checkpoint " + r.checkpoint);
        }
    }

    @Test
    public void reusesARequestLeftOverFromAShortenedPass() {
        GetChangesPipeline pipeline = pipeline(4);
        List<Pair<String, String>> tablets = tablets(10);

        pass(pipeline, tablets, 2);
        int sentBefore = sent.size();
        List<Request> secondPass = pass(pipeline, tablets, 5);

        assertEquals("tablet-2", secondPass.get(2).tabletId);
        assertEquals(0, secondPass.get(2).checkpoint);
        assertTrue(sent.subList(sentBefore, sent.size()).stream().noneMatch(r -> r.tabletId.equals("tablet-2")),
                "tablet-2's prefetched request is handed out again, not resent");
    }

    @Test
    public void keepsATimedOutRequestTheClientMayStillBeRetrying() {
        GetChangesPipeline pipeline = pipeline(4, 60_000);
        List<Pair<String, String>> tablets = tablets(3);

        GetChangesPipeline.Pending first = pipeline.take(tablets, 0);
        assertTrue(pipeline.timedOut(GetChangesPipeline.partition(tablets.get(0)), first));

        assertSame(first, pipeline.take(tablets, 0));
        assertEquals(3, sent.size());
    }

    @Test
    public void resendsATimedOutRequestPastTheClientDeadline() {
        GetChangesPipeline pipeline = pipeline(4, 0);
        List<Pair<String, String>> tablets = tablets(3);

        GetChangesPipeline.Pending first = pipeline.take(tablets, 0);
        assertFalse(pipeline.timedOut(GetChangesPipeline.partition(tablets.get(0)), first));

        assertNotSame(first, pipeline.take(tablets, 0));
        assertEquals(4, sent.size());
    }

    @Test
    public void resendsALeftoverRequestThatFailed() {
        GetChangesPipeline pipeline = pipeline(4);
        List<Pair<String, String>> tablets = tablets(3);
        pipeline.take(tablets, 0);
        Request tablet1 = sent.get(1);
        tablet1.response.callback(new Exception("tserver down"));

        GetChangesPipeline.Pending pending = pipeline.take(tablets, 1);

        assertNotSame(tablet1.response, pending.response);
        assertEquals("tablet-1", requestFor(pending).tabletId);
    }

    @Test
    public void reusesALeftoverRequestThatSucceeded() {
        GetChangesPipeline pipeline = pipeline(4);
        List<Pair<String, String>> tablets = tablets(3);
        pipeline.take(tablets, 0);
        Request tablet1 = sent.get(1);
        tablet1.response.callback(null);

        assertSame(tablet1.response, pipeline.take(tablets, 1).response);
    }

    @Test
    public void sequentialResendsAfterATimeout() {
        GetChangesPipeline pipeline = pipeline(1);
        List<Pair<String, String>> tablets = tablets(3);

        GetChangesPipeline.Pending first = pipeline.take(tablets, 0);
        assertFalse(pipeline.timedOut(GetChangesPipeline.partition(tablets.get(0)), first));

        assertNotSame(first, pipeline.take(tablets, 0));
        assertEquals(0, pipeline.inFlightCount());
    }

    @Test
    public void doesNotPrefetchTabletsTheLoopWillSkip() {
        skipped.add("tablet-1");
        GetChangesPipeline pipeline = pipeline(4);

        pipeline.take(tablets(4), 0);

        assertEquals(List.of("tablet-0", "tablet-2", "tablet-3"), sentTablets());
    }

    @Test
    public void keepsTheExplicitCheckpointItWasSentWith() {
        GetChangesPipeline pipeline = pipeline(4);
        List<Pair<String, String>> tablets = tablets(3);
        explicitCheckpoints.put("tablet-1", 7L);

        pipeline.take(tablets, 0);
        explicitCheckpoints.put("tablet-1", 9L);
        GetChangesPipeline.Pending pending = pipeline.take(tablets, 1);

        assertEquals(7L, pending.explicitCheckpoint.getIndex());
    }

    @Test
    public void retainOnlyDropsTabletsNoLongerPolled() {
        GetChangesPipeline pipeline = pipeline(4);
        List<Pair<String, String>> tablets = tablets(4);
        pipeline.take(tablets, 0);

        pipeline.retainOnly(Set.of(GetChangesPipeline.partition(tablets.get(2)).getId()));

        assertEquals(1, pipeline.inFlightCount());
    }

    @Test
    public void closeDropsEverything() {
        GetChangesPipeline pipeline = pipeline(4);
        pipeline.take(tablets(4), 0);

        long start = System.nanoTime();
        pipeline.close(50);

        assertEquals(0, pipeline.inFlightCount());
        assertTrue(System.nanoTime() - start < 5_000_000_000L, "waits at most the drain time for unanswered requests");
    }

    @Test
    public void closeReturnsAsSoonAsRequestsFinish() {
        GetChangesPipeline pipeline = pipeline(4);
        pipeline.take(tablets(4), 0);
        sent.forEach(r -> r.response.callback(null));

        long start = System.nanoTime();
        pipeline.close(10_000);

        assertTrue(System.nanoTime() - start < 5_000_000_000L);
    }

    private List<String> sentTablets() {
        List<String> tablets = new ArrayList<>();
        sent.forEach(r -> tablets.add(r.tabletId));
        return tablets;
    }
}
