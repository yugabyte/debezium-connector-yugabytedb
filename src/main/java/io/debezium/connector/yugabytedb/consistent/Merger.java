package io.debezium.connector.yugabytedb.consistent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;

import java.math.BigInteger;
import java.util.*;

/**
 * @author Rajat Venkatesh, Vaibhav Kushwaha
 */
public class Merger {
    /*
        Assumptions made:
          1. The elements in a merge slot are sorted already because we assume a sorted order
             from the WAL level itself.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Merger.class);
    private final PriorityQueue<Message> queue = new PriorityQueue<>();
    private final Map<String, List<Message>> mergeSlots = new HashMap<>();
    private final Map<String, BigInteger> tabletSafeTime = new HashMap<>();

    public Merger(List<String> tabletList) {
        tabletList.forEach(tabletId -> {
            mergeSlots.put(tabletId, new ArrayList<>());
            this.tabletSafeTime.put(tabletId, BigInteger.ZERO);
        });
    }

    /**
     * Add the passed message to the priority queue as well as the respective merge slot for
     * further processing. This also updates the tablet safetime after each message is added to the
     * queue.
     * @param message the message to be added to the queue
     * @see Message
     */
    public synchronized void addMessage(Message message) {
        setTabletSafeTime(message.tablet, message.commitTime);

        assert message.record.getRowMessage().getOp() != CdcService.RowMessage.Op.DDL;

        if (message.record.getRowMessage().getOp() == CdcService.RowMessage.Op.SAFEPOINT) {
            LOGGER.debug("Received safe point message {}", message);
            return;
        }

        // TODO: Wrap these checks under a flag maybe.
        if (!this.mergeSlots.get(message.tablet).isEmpty()
                && message.compareTo(this.mergeSlots.get(message.tablet)
                    .get(this.mergeSlots.get(message.tablet).size() - 1)) < 0) {
            throw new AssertionError("Commit time of the newly added message is less than the " +
                                     "last message in the merge slot");
        }

        queue.add(message);
        mergeSlots.get(message.tablet).add(message);
        LOGGER.debug("Add message {}", message);
    }

    public BigInteger streamSafeTime() {
        // tabletSafeTime.entrySet().stream().forEach(e -> LOGGER.info("Tablet {}, safe time {}", e.getKey(), e.getValue()));
        return Collections.min(tabletSafeTime.values());
    }

    /**
     * Set the safetime for the tablet to the given value.
     * @param tabletId the tablet UUID for which the safetime needs to be set
     * @param safeTime the safetime to be set
     */
    public void setTabletSafeTime(String tabletId, BigInteger safeTime) {
        // TODO: Wrap this assert under the flag.
        // If the safetime we are setting is less than the already set value then it would indicate
        // that we are moving backward in time, which is wrong. Throw an assertion error in that case.
        if (safeTime.compareTo(this.tabletSafeTime.get(tabletId)) < 0) {
            final String errorMessage = "Merger tried to set tablet safetime to a lower value. Tablet: "
                                        + tabletId + " Current safetime value: "
                                        + this.tabletSafeTime.get(tabletId).toString()
                                        + " Attempted set value: " + safeTime.toString();
            throw new AssertionError(errorMessage);
        }
        LOGGER.info("Updating safetime for tablet {}:{}, verifying {}", tabletId, safeTime, this.tabletSafeTime.get(tabletId));
        this.tabletSafeTime.put(tabletId, safeTime);
    }

    public long totalQueueSize() {
        return queue.size();
    }

    public int pendingMessagesInTablet(String tabletId) {
        return mergeSlots.get(tabletId).size();
    }

    public BigInteger safeTimeForTablet(String tabletId) {
        return tabletSafeTime.get(tabletId);
    }

    /**
     * Retrieves a message from the queue if it is present. The retrieval is based on a condition
     * that the message in the front of the queue should have a commit time less than or equal to
     * the stream safetime, an empty {@link Optional} is returned if the condition fails.
     * @return a {@link Message} wrapped in an {@link Optional}, or an {@link Optional#empty()} if
     * message is null
     */
    private Optional<Message> peek() {
        Message message = queue.peek();

        if (message == null) {
            LOGGER.warn("Message after peeking is null (actually means no message in queue)");
        } else {
            LOGGER.warn("Message is not null in queue - actual message is {}", message);
            if (!(message.commitTime.compareTo(this.streamSafeTime()) <= 0)) {
                LOGGER.warn("Comparison commit time for message and stream safetime failed (may indicate issues)");
                LOGGER.warn("Stream safetime {} and message commit time {}", this.streamSafeTime(), message.commitTime);
            }
        }

        Optional<Message> peeked = message != null && message.commitTime.compareTo(this.streamSafeTime()) <= 0
                ? Optional.of(message) : Optional.empty();

        if (peeked.isPresent() && peeked.get().record.getRowMessage().getOp() == CdcService.RowMessage.Op.INSERT) {
            LOGGER.debug("Stream Safe Time {}, Top message is {}", this.streamSafeTime(), peeked);
        }

        return peeked;
    }

    /**
     * Get the front message from the queue to process further. This function also removes the
     * message from the relevant merge slot.
     * @return a {@link Message} wrapped in an {@link Optional}, or an {@link Optional#empty()} if
     *      * message is null
     */
    public synchronized Optional<Message> poll() {
        Optional<Message> message = this.peek();

        if (message.isEmpty()) {
            LOGGER.warn("Empty message is being returned from poll (may indicate issues)");
            return message;
        }

        // Remove message from queue as well as mergeSlots
        queue.poll();
        Message polledMessage = message.get();
        LOGGER.info("Message is: {}", polledMessage);
        LOGGER.info("Records for tablet: {}", mergeSlots.get(polledMessage.tablet).size());
        mergeSlots.get(polledMessage.tablet).removeIf(item -> item.compareTo(polledMessage) == 0);

        // After removing the record, if the tablet safetime becomes less than that of the first
        // record in the merge slot then it indicates an issue, throw error with appropriate message.
        if (!mergeSlots.get(polledMessage.tablet).isEmpty()
            && this.tabletSafeTime.get(polledMessage.tablet)
                .compareTo(mergeSlots.get(polledMessage.tablet).get(0).commitTime) < 0) {
            throw new AssertionError("Tablet safetime is less than the commit time of first message in merge slot");
        }

        LOGGER.info("Records LEFT for tablet: {}", mergeSlots.get(polledMessage.tablet).size());
        return message;
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public boolean isSlotEmpty(String tabletId) {
        return mergeSlots.get(tabletId).isEmpty();
    }

    /**
     * Whether the messages in the merge slot are sorted properly by the virtue of their
     * commit time.
     * @param mergeSlot the merge slot to check sorting for
     * @return true if messages are sorted, false otherwise
     */
    public boolean isMergeSlotSorted(List<Message> mergeSlot) {
        Message prev = null;
        for (Message m : mergeSlot) {
            if (prev != null && prev.commitTime.compareTo(m.commitTime) > 0) {
                return false;
            }
            prev = m;
        }

        return true;
    }

    /**
     * Print the tablet related information such as the safetime for tablet as well as the first
     * message in its merge slot if any exists.
     * @param tabletId tablet UUID
     */
    public void logTabletInfo(String tabletId) {
        LOGGER.info("Safetime for tablet {}: {} with first message: {}",
                tabletId, this.tabletSafeTime.get(tabletId),
                this.mergeSlots.get(tabletId).isEmpty()
                        ? "Merge slot empty" : this.mergeSlots.get(tabletId).get(0));
    }

    /**
     * Print the tablet safetime of the given tablet, stream safetime and the first message in the
     * merge slot for the tablet. If the tabletID passed is null or empty, it will print the
     * of all the tablets.
     * @param tabletId tablet UUID
     */
    public void dumpState(String tabletId) {
        LOGGER.info("Stream safetime: {}", this.streamSafeTime());

        if (tabletId != null && !tabletId.isEmpty()) {
            logTabletInfo(tabletId);
        } else {
            // The keys of the map are the tablet IDs.
            for (String tablet : this.tabletSafeTime.keySet()) {
                logTabletInfo(tablet);
            }
        }
    }

    /**
     * Print the safetime info of all the tablets, the stream safetime as well as the first message
     * in the merge slot of that tablet if the merge slot is not empty.
     */
    public void dumpState() {
        // Passing null so that it can print the details of all the tablets.
        this.dumpState(null);
    }
}
