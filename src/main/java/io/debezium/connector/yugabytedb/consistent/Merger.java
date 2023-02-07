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
    private static final Logger LOGGER = LoggerFactory.getLogger(Merger.class);
    private final PriorityQueue<Message> queue = new PriorityQueue<>();
    private final Map<String, List<Message>> mergeSlots = new HashMap<>();
    private final Map<String, BigInteger> tabletSafeTime = new HashMap<>();

    public Merger(List<String> tabletList) {
        tabletList.forEach(tabletId -> {
            mergeSlots.put(tabletId, new ArrayList<>());
            setTabletSafeTime(tabletId, BigInteger.ZERO);
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
        if (message.record.getRowMessage().getOp() == CdcService.RowMessage.Op.SAFEPOINT) {
            LOGGER.debug("Received safe point message {}", message);
            return;
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
        assert safeTime.compareTo(this.tabletSafeTime.get(tabletId)) != -1;
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
            LOGGER.warn("Message after peeking is null (actually means to message in queue)");
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
     * message from the relevant merge slot and update the tablet safetime if required.
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

        // After removing the record, if there is any record left in the slot then update the tablet
        // safetime to the value of the first record in the merge slot, iff the tablet safetime is
        // less than that of the next message in the slot.
        if (!mergeSlots.get(polledMessage.tablet).isEmpty()
            && this.tabletSafeTime.get(polledMessage.tablet)
                   .compareTo(mergeSlots.get(polledMessage.tablet).get(0).commitTime) == -1) {
                setTabletSafeTime(polledMessage.tablet, mergeSlots.get(polledMessage.tablet).get(0).commitTime);
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
}
