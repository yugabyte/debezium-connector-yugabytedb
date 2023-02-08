package io.debezium.connector.yugabytedb.consistent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.cdc.CdcService;

import java.math.BigInteger;
import java.util.*;

/**
 * @author Rajat Venkatesh
 */
public class Merger {
    private static final Logger LOGGER = LoggerFactory.getLogger(Merger.class);
    private final PriorityQueue<Message> queue = new PriorityQueue<>();
    private final Map<String, List<Message>> mergeSlots = new HashMap<>();
    private final Map<String, BigInteger> tabletSafeTime = new HashMap<>();

    public Merger(List<String> tabletList) {
        tabletList.forEach(tabletId -> {
            mergeSlots.put(tabletId, new ArrayList<>());
            tabletSafeTime.put(tabletId, BigInteger.ZERO);
        });
    }

    public synchronized void addMessage(Message message) {
        setTabletSafeTime(message.tablet, message.commitTime);
        if (message.record.getRowMessage().getOp() == CdcService.RowMessage.Op.SAFEPOINT) {
            LOGGER.debug("Received safe point message {}", message);
            return;
        }

        queue.add(message);
        // Assumption is that everything is sorted within a merge slot.
        mergeSlots.get(message.tablet).add(message);

        // Throw an exception if things are not sorted in merge slot
        // Verify that the merge slot is sorted in itself
        BigInteger lastTime = BigInteger.ZERO;
        for (Message m : mergeSlots.get(message.tablet)) {
            assert lastTime.compareTo(m.commitTime) <= 0; // last time should always be smaller than or equal to current element
            lastTime = m.commitTime;
        }

        LOGGER.debug("Add message {}", message);
    }

    public BigInteger streamSafeTime() {
        // tabletSafeTime.entrySet().stream().forEach(e -> LOGGER.info("Tablet {}, safe time {}", e.getKey(), e.getValue()));
        return Collections.min(tabletSafeTime.values());
    }

    public void setTabletSafeTime(String tabletId, BigInteger safeTime) {
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

    public void dumpState() {
        for (Map.Entry entry : tabletSafeTime.entrySet()) {
            LOGGER.info("Tablet {} with safetime {}", entry.getKey(), entry.getValue());
        }

        for (Map.Entry entry : mergeSlots.entrySet()) {
            List<Message> l = (List<Message>) entry.getValue();
            if (!l.isEmpty()) {
                LOGGER.info("Tablet {} with first message {}", entry.getKey(), l.get(0));
            }
        }
    }

    private synchronized Optional<Message> peek() {
        Message message = queue.peek();
        if (message == null) {
            LOGGER.info("Message is null in peek");
        } else {
            LOGGER.info("Message is not null in peek with message {}", message);
            if (!(message.commitTime.compareTo(this.streamSafeTime()) <= 0)) {
                LOGGER.info("Commit time compareTo condition is getting false");
                LOGGER.info("Stream safetime {} and message commit time {}", this.streamSafeTime(), message.commitTime);
                dumpState();
            }
        }
        Optional<Message> peeked = message != null && message.commitTime.compareTo(this.streamSafeTime()) <= 0
                ? Optional.of(message) : Optional.empty();

        if (peeked.isPresent() && peeked.get().record.getRowMessage().getOp() == CdcService.RowMessage.Op.INSERT) {
            LOGGER.debug("Stream Safe Time {}, Top message is {}", this.streamSafeTime(), peeked);
        }

        return peeked;
    }

    public synchronized Optional<Message> poll() {
        Optional<Message> message = this.peek();

        if (message.isEmpty()) {
            LOGGER.info("Empty message being returned from poll");
            return message;
        }

        // Remove message from queue as well as mergeSlots
        queue.poll();
        Message polledMessage = message.get();
        LOGGER.info("Message is: {}", polledMessage);
        LOGGER.info("Records for tablet: {}", mergeSlots.get(polledMessage.tablet).size());
        mergeSlots.get(polledMessage.tablet).removeIf(item -> item.compareTo(polledMessage) == 0);

        // After removing the record, if there is any record left in the slot then update the tablet safetime
        // to the value of the first record in the merge slot.
        if (!mergeSlots.get(polledMessage.tablet).isEmpty()) {
            // Assert that the tablet safetime is greater than or equal to the next message.
//            assert this.tabletSafeTime.get(polledMessage.tablet).compareTo(mergeSlots.get(polledMessage.tablet).get(0).commitTime) >= 0;

            // If tablet safetime is less than that of the next message in the slot, then update the safetime so that the connector doesn't get stuck
            if (this.tabletSafeTime.get(polledMessage.tablet).compareTo(mergeSlots.get(polledMessage.tablet).get(0).commitTime) == -1) {
                setTabletSafeTime(polledMessage.tablet, mergeSlots.get(polledMessage.tablet).get(0).commitTime);
            }
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
