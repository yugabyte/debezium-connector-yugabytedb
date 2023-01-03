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
        if (message.record.getRowMessage().getOp() == CdcService.RowMessage.Op.SAFEPOINT) {
            LOGGER.debug("Received safe point message {}", message);
            tabletSafeTime.put(message.tablet, message.commitTime);
            LOGGER.debug("Put {}:{}, verifying {}", message.tablet, message.commitTime, tabletSafeTime.get(message.tablet));
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

    public long totalQueueSize() {
        return queue.size();
    }

    public int pendingMessagesInTablet(String tabletId) {
        return mergeSlots.get(tabletId).size();
    }

    public BigInteger safeTimeForTablet(String tabletId) {
        return tabletSafeTime.get(tabletId);
    }

    public Optional<Message> peek() {
        Message message = queue.peek();
        Optional<Message> peeked = message != null && message.commitTime.compareTo(this.streamSafeTime()) < 0
                ? Optional.of(message) : Optional.empty();

        if (peeked.isPresent() && peeked.get().record.getRowMessage().getOp() == CdcService.RowMessage.Op.INSERT) {
            LOGGER.debug("Stream Safe Time {}, Top message is {}", this.streamSafeTime(), peeked);
        }

        return peeked;
    }

    public synchronized Optional<Message> poll() {
        if (queue.isEmpty()) {
            return Optional.empty();
        }

        Message message = Objects.requireNonNull(queue.poll());

        if (message.commitTime.compareTo(this.streamSafeTime()) < 0) {
            LOGGER.info("Message is: {}", message);
            LOGGER.info("Records for tablet: {}", mergeSlots.get(message.tablet).size());
            mergeSlots.get(message.tablet).removeIf(item -> item.compareTo(message) == 0);
            LOGGER.info("Records LEFT for tablet: {}", mergeSlots.get(message.tablet).size());
            return Optional.of(message);
        } else {
            return Optional.empty();
        }
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public boolean isSlotEmpty(String tabletId) {
        return mergeSlots.get(tabletId).isEmpty();
    }
}
