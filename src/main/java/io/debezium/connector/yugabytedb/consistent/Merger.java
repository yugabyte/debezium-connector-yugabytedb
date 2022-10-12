package io.debezium.connector.yugabytedb.consistent;

import io.debezium.connector.yugabytedb.YugabyteDbConsistentStreaming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Merger {
    private static final Logger LOGGER = LoggerFactory.getLogger(Merger.class);
    private final PriorityQueue<Message> queue = new PriorityQueue<>();
    private final Map<String, List<Message>> mergeSlots = new HashMap<>();

    public Merger(List<String> tabletList) {
        tabletList.forEach(tabletId -> mergeSlots.put(tabletId, new ArrayList<>()));
    }

    public void addMessage(Message message) {
        queue.add(message);
        mergeSlots.get(message.tablet).add(message);
        LOGGER.info("Add message {}", message);
    }

    public Message peek() {
        return queue.peek();
    }

    public Message poll() {
        Message message = queue.poll();
        LOGGER.info("Message is: {}", message);
        LOGGER.info("Records for tablet: {}", mergeSlots.get(message.tablet).size());
        mergeSlots.get(message.tablet).removeIf(item -> item.compareTo(message) == 0);
        LOGGER.info("Records LEFT for tablet: {}", mergeSlots.get(message.tablet).size());
        return message;
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public boolean isSlotEmpty(String tabletId) {
        return mergeSlots.get(tabletId).isEmpty();
    }
}
