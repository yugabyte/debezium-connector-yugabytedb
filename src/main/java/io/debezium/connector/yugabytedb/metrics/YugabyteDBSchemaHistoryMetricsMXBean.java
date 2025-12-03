/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.metrics;

/**
 * JMX MBean interface for YugabyteDB schema history producer metrics.
 *
 * @author Michael Terranova
 */
public interface YugabyteDBSchemaHistoryMetricsMXBean {

    /**
     * @return the Kafka topic where schema history is published
     */
    String getTopicName();

    /**
     * @return true if the producer is disabled due to initialization failure
     */
    boolean isDisabled();

    /**
     * @return true if the producer has been initialized
     */
    boolean isInitialized();

    /**
     * @return total number of schema history events successfully sent
     */
    long getSchemaHistoryEventsSent();

    /**
     * @return total number of schema history events that failed to send
     */
    long getSchemaHistoryEventsFailed();

    /**
     * @return total number of schema history events queued for sending
     */
    long getSchemaHistoryEventsQueued();

    /**
     * @return timestamp (millis) of the last successful send, or 0 if none
     */
    long getLastSuccessfulSendTimestamp();

    /**
     * @return timestamp (millis) of the last failed send, or 0 if none
     */
    long getLastFailedSendTimestamp();

    /**
     * @return the last error message from a failed send, or null if none
     */
    String getLastErrorMessage();

    /**
     * @return current reference count (number of tasks using this producer)
     */
    int getReferenceCount();

    /**
     * Resets all counters to zero.
     */
    void reset();
}
