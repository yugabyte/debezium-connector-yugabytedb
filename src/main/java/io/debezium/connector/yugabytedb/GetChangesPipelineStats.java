/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb;

/** Counters {@link GetChangesPipeline} reports; implemented by the task's JMX metrics. */
public interface GetChangesPipelineStats {

    GetChangesPipelineStats NONE = new GetChangesPipelineStats() {
    };

    default void inFlight(int count) {
    }

    default void issued() {
    }

    default void reused() {
    }

    default void abandoned(int count) {
    }
}
