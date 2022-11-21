/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.metrics;

import io.debezium.pipeline.metrics.traits.ConnectionMetricsMXBean;

/**
 * Metrics specific to streaming change event sources scoped to a connector task.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public interface YugabyteDBStreamingTaskMetricsMXBean extends ConnectionMetricsMXBean, YugabyteDBTaskMetricsMXBean {
}
