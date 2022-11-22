/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.metrics;

import io.debezium.pipeline.metrics.traits.CommonEventMetricsMXBean;
import io.debezium.pipeline.metrics.traits.SchemaMetricsMXBean;

/**
 * Metrics scoped to a source partition that are common for both snapshot and streaming change event sources.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public interface YugabyteDBPartitionMetricsMXBean extends CommonEventMetricsMXBean, SchemaMetricsMXBean {

    void reset();
}
