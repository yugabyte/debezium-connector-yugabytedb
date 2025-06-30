/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.metrics;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.yugabytedb.YBPartition;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.metrics.ChangeEventSourceMetrics;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Base implementation of task-scoped multi-partition SQL Server connector metrics.
 * 
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
abstract class AbstractYugabyteDBTaskMetrics<B extends AbstractYugabyteDBPartitionMetrics> extends YugabyteDBMetrics
        implements ChangeEventSourceMetrics<YBPartition>, YugabyteDBTaskMetricsMXBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractYugabyteDBTaskMetrics.class);
    private final ChangeEventQueueMetrics changeEventQueueMetrics;
    private final Function<YBPartition, B> beanFactory;
    private final Map<YBPartition, B> beans = new HashMap<>();

    public AbstractYugabyteDBTaskMetrics(CdcSourceTaskContext taskContext,
                                         String contextName,
                                         ChangeEventQueueMetrics changeEventQueueMetrics,
                                         Collection<YBPartition> partitions,
                                         Function<YBPartition, B> beanFactory,
                                         YugabyteDBConnectorConfig connectorConfig,
                                         String taskId) {
        super(taskId, connectorConfig, contextName, true /* multipartition mode */);
        this.changeEventQueueMetrics = changeEventQueueMetrics;
        this.beanFactory = beanFactory;

        for (YBPartition partition : partitions) {
            beans.put(partition, beanFactory.apply(partition));
        }
    }

    @Override
    public synchronized void register() {
        super.register();
        beans.values().forEach(YugabyteDBMetrics::register);
    }

    @Override
    public synchronized void unregister() {
        beans.values().forEach(YugabyteDBMetrics::unregister);
        super.unregister();
    }

    @Override
    public void reset() {
        beans.values().forEach(B::reset);
    }

    @Override
    public void onEvent(YBPartition partition, DataCollectionId source, OffsetContext offset, Object key,
                        Struct value, Operation operation) {
        onPartitionEvent(partition, bean -> bean.onEvent(source, offset, key, value, operation));
    }

    @Override
    public void onFilteredEvent(YBPartition partition, String event) {
        onPartitionEvent(partition, bean -> bean.onFilteredEvent(event));
    }

    @Override
    public void onFilteredEvent(YBPartition partition, String event, Operation operation) {
        onPartitionEvent(partition, bean -> bean.onFilteredEvent(event, operation));
    }

    @Override
    public void onErroneousEvent(YBPartition partition, String event) {
        onPartitionEvent(partition, bean -> bean.onErroneousEvent(event));
    }

    @Override
    public void onErroneousEvent(YBPartition partition, String event, Operation operation) {
        onPartitionEvent(partition, bean -> bean.onErroneousEvent(event, operation));
    }

    @Override
    public void onConnectorEvent(YBPartition partition, ConnectorEvent event) {
        onPartitionEvent(partition, bean -> bean.onConnectorEvent(event));
    }

    @Override
    public int getQueueTotalCapacity() {
        return changeEventQueueMetrics.totalCapacity();
    }

    @Override
    public int getQueueRemainingCapacity() {
        return changeEventQueueMetrics.remainingCapacity();
    }

    @Override
    public long getMaxQueueSizeInBytes() {
        return changeEventQueueMetrics.maxQueueSizeInBytes();
    }

    @Override
    public long getCurrentQueueSizeInBytes() {
        return changeEventQueueMetrics.currentQueueSizeInBytes();
    }

    protected void onPartitionEvent(YBPartition partition, Consumer<B> handler) {
        B bean = beans.get(partition);
        
        if (bean == null) {
            LOGGER.info("MBean for partition {} are not registered, registering them now", partition);
            beans.put(partition, beanFactory.apply(partition));
            bean = beans.get(partition);
            bean.register();
        }

        handler.accept(bean);
    }
}
