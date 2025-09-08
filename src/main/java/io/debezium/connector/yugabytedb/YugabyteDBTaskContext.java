/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb;

import java.sql.SQLException;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.yugabytedb.connection.ReplicationConnection;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * The context of a {@link YugabyteDBConnectorTask}. This deals with most of the brunt of reading
 * various configuration options and creating other objects with these various options.
 *
 * @author Suranjan Kumar (skumar@yugabyte.com)
 */
@ThreadSafe
public class YugabyteDBTaskContext extends CdcSourceTaskContext {

    protected final static Logger LOGGER = LoggerFactory.getLogger(YugabyteDBTaskContext.class);

    private final YugabyteDBConnectorConfig config;
    private final TopicNamingStrategy<TableId> topicNamingStrategy;
    private final YugabyteDBSchema schema;

    private final boolean sendBeforeImage;

    private final boolean enableExplicitCheckpointing;

    protected YugabyteDBTaskContext(YugabyteDBConnectorConfig config, YugabyteDBSchema schema,
                                    TopicNamingStrategy<TableId> topicNamingStrategy, String taskId,
                                    boolean sendBeforeImage, boolean enableExplicitCheckpointing) {
        super(config, taskId, Collections.emptyMap(), null);
        this.config = config;
        this.topicNamingStrategy = topicNamingStrategy;
        assert schema != null;
        this.schema = schema;
        this.sendBeforeImage = sendBeforeImage;
        this.enableExplicitCheckpointing = enableExplicitCheckpointing;
    }

    protected TopicNamingStrategy<TableId> topicNamingStrategy() {
        return topicNamingStrategy;
    }

    protected YugabyteDBSchema schema() {
        return schema;
    }

    protected YugabyteDBConnectorConfig config() {
        return config;
    }

    protected boolean isBeforeImageEnabled() {
        return this.sendBeforeImage;
    }

    protected boolean shouldEnableExplicitCheckpointing() {
        return this.enableExplicitCheckpointing;
    }

    protected void refreshSchema(YugabyteDBConnection connection,
                                 boolean printReplicaIdentityInfo)
            throws SQLException {
        // schema.refresh(connection, printReplicaIdentityInfo);
    }

    protected ReplicationConnection createReplicationConnection(boolean doSnapshot)
            throws SQLException {
        return ReplicationConnection.builder(config)
                // .withSlot(config.slotName())
                // .withPublication(config.publicationName())
                .withTableFilter(config.getTableFilters())
                // .withPublicationAutocreateMode(config.publicationAutocreateMode())
                .withPlugin(config.plugin())
                .withTruncateHandlingMode(config.truncateHandlingMode())
                // .dropSlotOnClose(dropSlotOnStop)
                .streamParams(config.streamParams())
                .statusUpdateInterval(config.statusUpdateInterval())
                .withTypeRegistry(schema.getTypeRegistry())
                .doSnapshot(doSnapshot)
                .withSchema(schema)
                .build();
    }

    @Override
    public CommonConnectorConfig getConfig() {
        return config;
    }
}
