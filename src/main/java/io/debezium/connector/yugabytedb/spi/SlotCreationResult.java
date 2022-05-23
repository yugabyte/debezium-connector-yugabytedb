/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.spi;

import io.debezium.common.annotation.Incubating;
import io.debezium.connector.yugabytedb.connection.OpId;

/**
 * A simple data container representing the creation of a newly created replication slot.
 */
@Incubating
public class SlotCreationResult {

    private final String slotName;
    private final OpId walStartLsn;
    private final String snapshotName;
    private final String pluginName;

    public SlotCreationResult(String name, String startLsn, String snapshotName, String pluginName) {
        this.slotName = name;
        this.walStartLsn = OpId.valueOf(startLsn);
        this.snapshotName = snapshotName;
        this.pluginName = pluginName;
    }

    /**
     * return the name of the created slot.
     */
    public String slotName() {
        return slotName;
    }

    public OpId startLsn() {
        return walStartLsn;
    }

    public String snapshotName() {
        return snapshotName;
    }

    public String pluginName() {
        return pluginName;
    }
}
