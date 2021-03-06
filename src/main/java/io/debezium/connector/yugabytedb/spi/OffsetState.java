/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yugabytedb.spi;

import java.time.Instant;

import io.debezium.common.annotation.Incubating;
import io.debezium.connector.yugabytedb.connection.OpId;

/**
 * A simple data container that represents the last seen offset
 * which was written by debezium.
 *
 * This data may differ based on decoding plugin and settings, such as
 * lastSeenXmin being null if xmin tracking isn't enabled
 */
@Incubating
public class OffsetState {
    private final OpId lsn;
    private final String txId;
    private final Long xmin;
    private final Instant commitTs;
    private final boolean snapshotting;

    public OffsetState(OpId lsn, String txId, Long xmin, Instant lastCommitTs, boolean isSnapshot) {
        this.lsn = lsn;
        this.txId = txId;
        this.xmin = xmin;
        this.commitTs = lastCommitTs;
        this.snapshotting = isSnapshot;
    }

    /**
     * @return the last LSN seen by debezium
     */
    public OpId lastSeenLsn() {
        return lsn;
    }

    /**
     * @return the last txid seen by debezium
     */
    public String lastSeenTxId() {
        return txId;
    }

    /**
     * @return the last xmin seen by debezium
     */
    public Long lastSeenXmin() {
        return xmin;
    }

    /**
     * @return the last commit timestamp seen by debezium
     */
    public Instant lastCommitTs() {
        return commitTs;
    }

    /**
     * @return indicates if a snapshot is happening
     */
    public boolean snapshotInEffect() {
        return snapshotting;
    }
}
