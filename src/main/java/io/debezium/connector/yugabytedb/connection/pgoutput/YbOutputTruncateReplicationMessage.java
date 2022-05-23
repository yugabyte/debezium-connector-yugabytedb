/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.yugabytedb.connection.pgoutput;

import java.time.Instant;

public class YbOutputTruncateReplicationMessage extends YbOutputReplicationMessage {

    private final boolean lastTableInTruncate;

    public YbOutputTruncateReplicationMessage(Operation op, String table, Instant commitTimestamp, long transactionId,
                                              boolean lastTableInTruncate) {
        super(op, table, commitTimestamp, transactionId, null, null);
        this.lastTableInTruncate = lastTableInTruncate;
    }

    @Override
    public boolean isLastEventForLsn() {
        return lastTableInTruncate;
    }

}
