package io.debezium.connector.yugabytedb.snapshot.lock;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.connector.yugabytedb.YugabyteDBConnectorConfig;
import io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector;
import io.debezium.snapshot.spi.SnapshotLock;

@ConnectorSpecific(connector = YugabyteDBgRPCConnector.class)
public class NoSnapshotLock implements SnapshotLock {

    @Override
    public String name() {
        return YugabyteDBConnectorConfig.SnapshotLockingMode.NONE.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Optional<String> tableLockingStatement(Duration lockTimeout, String tableId) {

        return Optional.empty();
    }
}

