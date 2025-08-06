package io.debezium.connector.yugabytedb.snapshot.query;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector;
import io.debezium.snapshot.spi.SnapshotQuery;

@ConnectorSpecific(connector = YugabyteDBgRPCConnector.class)
public class SelectAllSnapshotQuery implements SnapshotQuery {

    public SelectAllSnapshotQuery() {

    }

    @Override
    public String name() {
        return CommonConnectorConfig.SnapshotQueryMode.SELECT_ALL.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {
    }

    @Override
    public Optional<String> snapshotQuery(String tableId, List<String> snapshotSelectColumns) {
        return Optional.empty();
    }
}
