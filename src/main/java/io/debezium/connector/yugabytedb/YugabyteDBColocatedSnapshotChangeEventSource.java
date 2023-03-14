package io.debezium.connector.yugabytedb;

import io.debezium.connector.yugabytedb.connection.OpId;
import io.debezium.connector.yugabytedb.connection.YugabyteDBConnection;
import io.debezium.connector.yugabytedb.spi.Snapshotter;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.client.AsyncYBClient;
import org.yb.client.YBClient;

import java.sql.SQLException;

/**
 * Class to facilitate the processing of snapshot for only the colocated tables in YugabyteDB. We
 * have made one assumption here that all the tables which will become a part of the task executing
 * this class will be colocated tables.
 *
 * @author Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class YugabyteDBColocatedSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource<YBPartition, YugabyteDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YugabyteDBColocatedSnapshotChangeEventSource.class);
    private final YugabyteDBConnectorConfig connectorConfig;
    private final YugabyteDBSchema schema;
    private final SnapshotProgressListener snapshotProgressListener;
    private final YugabyteDBTaskContext taskContext;
    private final EventDispatcher<YBPartition, TableId> dispatcher;
    protected final Clock clock;
    private final Snapshotter snapshotter;
    private final YugabyteDBConnection connection;
    private final YBClient syncClient;

    private OpId lastCompletelyProcessedLsn;

    private YugabyteDBTypeRegistry yugabyteDbTypeRegistry;

    public YugabyteDBColocatedSnapshotChangeEventSource(
            YugabyteDBConnectorConfig connectorConfig, YugabyteDBTaskContext taskContext,
            Snapshotter snapshotter, YugabyteDBConnection connection, YugabyteDBSchema schema,
            YugabyteDBEventDispatcher<TableId> dispatcher, Clock clock,
            SnapshotProgressListener snapshotProgressListener) {
        super(connectorConfig, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.schema = schema;
        this.taskContext = taskContext;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotter = snapshotter;
        this.connection = connection;
        this.snapshotProgressListener = snapshotProgressListener;

        AsyncYBClient asyncClient = new AsyncYBClient.AsyncYBClientBuilder(connectorConfig.masterAddresses())
                .defaultAdminOperationTimeoutMs(connectorConfig.adminOperationTimeoutMs())
                .defaultOperationTimeoutMs(connectorConfig.operationTimeoutMs())
                .defaultSocketReadTimeoutMs(connectorConfig.socketReadTimeoutMs())
                .numTablets(connectorConfig.maxNumTablets())
                .sslCertFile(connectorConfig.sslRootCert())
                .sslClientCertFiles(connectorConfig.sslClientCert(), connectorConfig.sslClientKey())
                .build();

        this.syncClient = new YBClient(asyncClient);

        this.yugabyteDbTypeRegistry = taskContext.schema().getTypeRegistry();
    }

    @Override
    protected SnapshotResult<YugabyteDBOffsetContext> doExecute(ChangeEventSourceContext context, YugabyteDBOffsetContext previousOffset, SnapshotContext<YBPartition, YugabyteDBOffsetContext> snapshotContext, SnapshottingTask snapshottingTask) throws Exception {
        return null;
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(YBPartition partition, YugabyteDBOffsetContext previousOffset) {
        return null;
    }

    @Override
    protected SnapshotContext<YBPartition, YugabyteDBOffsetContext> prepare(YBPartition partition) throws Exception {
        return new YugabyteDBColocatedSnapshotContext(partition, connectorConfig.databaseName());
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class YugabyteDBColocatedSnapshotContext extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<YBPartition, YugabyteDBOffsetContext> {

        public YugabyteDBColocatedSnapshotContext(YBPartition partition, String catalogName) throws SQLException {
            super(partition, catalogName);
        }
    }
}
