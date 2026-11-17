package io.debezium.connector.yugabytedb;

public final class YugabyteDBgRPCConnectorTestAccess {
    private YugabyteDBgRPCConnectorTestAccess() {
    }

    public static int effectiveMaxTabletsPerTask(int configured, int concurrency) {
        return YugabyteDBgRPCConnector.effectiveMaxTabletsPerTask(configured, concurrency);
    }

    public static int autoscaleTaskCount(long tablets, int maxTabletsPerTask, int maxGroups, int min, Integer planned) {
        return YugabyteDBgRPCConnector.autoscaleTaskCount(tablets, maxTabletsPerTask, maxGroups, min, planned);
    }
}
