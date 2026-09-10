# Non-Retriable CDC Error Retry Runbook

This note documents how to check where CDC errors are logged and retried: inside `yb-client` RPC retry logic or inside connector retry loops.

## Key Retry Paths

- `yb-client` retries happen inside `AsyncYBClient`, configured in `YBClientUtils.getYbClient()` with `maxRpcAttempts()` and `sleepTime()`.
- Connector retries happen in explicit loops using `max.connector.retries` and `connector.retry.delay.ms`.
- `YugabyteDBStreamingChangeEventSource#getChanges2` catches `CDCErrorException`, handles `TABLET_SPLIT` and currently also `INVALID_REQUEST` as split, and throws other errors to the outer connector retry loop.
- `YBClientUtils#getTabletListToPollForCDCWithRetry` retries all exceptions while fetching tablets.
- `YBClientUtils#getCheckpointWithRetry` retries all exceptions while fetching checkpoints.
- `YugabyteDBSnapshotChangeEventSource` has separate connector retry loops for snapshot streaming and marking snapshot complete.
- `YugabyteDBTablePoller` retries publication/stream polling errors up to its local `MAX_RETRY_COUNT`.

## Enable Verbose Connector Logs

Edit `src/test/resources/logback-test.xml` temporarily:

```xml
<root level="debug">
    <appender-ref ref="CONSOLE" />
</root>
<logger name="io.debezium.connector.yugabytedb" level="debug" additivity="false">
    <appender-ref ref="CONSOLE" />
</logger>
<logger name="org.yb.client" level="debug" additivity="false">
    <appender-ref ref="CONSOLE" />
</logger>
<logger name="org.yb.client.TabletClient" level="debug" additivity="false">
    <appender-ref ref="CONSOLE" />
</logger>
```

For CDC RPC errors, add temporary logging in the `CDCErrorException` catch:

```java
LOGGER.warn("CDC error code: {}", cdcException.getCDCError().getCode());
if (cdcException.getCDCError().hasStatus()) {
    LOGGER.warn("App status code: {}", cdcException.getCDCError().getStatus().getCode());
    LOGGER.warn("App status message: {}", cdcException.getCDCError().getStatus().getMessage());
    LOGGER.warn("Full app status: {}", cdcException.getCDCError().getStatus());
}
```

## Local Connector-Side Commands

Build quickly:

```sh
mvn clean package -Dquick
```

Run a focused tablet split test against local `yugabyted` on `127.0.0.1`:

```sh
mvn -Dtest=YugabyteDBTabletSplitTest#shouldConsumeDataAfterTabletSplit test \
  -Ddebezium.test.records.waittime=5 \
  | tee /tmp/yb-connector-tablet-split.log
```

Run publication tests:

```sh
mvn -Dtest=YugabyteDBPublicationReplicationTest test \
  | tee /tmp/yb-connector-publication.log
```

Run restart-related tests:

```sh
mvn -Dtest=YugabyteDBRestartTest,YugabyteDBSnapshotResumeTest test \
  | tee /tmp/yb-connector-restart.log
```

Search retry source from connector logs:

```sh
grep -E "will attempt retry|Too many errors|CDC error code|App status code|GetTabletListToPollForCDC|GetChanges|YRpc|attempt=" /tmp/yb-connector-*.log
```

Interpretation:

- Logs like `will attempt retry X of Y` are connector retry loops.
- Logs containing `YRpc(... attempt=N, maxAttempts=M ...)` are `yb-client` RPC attempts.
- Logs from `org.yb.client.TabletClient` or `AsyncYBClient` before connector retry logs indicate client-side retry/dispatch behavior.

## Stress Test Notes

There is no dedicated stress-test script in this repository. For true YB stress runs, use the external YugabyteDB stress framework and collect:

- connector logs
- YB master logs
- YB tserver logs
- connector config used for `max.connector.retries`, `max.rpc.retry.attempts`, `rpc.retry.sleep.time.ms`, and `connector.retry.delay.ms`

Run log extraction after the stress job:

```sh
grep -RniE "CDCErrorException|CDC error|App status|TABLE_NOT_FOUND|INVALID_REQUEST|TABLET_SPLIT|CHECKPOINT_TOO_OLD|SUBSCRIBER_NOT_FOUND|will attempt retry|Too many errors|YRpc|attempt=" <stress-log-dir>
```

## Non-Retriable Error Checks

For each injected YB error:

1. Trigger the error from YB.
2. Capture connector logs with DEBUG enabled.
3. Record:
   - CDC error code: `cdcException.getCDCError().getCode()`
   - app status code: `cdcException.getCDCError().getStatus().getCode()`
   - app status message
   - whether retry came from `yb-client` or connector
4. If the error is permanent, change connector logic to throw immediately instead of entering a connector retry loop.
5. Move to the next error after confirming logs show no connector retry.

Permanent errors that should usually fail fast:

- wrong stream id
- wrong tablet id
- table not in stream/publication
- subscriber/stream not found
- checkpoint too old, unless there is a known recovery path

Potentially retriable edge case:

- `TABLE_NOT_FOUND` when a table was just added to a publication, but CDC stream metadata has not caught up yet. Retry can succeed only if the table is expected to be in the stream.

## Suggested Code Direction

Add a small helper that classifies `CDCErrorException`:

- `TABLET_SPLIT`: handle split
- transient client/server availability errors: allow connector retry
- permanent request/config errors: fail fast
- `TABLE_NOT_FOUND`: retry only when publication/stream metadata lag is possible; otherwise fail fast
- `INVALID_REQUEST`: do not treat as tablet split unless app status/message clearly says tablet split

## Local Observation: Tablet Split Run

Command used:

```sh
export JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-25.jdk/Contents/Home
export PATH=$JAVA_HOME/bin:/Users/pranjalkatte/yugabyte-2026.1.0.1/bin:$PATH
mvn -Dtest=YugabyteDBTabletSplitTest test 2>&1 | tee /tmp/yb-connector-tablet-split.log
```

Extraction command:

```sh
grep -En "Code received in CDCErrorException|CDC app status code|CDC app status message|will attempt retry|Too many errors|BUILD FAILURE|BUILD SUCCESS|\[ERROR\]|Tests run|YRpc\(.*attempt=[2-9]|YRpc\(.*attempt=[1-9][0-9]+" /tmp/yb-connector-tablet-split.log
```

Current findings from this run:

- The test logging is fully verbose for `io.debezium`, `io.debezium.connector.yugabytedb`, and `org.yb.client`.
- `yb-client` retries were visible as `YRpc(... attempt=2, ...)`. Observed RPCs include `ListTables`, `CreateCDCStream`, `GetCDCDBStreamInfo`, `GetTabletListToPollForCDC`, `GetCheckpoint`, and `GetTableSchema`.
- Connector retries were not observed in this run. The connector-side retry marker `will attempt retry` did not appear.
- CDC error handling was not reached in the visible output. The new markers `Code received in CDCErrorException`, `CDC app status code`, and `CDC app status message` did not appear.
- The run progressed through `SplitTablet` and then `GetTableLocations`, followed by repeated JDBC connection checks and yb-client channel close/heartbeat-style decode logs. Maven was still running and no `target/surefire-reports` output had been created yet, so this run should be treated as hung or still waiting for split propagation rather than completed.

