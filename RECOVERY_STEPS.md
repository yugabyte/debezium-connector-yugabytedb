# Recovery Steps for Stuck Connector

## The Problem

Your connector is **stuck in task configuration** because of a bug in the table poller code that tries to open dropped tables, causing timeouts.

## What Was Fixed

### File: `YugabyteDBTablePoller.java`

**The Bug (lines 151-169):**
```java
// Check if any REMOVED tables were being streamed
for (TableInfo tableInfo : removedTables) {
  try {
    if (isTableIncludedForStreaming(tableInfo.getTableId().toStringUtf8())) {
      // This calls ybClient.openTableByUUID() which HANGS if table is dropped!
```

**The Fix:**
```java
// Check if any REMOVED tables were being streamed
// Note: We cannot call isTableIncludedForStreaming() for removed tables as they might be dropped
// and openTableByUUID() would timeout or fail. Instead, if ANY table is removed from the stream,
// we trigger reconfiguration to be safe.
if (!removedTables.isEmpty()) {
  LOGGER.warn("Detected {} table(s) removed from stream, signalling context reconfiguration", removedTables.size());
  shouldRestart = true;
}
```

## Immediate Recovery Steps

### Step 1: Force Stop the Stuck Worker Process

Since the connector is stuck in task configuration, you need to restart the Kafka Connect worker:

```bash
# Find the Kafka Connect process
ps aux | grep connect

# Kill it (use appropriate method for your deployment)
# For standalone:
pkill -f ConnectStandalone

# For distributed (restart the service)
sudo systemctl restart kafka-connect
# OR
kubectl rollout restart deployment/kafka-connect  # if using Kubernetes
```

### Step 2: Rebuild the Connector with Fixes

```bash
cd /Users/ahmedbayraktar/src/github.com/Shopify/debezium-connector-yugabytedb

# Clean and rebuild
mvn clean package -DskipTests

# Copy the new JAR to your Kafka Connect plugins directory
# (adjust path based on your setup)
cp target/debezium-connector-yugabytedb-*.jar /path/to/kafka-connect/plugins/
```

### Step 3: Restart Kafka Connect

```bash
# Start Kafka Connect with the new connector
# The exact command depends on your setup
```

### Step 4: Delete the Stuck Connector

Once Kafka Connect restarts, try deleting the stuck connectors:

```bash
# Delete the stuck connector
curl -X DELETE http://localhost:8083/connectors/globaldb-core-product-variants

# Also delete the one you originally tried
curl -X DELETE http://localhost:8083/connectors/globaldb-core-products

# Verify they're gone
curl http://localhost:8083/connectors
```

### Step 5: Recreate Connectors with New Build

```bash
# Recreate your connector with the fixed version
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @your-connector-config.json
```

## Alternative: Force Delete via Kafka Topics

If the connector is still stuck after restarting, you may need to clean up the internal Kafka Connect state:

```bash
# List Connect internal topics
kafka-topics --bootstrap-server localhost:9092 --list | grep connect

# Delete connector configs (BE CAREFUL - this affects ALL connectors)
kafka-configs --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name connect-configs \
  --delete-config cleanup.policy

# Then restart Kafka Connect
```

## Preventing This Issue

### The Root Cause
The `isTableIncludedForStreaming()` method calls `ybClient.openTableByUUID()`, which:
- **Hangs or times out** if the table has been dropped
- **Blocks the entire task configuration** process
- Cannot be called on removed/dropped tables

### The Solution Applied
When tables are removed from the stream:
1. ✅ Do NOT try to open them by UUID
2. ✅ Simply trigger reconfiguration if ANY table is removed
3. ✅ Let the connector restart and discover the new table list

This is conservative but safe - any table removal triggers reconfiguration.

## Verification

After rebuilding and restarting:

1. **Check connector status:**
```bash
curl http://localhost:8083/connectors/your-connector/status
```

2. **Monitor logs for table changes:**
```bash
# Look for these log messages
grep "Detected.*table.*removed from stream" kafka-connect.log
grep "Found.*new table" kafka-connect.log
```

3. **Verify task configuration completes:**
```bash
# This should complete quickly now (not timeout)
curl http://localhost:8083/connectors/your-connector/tasks
```

## Files Modified

1. **YugabyteDBTablePoller.java** (lines 151-159)
   - Fixed: Removed dangerous `isTableIncludedForStreaming()` call on removed tables
   - Now: Simply checks if `removedTables` is not empty

2. **YugabyteDBEventDispatcher.java** (lines 154-168)
   - Fixed: Added full exception logging in all error modes
   - Now: You can see WHY events fail to process

## Additional Notes

### Why the Timeout Happened
1. A table was dropped from your database
2. The stream detected the table was removed
3. The poller tried to check if it should trigger reconfiguration
4. The `openTableByUUID()` call hung because the table didn't exist
5. Kafka Connect timed out waiting for task configs

### Performance Impact
The new code is actually **faster** because:
- No need to open tables that might not exist
- No network calls for removed tables
- Immediate reconfiguration trigger

### Future Improvements
Consider maintaining a cache of "tables currently being streamed" in memory, so you can:
- Check the cache instead of calling `isTableIncludedForStreaming()`
- Only trigger reconfiguration if a STREAMED table is removed
- Avoid unnecessary reconfigurations for filtered-out tables

