# Actual Debugging Steps for Task Config Timeout

## The Real Problem

**Multiple connectors are stuck in a cycle** during task configuration generation. This is NOT related to the local code changes - those aren't deployed yet.

## Evidence
```
16:21 → globaldb-core-addresses stuck
16:23 → globaldb-core-price-calculation-rules stuck  
16:27 → globaldb-core-product-variants stuck
16:36 → globaldb-core-product-variant-option-values stuck
16:52 → globaldb-core-addresses stuck (again)
17:02 → globaldb-core-addresses stuck (again)
17:10 → globaldb-core-product-publications stuck
```

## Root Cause Analysis

Looking at `YugabyteDBgRPCConnector.taskConfigs()` method, the following operations could hang:

### 1. Database Connection (Line 118)
```java
try (YugabyteDBConnection connection = new YugabyteDBConnection(...)) {
```
**Could hang if**: Database is overloaded, network issues, connection pool exhausted

### 2. Type Registry Query (Line 120)
```java
YugabyteDBTypeRegistry typeRegistry = new YugabyteDBTypeRegistry(connection);
```
**Could hang if**: Querying `pg_type` table is slow or blocked

### 3. Publication Table List Query (Line 142)
```java
String tableIncludeList = YugabyteDBConnectorConfig.extractTableListFromPublication(config);
```
**Could hang if**: 
- `pg_publication_tables` query is slow
- Publication has many tables
- Database locks

### 4. YBClient Stream Metadata Calls (Lines 167-168)
```java
sendBeforeImage = YBClientUtils.isBeforeImageEnabled(this.yugabyteDBConnectorConfig);
enableExplicitCheckpointing = YBClientUtils.isExplicitCheckpointingEnabled(this.yugabyteDBConnectorConfig);
```
**Could hang if**: 
- Master servers unresponsive
- GetDBStreamInfo RPC hanging
- Network partition

## Immediate Actions to Take

### Step 1: Check What's Actually Stuck

SSH into your Kafka Connect worker and check the thread dump:

```bash
# Get the Kafka Connect process ID
jps -l | grep Connect

# Get thread dump
jstack <PID> > thread_dump.txt

# Look for threads stuck in taskConfigs
grep -A 50 "taskConfigs" thread_dump.txt
```

### Step 2: Check Database Status

```bash
# Connect to YugabyteDB
ycqlsh  # or ysqlsh

# Check for long-running queries
SELECT pid, now() - query_start AS duration, state, query 
FROM pg_stat_activity 
WHERE state != 'idle' 
ORDER BY duration DESC;

# Check for locks
SELECT * FROM pg_locks WHERE NOT granted;

# Check connection count
SELECT count(*) FROM pg_stat_activity;
```

### Step 3: Check YugabyteDB Master Status

```bash
# Check master server health
curl http://<master-host>:7000/api/v1/health-check

# Check if masters are responsive
curl http://<master-host>:7000/api/v1/masters
```

### Step 4: Check Kafka Connect Logs

```bash
# Look for what's happening during taskConfigs
tail -f kafka-connect.log | grep -i "taskconfig\|timeout\|hang"

# Look for database connection errors
tail -f kafka-connect.log | grep -i "connection\|timeout\|pgsql"

# Look for YBClient errors
tail -f kafka-connect.log | grep -i "ybclient\|master\|rpc"
```

## Likely Root Causes (In Order of Probability)

### 1. **Database Connection Pool Exhaustion** (Most Likely)
Each connector creates a new connection in `taskConfigs()`. With 6 connectors all requesting reconfig simultaneously, you could be hitting connection limits.

**Check:**
```sql
-- Check max connections
SHOW max_connections;

-- Check current connections
SELECT count(*) FROM pg_stat_activity;
```

**Fix:** Increase `max_connections` in YugabyteDB or reduce connector count

### 2. **YBClient Master Server Timeout** (Very Likely)
The YBClient calls to get stream metadata might be timing out due to master overload.

**Check:**
```bash
# Check master server load
ssh <master-host>
top
netstat -an | grep 7100 | wc -l  # Check master RPC connections
```

**Fix:** 
- Increase timeout values in connector config
- Check master server resources
- Reduce number of simultaneous reconfigurations

### 3. **Slow pg_publication_tables Query** (If using publications)
With many tables in a publication, this query can be slow.

**Check:**
```sql
-- Time the query
\timing on
SELECT * FROM pg_publication_tables WHERE pubname = 'your_publication';
```

**Fix:** 
- Cache the table list
- Reduce tables in publication
- Add index if needed

### 4. **Table Poller Triggering Too Many Reconfigurations** (Possible)
The table poller might be detecting changes and triggering reconfigs too frequently, causing a cascade.

**Check logs for:**
```
"Found .* new table(s), signalling context reconfiguration"
```

**Fix:** 
- Increase `new.table.poll.interval.ms`
- Temporarily disable table monitoring

## Quick Fix to Get Unstuck RIGHT NOW

### Option 1: Restart Kafka Connect (Fastest)
```bash
sudo systemctl restart kafka-connect
# or
kubectl rollout restart deployment/kafka-connect
```

### Option 2: Increase Timeouts
Add/update these in your connector configs:

```json
{
  "admin.operation.timeout.ms": 120000,
  "operation.timeout.ms": 120000,
  "socket.read.timeout.ms": 120000,
  "connector.retry.delay.ms": 5000
}
```

### Option 3: Temporarily Disable Table Monitoring
Add to connector config:
```json
{
  "new.table.poll.interval.ms": 3600000
}
```
This reduces checks to once per hour instead of default (30 seconds).

### Option 4: Pause Connectors to Break the Cycle
```bash
# Pause all connectors
for conn in globaldb-core-products globaldb-core-product-variants globaldb-core-product-publications globaldb-core-price-calculation-rules globaldb-core-product-variant-option-values globaldb-core-addresses; do
  curl -X PUT http://localhost:8083/connectors/$conn/pause
done

# Wait a minute, then resume one at a time
sleep 60
curl -X PUT http://localhost:8083/connectors/globaldb-core-products/resume
# ... resume others gradually
```

## What Your Local Changes Actually Do

The changes to `YugabyteDBTablePoller.java` and `YugabyteDBEventDispatcher.java`:

1. **YugabyteDBTablePoller.java**: 
   - Improves handling of removed tables
   - PREVENTS a potential future issue (not causing current problem)
   - Still worth deploying

2. **YugabyteDBEventDispatcher.java**: 
   - Adds better error logging
   - Helps debug the original ConnectException you mentioned
   - Definitely worth deploying

**These are good fixes**, but they won't solve the current taskConfig timeout issue.

## Next Steps

1. **Immediate**: Restart Kafka Connect to break the cycle
2. **Short-term**: Check database connections and master server load
3. **Medium-term**: Deploy your local changes (they're good improvements)
4. **Long-term**: Investigate why connectors are requesting reconfig so frequently

## Questions to Answer

1. What triggered the initial reconfiguration at 16:21?
2. Are tables being added/dropped in your database?
3. What's the connection count to YugabyteDB?
4. Are master servers under high load?
5. How many tables are in each publication/stream?

Check the logs and thread dump to identify which specific operation in `taskConfigs()` is hanging.

