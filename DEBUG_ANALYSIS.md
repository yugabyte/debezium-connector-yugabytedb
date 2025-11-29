# Debugging ConnectException Error Analysis

## Error Message
```
org.apache.kafka.connect.errors.ConnectException: Error while processing event at offset {c2231c29767a4e7f944a56db455ed004=22:19258887::0:7220735763257724927}
```

## Root Cause Location
The error is being thrown in `YugabyteDBEventDispatcher.java` at line 155 (after the fix).

## Issue Found
**Critical Bug**: The original error handling code was **NOT logging the actual exception details** in WARN and SKIP modes, making debugging impossible! You only saw which offset failed, but not WHY it failed.

## Fixes Applied

### 1. Enhanced Error Logging (`YugabyteDBEventDispatcher.java`)
- **FAIL mode**: Now logs the full exception with stack trace BEFORE throwing
- **WARN mode**: Now logs exception message and full stack trace
- **SKIP mode**: Now logs exception message and full stack trace  
- **IGNORE mode**: Added missing case to fix linter warning

### Changes:
```java
// Before - NO exception details logged!
case WARN:
    LOGGER.warn("Error while processing event at offset {}", offset);
    break;

// After - FULL exception details logged!
case WARN:
    LOGGER.warn("Error while processing event at offset {}, exception: {}",
                offset, e.getMessage(), e);
    break;
```

## Understanding the Offset Format
The offset `c2231c29767a4e7f944a56db455ed004=22:19258887::0:7220735763257724927` breaks down as:
- `c2231c29767a4e7f944a56db455ed004` = Tablet ID
- `22:19258887::0:7220735763257724927` = OpId format: `term:index:key:write_id:time`
  - term = 22
  - index = 19258887
  - key = "" (empty - this is why you see `::`)
  - write_id = 0
  - time = 7220735763257724927

The double colon `::` indicates an empty byte array for the key, which is **valid and expected** for certain record types.

## Next Steps to Debug Your Error

### Step 1: Check Your Logs with Enhanced Logging
After applying this fix, rebuild and redeploy the connector:

```bash
cd /Users/ahmedbayraktar/src/github.com/Shopify/debezium-connector-yugabytedb
mvn clean package -DskipTests
```

Then restart your connector and look for the **full stack trace** in the logs. You should now see something like:

```
ERROR Error while processing event at offset {c2231c29767a4e7f944a56db455ed004=22:19258887::0:7220735763257724927}
<full exception stack trace here>
```

### Step 2: Common Causes of Event Processing Errors

Based on the code, the exception could be thrown from:

1. **Schema Issues** (line 112-121):
   - Schema not found for the table
   - Schema mismatch between database and connector

2. **Data Type Conversion Errors** (line 124):
   - Invalid date/time format
   - Data type mismatch (e.g., string in a numeric column)
   - NULL handling issues

3. **Key Schema Issues** (line 319-328 in `YugabyteDBChangeRecordEmitter.java`):
   - Primary key missing or invalid
   - Empty column values when primary key is required

4. **Enum/Custom Type Issues**:
   - Unknown enum value
   - Custom PostgreSQL type not recognized

### Step 3: Temporarily Change Error Handling Mode (Optional)

If you want the connector to continue processing despite errors, you can change the failure handling mode:

In your connector configuration, set:
```properties
# Options: fail, warn, skip, ignore
# Default is 'fail' which stops the connector
event.processing.failure.handling.mode=warn
```

- `warn`: Logs the error and continues (recommended for debugging)
- `skip`: Silently skips the record
- `ignore`: Completely ignores the error
- `fail`: Stops the connector (default)

### Step 4: Check Specific Table/Record

Once you have the full exception details from the logs, you can:

1. Identify which table is causing the issue (from the stack trace)
2. Query that specific record at the offset to inspect the data
3. Check the table schema in YugabyteDB
4. Look for any special characters, NULL values, or unusual data types

### Step 5: Enable Detailed Logging

Add these to your logging configuration:
```xml
<logger name="io.debezium.connector.yugabytedb.YugabyteDBEventDispatcher" level="DEBUG"/>
<logger name="io.debezium.connector.yugabytedb.YugabyteDBChangeRecordEmitter" level="DEBUG"/>
```

## Common Solutions

### If it's a Schema Issue:
- Ensure the table schema is properly captured during snapshot
- Check if there were recent DDL changes
- Verify that the connector has proper permissions to read table metadata

### If it's a Data Type Issue:
- Check for infinity/-infinity values in timestamp columns
- Look for invalid UTF-8 characters
- Verify JSONB/JSON content is valid

### If it's a Key Issue:
- Ensure tables have valid primary keys defined
- Check if the primary key columns have NULL values (shouldn't happen)

## Files Modified
- `src/main/java/io/debezium/connector/yugabytedb/YugabyteDBEventDispatcher.java`
  - Enhanced error logging in all failure handling modes
  - Added missing IGNORE case

## Additional Information

The changes to `YugabyteDBTablePoller.java` in your current branch (handling table additions/removals) are unrelated to this error and should not cause this issue.

## Contact Points for Further Investigation
If the error persists after reviewing the full stack trace, check:
1. The specific table schema at `c2231c29767a4e7f944a56db455ed004`
2. Recent changes to that table
3. Any custom data types or PostgreSQL extensions in use

