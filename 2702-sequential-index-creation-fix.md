# Issue #2702: NeedRetryException when creating indexes sequentially on large datasets

## Summary

Fixed the issue where creating multiple indexes sequentially on large datasets would fail with `NeedRetryException` when background LSMTree compaction was still running from a previous index creation.

**Issue:** https://github.com/ArcadeData/arcadedb/issues/2702

## Problem

When creating indexes on tables with millions of records:
1. The first `CREATE INDEX` succeeds and triggers background LSMTree compaction (can take 30-60+ seconds)
2. Subsequent `CREATE INDEX` commands fail immediately with:
   ```
   NeedRetryException: Cannot create a new index while asynchronous tasks are running
   ```
3. This forced applications to implement manual retry logic with delays

### Root Cause

Both `TypeIndexBuilder.create()` and `ManualIndexBuilder.create()` checked if async processing (compaction) was running and threw `NeedRetryException` immediately:

```java
if (database.isAsyncProcessing())
  throw new NeedRetryException("Cannot create a new index while asynchronous tasks are running");
```

This defensive check prevented concurrent index creation but made it impossible to create multiple indexes sequentially without explicit retry logic.

## Solution

Implemented **Option 1: Synchronous Blocking** from the issue suggestions.

Changed the behavior to **wait** for async processing to complete instead of throwing an exception:

```java
// Wait for any running async tasks (e.g., compaction) to complete before creating new index
// This prevents NeedRetryException when creating multiple indexes sequentially on large datasets
if (database.isAsyncProcessing())
  database.async().waitCompletion();
```

### Benefits

- ✅ Simple, predictable behavior
- ✅ No API changes needed
- ✅ Works like other databases
- ✅ No manual retry logic required
- ✅ Transparent to client code

### Trade-offs

- The calling thread blocks until compaction completes
- This is the same behavior as other major databases (PostgreSQL, MySQL, etc.)
- For applications that need non-blocking behavior, they can still use async database operations

## Changes Made

### Modified Files

1. **engine/src/main/java/com/arcadedb/schema/TypeIndexBuilder.java**
   - Line 86-88: Changed from throwing `NeedRetryException` to waiting for async completion
   - Added explanatory comment

2. **engine/src/main/java/com/arcadedb/schema/ManualIndexBuilder.java**
   - Line 47-49: Same change as TypeIndexBuilder
   - Added explanatory comment

3. **engine/src/test/java/com/arcadedb/index/Issue2702SequentialIndexCreationTest.java** (NEW)
   - Comprehensive test reproducing the issue scenario
   - Tests sequential index creation on large dataset (100K records)
   - Tests index creation while async compaction is running
   - Verifies all indexes work correctly after creation

## Testing

### New Test

Created `Issue2702SequentialIndexCreationTest` with two test methods:

1. **testSequentialIndexCreation()**: Creates 100K records and 3 sequential indexes
   - Configures low compaction RAM to trigger compaction
   - Creates indexes on different properties sequentially
   - Verifies all indexes were created and work correctly

2. **testIndexCreationWaitsForAsyncCompaction()**: Explicitly tests the waiting behavior
   - Forces async compaction to run
   - Creates a new index while compaction is active
   - Verifies index creation waits and completes successfully

### Regression Testing

Ran existing index-related tests to ensure no regressions:

```bash
# All passed successfully
mvn test -Dtest="*IndexBuilder*,*IndexCompaction*,LSMTreeIndexTest,TypeLSMTreeIndexTest"
mvn test -Dtest="CreateIndexByKeyValueTest,IndexSyntaxTest,DropIndexTest"
mvn test -Dtest=Issue2702SequentialIndexCreationTest
```

**Results:** All tests pass (57 tests total)

## Impact Analysis

### Positive Impacts

- **Developer Experience**: No more manual retry logic needed for batch index creation
- **API Consistency**: Aligns with behavior of other database operations
- **Batch Scripts**: Can now create multiple indexes in a single script
- **Predictability**: Index creation always succeeds (eventually)

### Performance Considerations

- Index creation may take longer when compaction is running
- This is expected and transparent - the operation simply waits
- Applications can monitor progress if needed
- Overall throughput unchanged - work still happens sequentially

### Backward Compatibility

- **Fully backward compatible**: No API changes
- Existing code that catches `NeedRetryException` will still work (exception no longer thrown)
- Applications using retry logic will work fine (retry logic becomes unnecessary but harmless)

## Verification

Before the fix:
```python
# This would fail with NeedRetryException
for table, column, uniqueness in indexes:
    db.command("sql", f"CREATE INDEX ON {table} ({column}) {uniqueness}")
```

After the fix:
```python
# This now works without any retry logic
for table, column, uniqueness in indexes:
    db.command("sql", f"CREATE INDEX ON {table} ({column}) {uniqueness}")
```

## Recommendations

### For Users

1. **Remove manual retry logic**: If you added retry logic to work around this issue, you can now remove it
2. **Monitor long-running operations**: If index creation seems slow, compaction might be running - this is normal
3. **Use async operations**: For non-blocking behavior, use the database's async API

### For Future Development

1. Consider adding progress callbacks for long-running index creation
2. Consider logging when index creation waits for compaction
3. Document the blocking behavior in CREATE INDEX documentation
4. Consider timeout options for index creation operations

## Related Issues

- Issue #2701: Duplicate timestamped indexes during compaction (separate issue but related to LSMTree compaction)

## Conclusion

The fix successfully addresses the issue by implementing synchronous blocking behavior for index creation when async tasks are running. This is the simplest and most predictable solution, aligning ArcadeDB's behavior with other major databases while maintaining full backward compatibility.
