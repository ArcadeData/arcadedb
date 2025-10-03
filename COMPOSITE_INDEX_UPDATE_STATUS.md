# Composite Index Update - Work in Progress

## Summary

This document tracks the work done to fix composite index update issues in ArcadeDB.

## Issues Addressed

### Issue 1: DELETE+CREATE in Same Transaction (✅ FIXED)
- **Test**: `ACIDTransactionTest.testDeleteOverwriteCompositeKeyInTx`
- **Problem**: When DELETE + CREATE operations occur with the same composite key values within a single transaction, a DuplicatedKeyException was thrown incorrectly.
- **Root Cause**: The REMOVE operation was overwritten by REPLACE operation in the transaction context map, losing the deleted RID information needed for validation.
- **Solution**: Added separate `deletedEntries` map to track deleted RIDs independently, preserving them even when operations are overwritten.
- **Status**: ✅ FIXED - Test passes

### Issue 2: UPDATE to Different Value Then Back to Original (⚠️ PARTIAL)
- **Test**: `UpdateStatementExecutionTest.testSelectAfterUpdate`
- **Problem**: When updating a record's indexed field across multiple transactions (A→B→A), the composite index isn't properly updated, causing queries by the index to fail.
- **Root Cause**: ADD operations for unique indexes with same RID but different keys aren't converted to REPLACE operations, causing index update failures.
- **Attempted Solution**: Added `removedRidsPerIndex` map to track which RIDs were removed in a transaction, allowing ADD-to-REPLACE conversion even when keys are different.
- **Status**: ⚠️ PARTIAL - Fix logic implemented but test still failing

## Changes Made

### TransactionIndexContext.java

1. **Added Field** (line 40):
```java
private Map<String, Map<ComparableKey, RID>> deletedEntries = new LinkedHashMap<>();
```
Tracks deleted RIDs separately to prevent information loss when REMOVE is overwritten by REPLACE.

2. **Added Field** (line 41):
```java
private Map<String, Set<RID>> removedRidsPerIndex = new LinkedHashMap<>();
```
Tracks which RIDs were removed in the current transaction for detecting REPLACE operations.

3. **Modified addIndexKeyLock()** (lines 292-323):
   - Added tracking of REMOVE operations in both `deletedEntries` and `removedRidsPerIndex`
   - Added logic to convert ADD to REPLACE when the RID was previously removed
   - Added logging for debugging

4. **Modified reset()** (lines 351-355):
   - Clear all three maps: `indexEntries`, `deletedEntries`, `removedRidsPerIndex`

5. **Modified commit()** (lines 217-219):
   - Clear all three maps after commit

6. **Modified getTxDeletedEntries()** (lines 398-417):
   - Return the separately tracked `deletedEntries` map instead of scanning indexEntries

## Test Results

✅ **Passing Tests**:
- `ACIDTransactionTest.testDeleteOverwriteCompositeKeyInTx`
- `Issue2590Test` (all tests related to unique constraint enforcement)

❌ **Failing Tests**:
- `UpdateStatementExecutionTest.testSelectAfterUpdate`
- `SimpleUpdateIndexTest.testCompositeIndexUpdate` (new test created for debugging)

## Analysis

### Why Issue 1 Fix Works
The `deletedEntries` map preserves deleted RID information even when the REMOVE operation is overwritten by REPLACE in the transaction context. This allows uniqueness validation to correctly identify that a RID was deleted and can be re-added with a new key value.

### Why Issue 2 Fix Doesn't Fully Work
The `removedRidsPerIndex` tracking should detect when an ADD operation involves a RID that was removed with a different key in the same transaction. However, the test still fails, suggesting:

1. **Possible ordering issues**: The ADD might be processed before the REMOVE
2. **Multiple index instances**: There might be bucket-specific indexes that aren't being tracked correctly
3. **TypeIndex vs bucket index confusion**: The tracking might be at the wrong level
4. **Commit logic gap**: Even if ADD is converted to REPLACE, there might be an issue in how REPLACE operations update composite indexes

## Next Steps

1. **Add detailed transaction context logging** before commit to see exact operation sequence
2. **Verify order of REMOVE/ADD operations** in DocumentIndexer.updateDocument()
3. **Check if bucket-specific indexes** are handled correctly
4. **Review commit logic** for REPLACE operations on composite indexes
5. **Consider alternative approach**: Track at TypeIndex level instead of individual index level

## Files

- Modified: `engine/src/main/java/com/arcadedb/database/TransactionIndexContext.java`
- Created: `UPDATE_INDEX_BUG_ANALYSIS.md` (detailed analysis from java-architect agent)
- Created: `engine/src/test/java/com/arcadedb/SimpleUpdateIndexTest.java` (minimal reproduction test)
- Created: `2590-composite-index-analysis.md` (Issue 1 analysis and solution)

## Related Issues

- GitHub Issue #2590: Unique constraint not enforced on UPDATE
- Related to composite index (status, id) updates
- Affects scenarios where indexed fields are updated back to previous values across transactions
