# Composite Index REPLACE Operation Bug - Deep Analysis

## Executive Summary

**Bug**: When updating a document's indexed field back to a previously-held value across multiple transactions, the composite unique index fails to return the record in subsequent queries, even though the document itself is correctly updated.

**Severity**: HIGH - Causes data to become invisible to index-based queries while remaining accessible via table scans.

**Status**: Root cause identified, fix in progress

## Reproduction Scenario

```sql
-- Transaction 1
INSERT INTO Order SET id = 1, status = 'PENDING'  -- Works ✅

-- Transaction 2
UPDATE Order SET status = 'ERROR' WHERE id = 1   -- Works ✅

-- Transaction 3
UPDATE Order SET status = 'PENDING' WHERE id = 1  -- Index broken ❌
SELECT FROM Order WHERE status = 'PENDING'  -- Returns NOTHING despite record existing
```

## Key Findings

### 1. Document vs Index Discrepancy

After the final UPDATE:
- **Document**: Contains correct value `status='PENDING'` (verified via table scan)
- **Composite Index (status, id)**: Does NOT contain entry for `(PENDING, 1)`
- **TypeIndex**: Appears empty when inspected
- **Bucket Index**: Has entries but cursor returns null

### 2. Transaction Context Operations Are Correct

The transaction logging confirms operations are correctly tracked:

```
Transaction 3:
>>> REMOVE tracked: index=Order_0_xxx, key=[ERROR, 1], rid=#1:0
>>> ADD converted to REPLACE: key=[PENDING, 1], rid=#1:0 (RID was removed in this tx)

Commit:
>>> Processing REMOVE operations:
>>>   Removing: index=Order_0_xxx, key=[ERROR, 1], rid=#1:0
>>> Processing ADD/REPLACE operations:
>>>   Adding: index=Order_0_xxx, operation=REPLACE, key=[PENDING, 1], rid=#1:0
>>>     put() succeeded
>>> COMMIT COMPLETE
```

### 3. Put() Succeeds But Index Still Empty

- The `index.put(key, rid)` call completes successfully without exception
- Yet subsequent index queries return no results
- The entry is not visible when iterating the index

### 4. Pattern: Returning to Previous Value

**Transaction 1**: ADD (PENDING, 1) → Query works ✅
**Transaction 2**: REMOVE (PENDING, 1) + REPLACE (ERROR, 1) → Query works ✅
**Transaction 3**: REMOVE (ERROR, 1) + REPLACE (PENDING, 1) → **Query FAILS** ❌

The bug manifests specifically when updating back to a value that was previously in the index.

## Root Cause Hypothesis

### LSM Tree Deletion Markers

When an entry is REMOVED from an LSM tree index, it's not immediately deleted. Instead, a **deletion marker** (tombstone) is written to the LSM tree.

**Hypothesis**: When we later PUT the same key with a REPLACE operation:
1. Transaction 2: REMOVE (PENDING, 1) → Writes tombstone for (PENDING, 1)
2. Transaction 3: PUT (PENDING, 1) with REPLACE → Writes new entry for (PENDING, 1)
3. **Problem**: The tombstone and the new entry coexist, causing queries to skip the entry

This would explain why:
- `put()` succeeds (it writes the entry)
- Cursor iteration hits the tombstone and returns null
- Queries don't find the entry (tombstone marks it as deleted)

### Alternative: Transaction Status Check Issue

During commit, when we call `index.put()`:
- Transaction status is `COMMIT_1ST_PHASE`
- LSMTreeIndex.put() checks: `if (status == BEGUN)` → FALSE
- So it calls `mutable.put(key, rid)` directly

**Potential issue**: If there's a race condition or caching issue between:
1. The REMOVE operation marking the entry as deleted
2. The PUT operation writing the new entry
3. The compaction/merge process in the LSM tree

Then the entry might not be visible immediately after commit.

## Code Locations

### TransactionIndexContext.java

**Lines 292-326**: ADD→REPLACE conversion logic
```java
// ALSO CHECK FOR REMOVED RID EVEN IF NO EXISTING ENTRY FOR THIS KEY
if (v.operation == IndexKey.IndexKeyOperation.ADD && index.isUnique()) {
  final Set<RID> removedRids = removedRidsPerIndex.get(indexName);
  if (removedRids != null && removedRids.contains(rid)) {
    v.operation = IndexKey.IndexKeyOperation.REPLACE;  // ✅ This works
  }
}
```

**Lines 180-230**: Commit logic
```java
// REMOVE operations processed first
for (IndexKey key : values) {
  if (key.operation == IndexKey.IndexKeyOperation.REMOVE) {
    index.remove(key.keyValues, key.rid);  // Writes tombstone?
  }
}

// ADD/REPLACE operations processed second
for (IndexKey key : values) {
  if (key.operation == IndexKey.IndexKeyOperation.ADD ||
      key.operation == IndexKey.IndexKeyOperation.REPLACE) {
    index.put(key.keyValues, new RID[] { key.rid });  // Writes entry but doesn't clear tombstone?
  }
}
```

### LSMTreeIndex.java

**Lines 431-445**: put() implementation
```java
public void put(final Object[] keys, final RID[] rids) {
  if (getDatabase().getTransaction().getStatus() == TransactionContext.STATUS.BEGUN) {
    // Adds to transaction context - NOT our case during commit
  } else {
    lock.executeInReadLock(() -> {
      mutable.put(convertedKeys, rids);  // ← This is called during commit
      return null;
    });
  }
}
```

**Lines 448-477**: remove() implementation
```java
public void remove(final Object[] keys, final RID rid) {
  if (transaction.getStatus() == TransactionContext.STATUS.BEGUN) {
    // Adds to transaction context - NOT our case during commit
  } else {
    lock.executeInReadLock(() -> {
      mutable.remove(convertedKeys, rid.getIdentity());  // ← Writes tombstone?
      return null;
    });
  }
}
```

### LSMTreeIndexMutable.java

Need to investigate:
- `put(Object[] keys, RID[] rids)` - How it handles keys that have tombstones
- `remove(Object[] keys, RID rid)` - How it writes tombstones
- Whether REPLACE operations need special handling vs ADD operations

## Impact

**Affected Scenarios**:
1. Cyclic status updates (PENDING → ERROR → PENDING)
2. Undoing previous changes
3. Restoring original values
4. Any UPDATE that returns an indexed field to a previous value across transactions

**Severity**:
- Data becomes invisible to queries using the index
- Table scans still find the data (performance impact)
- Unique constraint validation may fail or succeed incorrectly
- Data integrity appears broken to users

## Next Steps

### 1. Investigate LSM Tree Tombstone Handling

```java
// Check if tombstones and entries can coexist for same key
// Check if put() needs to explicitly clear tombstones
// Check if there's a merge/compaction delay
```

### 2. Consider Fix Options

**Option A**: Clear tombstones before PUT
```java
if (key.operation == IndexKey.IndexKeyOperation.REPLACE) {
  // First ensure any tombstones are cleared
  index.clearTombstones(key.keyValues);  // If such method exists
  index.put(key.keyValues, new RID[] { key.rid });
}
```

**Option B**: Use different LSM operation for REPLACE
```java
if (key.operation == IndexKey.IndexKeyOperation.REPLACE) {
  // Use update/upsert instead of put
  index.update(key.keyValues, new RID[] { key.rid });
}
```

**Option C**: Force compaction after REPLACE
```java
if (key.operation == IndexKey.IndexKeyOperation.REPLACE) {
  index.put(key.keyValues, new RID[] { key.rid });
  index.compact();  // Force immediate compaction to merge tombstone + new entry
}
```

### 3. Test Verification

Create tests for:
- Cyclic updates: A → B → A
- Multiple cycles: A → B → A → B → A
- Different data types (String, Integer, Date)
- Composite vs single-property indexes
- Unique vs non-unique indexes

### 4. Performance Considerations

Any fix must not:
- Significantly impact transaction commit performance
- Cause excessive compaction overhead
- Introduce new concurrency issues
- Break existing functionality

## Files Modified

1. `/Users/frank/projects/arcade/arcadedb/engine/src/main/java/com/arcadedb/database/TransactionIndexContext.java`
   - Added `removedRidsPerIndex` map to track removed RIDs
   - Enhanced `addIndexKeyLock()` to convert ADD→REPLACE for same RID with different keys
   - Added extensive logging for debugging

2. `/Users/frank/projects/arcade/arcadedb/engine/src/test/java/com/arcadedb/SimpleUpdateIndexTest.java`
   - Created minimal reproduction test
   - Added index inspection code
   - Added query verification at each step

3. Documentation:
   - `UPDATE_INDEX_BUG_ANALYSIS.md` - Initial analysis
   - `COMPOSITE_INDEX_UPDATE_STATUS.md` - Status tracking
   - This file - Deep analysis

## References

- Issue #2590: Unique constraint enforcement (successfully fixed)
- LSM Tree documentation: Understanding tombstones and compaction
- ArcadeDB transaction commit flow: 1st phase vs 2nd phase

## Testing Commands

```bash
# Run the failing test
../mvnw test -Dtest=SimpleUpdateIndexTest

# Run all related tests
../mvnw test -Dtest="*UpdateIndex*,*Issue2590*,ACIDTransactionTest#testDeleteOverwriteCompositeKeyInTx"

# Check test output for index state
../mvnw test -Dtest=SimpleUpdateIndexTest 2>&1 | grep -A30 "Checking indexes"
```

## Conclusion

The ADD→REPLACE conversion logic is working correctly. The problem is not in the transaction context tracking. The issue is in how the LSM tree index handles REPLACE operations when the key was previously deleted (has a tombstone).

The next step is to investigate the LSMTreeIndexMutable implementation to understand how tombstones work and whether `put()` operations properly clear tombstones for the same key.
