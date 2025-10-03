# Issue Analysis: DELETE + CREATE Composite Index in Same Transaction

## Problem Statement
Test `testDeleteOverwriteCompositeKeyInTx` fails when executing DELETE followed by CREATE with the same composite key values within a single transaction.

## Error
```
DuplicatedKeyException: Duplicated key [zone1, device1] found on index 'zone_device[from_id,to_id]' already assigned to record #8:1
```

## Root Cause Analysis

### The Issue
The transaction context stores index operations in a `Map<IndexKey, IndexKey>` structure where:
- For unique indexes, IndexKey equality is based ONLY on key values (not RID)
- This means multiple operations on the same key **overwrite each other** in the map

### Sequence of Events

**Transaction 4 (failing):**
```sql
DELETE FROM zone_device WHERE from_id='zone1' and to_id='device1';  -- Deletes RID #8:1
CREATE EDGE zone_device ... SET from_id='zone2', to_id='device1';  -- Creates RID #8:2
CREATE EDGE zone_device ... SET from_id='zone1', to_id='device1';  -- Creates RID #8:3
```

**What happens in TransactionIndexContext:**

1. **DELETE (zone1, device1) #8:1**:
   - Adds entry: `(zone1, device1) → REMOVE #8:1`

2. **CREATE (zone2, device1) #8:2**:
   - Adds entry: `(zone2, device1) → ADD #8:2`

3. **CREATE (zone1, device1) #8:3**:
   - Calls `addIndexKeyLock()` which finds existing entry `(zone1, device1) → REMOVE #8:1`
   - At line 287-292, checks if existing operation is ADD
   - Since it's REMOVE, doesn't throw exception
   - At line 292: converts operation to REPLACE
   - **OVERWRITES the map entry**: `(zone1, device1) → REPLACE #8:3`
   - **The REMOVE #8:1 information is LOST!**

4. **At commit time - checkUniqueIndexKeys()**:
   - `getTxDeletedEntries()` is called (with our fix, returns empty map now)
   - Validates REPLACE (zone1, device1) #8:3
   - Looks up index, finds #8:1 still in database
   - `deleted` RID = null (because REMOVE was lost)
   - Comparison fails: #8:1 != #8:3 → **throws DuplicatedKeyException**

## The Core Problem

**When ADD is converted to REPLACE (line 292), we lose track of what operation it's replacing.**

Specifically:
- If replacing a REMOVE: we lose the deleted RID information
- If replacing an ADD: we lose the original ADD's RID

## Solution Options

### Option 1: Don't Convert REMOVE to REPLACE
When ADD finds an existing REMOVE operation, keep it as ADD and remove the REMOVE from the map.

**Pros:** Simple, preserves deleted RID in getTxDeletedEntries
**Cons:** Changes fundamental behavior

### Option 2: Store Deleted RID in IndexKey
Extend IndexKey to track the "replacedRID" when operation is REPLACE.

**Pros:** Preserves all information
**Cons:** Requires modifying IndexKey class, more complex

### Option 3: Separate Tracking of Deleted RIDs
Maintain a separate map of deleted RIDs that doesn't get overwritten.

**Pros:** Clean separation of concerns
**Cons:** More memory, need to ensure consistency

### Option 4: Don't Validate REPLACE Operations
REPLACE means "already validated in this transaction", so skip the check.

**Pros:** Simple
**Cons:** Might miss some edge cases, reduces validation coverage

## Recommended Solution: Option 1

Modify `addIndexKeyLock()` to handle REMOVE → ADD differently:

```java
if (v.operation == IndexKey.IndexKeyOperation.ADD) {
  if (index.isUnique()) {
    final IndexKey entry = values.get(v);
    if (entry != null) {
      if (entry.operation == IndexKey.IndexKeyOperation.ADD && !entry.rid.equals(rid))
        throw new DuplicatedKeyException(indexName, Arrays.toString(keysValues), entry.rid);
      else if (entry.operation == IndexKey.IndexKeyOperation.REMOVE) {
        // Replacing a deleted key - this is allowed
        // Keep as ADD operation, remove the REMOVE entry so deleted RID can be tracked
        values.remove(entry);  // Remove the REMOVE operation
        // v stays as ADD
      } else {
        // REPLACE EXISTENT WITH THIS
        v.operation = IndexKey.IndexKeyOperation.REPLACE;
      }
    }
  }
}
values.put(v, v);
```

Wait, this won't work because `values.remove(entry)` followed by `values.put(v, v)` will just add it back...

## Better Solution: Option 4 with Enhancement

Don't validate REPLACE operations for uniqueness, but DO validate them if they're replacing a REMOVE (which means the old value was deleted in this transaction).

Actually, if REPLACE is replacing a REMOVE, the database index still has the old value, so the validation should pass because:
- Index has: #8:1
- We deleted: #8:1
- We're adding: #8:3
- Since deleted == #8:1 from index, validation should pass

But we lost the deleted RID!

## Actual Fix Needed

The real fix is to **preserve the REMOVE operation** when converting to REPLACE. We need to track it somewhere.

**Solution: Don't overwrite REMOVE with REPLACE**

When we have:
1. REMOVE (zone1, device1) #8:1
2. ADD (zone1, device1) #8:3

Instead of overwriting to create REPLACE #8:3, keep BOTH operations in the map by using a different key structure.

But the map key is IndexKey which for unique indexes only uses key values...

**Alternative: Store deleted RID in a separate field**

Let me check the IndexKey class structure...

## Implementation - SOLUTION IMPLEMENTED

The fix uses **Option 3: Separate Tracking of Deleted RIDs**.

### Changes Made to TransactionIndexContext.java

1. **Added new field** (line 40):
```java
private Map<String, Map<ComparableKey, RID>> deletedEntries = new LinkedHashMap<>();
```

2. **Track REMOVE operations when they occur** (lines 330-336):
```java
// TRACK REMOVE OPERATIONS IN SEPARATE MAP TO PRESERVE DELETED RID EVEN IF OVERWRITTEN BY REPLACE
if (v.operation == IndexKey.IndexKeyOperation.REMOVE && index.isUnique()) {
  final TypeIndex typeIndex = index.getTypeIndex();
  if (typeIndex != null) {
    Map<ComparableKey, RID> deleted = deletedEntries.computeIfAbsent(typeIndex.getName(), key -> new HashMap<>());
    deleted.put(k, rid);
  }
}
```

3. **Preserve deleted RID when REMOVE is overwritten by ADD→REPLACE** (lines 292-299):
```java
// IF REPLACING A REMOVE OPERATION, PRESERVE THE DELETED RID FOR VALIDATION
if (entry != null && entry.operation == IndexKey.IndexKeyOperation.REMOVE) {
  final TypeIndex typeIndex = index.getTypeIndex();
  if (typeIndex != null) {
    Map<ComparableKey, RID> deleted = deletedEntries.computeIfAbsent(typeIndex.getName(), key -> new HashMap<>());
    deleted.put(k, entry.rid);
  }
}
```

4. **Rewrote getTxDeletedEntries()** (lines 417-436) to return the separately tracked deleted entries instead of scanning through index entries.

5. **Clear deletedEntries** in `reset()` and `commit()` methods.

### How It Works

The key insight is that when a REMOVE operation gets overwritten by a REPLACE operation in the `Map<IndexKey, IndexKey>`, we lose the deleted RID. The solution is to track deleted RIDs in a **separate map** that doesn't get overwritten.

**Sequence for DELETE + CREATE with same key:**

1. **DELETE (zone1, device1) #8:1**:
   - Adds `REMOVE` entry to indexEntries map
   - **ALSO adds #8:1 to deletedEntries map**

2. **CREATE (zone1, device1) #8:3**:
   - Finds existing `REMOVE` entry in indexEntries map
   - **Before overwriting, preserves #8:1 in deletedEntries map** (defensive duplication)
   - Converts to `REPLACE` and overwrites the map entry

3. **At validation time**:
   - `getTxDeletedEntries()` returns deletedEntries map
   - Contains the correct deleted RID (#8:1)
   - Validation compares: index has #8:1, deleted = #8:1 → **PASS**

### Why This Works

- **Preserves information**: Deleted RIDs are never lost, even when map entries are overwritten
- **Minimal changes**: Only adds one new field and tracking logic, doesn't change core behavior
- **Clean separation**: Deleted RIDs are tracked independently from the operation state machine
- **Thread-safe**: Maps are managed consistently within transaction context

### Test Results

✅ `testDeleteOverwriteCompositeKeyInTx` - PASSED
✅ `Issue2590Test` (all 3 tests) - PASSED

The fix successfully handles the DELETE + CREATE scenario while maintaining the fix for issue #2590.
