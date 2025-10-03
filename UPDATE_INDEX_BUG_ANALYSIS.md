# Bug Analysis: UpdateStatementExecutionTest.testSelectAfterUpdate Failure

## Executive Summary
The test fails when updating a document's indexed field back to its original value across multiple transactions. The composite unique index `(status, id)` is not properly updated, causing subsequent queries by `status` to fail even though the document is correctly stored with the expected values.

## Test Scenario
```java
1. CREATE Order type with properties: id (INTEGER), status (STRING)
2. CREATE unique index on `id`
3. CREATE unique composite index on `(status, id)`
4. INSERT Order: id=1, status='PENDING'
5. UPDATE: status='ERROR' (Transaction 2)
6. UPDATE: status='PENDING' with condition (Transaction 3)
7. SELECT WHERE status='PENDING' OR status='READY' → FAILS ❌
```

## Observed Behavior
- **Line 875**: Query by `id=1` successfully returns the record with `status='PENDING'` ✅
- **Line 880-881**: Query by `status='PENDING'` returns NO results ❌

This indicates:
- The document is correctly updated in storage
- The single-property index on `id` works correctly
- The composite index `(status, id)` is **NOT** properly updated

## Debug Output Analysis

###Transaction 3 Context (Before Commit)
```
Index: Order_0_1395070120875666
  ComparableKey: [ERROR, 1]
    Operation: REMOVE, KeyValues: [ERROR, 1], RID: #1:0
  ComparableKey: [PENDING, 1]
    Operation: ADD, KeyValues: [PENDING, 1], RID: #1:0
```

The transaction context correctly contains:
1. REMOVE operation for key (ERROR, 1)
2. ADD operation for key (PENDING, 1)

Both operations target the same RID (#1:0) but with different key values.

## Root Cause Analysis

### Location
**File**: `/Users/frank/projects/arcade/arcadedb/engine/src/main/java/com/arcadedb/database/TransactionIndexContext.java`
**Method**: `addIndexKeyLock()`
**Lines**: 286-305

### The Problem

When `DocumentIndexer.updateDocument()` processes the composite index update, it calls:
1. `index.remove([ERROR, 1], #1:0)` → Creates REMOVE operation
2. `index.put([PENDING, 1], [#1:0])` → Creates ADD operation

In `TransactionIndexContext.addIndexKeyLock()`, there's logic to convert ADD operations to REPLACE for unique indexes:

```java
if (v.operation == IndexKey.IndexKeyOperation.ADD) {
  if (index.isUnique()) {
    final IndexKey entry = values.get(v);  // ← THE PROBLEM
    if (entry != null && entry.operation == IndexKey.IndexKeyOperation.REMOVE) {
      // ... track deleted entry ...
      v.operation = IndexKey.IndexKeyOperation.REPLACE;
    }
  }
}
```

**The Bug**: `values.get(v)` looks up an existing `IndexKey` in the `values` map for the current `ComparableKey`. However:
- The REMOVE operation has `ComparableKey([ERROR, 1])`
- The ADD operation has `ComparableKey([PENDING, 1])`

These are **stored as separate entries** in the `indexEntries` TreeMap:
```java
TreeMap<ComparableKey, Map<IndexKey, IndexKey>> keys
  ├─ ComparableKey([ERROR, 1]) → Map{ REMOVE operation }
  └─ ComparableKey([PENDING, 1]) → Map{ ADD operation }
```

When processing the ADD for (PENDING, 1), the code looks in `values` for the (PENDING, 1) key, finds no existing entry (because the REMOVE is for (ERROR, 1)), and therefore **does NOT convert ADD to REPLACE**.

### Why REPLACE Matters

The REPLACE operation semantically means "I'm replacing an existing entry" vs ADD which means "I'm adding a new entry". This distinction is critical for:

1. **Uniqueness validation** in `checkUniqueIndexKeys()` (line 168, called at commit)
2. **Deleted entry tracking** via `deletedEntries` map (lines 293-300, 331-337)

The `deletedEntries` map is used during commit to track which RIDs were deleted with which keys, so the uniqueness check can allow re-adding the same RID with a different key.

### Uniqueness Check Flow

During commit, `checkUniqueIndexKeys()` (lines 395-417) processes each ADD/REPLACE operation:

```java
for (final IndexKey entry : valuesPerKey.values()) {
  if (entry.operation == IndexKey.IndexKeyOperation.ADD ||
      entry.operation == IndexKey.IndexKeyOperation.REPLACE) {
    final Map<ComparableKey, RID> deletedEntries = deletedKeys.get(typeIndex);
    final RID deleted = deletedEntries != null ?
      deletedEntries.get(new ComparableKey(entry.keyValues)) : null;  // ← ISSUE
    checkUniqueIndexKeys(index, entry, deleted);
  }
}
```

For our ADD (PENDING, 1) operation:
- It looks up `deletedEntries` for the key (PENDING, 1)
- But the deleted entry is for key (ERROR, 1), not (PENDING, 1)
- So `deleted` is **null**

Then in `checkUniqueIndexKeys(index, entry, deleted)` (lines 354-390):
```java
final IndexCursor found = idx.get(key.keyValues, 2);
if (found.hasNext()) {
  final Identifiable firstEntry = found.next();
  // ...
  if (found.hasNext() || (totalEntries == 1 && !firstEntry.equals(key.rid))) {
    if (firstEntry.equals(deleted))  // ← deleted is null, so this check fails
      return;

    // Throws DuplicatedKeyException or removes stale entry
  }
}
```

### Hypothesis on Failure Mechanism

One of the following may be happening:

**Hypothesis 1**: The uniqueness check finds the old (PENDING, 1) entry (if it wasn't properly removed in Transaction 2), and because `deleted` is null, it either:
- Throws a `DuplicatedKeyException` that's silently caught/suppressed
- Calls `idx.remove(key.keyValues)` to clean up a stale entry, but then doesn't re-add it

**Hypothesis 2**: There's an interaction between the REMOVE and ADD operations during commit where:
- The REMOVE (ERROR, 1) is processed first (lines 170-181)
- Then the ADD (PENDING, 1) is processed (lines 183-214)
- But something in the commit flow prevents the ADD from being properly executed

**Hypothesis 3**: The `deletedEntries` tracking (lines 293-300, 331-337) isn't correctly preserving the deleted RID information across different keys, causing issues with the uniqueness validation.

## Impact

This bug affects scenarios where:
1. A document's indexed field is updated from value A to value B
2. Then updated back from value B to value A (or to any previous value)
3. The index is a composite unique index
4. The updates happen across multiple transactions

## Files Involved

1. **TransactionIndexContext.java** (Primary issue location)
   - `addIndexKeyLock()` method (lines 260-340)
   - `commit()` method (lines 167-218)
   - `checkUniqueIndexKeys()` methods (lines 354-390, 395-417)

2. **DocumentIndexer.java**
   - `updateDocument()` method (lines 74-120)

3. **LSMTreeIndex.java**
   - `put()` method (lines 431-445)
   - `remove()` methods (lines 448-477)

4. **LocalDatabase.java**
   - `updateRecord()` method (lines 872-919)

## Recommended Fix

The `addIndexKeyLock()` method needs enhancement to detect when an ADD operation on a unique index is for a RID that already has a REMOVE operation for a **different** key in the same transaction.

### Option 1: Enhanced REPLACE Detection

Modify `addIndexKeyLock()` to scan all entries in `indexEntries` for the current index to find if there's a REMOVE operation for the same RID with a different key:

```java
if (v.operation == IndexKey.IndexKeyOperation.ADD && index.isUnique()) {
  // Check if this RID has a REMOVE operation for a different key
  for (Map.Entry<ComparableKey, Map<IndexKey, IndexKey>> keyEntry : keys.entrySet()) {
    if (!keyEntry.getKey().equals(k)) {  // Different key
      for (IndexKey existingKey : keyEntry.getValue().values()) {
        if (existingKey.operation == IndexKey.IndexKeyOperation.REMOVE &&
            existingKey.rid.equals(rid)) {
          // Same RID being removed with different key → Convert to REPLACE
          v.operation = IndexKey.IndexKeyOperation.REPLACE;

          // Track the deleted entry for uniqueness validation
          final TypeIndex typeIndex = index.getTypeIndex();
          if (typeIndex != null) {
            Map<ComparableKey, RID> deleted = deletedEntries.computeIfAbsent(
              typeIndex.getName(), key -> new HashMap<>());
            deleted.put(k, existingKey.rid);
          }
          break;
        }
      }
    }
  }
}
```

### Option 2: Track RID-based Removals

Maintain a separate map to track which RIDs have been removed in the current transaction (regardless of key), and use this to determine REPLACE operations:

```java
private Map<String, Set<RID>> removedRidsPerIndex = new LinkedHashMap<>();

// In addIndexKeyLock():
if (operation == IndexKey.IndexKeyOperation.REMOVE) {
  removedRidsPerIndex.computeIfAbsent(indexName, k -> new HashSet<>()).add(rid);
}

if (v.operation == IndexKey.IndexKeyOperation.ADD && index.isUnique()) {
  Set<RID> removedRids = removedRidsPerIndex.get(indexName);
  if (removedRids != null && removedRids.contains(rid)) {
    v.operation = IndexKey.IndexKeyOperation.REPLACE;
  }
}
```

### Option 3: Improved Deleted Entry Tracking

Enhance the `deletedEntries` map to track RID-to-Key mappings instead of Key-to-RID, allowing the uniqueness check to find the deleted key for a given RID:

```java
private Map<String, Map<RID, ComparableKey>> deletedEntriesByRid = new LinkedHashMap<>();
```

## Testing Recommendations

1. **Extend the existing test** to verify all index types (single, composite, unique, non-unique)
2. **Add test for cyclic updates**: A → B → C → A
3. **Test with multiple concurrent transactions** updating the same record
4. **Performance test** the fix to ensure no significant overhead

## Additional Notes

- The `deletedEntries` map was introduced in version 25.3.2 (see comment at line 49)
- The REPLACE operation was also introduced in 25.3.2 (see enum comment at line 49)
- This suggests recent refactoring that may not have fully addressed all edge cases

## Test Files Created for Debugging

1. `/Users/frank/projects/arcade/arcadedb/engine/src/test/java/com/arcadedb/query/sql/executor/DebugUpdateIndexTest.java`
2. `/Users/frank/projects/arcade/arcadedb/engine/src/test/java/com/arcadedb/query/sql/executor/DebugUpdateIndexTest3.java`

These tests demonstrate the bug and provide transaction context inspection for debugging.
