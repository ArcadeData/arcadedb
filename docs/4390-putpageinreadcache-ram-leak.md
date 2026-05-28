# Issue #4390 - PageManager.putPageInReadCache RAM accounting leak

## Summary

`putPageInReadCache` only increments `totalReadCacheRAM` on first insert (when `put()` returns null).
When a page is replaced (same PageId, newer version), the delta between old and new physical sizes is
never applied. For pages with different sizes (possible in theory), this causes under-counting.
The eviction trigger `totalReadCacheRAM >= maxRAM` then fires too rarely, allowing unbounded heap growth.

## Root Cause

`engine/src/main/java/com/arcadedb/engine/PageManager.java:529`:

```java
private void putPageInReadCache(final CachedPage page) {
  if (readCache.put(page.getPageId(), page) == null)  // only on first insert
    totalReadCacheRAM.addAndGet(page.getPhysicalSize());
  checkForPageDisposal();
}
```

When `put()` returns a non-null old page, no adjustment is made to `totalReadCacheRAM`,
even though the old page's memory is released and the new page's memory is taken.
For same-size pages this is a no-op (correct), but for different-size pages the counter drifts.

## Fix

Apply the delta (new size - old size) when replacing:

```java
private void putPageInReadCache(final CachedPage page) {
  final CachedPage prev = readCache.put(page.getPageId(), page);
  if (prev == null)
    totalReadCacheRAM.addAndGet(page.getPhysicalSize());
  else
    totalReadCacheRAM.addAndGet((long) page.getPhysicalSize() - prev.getPhysicalSize());
  checkForPageDisposal();
}
```

## Files Changed

- `engine/src/main/java/com/arcadedb/engine/PageManager.java` - fix putPageInReadCache
- `engine/src/test/java/com/arcadedb/engine/PageManagerReadCacheRamAccountingTest.java` - regression test

## Test Results

**New regression test:** `PageManagerReadCacheRamAccountingTest.readCacheRamIsConsistentAfterPageReplacements()`
- Inserts 2000 records with 10KB payloads (20MB total) into 4MB cache
- Updates records 3 times to force page replacements
- Verifies: readCacheRAM > 0, avg page size 32-512KB (consistent with actual pages), evictionRuns > 0
- **PASS** ✓

**Related tests (engine module):**
- `PageManagerFlushQueueRaceTest` - PASS ✓
- `PageManagerStressTest` - SKIP (disabled, performance test)
- `BucketSQLTest`, `BucketAlignedCompactionTest` - PASS ✓ (7 tests total)

**Full engine module compilation:** PASS ✓

## Implementation Notes

The fix is minimal and surgical:
- Capture the old page on `put()` return value
- If old == null (first insert): add full page size (original behavior)
- If old != null (replacement): add delta = newSize - oldSize
- This handles same-size replacements (delta=0, no-op) and different-size replacements correctly

The delta approach is numerically stable (signed arithmetic on AtomicLong) and thread-safe
because each call holds the `prev` reference locally.
