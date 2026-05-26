# Fix #4335: LSMVectorIndexCompactor old-format tombstone rewind

## Issue

`LSMVectorIndexCompactor.mergePages` always reads the quantization-type byte unconditionally, even for tombstone entries. `LSMVectorIndexPageParser.skipQuantizationData` already handles the old-format tombstone case (pre-issue #3722) where tombstone entries were written WITHOUT the quantization-type byte. The compactor did not apply this logic, causing it to consume the first byte of the next entry's vectorId for each old-format tombstone, mis-aligning every subsequent entry on that page.

On any entry parse exception the compactor then `break`ed out of the entry loop and called `splitIndex(...)`, atomically replacing source pages with whatever partial data survived - no rollback.

## Root Cause

`LSMVectorIndexCompactor.mergePages` (lines 289-291) reads `quantTypeOrdinal` with an unconditional `readByte` + `currentOffset += 1` after the deleted flag, without applying the old-format tombstone detection logic that exists in `LSMVectorIndexPageParser.skipQuantizationData`.

## Fix

Two changes to `LSMVectorIndexCompactor.mergePages`:

1. **Tombstone rewind**: When `deleted == true`, delegate to `LSMVectorIndexPageParser.skipQuantizationData(page, currentOffset, true)` instead of unconditionally reading the quantType byte. This reuses the existing old-format detection logic.

2. **Abort on parse failure**: Change the inner `catch` from `break` (silently drop rest of page then call splitIndex) to storing the exception and rethrowing it as `IOException` after the outer page-level catch, so compaction aborts entirely and `splitIndex` is never called with partial data.

## Tests

New test `compactionSurvivesOldFormatTombstone` added to `LSMVectorIndexRecoveryTest`:
- Inserts 1500 vectors (spans 2 pages with 8KB page size)
- Deletes vector #400 (creates a tombstone on page 0)
- Patches the tombstone bytes to simulate old-format (removes quantType byte via byte-shift)
- Asserts there are >= 2 pages and compaction runs (`compact()` returns true)
- Asserts entry count after compaction is > 1400 (all live vectors survived)
- Asserts vectorNeighbors query for a vector after the tombstone returns results

## Verification

- `LSMVectorIndexRecoveryTest` (23 tests): all pass
- `LSMVectorIndexTest` (26 tests): all pass
- `LSMVectorIndexQuantizationTest` (8 tests): all pass

## Files Changed

- `engine/src/main/java/com/arcadedb/index/vector/LSMVectorIndexCompactor.java`
- `engine/src/test/java/com/arcadedb/index/vector/LSMVectorIndexRecoveryTest.java`
