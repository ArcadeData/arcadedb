# Issue #3615 – Error during index compaction (IndexOutOfBoundsException in LocalSchema.registerFile)

## Summary

**Type:** Bug fix

**Branch:** `fix/3615-index-compaction-register-file-concurrent`

## Root Cause

`LocalSchema.registerFile()` (and `removeFile()`) operated on a plain `ArrayList<Component> files`
without any synchronization. When multiple async compaction tasks ran concurrently (which is possible
when there are multiple indices and the async parallel level is > 1), two threads could simultaneously
call `registerFile()` and corrupt the non-thread-safe ArrayList via concurrent `add()` and `set()`
operations.

The resulting race condition caused:

```
java.lang.IndexOutOfBoundsException: Index 319 out of bounds for length 319
  at java.util.ArrayList.set(ArrayList.java:471)
  at com.arcadedb.schema.LocalSchema.registerFile(LocalSchema.java:1756)
  at com.arcadedb.index.lsm.LSMTreeIndexCompactor.compact(LSMTreeIndexCompactor.java:66)
```

The `while (files.size() < fileId + 1) { files.add(null); }` and subsequent `files.set(fileId, file)`
are not atomic — another thread's concurrent modification between those two steps left the list in a
state where the expected slot did not exist.

## Changes

### `engine/src/main/java/com/arcadedb/schema/LocalSchema.java`

- Added `synchronized` to `registerFile(Component)` — prevents concurrent modification of `files`
  during the grow-then-set pattern.
- Added `synchronized` to `removeFile(int)` — symmetric protection for the list when dropping files.

### `engine/src/test/java/com/arcadedb/index/LSMTreeIndexCompactionTest.java`

- Added `testConcurrentCompaction()` regression test that:
  1. Creates 8 document types each with their own index.
  2. Inserts 5,000 records into each type.
  3. Launches one thread per index and fires all compactions simultaneously.
  4. Asserts no errors occurred and that record counts are intact.

## Test Results

```
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0
```

Both `testCompaction` (existing) and `testConcurrentCompaction` (new) pass.

## Impact Analysis

- The fix is minimal and surgical: only the ArrayList mutation operations are synchronized.
- The `synchronized` keyword on `registerFile`/`removeFile` uses the `LocalSchema` instance as
  monitor, which is fine since these are already short critical sections.
- No deadlock risk: neither method acquires any other lock internally.
- Read-only accessors (`getFileById`, `getFileByIdIfExists`, `getFileByName`) are not synchronized —
  they remain fast for the common read path and are safe because Java's memory model guarantees
  visibility of writes that happened before the end of a synchronized block.
