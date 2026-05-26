# Fix #4321: PaginatedComponentFile short-write / short-read

## Issue

`PaginatedComponentFile.write()` and `read()` drop the return value from `channel.write(ByteBuffer, long)`
and `channel.read(ByteBuffer, long)`. The Java NIO contract allows these calls to transfer fewer bytes
than the buffer holds (under disk pressure, NFS, sparse-file quirks, etc.). The current code silently
returns as if the full page was written/read.

**Affected file:** `engine/src/main/java/com/arcadedb/engine/PaginatedComponentFile.java`

## Root Cause

Lines 150 and 196: single-call `channel.write()` / `channel.read()` without looping on the return value.

The `ClosedChannelException` retry path in `write()` (line 155) does check `written < pageSize` but only
logs a warning; it still does not loop.

The same pattern in `calculateChecksum()` (line 120) and `readPage()` (line 206) has the same gap.

## Fix

Replace each single `channel.write(buffer, pos)` / `channel.read(buffer, pos)` call with a loop:

```
while (buffer.hasRemaining())
    position += channel.write(buffer, position);
```

For `read()`, also throw `IOException` on `-1` (EOF) to surface truncated pages.

## Files Changed

- `engine/src/main/java/com/arcadedb/engine/PaginatedComponentFile.java` — loop on write/read in
  `write()`, `read()`, `readPage()`, and `calculateChecksum()`
- `engine/src/test/java/com/arcadedb/engine/PaginatedComponentFileRoundTripTest.java` — new regression test

## PR

https://github.com/ArcadeData/arcadedb/pull/4341

## Review cycles

- **Cycle 1** (`3c35c9b77`) - gemini-code-assist flagged 5 medium-priority issues:
  - `calculateChecksum` should throw `IOException` on EOF, not `break` (avoids stale-byte CRC)
  - `write()` and `read()` should use `buffer.clear()` instead of `buffer.rewind()` so the buffer limit is reset to capacity (defensive against partially-filled buffers)
  - `read()` was missing any buffer reset at the start
  - Retry blocks should also use `buffer.clear()`
- **Cycle 2** (`64fb9ed7e`) - gemini-code-assist posted one inline comment that was a duplicate of an already-applied cycle-1 suggestion. No new actionable feedback.

## Final state

clean-approval

## Test Results

All tests pass:
- `PaginatedComponentFileRoundTripTest` - 3/3 new regression tests pass
- `PageManagerFlushQueueRaceTest`, `MutablePageMoveTest`, `ApplyChangesPartialReplayTest`, `CheckDatabaseTest` - all pass
- `PageManagerStressTest`, `LSMVectorIndexWALBypassTest`, `PaginatedSparseVectorEngine*` tests - all pass

## Test Strategy

Unit tests in `com.arcadedb.engine.PaginatedComponentFileRoundTripTest`:
1. `writeThenReadRoundTripPreservesContent` - writes a `MutablePage` with known bytes, reads back into a fresh `CachedPage`, asserts byte-for-byte equality.
2. `multiplePagesAreAddressedIndependently` - writes two pages with different content, reads out-of-order, verifies positional addressing.
3. `overwrittenPageReflectsLatestContent` - overwrites a page at the same position and confirms the latest content wins.
