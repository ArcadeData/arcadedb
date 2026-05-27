# Fix #4371 — FileManager ArrayList thread safety

## Issue

`FileManager` uses a plain `ArrayList<ComponentFile>` (`files`) that is mutated from
multiple threads without synchronization:

- `newFileId()` is `synchronized` — adds `RESERVED_SLOT` and returns the new index.
- `registerFile()` is NOT synchronized — grows and fills `files` based on the new file's ID.
- `dropFile()` is NOT synchronized — calls `files.set(fileId, null)`.
- `getFiles()` returns `Collections.unmodifiableList(files)` — a **live view**, not a snapshot.
  Iterating it while another thread calls `newFileId()` triggers `ConcurrentModificationException`.

`fileIdMap` and `fileNameMap` are `ConcurrentHashMap` so those are individually safe,
but the composite update (modify both `files` and `fileIdMap` atomically) was broken.

## Root cause

`ArrayList` is not thread-safe. Any structural modification (add/set/clear) from one thread
invalidates iterators held by other threads. `getFiles()` exposed a live view, making callers
vulnerable to CME even though they held no lock themselves.

## Fix

Synchronize all methods that mutate or take a snapshot of `files` on `this`
(matching the existing `synchronized newFileId()` lock):

| Method | Change |
|---|---|
| `registerFile()` | added `synchronized` |
| `dropFile()` | added `synchronized` |
| `close()` | added `synchronized` |
| `getFiles()` | added `synchronized` + returns defensive copy via `new ArrayList<>(files)` |

This ensures:
- Mutations to `files` and `fileIdMap`/`fileNameMap` are atomic per operation.
- Callers of `getFiles()` receive a stable snapshot — iterating it never throws CME.

## Test

Added two tests to `FileManagerTest`:

1. **`getFiles_noExceptionUnderConcurrentMutation`** — 4 writer threads call `newFileId()`
   (500 iterations each) while 4 reader threads iterate the list returned by `getFiles()`.
   Before the fix this reliably throws `ConcurrentModificationException`; after the fix it
   completes without exception. Tagged `@Tag("slow")`.

2. **`newFileId_returnsUniqueIdsUnderConcurrency`** — 8 threads each call `newFileId()` 200
   times concurrently. Verifies all returned IDs are unique and the list size matches the
   total call count.

## Test results

- New tests: PASS after fix.
- Existing `FileManagerTest`: PASS.
- `engine` module full test suite: PASS (no regressions).
