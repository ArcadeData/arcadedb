# Issue #4371 - FileManager thread-safety fix

## Root Cause

`FileManager.files` is a plain `ArrayList<ComponentFile>` (line 31). Several methods mutate
or iterate it without holding the intrinsic lock:

| Method | Access | Synchronized? |
|---|---|---|
| `newFileId()` | `files.add(RESERVED_SLOT)` | YES (method-level) |
| `registerFile()` | `files.size()`, `files.add(null)`, `files.set()` | NO |
| `dropFile()` | `files.set(fileId, null)` | NO |
| `getFiles()` | wraps live list in unmodifiable view | NO |
| `close()` | `files.clear()` | NO |

Because `newFileId()` holds the lock on `this` while other methods access `files` without it,
the backing ArrayList is exposed to concurrent structural modification. This causes:

- `ConcurrentModificationException` when `getFiles()` is iterated while `newFileId()` adds a slot
- `ArrayIndexOutOfBoundsException` when `registerFile()` grows the list while `newFileId()` also modifies it
- Torn reads: `fileIdMap.containsKey(id) == true` while `files.get(id) == null`

## Fix

Guard all mutations and reads of `files` with the same `this` lock that `newFileId()` already uses:

1. `dropFile()` - add `synchronized`
2. `getOrCreateFile(String, String, MODE)` - add `synchronized`
3. `getOrCreateFile(int, String)` - add `synchronized`
4. `getFiles()` - add `synchronized` and return a snapshot copy (not a live view) to prevent CME on callers iterating the returned list
5. `close()` - add `synchronized`

`registerFile()` (private) is called from the constructor (single-threaded) and from `getOrCreateFile()` (now synchronized), so it remains undecorated - it is always called under the lock after the fix.

## Test

New test in `FileManagerTest`: `getFiles_concurrentWithNewFileId_noConcurrentModificationException`
- 8 writer threads each call `newFileId()` 500 times
- 8 reader threads each call `getFiles()` and fully iterate the result 500 times
- Both sets of threads start simultaneously via a `CountDownLatch`
- Asserts no `ConcurrentModificationException` was thrown

## Verification

- New test reproduces CME with current code; passes after fix
- Existing `FileManagerTest` tests continue to pass

## PR

https://github.com/ArcadeData/arcadedb/pull/4399

## Review Cycles

### Cycle 1 - SHA 08b5b07a

Gemini reviewed (3 medium-priority inline comments); Claude did not respond (timeout after 15 min).

Gemini feedback applied:
- `dropFile()`: narrowed lock scope - `file.drop()` moved outside the synchronized block so blocking disk I/O does not hold the mutex. Early `return` inside the lock replaced with `else` branch to preserve the existing behavior of always calling `file.drop()` when the file is non-null (Gemini's suggestion had a subtle bug in this path that was corrected).
- `getOrCreateFile(String, String, MODE)`: lock-free first lookup on `fileNameMap` (ConcurrentHashMap), double-checked inside `synchronized` block only when absent.
- `getOrCreateFile(int, String)`: same double-checked locking pattern using `fileIdMap`.

Commit: eb83aee2

### Cycle 2 - SHA eb83aee2

Neither bot responded within the 15-minute timeout.

## Final State

`timeout` - loop stopped after cycle 2 timeout with no bot reviews on the post-review push. PR is left open for developer merge.
