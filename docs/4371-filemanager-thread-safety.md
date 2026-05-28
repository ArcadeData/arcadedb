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
