# Issue #4316 — TransactionContext.removeFile autoboxing pitfall

## Summary

`TransactionContext.removeFile(int fileId)` called `lockedFiles.remove(fileId)` where
`lockedFiles` is `List<Integer>` and `fileId` is a primitive `int`. Java resolves this to
`List.remove(int index)` (index-based) instead of `List.remove(Object)` (value-based),
silently removing the wrong entry or throwing `IndexOutOfBoundsException` when
`fileId >= lockedFiles.size()`.

## Root cause

`engine/src/main/java/com/arcadedb/database/TransactionContext.java` line 844 (pre-fix):

```java
if (lockedFiles != null)
    lockedFiles.remove(fileId);   // int primitive → List.remove(int index), NOT List.remove(Object)
```

## Fix

`engine/src/main/java/com/arcadedb/database/TransactionContext.java` — box explicitly:

```java
if (lockedFiles != null)
    lockedFiles.remove(Integer.valueOf(fileId));
```

## Test

`engine/src/test/java/com/arcadedb/database/TransactionContextRemoveFileTest.java`

Three cases:
1. `removeFileRemovesValueNotIndex` — injects `lockedFiles = [10, 20, 30]`, calls `removeFile(20)`,
   asserts result is `[10, 30]`. Without the fix: `IndexOutOfBoundsException` (list size 3,
   requested index 20).
2. `removeFileForAbsentIdIsNoOp` — fileId not in list, list unchanged.
3. `removeFileWhenLockedFilesNullIsNoOp` — `lockedFiles == null`, no exception.

## Verification

```
Tests run: 3, Failures: 0, Errors: 0 (TransactionContextRemoveFileTest)
Tests run: 7, Failures: 0, Errors: 0 (TransactionCallbackTest, MVCCTest, LockFilesInOrderFileMigrationTest)
```
