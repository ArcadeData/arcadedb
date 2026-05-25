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

## Pull Request

[#4323](https://github.com/ArcadeData/arcadedb/pull/4323)

## Review cycles

| Cycle | HEAD | Bot activity | Action taken |
|---|---|---|---|
| 1 | `5402ba3e` | gemini-code-assist: `removeIf` style + `explicitLockedFiles` mirror. claude: drop class-level Javadoc with issue ref; explicitLockedFiles follow-up. | Dropped Javadoc per memory `feedback_no_tracking_files_in_repo_root.md` and project convention. Deferred the two gemini items with justification. |
| 2 | `0fd9c75f` | gemini: no re-review (per memory). claude: drop `review-deferred-*.md` artifact; open follow-up issue for explicitLockedFiles; reflection test acceptable. | Deleted `review-deferred-5402ba3e.md`. Merged deferred-item analysis into this tracking doc. Follow-up issue creation deferred to developer (auto-mode denied). |
| 3 | `3f46c73b` | gemini: no re-review. claude: ready to merge as-is; explicitLockedFiles still flagged as low-risk follow-up. | No actionable changes. Final state. |

Final state: `clean-approval` (claude converged; gemini did not re-review follow-up pushes per documented behavior).

## Follow-up — explicitLockedFiles gap

Surfaced during PR #4323 review (gemini-code-assist + claude[bot]).

`removeFile` only clears `lockedFiles`. The sibling list `explicitLockedFiles`
(populated by `explicitLock()` at line 877, used by `checkExplicitLocks()` at
line 924/959/968) is not cleared, so a `DROP` issued between `explicitLock()`
and commit leaves a stale file ID in `explicitLockedFiles`.

`explicitLock()` requires no in-flight modifications (line 871-875), so in normal
flow the explicit lock is acquired on an empty transaction. The edge case is a
same-transaction `DROP` after explicit lock acquisition. Suggested mirror fix:

```java
if (explicitLockedFiles != null)
    explicitLockedFiles.remove(Integer.valueOf(fileId));
```

Out of scope for #4316. Open a follow-up issue if a reproducer materializes.
