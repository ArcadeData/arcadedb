# Review deferral notes — PR #4323 cycle 1 (HEAD 5402ba3e)

## Deferred (separate scope)

### Gemini: also clear `explicitLockedFiles` in `removeFile`

> Additionally, `explicitLockedFiles` should also be updated to ensure consistency
> if a file is removed after an explicit lock has been acquired but before the
> transaction is committed.

`explicitLockedFiles` is only populated by `explicitLock()` (line 877), which guards
against any active modifications (line 871-875 throws `TransactionException` if
`indexChanges`/`immutablePages`/`modifiedPages`/`newPages` are non-empty). So in the
current code, an explicit lock is always acquired on an empty transaction, then
modifications happen, then commit moves `explicitLockedFiles → lockedFiles`.

A `removeFile` call between `explicitLock()` and commit IS theoretically possible
(e.g., a DROP within the same transaction). The current code does not clear that
entry from `explicitLockedFiles`, so `checkExplicitLocks` would later observe a
stale file ID. This is a real edge case, but it is a NEW behavior (not in scope
of issue #4316, which is strictly about the `List.remove(int)` autoboxing pitfall
on `lockedFiles`).

**Deferred to a separate issue.** Open a follow-up if a reproducer materializes.

## Skipped (disagree with justification)

### Gemini: use `removeIf(id -> id == fileId)` for consistency

> While `Integer.valueOf(fileId)` correctly fixes the autoboxing pitfall, using
> `removeIf` would be more consistent with the other removal operations in this
> method (e.g., lines 830, 835, 837, 841, and 848).

The other `removeIf` calls filter collections of complex objects (`MutablePage`,
`Page`, `Record`, `Map.Entry`) by `pageId.getFileId() == fileId` or similar.
`lockedFiles` is a `List<Integer>` of file IDs where the natural operation is
`remove(Object)`. `Integer.valueOf(fileId)` is the canonical fix for this exact
autoboxing pitfall and is exactly what the issue reporter suggested in #4316.

Skipping — minimal, idiomatic, mirrors the reporter's suggested fix.
