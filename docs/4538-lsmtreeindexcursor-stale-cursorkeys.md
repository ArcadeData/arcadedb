# Issue #4538 — LSMTreeIndexCursor uses stale `cursorKeys[i]` after the `removedKeys` branch

## Summary

`LSMTreeIndexCursor`'s constructor validates each underlying page cursor. In the
`removedKeys.contains(keys)` branch (around lines 188-196) the code advances the
page cursor with `pageCursor.next()` and `continue`s the loop, but it does NOT
update `cursorKeys[i]`. The subsequent iteration's exclusive-`fromKeys` check
(line ~199) reads the stale `cursorKeys[i]` while the `removedKeys` check (line
~188) and the `toKeys` check (line ~212) read the fresh `pageCursor.getKeys()`.

Result: the predicate that decides whether the exclusive lower-bound key must be
skipped runs against the previous (stale) key, so a `range(..., exclusive)` scan
that follows a delete-then-reinsert sequence may include the excluded boundary
key or skip a valid entry.

## Root cause

`cursorKeys[i]` is the cached "current key" for page cursor `i`. Every place that
advances the cursor must keep this cache in sync. The `removedKeys` branch was
the only advance site that forgot to refresh the cache before `continue`.

## Fix

After `pageCursor.next()` in the `removedKeys` branch, refresh
`cursorKeys[i] = pageCursor.getKeys()` before `continue`, mirroring the
exclusive-`fromKeys` branch that already does this.

## Affected components

- `engine/src/main/java/com/arcadedb/index/lsm/LSMTreeIndexCursor.java`

## Verification

- New regression test exercising an exclusive-lower-bound range scan after a
  delete-then-reinsert sequence on an LSM tree index spanning multiple pages.
- Existing `LSMTreeIndexTest` range/scan suite to confirm no regression.
