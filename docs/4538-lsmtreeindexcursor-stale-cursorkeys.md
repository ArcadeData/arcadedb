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

- New regression test `LSMTreeIndexCursorStaleKeyTest` reproducing the bug: a
  key-wide tombstone on the minimum key isolated on the newest page while older
  pages hold live copies. Fails on the unfixed code (the entry right after the
  removed key is dropped) and passes with the fix. A second method guards the
  delete-then-reinsert variant from the issue.
- Existing `LSMTreeIndexTest` range/scan suite (inclusive/exclusive bounds,
  `rangeFromHead`) and related index tests confirm no regression. The
  `LockFilesInOrderFileMigrationTest` failure on this checkout is pre-existing and
  unrelated (verified on the unmodified baseline).

## Pull request

- PR: https://github.com/ArcadeData/arcadedb/pull/4570

## Review cycles

### Cycle 1 - head 7d42ea6a

- Reviews: `claude[bot]` (PR comment), `gemini-code-assist` (no actionable items),
  `codacy-production` (0 issues). Core fix approved as correct, minimal, and safe.
- Applied (actionable & clear):
  - Trimmed the source comment to a single short line, matching the sibling
    exclusive-`fromKeys` branch that does the same assignment uncommented.
  - Trimmed the test class-level Javadoc to one concise sentence (method Javadocs
    already explain the page topology).
- Skipped (with rationale):
  - Removing this tracking doc: kept intentionally - the resolve-issue workflow
    mandates and updates it; it is a workflow-owned artifact, not ad-hoc docs.
- Deferred to a follow-up issue (out of scope for #4538):
  - The `validIterators == 0` advance path also calls `pageCursor.next()` without
    refreshing `cursorKeys[i]`. claude flags it as a narrower, structurally-similar
    defect "worth a follow-up issue"; folding it in would broaden scope beyond the
    reported bug.

## Final state

- max-cycles-reached / handed off: both gating bots reviewed cycle 1; actionable
  cosmetic feedback applied and pushed. Merge remains the developer's
  responsibility.
