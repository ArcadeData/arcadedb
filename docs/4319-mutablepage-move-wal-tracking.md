# Fix #4319: MutablePage.move WAL tracking for backward shifts

## Summary

`MutablePage.move(startPosition, destPosition, length)` called
`updateModifiedRange(startPosition, destPosition + length)`. For backward shifts
(`destPosition < startPosition` — the case in `LocalBucket.defragPage`), the
lower bound was `startPosition` instead of `destPosition`, leaving the range
`[destPosition, startPosition)` untracked in the WAL delta. A crash after
`defragPage` would replay WAL without those bytes, corrupting the bucket page.

## Root cause

`defragPage` calls `page.move(from, to, length)` with `to < from` (shifting records
leftward into freed holes). The old `updateModifiedRange(startPosition, destPosition + length)`
used `startPosition` (= `from + PAGE_HEADER_SIZE`) as the lower bound, but the
`System.arraycopy` in `Binary.move` writes to `[destPosition, destPosition + length - 1]`,
so bytes `[destPosition, startPosition)` were written but not logged.

## Fix

`engine/src/main/java/com/arcadedb/engine/MutablePage.java:223`

```diff
- updateModifiedRange(startPosition, destPosition + length);
+ if (length > 0)
+   updateModifiedRange(Math.min(startPosition, destPosition), Math.max(startPosition, destPosition) + length - 1);
```

The lower bound is `min(start, dest)` so backward shifts start tracking at
`destPosition`. The upper bound uses inclusive semantics (`+ length - 1`)
consistent with all other `updateModifiedRange` call sites. The `length > 0`
guard prevents an inverted range when `length == 0`. For a full-page backward
shift the upper bound evaluates to `physicalSize - 1`, which is the last
valid byte index.

## Test

New test class: `engine/src/test/java/com/arcadedb/engine/MutablePageMoveTest.java`

- `backwardShiftModifiedRangeStartsAtDest` — verifies range starts at `dest + HEADER`
- `forwardShiftModifiedRangeStartsAtSource` — verifies forward shifts are unaffected
- `backwardShiftModifiedRangeCoversWrittenBytes` — verifies full written range is covered
- `samePositionShiftTracksRange` — verifies no-op shift still tracks correctly
- `moveToEndOfPageDoesNotThrow` — boundary regression: move to last content byte

## PR

https://github.com/ArcadeData/arcadedb/pull/4325

## Review cycles

### Cycle 1 — `809a11f5`

Reviewer: `gemini-code-assist` (state: COMMENTED)

Changes applied:
- Use inclusive upper bound `Math.max(start, dest) + length - 1` instead of `destPosition + length`
- Add `if (length > 0)` guard to prevent inverted range when length is zero
- Add `moveToEndOfPageDoesNotThrow` test for boundary regression

Follow-up commit: `33b7da7c`

### Final state: timeout

`claude` bot is not configured in this repository; `gemini-code-assist` does not
re-review follow-up pushes. Cycle 2 would time out with no new feedback.
