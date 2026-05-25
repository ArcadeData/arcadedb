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
+ updateModifiedRange(Math.min(startPosition, destPosition), destPosition + length);
```

The lower bound is now `min(start, dest)` so backward shifts correctly start
tracking at `destPosition`. The upper bound stays `destPosition + length`,
which is always safe — `content.move` already validates that
`destPosition + length <= content.length` before we reach this call.

Note: the issue's suggested fix used `Math.max(start, dest) + length` for the
upper bound, but that over-extends the range for backward shifts where
`start + length` can reach the page boundary and trigger the bounds check in
`updateModifiedRange`. Using `destPosition + length` is both correct and safe.

## Test

New test class: `engine/src/test/java/com/arcadedb/engine/MutablePageMoveTest.java`

- `backwardShiftModifiedRangeStartsAtDest` — verifies range starts at `dest + HEADER`
- `forwardShiftModifiedRangeStartsAtSource` — verifies forward shifts are unaffected
- `backwardShiftModifiedRangeCoversWrittenBytes` — verifies full written range is covered
- `samePositionShiftTracksRange` — verifies no-op shift still tracks correctly
