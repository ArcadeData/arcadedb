# Issue #6579: Native select `.limit(0)` returns count 1 instead of 0

## Root cause

`SelectExecutor.executeCount()` incremented `count` before comparing it against `select.limit`:

```java
count++;
if (select.limit > -1 && count >= select.limit)
  break;
```

For `limit == 0`, the first matching record already bumped `count` to `1` before the `count >= limit`
(`1 >= 0`) check ever got a chance to see `limit == 0` - so the wrong value (`1`) was already counted by
the time the loop broke.

## Fix

Reordered the check to run before the increment (`engine/src/main/java/com/arcadedb/query/select/SelectExecutor.java`,
`executeCount()`):

```java
if (select.limit > -1 && count >= select.limit)
  break;
if (filterOutRecords != null)
  filterOutRecords.add(record.getIdentity());
count++;
```

## Investigated but not affected

The issue asked to also check `SelectIterator`'s `hasNext()`/`fetchNext()` path (used by `.documents()`/
`.vertices()`/etc.). Traced it: the constructor only consumes `skip` records (0 iterations when
`skip == 0`), and `hasNext()` checks `returned >= (long) limit + skip` **before** calling `fetchNext()`.
For `limit == 0, skip == 0` that is `0 >= 0`, true, so `hasNext()` returns `false` immediately without
ever pulling a record. Same reasoning holds for the `ORDER BY` materialization path
(`fetchResultInCaseOfOrderBy()`), whose `end = min(materialized.size(), skip + limit)` is `0` for
`limit == 0`. Confirmed with the existing `okLimit`/`okSkip` tests plus manual review - no code change
needed there.

## Test

Added `SelectExecutionTest.okCountWithLimitZero()` (TDD: written first, confirmed red against the
pre-fix code with `expected: 0L but was: 1L`, green after the fix). Covers both repro shapes from the
issue:
- no-`WHERE` (unindexed full-type-scan) path
- `AND`-shaped `WHERE` (the uncapped fallback path)

## Verification

- `mvn -pl engine -am test -Dtest=SelectExecutionTest` - 26/26 pass
- `mvn -pl engine -am test -Dtest='Select*Test'` - 315/315 pass (all native-select and SQL select suites)
- `mvn -pl engine -am test -Dtest=Issue6565SelectIndexCandidateLimitTest` - 20/20 pass (adjacent
  candidate-cap logic touched by the same class, per the issue's own cross-reference to #6571/#6565)

## PR

https://github.com/ArcadeData/arcadedb/pull/6683

## Review cycles

- **Cycle 1** - head `e718976202d273e2d9c2957fa35d1956e5416177`. `claude` review: no blocking issues;
  confirmed the reorder is correct, confirmed `limit > 0` behavior is unchanged by hand-tracing, and noted
  the already-safe indexed-cursor path (`MultiIndexCursor` stops before pulling any candidate for
  `limit(0)`). One optional nit: add an assertion pinning that indexed-cursor path against a future
  regression in `computeExactCandidateLimit()`. Applied (see `okCountWithLimitZero`'s third assertion,
  `.where().property("id").eq().value(5).limit(0).count()`); no deferred items.
- **Cycle 2** - head `bc85c6c51f9abaea4317eabdd6ee97019d77ae73`. `claude` review found a real regression
  the cycle-1 fix introduced: the pre-increment `limit == 0` guard, on its own, moves the `break` so it
  only fires from inside the `evaluateWhere()` match branch - so once `limit` is reached, the loop can
  only stop on the *next* matching record, not the current one. With a selective WHERE and no
  `(limit+1)`th match, `.where(...).limit(N).count()` degrades from an early-exit scan into a full scan of
  the rest of the type. None of the existing tests caught it because they all use a WHERE matching every
  row (or a unique-indexed single hit), so there's always an "(N+1)th" match available - and the returned
  count is identical either way, only the number of records scanned changes. Verified the analysis by hand
  and by reproducing it (temporarily reverting to the single-check version and confirming the suggested
  regression test goes red). Fixed by restoring the original post-increment `count >= limit` check
  alongside the new pre-increment one (the post-check is what gives the same-iteration early exit for
  `limit > 0`; the pre-check only ever fires for `limit == 0`, since `count` can't reach a `limit > 0`
  without the post-check already breaking the loop first). Added
  `okCountWithLimitStopsEarlyDespiteReorder()` (also suggested by the review) to pin `evaluatedRecords` for
  a `WHERE` with exactly one match followed by 500 non-matching records - proved it fails against the
  single-check version before restoring the fix. No deferred items.
- **Cycle 3** - head `4bf062d2382389b447e92a52c76ae6f4277be852`. `claude` review: clean approval. Manually
  re-traced the pre/post-increment control flow for `limit == 0`, `limit > 0`, and `skip` interaction and
  confirmed all three are correct; called out `okCountWithLimitStopsEarlyDespiteReorder` as "the right way
  to catch a 'correct count, wrong complexity' regression that a value-only assertion would miss". One
  non-blocking style nit (the new inline comment is long/all-caps, though consistent with the file's
  existing `#6565`/`#5662` comment convention) - no action needed. No correctness, performance, or
  security issues found; working tree stays clean. **Final state: clean-approval.**

## Deferred items

None.
