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
