# Issue #6438: CONTAINSTEXT with a non-String argument diverges by index presence

## Root cause

`ContainsTextCondition.evaluate()` (both overloads, in
`engine/src/main/java/com/arcadedb/query/sql/parser/ContainsTextCondition.java`) refused any right-hand value that
was not already a `String`:

```java
final Object rightValue = right.execute(currentRecord, context);
if (!(rightValue instanceof String))
  return false;
```

This is the fallback path used whenever no full-text index covers the property. The index path
(`FetchFromIndexStep.processFullTextBlock` -> `LSMTreeFullTextIndex.get`) has no such restriction: it takes whatever
non-null, non-multivalue value the right expression evaluates to and calls `.toString()` on it before analyzing/
searching. So `title CONTAINSTEXT 2024` matched a row with `title = 'report 2024'` when `title` was covered by a
full-text index, and matched nothing when it was not - the same operator meaning two different things depending on
an index that has nothing to do with the query's semantics.

## Fix

Chosen direction (per the issue's own recommendation - "the more useful behaviour"): **coerce in the evaluator**,
matching what the index path already does. `ContainsTextCondition.evaluate()` now stringifies any non-null
right-hand value with `.toString()` instead of rejecting non-`String` values outright. `null` still means "no match"
(unchanged), and the left-hand side is unchanged - it must already be a `String` (the text being searched in).

## Tests

`engine/src/test/java/com/arcadedb/query/sql/operator/Issue6438ContainsTextNonStringArgumentTest.java`:
- `numericLiteralMatchesTheSameWayIndexedAndUnindexed` - the same numeric literal against the same text matches on
  both an indexed and an unindexed property (the assertion that could not pass before the fix, per the issue body).
- `numericLiteralWithNoMatchingSubstringMatchesNothingEitherWay` - a numeric literal that is not a substring still
  matches nothing (no false positives introduced by the coercion).

## Verification

- `mvn -o -pl engine -am test -Dtest=Issue6438ContainsTextNonStringArgumentTest -Dmaven.repo.local=<worktree>/.m2repo`
- Full `ContainsText*` / full-text index regression suite re-run to confirm no regressions.
