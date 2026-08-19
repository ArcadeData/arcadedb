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

## PR

https://github.com/ArcadeData/arcadedb/pull/6477

## Review cycles

- **Cycle 1** (head `34f61f6`): Claude review flagged two items:
  1. Fully-qualified `java.util.*` names in the new test (style, contradicts `CLAUDE.md`).
  2. A multivalue (list/array) right-hand side would now be stringified via `List.toString()` into a
     bracket-notation substring search instead of the old unconditional `false`, with the existing
     `ContainsTextWithArrayTest.containsTextWithArrayOfStrings` now passing "by coincidence" rather than by design.
  Both applied: added a `MultiValue.isMultiValue(rightValue)` guard (mirroring `FetchFromIndexStep`'s own guard) so
  a multivalue right-hand side still answers "no match", fixed the FQNs, and added two more regression tests
  (`multiValueRightHandSideStillMatchesNothing`, `nullRightHandSideStillMatchesNothing`) per the reviewer's
  "nice to have" suggestion. Pushed as `2178ae5`.
- **Cycle 2** (head `2178ae5`): Claude review found the fix itself correct and the new test coverage "more thorough
  than the minimum needed", but flagged one stale neighboring doc comment: `FetchFromIndexStep.java`'s Javadoc for
  `processFullTextBlock` said `ContainsTextCondition#evaluate` treats "a non-String value" as never a match, which
  this PR made false (only `null` and multivalue are never a match now). Applied: reworded that comment. Pushed as
  `85ee3e4`.
- **Cycle 3** (head `85ee3e4`): Claude review - "No blocking issues found." One non-blocking observation (whether
  `docs/<issue>.md` tracking files are the right long-term convention) explicitly framed as "a call for the
  maintainers on repo convention, not a defect" - no action taken, this file follows the same convention every
  other tracked issue in this repo's `docs/` directory already uses. **Clean approval, loop ended.**

## Deferred items

None.

## Final state

`clean-approval` after 3 review cycles.
