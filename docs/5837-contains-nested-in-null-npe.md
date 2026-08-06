# Issue #5837: IN condition throws an exception when inside CONTAINS

Issue: https://github.com/ArcadeData/arcadedb/issues/5837

## Root cause

`WHERE embedded_list CONTAINS (embedded.name IN ['CPU', 'motherboard'])` throws:

```
Cannot invoke "java.lang.Boolean.booleanValue()" because the return value of
"com.arcadedb.query.sql.parser.BooleanExpression.evaluate(...)" is null
```

`InCondition.evaluate()` correctly implements SQL three-valued logic: it returns `Boolean.TRUE`,
`Boolean.FALSE`, or `null` (UNKNOWN) when the left operand is `null` and no match is found. That is
by design and matches the SQL standard (see `NullInConditionTest`, issue #4591) - `WhereClause`
already collapses UNKNOWN to `false` at the outermost WHERE boundary
(`result != null ? result : false`).

The actual bug is in `ContainsCondition.evaluate()` (and the same copy-pasted pattern in
`ContainsAllCondition` and `ContainsAnyCondition`): the nested nested `condition`/`rightBlock`
boolean expression's `Boolean` result was unboxed directly inside an `if (... && condition.evaluate(...))`
/ `!rightBlock.evaluate(...)` expression, with no null guard. When the nested condition (e.g. an `IN`
whose left side is `null`) evaluates to UNKNOWN, auto-unboxing throws the reported NPE.

`ContainsValueCondition` has the identical unguarded-unboxing pattern for its `condition` (`OrBlock`)
field, but the ANTLR grammar (`SQLParser.g4`) only defines `expression CONTAINSVALUE expression` -
there is no `CONTAINSVALUE (whereClause)` production, so that code path is currently unreachable from
SQL. Fixed anyway for consistency/defense-in-depth; no SQL-level regression test was possible for it.

## Fix

Added a shared helper, `BooleanExpression.isTrue(Boolean)` (`Boolean.TRUE.equals(value)`), matching
the existing collapse-UNKNOWN-to-false pattern from `WhereClause.matchesFilters`. Applied it at every
unguarded unboxing site in:

- `ContainsCondition.evaluate(Identifiable, ...)` / `evaluate(Result, ...)`
- `ContainsAllCondition.evaluate(Identifiable, ...)` / `evaluate(Result, ...)`
- `ContainsAnyCondition.evaluate(Identifiable, ...)` / `evaluate(Result, ...)`
- `ContainsValueCondition.evaluate(Identifiable, ...)` / `evaluate(Result, ...)` (defensive; unreachable from SQL today)

`InCondition` itself was intentionally left unchanged - its `null` (UNKNOWN) return is correct SQL
semantics, and changing it to always return `false` would corrupt three-valued logic elsewhere (e.g.
`NOT (x IN (...))` under De Morgan's laws, already regression-tested by `NullInConditionTest`).

## Tests

New file: `engine/src/test/java/com/arcadedb/query/sql/operator/ContainsNestedInConditionNullTest.java`

- `containsWithFieldMissingOnNestedItemDoesNotThrow` - faithful reproduction of the issue's repro
  (nested condition references a field absent on the iterated item -> UNKNOWN -> previously NPE'd).
- `containsWithMatchingNestedFieldReturnsTrue` - positive control, a genuine match still returns true.
- `containsWithNullItemAmongMatchingItemsStillMatches` - an UNKNOWN item earlier in the iteration must
  not suppress a real match found later.
- `containsWithNoMatchAndNullItemReturnsFalseNotThrow` - no exception, correct `false` result.

`ContainsAllCondition`/`ContainsAnyCondition` got the same defensive `isTrue()` guard in source, but no
SQL-level test could be added for them - see the blocking pre-existing bug below, discovered while
writing those tests.

## Discovered while testing (not fixed here, out of scope): CONTAINSALL/CONTAINSANY nested-condition parsing is broken

`SQLASTBuilder.visitContainsAllCondition` / `visitContainsAnyCondition` do:

```java
condition.rightBlock = (OrBlock) whereClause.baseExpression;
```

`visitOrBlock`/`visitAndBlock` collapse a single child straight through without wrapping it (see the
same file), so `whereClause.baseExpression` is only ever an actual `OrBlock` when the nested condition
has an explicit top-level `OR`. Any bare condition - `x CONTAINSALL (name IN [...])`,
`x CONTAINSALL (name = 'CPU')`, anything without `OR` at the top - throws `ClassCastException` while
parsing, before `evaluate()` (and therefore before this fix) is ever reached. `ContainsCondition`'s
equivalent AST-builder code assigns to a `BooleanExpression`-typed field and has no such bug.
Recommend filing a follow-up issue; fixing it means widening `ContainsAllCondition.rightBlock` /
`ContainsAnyCondition.rightBlock` from `OrBlock` to `BooleanExpression`, which is a larger change than
warranted here.

Separately, `ContainsAnyCondition`'s nested-condition branch returns `false` as soon as *any* item
fails the condition and `true` only once *every* item has matched - i.e. it implements "ALL", identical
to `ContainsAllCondition`, not "ANY". Also pre-existing, also out of scope, also worth its own issue
once the parsing bug above is fixed and the branch becomes reachable at all.

## Verification

- `mvn -pl engine -am compile` - compiles clean.
- `mvn -pl engine -am test -Dtest=ContainsNestedInConditionNullTest,NullInConditionTest,ContainsConditionTest,ContainsAllConditionTest,ContainsAnyConditionTest,ContainsValueOperatorTest,ContainsKeyOperatorTest` - all pass (see PR for CI results).
