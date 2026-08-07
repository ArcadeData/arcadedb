# Issue #5835: AND/OR evaluate result-irrelevant operands instead of short-circuiting

## Root cause

Cypher's `AND`/`OR` are built as one of two independent AST node types depending on where
the expression appears:

- `TernaryLogicalExpression` (`engine/src/main/java/com/arcadedb/query/opencypher/ast/TernaryLogicalExpression.java`)
  for `RETURN`/`WITH` projections.
- `LogicalExpression` (`engine/src/main/java/com/arcadedb/query/opencypher/ast/LogicalExpression.java`)
  for `WHERE` predicates.

Both `evaluateAnd`/`evaluateOr` methods unconditionally evaluated **both** operands before
applying the three-valued-logic truth table, even when the left operand alone already
determined the boolean result (`false AND E`, `true OR E`). If the right operand's
evaluation raised a runtime error (e.g. `left('abc', -1)`), that error surfaced even though
the operand's value could not affect the outcome.

The equivalent unreachable branch of a `CASE` expression was already skipped, which made
the same runtime expression behave inconsistently depending only on which construct wrapped
it - a boolean guard (`flag AND riskyExpression`) was not safe the way an equivalent `CASE`
was.

A `WHERE`-clause optimization (`BooleanSimplifier`) already folds *literal* `true`/`false`
guards (`false AND x -> false`) at rewrite time, which is why the literal-guard `WHERE`
case looked fine in isolated testing. That rewrite pass does not run for `RETURN`/`WITH`
projections (`TernaryLogicalExpression`) at all, and even in `WHERE` it does not help when
the guard is not a literal (e.g. a comparison or a `WITH`-bound variable) - the runtime
`evaluateAnd`/`evaluateOr` methods still had the bug in the general case.

## Fix

Added true short-circuit evaluation to both `LogicalExpression.evaluateAnd`/`evaluateOr`
and `TernaryLogicalExpression.evaluateAnd`/`evaluateOr`:

- `AND`: if the left operand evaluates to `false`, return `false` immediately without
  evaluating the right operand.
- `OR`: if the left operand evaluates to `true`, return `true` immediately without
  evaluating the right operand.
- In every other case (left is `true`/`null` for `AND`, left is `false`/`null` for `OR`),
  the right operand's value is required to determine the result (including three-valued
  `null` propagation), so it is still evaluated exactly as before.

This matches Neo4j/Memgraph's observed behavior in the issue's compatibility matrix and
keeps three-valued (`null`) logic semantics unchanged - the only behavior change is that a
truly result-irrelevant operand is no longer evaluated (and can no longer throw).

## Files changed

- `engine/src/main/java/com/arcadedb/query/opencypher/ast/LogicalExpression.java`
- `engine/src/main/java/com/arcadedb/query/opencypher/ast/TernaryLogicalExpression.java`
- `engine/src/test/java/com/arcadedb/query/opencypher/Issue5835AndOrShortCircuitTest.java` (new)

## Tests

New regression test `Issue5835AndOrShortCircuitTest` (15 tests), covering both AST paths:

- `RETURN`/`WITH` (`TernaryLogicalExpression`): `false AND (static/runtime-bound error)`,
  `true OR (static error)` no longer throw.
- `WHERE` (`LogicalExpression`): `false AND (static/runtime-bound error)`,
  `true OR (static/runtime-bound error)` no longer throw, including cases where the guard
  is not a literal (so `BooleanSimplifier` cannot mask the underlying bug).
- Controls: `true AND (error)`, `false OR (error)` still evaluate the right operand and
  still throw (its value is required).
- Control: the unselected `CASE` branch still skips the runtime-bound error (unaffected by
  this fix).
- Three-valued logic regression: `null AND false = false`, `true AND null = null`,
  `null OR true = true`, `false OR null = null` all unaffected by the short-circuit change.

### Before the fix

4 of the 15 tests failed, reproducing the reported bug plus the additional runtime-bound
`WHERE`-clause case discovered while writing the regression test:

```
Issue5835AndOrShortCircuitTest.andWithFalseLeftShortCircuitsRuntimeBoundRightError
Issue5835AndOrShortCircuitTest.andWithFalseLeftShortCircuitsStaticRightError
Issue5835AndOrShortCircuitTest.orWithTrueLeftShortCircuitsStaticRightError
Issue5835AndOrShortCircuitTest.whereTrueOrShortCircuitsRuntimeBoundRightError
```

### After the fix

```
Tests run: 15, Failures: 0, Errors: 0, Skipped: 0 -- Issue5835AndOrShortCircuitTest
```

Also re-ran, with no regressions:

- `Issue5383BooleanNullFunctionTest` (8 tests)
- `com.arcadedb.query.opencypher.rewriter.*` (`BooleanSimplifierTest`, `ConstantFolderTest`,
  `ComparisonNormalizerTest`, `CompositeRewriterTest`)
- `com.arcadedb.query.opencypher.ast.*`
- CASE/WHERE/inline-filter suite: `CypherCaseTest`, `CypherInlinePatternWhereTest`,
  `CypherInlinePropertyFilterTest`, `CypherLabelCheckInWhereTest`, `CypherLabelFilteringTest`,
  `Issue5480NodeInlineWhereTest`, `Issue5489NodeInlineWhereRelVariableTest`,
  `Issue5490VarLengthRelInlineWhereTest`, `OpenCypherWhereClauseTest`,
  `WhereClauseListPredicateVariablesTest`, `OpenCypherMatchEnhancementsTest`,
  `OpenCypherOptionalMatchTest`, `Issue5294ToFloatBooleanTest`,
  `CypherUncorrelatedSubqueryCountPushDownIssue5686Test`
- NOT/EXISTS/MERGE suite: `CypherDoubleNotIssue5360Test`, `Issue4995ExistsAndNotExistsTest`,
  `OpenCypherMergeActionsTest`, `OpenCypherMergeTest`
- `mvn -pl engine -am compile` - full engine module compiles cleanly.

## Impact / recommendations

Boolean guards (`flag AND riskyExpression`, `fallbackAvailable OR riskyExpression`) are now
safe against the guarded expression raising an error when its value cannot affect the
result, matching `CASE`'s existing behavior and the observed Neo4j/Memgraph semantics. No
follow-up work identified.
