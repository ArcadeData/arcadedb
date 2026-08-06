# Issue #5825: EXISTS subquery silently accepts a variable dropped by WITH

## Summary

`WHERE EXISTS { ... }` (and the sibling `COUNT { ... }` / `COLLECT { ... }` subquery expressions)
did not run the parse-time variable-scope check that ordinary `RETURN`/`WITH` references already go
through. A variable dropped from scope by a preceding `WITH` was therefore silently treated as
missing/`null` inside the subquery body instead of raising `UndefinedVariable`, so the predicate just
evaluated to `false` (or an empty collection) rather than the client-facing scope error Neo4j and
Memgraph both raise.

## Root cause

`CypherSemanticValidator.checkExpressionScope(Expression, Set<String>)`
(`engine/src/main/java/com/arcadedb/query/opencypher/parser/CypherSemanticValidator.java`) is the
scope-aware, recursive validator that already threads the live `Set<String> scope` into every other
expression form (list comprehension, list predicate, `CALL { }` bodies via
`validateSubqueryScope`/`validateVariableScope`). It had no `instanceof` branch for
`ExistsExpression`, `CountExpression`, or `CollectExpression`, so those expressions fell through to
the trailing "no variables to check" comment and were never scope-checked at all.

This is a distinct mechanism from `validateNestedStatements`/`NestedStatementChecks`, the generic
walk that runs the other ten validation phases against every subquery body at any depth (issue
#5656). That walk is deliberately *not* used for scope validation, because scope validation needs the
scope live *at the point the expression was written*, which the generic walk does not carry.

## Fix

Added `ExistsExpression`/`CountExpression`/`CollectExpression` branches to `checkExpressionScope`,
each delegating to a new `checkSubqueryExpressionScope` helper that re-runs `validateVariableScope`
against the body's own `CypherStatement` (or each branch of a `UnionStatement`), seeded with a copy
of the current `outerScope`. No import gate is applied (unlike `CALL { }`): the whole live scope is
visible, mirroring how `CorrelatedSubqueryRunner.seedRow` hands the entire outer row to the body at
runtime. A `null` parsed subquery (the best-effort AST build declining the body, issue #5626) is
skipped, matching the existing fallback-to-text-execution behavior for that edge case.

File changed: `engine/src/main/java/com/arcadedb/query/opencypher/parser/CypherSemanticValidator.java`.

## Tests

New file: `engine/src/test/java/com/arcadedb/query/opencypher/Issue5825ExistsSubqueryScopeTest.java`

- `existsSubqueryReferencingDroppedVariableThrows` — the issue's failing repro (`EXISTS { ... }`),
  now raises `CommandSemanticException` mentioning `v`.
- `countSubqueryReferencingDroppedVariableThrows` — same shape for `COUNT { ... }`.
- `collectSubqueryReferencingDroppedVariableThrows` — same shape for `COLLECT { ... }`.
- `directReferenceToDroppedVariableThrows` — control, the pre-existing direct-reference rejection
  (unchanged behavior).
- `existsSubqueryReferencingPreservedVariableIsValid` — control, keeping `v` in scope still returns
  the expected two rows.

Before the fix, the three EXISTS/COUNT/COLLECT tests failed with "Expecting code to raise a
throwable" (the query silently succeeded with an empty/wrong result), confirming the tests exercise
the bug. After the fix, all five pass.

## Verification run

- `Issue5825ExistsSubqueryScopeTest`: 5/5 pass.
- Directly related existing regression tests re-run clean: `Issue5213NestedCallScopeTest` (5),
  `CypherSubqueryParseTimeValidationIssue5626Test` (30), `Issue5257CallSubqueryRelationshipScopeTest`
  (10), `Issue5179PatternComprehensionScopeTest` (4).
- Broader EXISTS/COUNT/COLLECT test surface (30 test classes covering correlated subqueries,
  count-pushdown optimizations, pattern predicates, foreach/case interactions, etc.): all pass, no
  regressions.
