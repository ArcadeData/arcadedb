# Issue #5800: Node-valued property is dereferenceable in the writing query but becomes a non-dereferenceable `DatabaseRID` after commit

## Summary

`SET holder.ref = target` (where `target` is a node) succeeds, and the assigned value is still
the live `Vertex` during the writing query, so `holder.ref.id` correctly resolves against it.
ArcadeDB persists the assigned node as a LINK/RID (`DatabaseRID`), which is the intended,
documented storage representation for a NODE-valued Cypher property. In a subsequent
transaction, `MATCH (holder:T) RETURN holder.ref.id` raised:

```
TypeError: Cannot access property 'id' on DatabaseRID value
```

surfaced as an HTTP 500 by the HTTP command API, breaking the transaction-boundary
independence of the same property-access expression.

## Root cause

The OpenCypher expression parser builds a chained property-access AST node,
`CypherExpressionBuilder.ChainedPropertyAccessExpression`, whenever a `.` postfix is applied
to something other than a plain variable (e.g. the outer `.id` in `holder.ref.id`, chained onto
the inner `holder.ref` property access). Its `evaluate()` method handled `Document`, `Map`,
`Result`, and several temporal types as the base value, but not `RID` (or its `DatabaseRID`
subclass).

The sibling AST node used for single-level, variable-bound property access,
`com.arcadedb.query.opencypher.ast.PropertyAccessExpression`, already dereferences a `RID` base
value via `rid.asVertex().get(propertyName)` (used today for lazy vertex resolution in
algorithm procedures). `ChainedPropertyAccessExpression` never got the equivalent branch, so
once `holder.ref` resolved (during evaluation of the inner `PropertyAccessExpression`) to a
persisted `DatabaseRID` instead of a live `Vertex`, the outer chained `.id` access fell through
to the generic "not a property-bearing type" `CommandExecutionException`.

## Fix

Added the missing `RID` branch to `ChainedPropertyAccessExpression.evaluate()` in
`engine/src/main/java/com/arcadedb/query/opencypher/parser/CypherExpressionBuilder.java`,
mirroring the existing behavior in `PropertyAccessExpression`: a `RID` base value is
transparently dereferenced via `rid.asVertex().get(propertyName)`. This keeps ArcadeDB's
intentional NODE-to-LINK/RID conversion on write, while making the persisted value
dereferenceable through the same expression shape across the transaction boundary - matching
the second option the issue proposed ("preserve stable dereference semantics across
transaction boundaries").

No storage format or write-path change was needed or made; this is a read-path fix only.

## Tests

New regression test:
`engine/src/test/java/com/arcadedb/query/opencypher/CypherNodeValuedPropertyDereferenceIssue5800Test.java`

- `chainedPropertyAccessOnPersistedLinkDereferencesAfterCommit` - reproduces the exact issue
  scenario: writes `holder.ref = target` and confirms `holder.ref.id` still works in the writing
  query, then opens a new transaction and confirms `holder.ref.id` resolves to `42` instead of
  throwing.
- `directPropertyAccessOnPersistedLinkStillDereferences` - control test pinning down that the
  single-level, variable-bound property access path (`WITH holder.ref AS r RETURN r.id`), which
  already handled `RID` before this fix, keeps working.

Both tests failed before the fix (the first with the exact reported `TypeError`, matching the
stack trace through `ChainedPropertyAccessExpression.evaluate`) and pass after it.

## Verification

- Reproduced the bug manually against the pre-fix `engine` jar with a standalone driver script
  replaying the issue's exact Cypher statements; observed the same `TypeError` reported in the
  issue.
- `mvn -pl engine test -Dtest=CypherNodeValuedPropertyDereferenceIssue5800Test`: failed before
  the fix (2 errors, matching the exact reported `TypeError` and stack trace through
  `ChainedPropertyAccessExpression.evaluate`), passed after it (2/2).
- Re-ran the standalone driver script against the fixed `engine` jar: `holder.ref` still reads
  back as a `DatabaseRID` (`#1:1`), and `holder.ref.id` now resolves to `42` instead of raising
  `TypeError`.
- Targeted regression sweep across property/SET-related OpenCypher unit tests (inline property
  filters/normalization/pattern binding, dynamic property mutation, `SET`, subquery parsing,
  edge mandatory property, parameterized property maps, existing property-access type-error
  test #5285, etc.): all passed.
- Broad regression sweep across `com.arcadedb.query.opencypher.{ast,executor,parser,procedures,
  rewriter,optimizer,functions}` (excluding the `benchmark`/`slow`-tagged and TCK-suite tests,
  per repository convention): see PR for pass/fail summary.

## Impact analysis

This is a narrowly-scoped read-path fix inside one AST node's `evaluate()` method. It only
changes behavior for the specific case that previously threw an exception (`RID` base value
reaching `ChainedPropertyAccessExpression`); all previously-handled base types (`Document`,
`Map`, `Result`, temporal types) are unaffected. `rid.asVertex()` follows the same lazy-loading
pattern already used by `PropertyAccessExpression`, so there is no new performance concern.

## Recommendations

- If ArcadeDB ever wants Neo4j-identical semantics (rejecting a NODE-valued property assignment
  outright, rather than converting it to a LINK/RID), that would be a separate, larger behavior
  change requiring its own design discussion - out of scope for this fix, which preserves
  ArcadeDB's existing intentional LINK-property behavior and only fixes its read-path
  inconsistency.
- Consider whether `holder.ref.someUndeclaredProp` (dereferencing a RID whose target vertex has
  since been deleted) should raise a clearer, Cypher-flavored error than the raw
  `RecordNotFoundException` `asVertex()` would throw - not reproduced or in scope here, but
  worth a follow-up issue if it surfaces in practice.
