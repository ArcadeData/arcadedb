# Issue #6573: isEdgeVariableReferenced has no CREATE/MERGE case

## Summary

`CypherVariableUsage.isEdgeVariableReferenced` decides whether a MATCH-bound relationship variable
keeps its real binding (vs. being anonymized for the CSR/GAV fast path) by walking every clause type
in `statement.getClausesInOrder()`. The top-level switch handled `RETURN`, `WITH`, `UNWIND`, `SET`,
`REMOVE`, `DELETE`, `FOREACH`, `SUBQUERY` - but had no `case CREATE` / `case MERGE`, so it fell to
`default` and the clause was never inspected.

Contrast with `foreachReferencesVariable`'s own inner switch (same file), which already treats a
nested `CREATE`/`MERGE` conservatively (`return true`). The top-level switch never got the same
treatment.

## Reproduction (from the issue)

```cypher
CREATE (a:Person {id:'a'})-[r:KNOWS {since: 2001}]->(b:Person {id:'b'})
```
```cypher
MATCH (a:Person)-[r:KNOWS]->(b:Person) CREATE (c:Note {since: r.since}) RETURN c.since AS s
```

Expected: `s = 2001`. Actual (before fix): `s = null` - the relationship step bound `r` under an
internal anonymous name (since nothing else in the query appeared, to this check, to reference it),
so the CREATE clause's `r.since` read a missing binding and silently evaluated to `null`.

## Fix

Added a `case CREATE: case MERGE:` arm to the top-level switch in `isEdgeVariableReferenced`,
mirroring `foreachReferencesVariable`'s conservative `return true`. A false positive there only
forgoes the CSR/GAV fast path; a false negative silently drops a still-referenced edge, which is the
failure class this whole method exists to prevent.

File: `engine/src/main/java/com/arcadedb/query/opencypher/executor/CypherVariableUsage.java`

## Tests

- `CypherVariableUsageTest.createReadsItInAnInlineProperty` / `.mergeReadsItInAnInlineProperty` -
  unit-level checks parallel to the existing cases in that file (`setReadsItAsTargetAndAsValue`,
  `removeReadsIt`, etc.), verified to fail without the fix.
- `Issue6573CreateMergeEdgeVariableTest` - new end-to-end regression test running the issue's own
  reproducer through the real Cypher engine (`createReadsTheEdgePropertyThroughAnInlineExpression`,
  `mergeReadsTheEdgePropertyThroughAnInlineExpression`), also verified to fail (with a `null` result,
  matching the issue's reported symptom exactly) without the fix, via a temporary `git stash` of the
  production change.

## Verification

- `CypherVariableUsageTest` + `Issue6573CreateMergeEdgeVariableTest`: 23/23 pass.
- Full `com.arcadedb.query.opencypher.**` package (excluding benchmark/slow/vector lanes): 8906
  tests, 0 failures, 0 errors, 98 skipped (pre-existing skips, unrelated to this change).
- Confirmed both new tests fail without the fix (temporarily stashed the production change) and pass
  with it restored.
