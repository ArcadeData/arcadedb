# Issue #5696: subquery body referencing an outer relationship variable silently matches nothing

https://github.com/ArcadeData/arcadedb/issues/5696

## Symptom

A `EXISTS { }` / `COUNT { }` / `COLLECT { }` subquery body seeded directly from an outer row (no
leading `WITH`) that re-mentions the outer query's **relationship** variable in its own `MATCH`
clause matched nothing, even though the same body re-mentioning an outer **node** variable
correlated correctly.

```cypher
MATCH (p:Person)-[r:KNOWS]->(q)
WHERE EXISTS { MATCH (p)-[r:KNOWS]->(x) }
RETURN count(*)
```

Expected `2` (one per `KNOWS` edge), returned `0`.

## Root cause

Two compounding defects in `CypherExecutionPlan`, both in the physical-plan builder that turns a
subquery body's clauses into an execution step chain (`buildExecutionStepsWithOrder` /
`buildMatchStep`):

1. **The relationship variable gets silently dropped to anonymous.** When building the
   `MatchRelationshipStep` for a hop, the plan builder decides whether to keep the user's
   relationship variable name or discard it (`relVar = null`, enabling the GAV/fast traversal
   path) based on `CypherVariableUsage.isEdgeVariableReferenced(statement, ...)` - which only
   scans the **body's own** parsed statement for another use of the name. A seeded body executes
   from a **freshly built `CypherExecutionPlan` scoped to just the body's text**
   (`CorrelatedSubqueryRunner.planFor`), so it has no way to see that the same name is used again
   in the *enclosing* query. `r` is never referenced a second time inside `MATCH (p)-[r:KNOWS]->(x)`
   alone, so the builder decided it was fresh/anonymous and dropped the variable entirely.

2. **Even had the variable been preserved, `boundVariables` starts empty for a seeded body.**
   `buildExecutionStepsWithOrder`'s local `boundVariables` set (used to detect "this pattern
   variable is already bound from an earlier clause") is only populated incrementally as clauses
   are processed - an explicit leading `WITH p, r` populates it before the `MATCH` runs, which is
   why `EXISTS { WITH p, r MATCH (p)-[r:KNOWS]->(x) }` already worked. A body seeded with no
   `WITH` never gets that population, so even a preserved relationship variable would not be
   recognised as "already bound" at plan time.

A visible side effect of (1): once the relationship variable becomes anonymous, the unrelated
`isEdgeAlreadyUsed` relationship-uniqueness guard in `MatchRelationshipStep` (which is supposed to
stop the *same* `MATCH` clause from reusing one edge across two hops) sees the outer-bound edge
sitting on the row under the name `r` and treats it as "a different relationship already used in
this pattern", explicitly excluding the one edge that should have matched - which is why the
result was `0`, not the full (uncorrelated) traversal.

## Fix

`engine/src/main/java/com/arcadedb/query/opencypher/executor/CypherExecutionPlan.java`

- `executeWithSeedRow` now passes the seed row's own variable names into
  `buildExecutionStepsWithOrder`, which pre-populates the local `boundVariables` set from them -
  exactly what an implicit leading `WITH` importing every seeded name would have done. This closes
  defect (2) and, as a side effect, makes `boundVariables.contains(...)` available as a signal for
  defect (1) as well.
- The relationship-variable-naming decision now also keeps the name when
  `boundVariables.contains(relPattern.getVariable())`, i.e. when the name is a seed-carried outer
  binding, not just when it is textually referenced again within the body itself. This closes
  defect (1).

`engine/src/main/java/com/arcadedb/query/opencypher/executor/steps/MatchRelationshipStep.java`

- The relationship-identity check (which validates a traversed edge against a pre-bound
  relationship variable) now also checks the **incoming row itself**
  (`lastResult.getPropertyNames().contains(relationshipVariable)`) in addition to the plan-time
  `boundVariableNames` set, mirroring the runtime check `MatchNodeStep` already does for a
  pre-bound node variable. This is defense-in-depth for any future seeded-body shape where
  `boundVariables` might not have been threaded through, consistent with the reviewer note in the
  original issue: "Make the MATCH step treat a pre-bound relationship variable the way it treats a
  pre-bound node variable."

Both changes are scoped to the ordered execution-plan builder used by every seeded body (`CALL { }`
and all three subquery expressions) and by top-level queries; the separate legacy plan builder
(`buildExecutionStepsLegacy`) is never reached by a seeded body and was left untouched.

## Tests

New file:
`engine/src/test/java/com/arcadedb/query/opencypher/Issue5696ExistsRelationshipVariableCorrelationTest.java`

- `existsCorrelatesOnTheOuterRelationshipVariable` - the issue's first reproducer (`EXISTS{}`).
- `countSubqueryCorrelatesOnTheOuterRelationshipVariable` - the issue's second reproducer
  (`COUNT{}`), asserted per row.
- `aFreshRelationshipVariableInTheBodyIsUnaffected` - control: an unrelated fresh relationship
  variable in the body must still behave as unbound.
- `callSubqueryWithExplicitWithImportStillCorrelates` / `existsWithLeadingWithStillCorrelates` -
  controls for the two shapes the issue reported as already working, to guard against regressing
  them.
- `existsCorrelatesOnTheRelationshipEvenWhenTheEdgeTypeWouldOtherwiseMatchMoreThanOne` - the outer
  row's specific edge is honoured even when the source vertex has more than one edge of the same
  type, so the fix is genuinely identity-based correlation and not just "any matching edge".

Verified the tests fail before the fix (confirmed `0` instead of the expected non-zero counts on
the three defect-covering assertions, `boundRel=null` traced under debug instrumentation to the
edge-uniqueness guard silently excluding the correlated edge) and pass after it.

Ran the full `com.arcadedb.query.opencypher.**` package, including the OpenCypher TCK compliance
suite (3897 scenarios) and every existing correlated-subquery regression test
(`CypherSubqueryBodyIssue5656Test`, `CypherExistsTest`, `CypherCountSubqueryCorrelatedTest`,
`CypherCollectSubqueryCorrelatedTest`, `Issue5464ExistsRelInlineWhereTest`,
`Issue5109ExistsRelPropertyTest`, etc.): all passed, 0 failures.
