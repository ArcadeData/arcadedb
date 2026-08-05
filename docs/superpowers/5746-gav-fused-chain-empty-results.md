# #5746: openCypher multi-hop patterns return zero rows when planned through GAVFusedChain

## Problem

With a Graph Analytical View active, an openCypher multi-hop pattern anchored by an inline property
map returned nothing:

```cypher
MATCH (p:Person {id: 0})-[:KNOWS]->(a:Person)-[:KNOWS]->(b:Person) RETURN DISTINCT b.id AS id
```

SQL `MATCH` over the same data was correct, the same Cypher query was correct before the view
existed, and rewriting the anchor filter as `WHERE p.id = 0` worked. No error and no warning: the
caller got an empty result set indistinguishable from a legitimate "no matches".

## Cause

Two independent defects on the fused-chain path, both of which had to be fixed.

**1. A filter pushed into the chain could read a variable the chain never materialized.**
`CypherOptimizer.fuseGAVExpandChain` decides which pattern variables to materialize as `Vertex`
objects by scanning WHERE, WITH, and RETURN; variables absent from that set are pure stepping stones
and get no output slot. Immediately afterwards the same method pushes the `FilterOperator` sitting
above the chain *into* it (`setPushedFilter`), where it is evaluated per path before a row is
emitted. An inline property map, `MATCH (p:Person {id: 0})`, becomes exactly such a filter, and it
names `p`, a variable that WHERE/WITH/RETURN need never mention. With `p` unmaterialized, `p.id`
read back null, `p.id = 0` was never true, and every row was dropped inside the chain.

That is why the reported discriminator was the inline map rather than the edge types: the
`WHERE p.id = 0` spelling puts the predicate in the statement WHERE clause, which the materialize
scan already covered. `applyDeferredVertexLoading` computed the same variable set the same way, so
the non-fused `GAVExpandAll` path carried the same latent defect for any filter left standing.

**2. The chain's output name and value arrays could disagree on slot 0.**
`GAVFusedChainOperator.buildOutputNames()` unconditionally reserved slot 0 for the source variable
(an `if/else` where both branches did the same thing), while `emitResult()` only wrote that slot
when the source was materialized. When it was not, every remaining variable shifted one slot: each
read back the *next* variable's vertex, and the last read back null. This surfaced independently of
any filter, for example `MATCH (p:Person)-[:KNOWS]->(a)-[:KNOWS]->(b) RETURN DISTINCT b.id`.

## Fix

- `CypherOptimizer.fuseGAVExpandChain`: add the pushed filter's variables to the materialize set.
- `CypherOptimizer.applyDeferredVertexLoading`: add the variables of every `FilterOperator` still in
  the operator tree, via a new `collectFilterVariables` walk that descends the same operator types
  `markDeferredRecursive` does.
- Extracted the duplicated materialize-set computation into `collectDownstreamVariables()` so the
  two call sites cannot drift apart again.
- `GAVFusedChainOperator.buildOutputNames()`: only reserve slot 0 when the source is materialized,
  matching `emitResult()`.

No behaviour change without a view, and no change to the traversal hot loop. Forcing an extra
variable to be materialized only adds a cheap `GAVVertex` wrapper, not an OLTP load.

## Regression test

`engine/src/test/java/com/arcadedb/query/opencypher/CypherGAVFusedChainIssue5746Test.java`.

Because a view is a performance feature, the oracle is that a query returns the same rows with and
without the view; every scenario is asserted both ways by `withAndWithoutView`. Covered shapes:
projection of the start, middle, and end of the chain; both ends together; `count(*)`;
`count(DISTINCT b)`; the pattern split across two MATCH clauses; mixed edge types; three hops; the
`WHERE`-clause workaround; and unfiltered multi-hop.

Both fixes were confirmed load-bearing by reverting each in isolation and observing the
corresponding test fail.

- `CypherGAVFusedChainIssue5746Test`: 12/12 pass; 6 of 11 failed before the fix.
- `com.arcadedb.query.opencypher.**`: 7936 tests, 0 failures.
- `com.arcadedb.graph.**` (includes `GraphAnalyticalViewTest`): 443 tests, 0 failures.

## Impact and workaround

Silent wrong answers on any fused multi-hop Cypher pattern where a variable bound by the chain was
not named by WHERE/WITH/RETURN. Only reachable with a Graph Analytical View present, which is why it
read as "I added a view to make my graph queries faster and they started returning nothing".

Workaround before this fix: spell the anchor filter as a `WHERE` clause instead of an inline
property map, or project the anchor variable so it gets materialized.

## Context

Issue #5306 shares the visible symptom but not the cause: that one was anchor selection over
unidirectional edges. This one is variable materialization inside the fused chain and reproduces on
the default bidirectional edge type.

Follow-up worth its own issue: `CypherOptimizer.collectExpressionVariables` extracts variables by
splitting `Expression.getText()` on a non-identifier regex, so function names and literals also
enter the materialize set. The failure mode is over-materialization, which is safe, but it is a
fragile heuristic that deserves a real AST walk.
