# #5690: bound-target hop cardinality estimate assumes filtering, but it can multiply

Issue: https://github.com/ArcadeData/arcadedb/issues/5690

## Root cause

`CypherOptimizer.createExpandIntoOperator` estimates the output of a hop whose far end is already
bound (a "bound-target" hop, marked `⭐ BOUND-TARGET` in `EXPLAIN` output) as:

```java
final long outputCardinality = (long) (inputCardinality * DEFAULT_EXPAND_INTO_SELECTIVITY); // 0.1
```

`DEFAULT_EXPAND_INTO_SELECTIVITY = 0.1` encodes "this hop filters": of ten input rows, one survives.
That was correct back when the operator behaved as a semi-join (issue predates #5684/#5663). Since
#5684 the operator is an expansion - it emits one row **per relationship joining the pair** - so on a
multigraph the hop multiplies instead of filtering, and the estimate is wrong by a factor that grows
with the pair's parallel-edge multiplicity. The estimate feeds `JoinOrderRule`, so an under-estimated
multiplying hop can mislead the planner into ordering subsequent operators as if the result were still
small.

## Affected components

- `engine/src/main/java/com/arcadedb/query/opencypher/optimizer/CypherOptimizer.java`
  (`createExpandIntoOperator`)
- `engine/src/main/java/com/arcadedb/query/opencypher/optimizer/statistics/StatisticsProvider.java`
  (new statistic)

## Fix

Added `StatisticsProvider.getMeanEdgesPerConnectedPair(edgeType)`: samples a bounded prefix (2000
edges) of the edge type via `Database.iterateType`, counts how many distinct (out, in) pairs the
sampled edges resolve to, and returns `sampledEdges / distinctPairs` (at least `1.0`, the simple-graph
default). Cached per edge type on the provider instance, matching the existing `getAverageDegree`
cache pattern.

`createExpandIntoOperator` now scales the estimate by the average of this statistic across the hop's
edge types:

```java
final double meanEdgesPerConnectedPair = estimateMeanEdgesPerConnectedPair(edgeTypes);
final long outputCardinality = (long) (inputCardinality * DEFAULT_EXPAND_INTO_SELECTIVITY * meanEdgesPerConnectedPair);
```

An untyped hop (no edge type restriction) cannot be attributed to one type's statistic and keeps
multiplicity `1.0` (today's behaviour, unchanged). `outputCardinality` is shared by the `GAVExpandInto`
branch of the same method, so the CSR-backed path picks up the same corrected estimate automatically.

This matches the issue's proposed formula (`connectivity * meanEdgesPerConnectedPair`) but keeps the
"connectivity" factor as the existing `DEFAULT_EXPAND_INTO_SELECTIVITY` heuristic rather than adding a
new statistic for it - the issue itself notes a sampled multiplicity estimate is "probably enough for a
cost model," and this stays scoped to that.

## Scope decisions vs. the issue's checklist

- [x] Add mean-edges-per-connected-pair per edge type to `StatisticsProvider`, sampled
- [ ] Exact CSR answer via `GraphAnalyticalView` when one covers the type - **not done**. The CSR
      infrastructure (`com.arcadedb.graph.olap`) exists and is production-quality, but wiring it into
      `StatisticsProvider`/the optimizer is new integration work, not a extension of the existing
      sampling method. Sampling alone already fixes the reported estimate defect; exact-when-available
      is a worthwhile follow-up, not a correctness requirement for this issue.
- [x] Use it in `createExpandIntoOperator` instead of the bare `DEFAULT_EXPAND_INTO_SELECTIVITY`
- [x] Check whether `createExpandAllOperator`'s `DEFAULT_AVG_DEGREE` has the mirror-image problem -
      see below. **Not fixed here** - it is a real, pre-existing, differently-shaped defect and fixing
      it is out of scope for this issue's regression test.
- [x] A test that a multigraph pattern's estimated cardinality tracks the real one

### `createExpandAllOperator` finding (not fixed in this PR)

`createExpandAllOperator` (`CypherOptimizer.java`) estimates `ExpandAll` cardinality with a flat
constant:

```java
final long outputCardinality = inputCardinality * (long) DEFAULT_AVG_DEGREE; // 10.0
```

This ignores `StatisticsProvider.getAverageDegree(relationshipType, sourceLabel, targetLabel)`, which
already exists and is already wired into `JoinOrderRule.estimateAverageDegree` for hop ordering - just
not into the `ExpandAll` operator's own cardinality. So `ExpandAll`'s estimate is disconnected from
real statistics in **both** directions (over- or under-estimating depending on the type's true average
degree), unlike `ExpandInto`'s defect which was specifically backwards on multigraphs. The class-level
`TODO` comment at `CypherOptimizer.java:94` ("replace with runtime statistics once the statistics
provider tracks per-type average degree") is stale - the statistics provider already tracks it; only
the wiring into this one call site is missing. Recommend a follow-up issue rather than folding it into
this PR, since it changes `ExpandAll` cost/cardinality broadly rather than the specific multigraph
bound-target defect reported here.

## Tests

- `StatisticsProviderTest`: three new tests for `getMeanEdgesPerConnectedPair` - simple graph (returns
  1.0), multigraph (5 parallel edges + 1 single edge -> mean 3.0), and fallback cases (unknown type,
  edge type with no edges, non-edge type).
- `CypherExpandIntoMultiplicityCardinalityTest` (new): builds a bound-target cycle
  (`(a:Account)-[:INITIATED]->(t:Txn)-[:SETTLED]->(a)`) where the closing `SETTLED` hop is joined by 5
  parallel edges for its one connected pair, and asserts (via `EXPLAIN`'s `rows=` on the
  `⭐ BOUND-TARGET` operator) that the estimate is `5`, not the pre-fix `1`. Verified this test fails
  (`expected: 5L but was: 1L`) against the pre-fix formula before restoring the fix, per TDD.

## Verification

- `CypherExpandIntoMultiplicityCardinalityTest`, `StatisticsProviderTest`: pass.
- Broader regression sweep: `Cypher*`, `*Optimizer*`, `*ExpandInto*`, `*GAV*`, `PhysicalOperatorTest`,
  `JoinOrderRuleTest`, `CostModelTest`, `ExpandIntoRuleTest` - all pass (`BUILD SUCCESS`).
- No existing test asserted the exact numeric output of `createExpandIntoOperator`'s old formula, so no
  existing assertions needed updating.
