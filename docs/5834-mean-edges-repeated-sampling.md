# Issue #5834: getMeanEdgesPerConnectedPair should avoid repeated per-query sampling

Follow-up from review of #5832 (fix for #5690). Three scope items, all addressed in this PR.

## Root cause

1. `StatisticsProvider` is instantiated fresh per `CypherOptimizer` (`CypherOptimizer.java:118`), so
   `meanEdgesPerConnectedPairCache` and `averageDegreeCache` never survive past one query's planning.
   Every distinct query that plans a bound-target hop over a given edge type re-samples up to 2000 edge
   records at plan time.
2. `createExpandIntoOperator` computes `outputCardinality` (which triggers the sampling scan) before
   checking `GraphTraversalProviderRegistry.findProvider(...)`. When a `GraphAnalyticalView` (CSR) covers
   the edge type, its adjacency arrays could answer pair multiplicity exactly and cheaply, but the code
   paid the 2000-edge sampling scan regardless.

## Scope

- [x] Decide where a database-scoped cache for `getMeanEdgesPerConnectedPair` (and `getAverageDegree`)
      would live, and its invalidation trigger.
- [x] Evaluate whether a cheap O(1) proxy can replace sampling in the common case.
- [x] Wire `GraphAnalyticalView`/CSR into `StatisticsProvider` for an exact answer when a view covers
      the edge type, short-circuiting the sample.

## Design decisions

### 1. Database-scoped cache (`GraphStatisticsCache`)

New class `com.arcadedb.query.opencypher.optimizer.statistics.GraphStatisticsCache`, one instance per
database, held on `LocalDatabase` (mirrors the existing `CypherPlanCache` pattern: field + constructor
init + getter on `LocalDatabase`, interface method on `DatabaseInternal`, delegating overrides on
`ServerDatabase` and `RaftReplicatedDatabase`).

**Invalidation trigger:** each cached entry is stamped with the edge type's record count
(`database.countType(edgeType, false)`, which is the same O(1) cached-bucket-counter read `count(*)`
uses) at the time it was computed. A read compares the entry's stamped count against the *current*
count; any mismatch (insert or delete on that edge type since the entry was cached) is treated as a
cache miss and triggers a fresh sample. This is deliberately conservative: a balanced insert+delete pair
that leaves the count unchanged would not be detected, but that is an acceptable heuristic-cache
tradeoff (a stale cost estimate degrades plan quality, not query correctness), and it requires zero new
event-listener plumbing, reusing infrastructure that already exists for `count(*)`.

`getAverageDegree`'s cache key already encodes `relationshipType:sourceLabel:targetLabel`; it reuses the
same count-stamped mechanism keyed off the edge type's record count only (not source/target vertex
counts) to keep the two cached quantities' invalidation semantics identical and easy to reason about.

The per-query `StatisticsProvider` caches (`meanEdgesPerConnectedPairCache`, `averageDegreeCache`) are
kept as-is: they still short-circuit repeat lookups of the same type *within* one query's planning, and
now also populate/consult the shared `GraphStatisticsCache` on the slow path.

### 2. O(1) proxy evaluation (no code change)

Considered replacing the sample with a purely count-derived heuristic (e.g., a fixed constant, or a
formula over `edgeCount` / vertex counts alone, with no scan). Rejected: the entire reason
`getMeanEdgesPerConnectedPair` exists is that this ratio cannot be derived from cardinalities alone - a
type with 10,000 edges could be a simple graph (mean 1.0) or a dense multigraph (mean 100+) with the
same edge count, and the two cases require very different cardinality estimates for a bound-target hop.
Any O(1) proxy that ignores the actual (out, in) pair distribution would be indistinguishable from
"always guess 1.0", which is the exact bug #5690 fixed. The sample (or the exact CSR-backed count in
item 3) stays the only sound source for this statistic; caching (item 1) and CSR short-circuiting (item
3) address the *repeated cost* instead of trying to avoid the underlying measurement.

### 3. CSR-backed exact multiplicity

- `GraphTraversalProvider` gets a new default method `getMeanEdgesPerConnectedPair(String edgeType)`
  returning `-1.0` ("unknown"), following the existing `countEdgesBetween` convention where a negative
  result means "the provider cannot answer exactly, fall back."
- `GraphAnalyticalView` overrides it: when its snapshot has no active delta overlay (uncommitted changes
  the CSR does not yet reflect) and holds a CSR for the type, it computes the *exact* mean by scanning
  the type's sorted forward-adjacency array once (total edges / distinct (node, neighbor) pairs -
  parallel edges are adjacent duplicates in the sorted per-node neighbor list, the same property
  `countEdgesBetween`'s binary search relies on). The result is memoized on the current `Snapshot`
  instance, so it is naturally invalidated by a CSR rebuild/compaction (which swaps in a new `Snapshot`)
  without any extra bookkeeping. When an overlay is active, or the type has no CSR, it returns `-1.0` and
  the caller falls back to sampling.
- `CypherOptimizer.estimateMeanEdgesPerConnectedPair` now looks up a covering
  `GraphTraversalProvider` once per hop and, for each edge type, prefers its exact answer over
  `StatisticsProvider`'s sampled one when the provider returns a non-negative value. This applies
  regardless of whether the hop ultimately executes as `GAVExpandInto` or plain `ExpandInto` - the
  original code only checked `findProvider` (for choosing which operator to run) after computing the
  sampled `outputCardinality`, so a hop that fell back to `ExpandInto` for other reasons (e.g. its edge
  variable is read) still paid the sampling cost even with a covering GAV. The fix makes the cardinality
  *estimate* independent of the *execution operator* choice.

## Test plan

- `GraphStatisticsCacheTest` (new): get/put round-trip, count-mismatch invalidation, `clear()`.
- `StatisticsProviderTest` (extended): two `StatisticsProvider` instances sharing one database reuse the
  cached multiplicity/degree; a data mutation between them forces a resample.
- `GraphAnalyticalViewTest` (extended): exact multiplicity on a multigraph CSR; `-1.0` when the type has
  no CSR entry; `-1.0` when a delta overlay is active.
- `CypherExpandIntoMultiplicityCardinalityTest` (extended): a fixture where the first 2000 edges of a
  type (sampled prefix) would yield a different mean than the type's true population - proves the
  optimizer used the exact CSR answer, not the sample, once a covering GAV exists.

## Test results

- `GraphStatisticsCacheTest` (new, 8 tests): pass.
- `StatisticsProviderTest` (extended, 18 tests total): pass.
- `GraphTraversalProviderCountEdgesBetweenTest` (extended): pass, including the new SPI-default test.
- `GraphAnalyticalViewTest` (extended, 132 tests total): pass.
- `CypherExpandIntoMultiplicityCardinalityTest` (extended, 5 tests total): pass. The new
  `boundTargetHopUsesTheExactGAVMultiplicityInsteadOfTheSample` test was verified to fail (667 expected,
  1000 actual) when the provider-first change is reverted, confirming it actually exercises the fix
  rather than passing vacuously.
- `CypherPlanCacheInvalidationTest`, `SQLMethodTransformTest`, `CypherOptimizerIntegrationTest`: pass.
- Full `com.arcadedb.query.opencypher.**` package (including the OpenCypher TCK suite, 3897 scenarios):
  pass, 0 failures.
- Full `com.arcadedb.graph.**` package: pass, 0 failures.
- `engine`, `server`, `ha-raft` modules compile cleanly together (`mvn -pl engine,server,ha-raft -am
  install -DskipTests`), verifying the `DatabaseInternal.getGraphStatisticsCache()` delegation in
  `ServerDatabase` and `RaftReplicatedDatabase`.

One regression surfaced and was fixed during implementation: `StatisticsProvider`'s constructor
unconditionally called `database.getGraphStatisticsCache()`, which NPE'd for the three existing test
doubles (`CostModelTest`, `AnchorSelectorTest`, `IndexSelectionRuleTest`) that subclass
`StatisticsProvider` with `super(null)`. Fixed by guarding the field assignment on `database != null`,
preserving the pre-existing "null database is fine as long as you don't call the DB-touching methods"
contract those test doubles rely on.

## Status

Implementation complete, all tests green. Ready for PR.
