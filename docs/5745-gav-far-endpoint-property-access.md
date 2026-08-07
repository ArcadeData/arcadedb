# Issue #5745: GAV far-endpoint property access is dominated by HashMap probing

## Root cause

`GraphAnalyticalView.getProperty(nodeId, propertyName)` resolves every single
property read through `ColumnStore.getValue()`, which did:

```java
final Column column = columns.get(propertyName);   // HashMap<String, Column>.get()
```

on **every call**, i.e. once per traversed edge for a far-endpoint property
read such as `MATCH (p:Person)-[:KNOWS]->(f:Person) RETURN f.city`. The
issue's profile (`dst_prop_only`, GAV-ON) shows `HashMap.getNode` at 32.09%
self time and `ColumnStore.getValue` at 18.85% inclusive - roughly 46% of the
profile is this repeated hashing of the *same* property name across rows,
because a query loop calls `getValue(nodeId, "city")` with a constant
`propertyName` thousands of times in a row.

The issue's author had already ruled out two adjacent micro-optimizations
(`ResultInternal.getProperty`'s redundant `containsKey`, and hoisting
`getPropertyNames()` in `GAVExpandAll`) by measuring them as noise, and
correctly pointed at the column-name hashing itself as the real cost.

## Fix

Added a single-slot memoization cache to `ColumnStore`: the last resolved
`(propertyName, Column)` pair is kept behind one `volatile` field
(`CachedColumn`, a record pairing both fields so a racing reader can never
observe a torn combination of an old name with a new column, or vice versa).
`getColumn(name)` checks the cached name first (a `String#equals`, no
hashing) and only falls back to the `HashMap` probe on a cache miss;
`getValue()` was refactored to go through `getColumn()` so both call sites
share the cache.

This is correctness-preserving because `ColumnStore.columns` is populated
exclusively during CSR build (`CSRBuilder`, single property name only ever
`createColumn`'d once per store — verified by tracing every call site) and is
never mutated once the `GraphAnalyticalView` snapshot is published, so a
cached entry can never go stale for the query-time lifetime of the store.

Query loops (Cypher property expressions such as `f.city` evaluated via
`GAVVertex.get()` -> `GraphAnalyticalView.getProperty()` ->
`ColumnStore.getValue()`) repeat the *same* property name across every row of
an expansion, which is exactly the pattern this single-slot cache turns from
an O(1)-amortized-but-still-hashing `HashMap.get` into a plain reference
compare + `String#equals`.

### Scope note

The issue's "structural observation" section flags
`GraphAnalyticalView.getBucketColumnStore(int)` as dead code with zero
callers, and speculates that a fuller fix would route Cypher operators through
bulk/vectorized column access instead of per-row `getProperty()` calls. That
is a materially larger change (touching the Cypher expression evaluator and/or
`GAVExpandAll`'s per-row result construction) that the issue explicitly did
not propose or measure a patch for ("we are not proposing a specific patch
for that ... the two we did measure taught us not to guess"). This PR fixes
the measured, profiled bottleneck (the redundant per-row hashing) with a
minimal, safe, well-tested change; wiring a vectorized/batched consumer onto
`getBucketColumnStore` remains a separate, larger follow-up and is out of
scope here.

## Changes

- `engine/src/main/java/com/arcadedb/graph/olap/ColumnStore.java`: single-slot
  `(name, Column)` cache shared by `getColumn()` and `getValue()`.
- `engine/src/test/java/com/arcadedb/graph/olap/ColumnStoreTest.java` (new):
  regression tests for the cache — repeated same-property access, alternating
  between two properties (would expose a torn/stale entry), a missing
  property not poisoning a subsequent valid lookup, repeated misses staying
  null, a null value on an existing column vs. a missing column, and the
  empty-store case.

## Test results

```
mvn -pl engine -am test -Dtest=ColumnStoreTest
Tests run: 6, Failures: 0, Errors: 0, Skipped: 0

mvn -pl engine -am test -Dtest='ColumnStoreTest,GraphAnalyticalViewTest,GraphAnalyticalViewRegistryTest,DeltaOverlayTest,DeltaOverlayCompactionDedupTest,GraphAlgorithmsTest,GraphAlgorithmsInterruptTest,CSRPerformanceTest'
Tests run: 173, Failures: 0, Errors: 0, Skipped: 0

mvn -pl engine -am test -Dtest='CypherGAVBoundTargetCardinalityTest,CypherGAVFusedChainIssue5746Test,GAVEligibilityTest,CypherExpandIntoUnreadEdgeVariableTest,CypherExpandIntoMultiplicityCardinalityTest,AlgoPageRankTest'
Tests run: 70, Failures: 0, Errors: 0, Skipped: 0
```

All green, no regressions in the GAV/Cypher-GAV suites.

## Impact analysis

- Correctness: unchanged behavior for every existing caller of
  `ColumnStore.getColumn()` / `getValue()`; the cache is purely an
  optimization layered on top of the same lookup semantics.
- Performance: eliminates the per-row `HashMap` probe on the property name
  for the dominant access pattern (same property name read across many
  rows), which the issue's profiling identified as ~46% of CPU time on the
  far-endpoint-property benchmark queries.
- Concurrency: the cache field is `volatile` and the cached name/column pair
  is read and written atomically as one object reference, so concurrent
  readers across multiple query threads sharing one `ColumnStore` can only
  ever observe a fully-formed `(name, column)` pair — never a torn one. In
  the worst case (concurrent queries alternating different property names on
  the same store) the cache simply thrashes back to hashmap-lookup-per-call
  behavior; it is never wrong.

## Recommendations for future improvements

- A fuller fix per the issue's structural observation would give
  `GAVExpandAll` (or the Cypher property-expression evaluator) a bulk/batched
  path onto `GraphAnalyticalView.getBucketColumnStore()` instead of one
  `getProperty()` call per row. That is a larger, separate change and was
  explicitly out of scope for this fix.
- If a follow-up benchmark shows the single-slot cache thrashing under mixed
  concurrent multi-property workloads sharing one `ColumnStore`, a small
  N-way (e.g. 2-4 slot) cache could be considered, but the profile in this
  issue is dominated by single-property repeated access, so a single slot
  matches the reported workload.
