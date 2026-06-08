# Issue #4514 — `GraphAlgorithms.localClusteringCoefficient` double-counts overlapping forward/backward neighbours

URL: https://github.com/ArcadeData/arcadedb/issues/4514

## Summary

`localClusteringCoefficient` builds an "undirected" adjacency list by concatenating, per node,
the CSR forward neighbours and CSR backward neighbours. When a vertex pair `(u, v)` is connected by
both an out-edge `u -> v` and an in-edge `u <- v` (reciprocal edges, or importers that materialise
both directions), `v` appears **twice** in `neighbors[u]`.

Consequences:
- The degree used in the LCC formula is `offsets[u+1] - offsets[u]`, i.e. the raw merged length,
  which over-counts (the denominator `deg*(deg-1)` grows quadratically).
- The sorted-merge triangle intersection treats each duplicate as a separate neighbour, so a single
  shared neighbour `w` that appears twice in both `N(u)` and `N(v)` is counted as multiple triangles.

Net result: LCC is materially wrong for any graph with reciprocal/bidirectional edges.

## Root cause

`lccBuildAndIntersect` never de-duplicates the merged adjacency list. The undirected neighbour set
must contain each distinct neighbour at most once.

## Fix

After building the sorted merged adjacency, compact each per-node list in place to remove duplicate
neighbours (and any self-loops, which are not valid for an undirected simple-graph LCC). Re-derive
the per-node degree and offsets from the compacted lengths, then run triangle counting and the LCC
formula against the compacted structure. The list is already sorted per node, so compaction is a
single linear walk per node.

## Verification

- New regression tests in `GraphAlgorithmsTest` covering reciprocal edges and a bidirectional
  triangle, asserting LCC equals the simple-graph value (1.0 for a triangle, 1/3 for a partial
  clique) rather than an inflated/deflated value.
- Existing LCC tests (`lccTriangle`, `lccStar`, `lccPartialClique`) must still pass.
- `AlgoLocalClusteringCoefficientTest` (Cypher procedure) must still pass.
