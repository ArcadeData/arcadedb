# Issue #6376 - algo.maxKCut CSR/OLTP weight misalignment

## Root cause

`AlgoMaxKCut.buildWeightedAdj` filled a weight array positionally against the CSR adjacency
(`graph.adjacency(dir, relTypes)`, ordered by dense node id when a Graph Analytical View or other
CSR provider backs the graph) by walking `graph.getVertex(i).getEdges(dir, relTypes)`, which is
always OLTP order. When a GAV exists (or a relTypes filter reorders edges) the two orders diverge,
so `adjW[i][j]` ends up being the weight of whatever edge OLTP iteration happened to visit at
position `j`, not the weight of the edge to `adj[i][j]`. This is the same defect class fixed for
APSP, Bellman-Ford, Steiner tree and the kShortestPaths CSR path by #6301 (see
`GraphData.weightedAdjacency` / `WeightedAdjacency` in `AbstractAlgoProcedure`); `maxKCut` still
hand-rolled the parallel array and was never converted.

## Fix

Replace the `graph.adjacency(...)` + `buildWeightedAdj(...)` pair with a single
`graph.weightedAdjacency(guard, dir, weightProperty, relTypes)` call, exactly as APSP/Bellman-Ford/
Steiner do. `WeightedAdjacency.neighbors()` and `.weights()` are produced together from one walk of
the same edges, so `weights[i][j]` is guaranteed to be the weight of the edge to `neighbors[i][j]`
by construction - no separate reconciliation step exists that can misalign them.

`buildWeightedAdj` and its now-unused `Edge`/`RID` imports are removed.

## Test

`Issue6376AlgoMaxKCutWeightAlignmentTest` (engine module), mirroring
`Issue6301AlgoSteinerTreeWeightAlignmentTest`'s pattern:
- builds a star hub H with three unequal leaf weights (H-A=1.0, H-B=10.0, H-C=100.0) plus one
  extra A-B=5.0 edge that breaks the leaf symmetry a pure star would have (without it, a
  misassigned weight would just relabel an equally-valued answer and could never expose a
  misalignment); the unique maximum 2-cut of this graph is 115.0, achieved by grouping {H, A}
  against {B, C}, independent of seed or which physical vertex a CSR provider numbers first;
- runs `algo.maxKCut(2, {weightProperty:'w'})` across 10 seeds with no view (OLTP path) and again
  with a `GraphAnalyticalView` built over the edge type + weight property (CSR path);
- asserts `cutWeight == 115.0` in both cases, and (in a separate test method) that a CSR run
  actually went through the provider (`CommandContext.CSR_ACCELERATED_VAR`), so the comparison is
  not vacuously true from a silent OLTP fallback.

## Verification

- `Issue6376AlgoMaxKCutWeightAlignmentTest` confirmed RED against the unfixed code before the fix
  was applied: the CSR/GAV path returned a non-integer `cutWeight` of `110.5` (a symptom of the
  bug - the cut sums each edge's weight from both endpoints' adjacency rows and halves it, and
  misaligned rows disagree with each other) instead of the true `115.0`, reproduced across seeds
  0-9. The OLTP-only path (no view, no misalignment possible) reliably returned `115.0` even
  pre-fix, confirming the bug is specific to the CSR/GAV path as the issue describes.
- After the fix: `Issue6376AlgoMaxKCutWeightAlignmentTest` and the existing `AlgoMaxKCutTest`
  suite are green (`mvn -pl engine -am test -Dtest=Issue6376AlgoMaxKCutWeightAlignmentTest,AlgoMaxKCutTest`).
- Full `com.arcadedb.query.opencypher.procedures.algo` package test run (492 tests): one
  unrelated pre-existing failure, `Issue6302AlgoGraphDrivenWorkGuardTest.apspObservesTheDeadlineInsideTheTripleLoop`
  (a timing-based deadline test on `algo.apsp`/`WorkGuard`, untouched by this change) - reproduced
  identically on a clean `main` checkout, confirming it is not a regression from this fix.
- `mvn -pl engine -am compile` is clean.

## Review cycles

(filled in by resolve-issue-with-review Phase 6)
