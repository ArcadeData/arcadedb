# Issue #4512 — GraphAnalyticalView.getNeighborsFromCSR slow path ignores DeltaOverlay.isEdgeDeleted

## Summary

The slow path of `getNeighborsFromCSR` (taken whenever a `DeltaOverlay` is
present) copies the full base adjacency from the CSR without consulting
`DeltaOverlay.isEdgeDeleted(edgeType, src, tgt)`. Edges deleted in the delta
overlay still appear in the neighbour list returned by `getVertices(...)`.

`countDirectional` already subtracts the deleted-edge counts, so `countEdges()`
and `getVertices(...).length` disagree on the same view after an edge delete.

## Root cause

`GraphAnalyticalView.getNeighborsFromCSR` (slow path, ~lines 1587-1623):
`baseOut` / `baseIn` are produced via `Arrays.copyOfRange` of the CSR forward /
backward neighbour slices, then merged with the overlay-added edges. The overlay
deleted-edge set (`ov.isEdgeDeleted`) is never applied to the base slices.

For an OUT base neighbour `n` of `nodeId`, the edge is `nodeId -> n`, so the
check is `ov.isEdgeDeleted(edgeType, nodeId, n)`.
For an IN base neighbour `n`, the edge is `n -> nodeId`, so the check is
`ov.isEdgeDeleted(edgeType, n, nodeId)`. This matches the directionality already
used by `isConnectedForType`.

## Affected component

- `engine/.../com/arcadedb/graph/olap/GraphAnalyticalView.java`

## TDD plan

1. Add a regression test that builds a view (SYNCHRONOUS), deletes an edge, and
   asserts `getVertices(...)` no longer returns the deleted neighbour, AND that
   `getVertices(...).length` equals `countEdges(...)`.
2. Confirm it fails on the unfixed code.
3. Filter the base slices through `ov.isEdgeDeleted(...)` in the slow path.
4. Confirm the new test and the existing suite pass.

## Implementation

Added `copyBaseExcludingDeleted(...)` helper in `GraphAnalyticalView`. The slow
path of `getNeighborsFromCSR` now routes the base OUT/IN slices through it. The
helper consults `ov.isEdgeDeleted(edgeType, nodeId, n)` for outgoing slices and
`ov.isEdgeDeleted(edgeType, n, nodeId)` for incoming slices, matching the
directionality already used by `isConnectedForType`. When no overlay is present
or nothing is deleted, the original slice is returned verbatim (no extra copy),
keeping the common case allocation-cheap.

The fast paths (no overlay) are unchanged - they are only reachable when
`ov == null`, so they were never affected.

## Test results

- New test `getVerticesExcludesEdgesDeletedInOverlay` fails on the unfixed code
  (returns `[bob, charlie]` where only `[charlie]` is expected), passes after the fix.
- `GraphAnalyticalViewTest`: 124 tests, 0 failures.
- `GraphAlgorithmsTest`: 20 tests, 0 failures.

## PR

- https://github.com/ArcadeData/arcadedb/pull/4525

## Review cycles

### Cycle 1 — head 98766bf
- `gemini-code-assist`: COMMENTED. One medium-priority actionable item: in
  `copyBaseExcludingDeleted`, `ov.isEdgeDeleted` autoboxes a packed long for a
  `Set` lookup and was called twice per neighbour (count pass + copy pass).
  Suggested caching deletion status in a lazily-allocated `boolean[]` so the call
  happens once per neighbour, halving lookups/allocations while keeping the
  no-deletion case allocation-free.
  - APPLIED: rewrote the helper as a single pass that caches results in a lazily
    allocated `boolean[]` mask. Tests still green (124 + 20).
- `claude`: no review produced within the 15-minute per-iteration window.

## Final state

- timeout: `claude` bot did not review within the per-iteration window.
  Gemini's actionable feedback was applied and pushed. PR left open for the
  developer. Merge remains the developer's responsibility.

## Status

- Fix implemented, verified, and Gemini review feedback addressed.
