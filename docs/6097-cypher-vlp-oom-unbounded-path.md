# #6097: Unbounded Cypher variable-length path queries OOM instead of streaming/deduplicating

Issue: https://github.com/ArcadeData/arcadedb/issues/6097

## Root cause

`MATCH (a)-[*1..3]->(b)` and similar variable-length patterns are always evaluated through
`ExpandPathStep`, which delegates to `VariableLengthPathTraverser`. Two independent problems
combined to make this combinatorially expensive in both memory and CPU, regardless of what the
consuming clause actually needed:

1. **Eager materialization.** Both `BreadthFirstTraverser.BFSPathIterator` and
   `DepthFirstTraverser.DFSPathIterator` ran their entire traversal to completion inside their
   constructor, collecting every matching path into a `List<TraversalPath>` before the first
   `hasNext()`/`next()` call. `ExpandPathStep`'s own batching `ResultSet` wrapper only looked lazy
   from the outside - the full result set was already built one layer down.
2. **BFS by default, unconditionally.** Both plan builders in `CypherExecutionPlan`
   (`buildExecutionStepsWithOptimizer` and the legacy `buildExecutionStepsLegacy`) hardcoded
   `useBFS=true` for every `ExpandPathStep`. Level-order BFS via a single FIFO queue structurally
   cannot avoid enqueuing an entire level's children before it can dequeue the first one - so even
   after fixing (1) with a genuinely lazy iterator, BFS's frontier queue still peaks at the width
   of the widest level, which is exactly the combinatorial blow-up the issue describes (`FANOUT^3`
   for a 3-hop pattern with uniform branching).

DFS does not have that structural problem: its active stack is bounded by `maxHops` frames
regardless of branching factor, because it fully explores one branch (backtracking) before moving
to the next sibling - it never needs to hold multiple siblings' whole subtrees at once.

## Fix

1. `DepthFirstTraverser.DFSPathIterator` rewritten from recursive eager enumeration to an explicit
   `Deque<Frame>`-based lazy generator. A path is emitted (pre-order) the moment it is discovered;
   only the current root-to-frontier chain is ever on the stack.
2. `BreadthFirstTraverser.BFSPathIterator` similarly rewritten to advance its FIFO queue lazily,
   one result at a time, instead of eagerly draining the whole queue into a results list first.
   This does **not** fix the worst-case memory bound (see root cause #2) but removes the
   redundant double-storage (queue + results list) and lets a `LIMIT` on a BFS-ordered
   (shortest-first) query stop early.
3. `CypherExecutionPlan`: both `ExpandPathStep` instantiation sites switched `useBFS` from `true`
   to `false` (DFS). A `MATCH`'s result order is unspecified without `ORDER BY`, so this is a pure
   implementation-strategy change, not a semantic one. Verified against the local
   `OpenCypherVariableLengthPathTest` suite and others below, none of which assert a specific
   multi-row order without `ORDER BY`.

### What this does and does not fix

- `count(DISTINCT b)`, `count(p)`, `RETURN b` without `ORDER BY`/without collecting all rows - now
  stream: each path is discovered, consumed (counted/checked), and eligible for GC, one at a time,
  in `O(maxHops)` active memory. Confirmed by `Issue6097Test` against the issue's own 200-fanout,
  3-hop reproduction (`FANOUT^3` = 8,000,000 distinct paths, previously OOM/hang; now completes in
  a few seconds).
- `LIMIT` now genuinely short-circuits traversal instead of paying for full enumeration first.
- `RETURN p` (collecting every path, e.g. into a list) is unaffected in complexity: returning all
  8,000,000 paths still requires holding data proportional to what was asked for. That is an
  inherent cost of the request, not a bug.
- This is a bounded-memory fix, not a bounded-CPU fix: `count(DISTINCT b)` on the worst-case input
  still does `O(total paths)` work under Cypher's default TRAIL semantics (no repeated
  relationship within one path), since determining "is there any qualifying trail to node X" in
  general requires examining the paths that could reach X. A true `O(nodes × maxHops)` fast path
  for `DISTINCT`-only consumers (mirroring `TRAVERSE ... MAXDEPTH`'s node-level visited-set) was
  considered and deliberately left out of this PR - see below.

## Deliberately out of scope

An optimizer-level "distinct destination" fast path - detecting that a `MATCH` clause's only
downstream consumer is `RETURN DISTINCT b` / `count(DISTINCT b)` (no path or relationship
variable used) and routing through the existing node-level visited-set traversal
(`GraphTraverser.traverse()` / `RidHashSet`, already used elsewhere) instead of per-path
enumeration - would close the remaining CPU gap for that specific query shape. It was left out
because it is not semantics-preserving in general: `GraphTraverser.traverse()` has no `TRAIL`
mode (only `WALK` or `ACYCLIC`), and node-level reachability with a single global visited set can
both under-report (`ACYCLIC` forbids revisiting a node that a valid `TRAIL` path could revisit)
and over-report (unconstrained `WALK` reachability may find a node only via a walk that reuses an
edge, which `TRAIL` forbids) relative to true `TRAIL`-mode path existence - particularly once
`minHops > 0`, where "reachable at the shortest depth" does not imply "reachable at some depth in
`[minHops, maxHops]`". Getting this exactly right for the default `TRAIL` mode is a harder,
separate design decision (a `(node, depth)`-layered reachability scheme handles `WALK`
exactly but is only an approximation of `TRAIL`); it deserves its own issue/PR rather than being
folded into this memory-safety fix.

## Testing

- New: `engine/src/test/java/com/arcadedb/query/opencypher/Issue6097Test.java` (`@Tag("slow")`),
  reproducing the issue's own 200-fanout/3-hop hub-and-spoke graph (8,000,000 distinct length-3
  paths, 601 nodes, 80,200 edges):
  - `countDistinctOverVariableLengthPathDoesNotMaterializeEveryPath` - before the fix, did not
    complete within 170s in local testing (killed); after the fix, both tests in the class
    complete in ~8s total.
  - `limitShortCircuitsInsteadOfExploringEveryPath` - asserts completion within 20s and exactly 5
    rows.
- Regression: full `com.arcadedb.query.opencypher.**` package (excluding `slow`/`benchmark`/
  `vector` lanes), including the openCypher TCK suite - **7,857 tests, 0 failures, 0 errors, 98
  skipped** (pre-existing, unrelated skips). One `SEVERE` log line from
  `CypherMultiLabelConstraintReloadTest` (a schema-close race, unrelated to variable-length paths)
  appeared during the run but did not fail any test.
