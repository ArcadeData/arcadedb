# ArcadeDB v.26.11.1 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.11.1 development cycle, so the release notes are ready at tag time.

## Breaking Changes (migration notes)

### Weighted path finders share one edge-weight rule (#9443)

Every weighted shortest-path finder now reads an edge's weight the same way:

- an edge without the weight property, or whose value is not a number, weighs **1** (the unweighted hop);
- an edge whose weight is **negative, NaN or infinite is not walked**. Negative weights remain the business of
  `bellmanFord()` / `algo.bellmanford`, which are unchanged.

Before, the finders disagreed, so the same query answered different distances and paths depending on the entry point:

| finder | missing weight before | negative weight before |
|---|---|---|
| `dijkstra()`, `astar()`, `algo.dijkstra`, `algo.astar` | **0** (every unweighted edge was free) | **walked** |
| `duanSSSP()` | 1 | **walked** |
| `algo.kShortestPaths`, `algo.steinerTree` | 1 | **walked** |
| `algo.dijkstra.singleSource` | 1 | skipped (a NaN weight was walked) |
| `cchShortestPath()`, `algo.cch.shortestPath` | 1 | skipped |

Walking a negative weight breaks the invariant Dijkstra and A* settle vertices by, so the answer was an arbitrary path
rather than a shortest one; it is now a shortest path over the edges that can be walked.

**Who sees a different answer.** A graph where some of the routed edges have no weight property: `dijkstra()`,
`astar()`, `algo.dijkstra` and `algo.astar` used to treat them as free, and now count each as one hop. The `weight`
yielded by `algo.dijkstra` and `algo.astar` changes accordingly. A graph with negative weights now routes around those
edges; use `bellmanFord()` when they are meant to be walked.

**`duanSSSP()` accepts an edge type filter.** Its fourth argument may now be an options map,
`{ direction, edgeTypeNames }`, like `dijkstra()` and `cchShortestPath()`; a plain direction string still works. It now
answers through the same engine as `cchShortestPath()` (a Customizable Contraction Hierarchy when a Graph Analytical
View keeps one, bidirectional Dijkstra otherwise) and honours the command timeout.

`algo.steinerTree` also picks the cheapest of several parallel edges for the tree it reports, rather than the first
one the adjacency listed.
