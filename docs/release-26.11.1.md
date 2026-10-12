# ArcadeDB v.26.11.1 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.11.1 development cycle, so the release notes are ready at tag time.

## Breaking Changes (migration notes)

### A plain `=` / `IN` on a `COLLATE ci` index is case sensitive (#9403)

A `COLLATE ci` index folds its keys, and until now the planner returned every case variant for `name = 'JOHN'` while the
same predicate over a subquery (a scan) returned only the exact-case rows, so the answer depended on whether the planner
picked the index. The index no longer changes the answer: a plain `=` or `IN` still uses the index but is re-checked on
the rows it returns, so `name = 'JOHN'` matches `'JOHN'` only, with or without the index.

**Who sees a different answer.** A query that relied on `name = 'john'` finding `'John'` through a `COLLATE ci` index now
returns fewer rows. Write `name.toLowerCase() = 'john'` (or `name.toLowerCase() IN ['john', 'mary']`) to keep the
case-insensitive lookup; that spelling is served by the index and needs no re-check when the literal is lower case.

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
View keeps one, bidirectional Dijkstra otherwise) and honours the command timeout. Among several paths of the same total
weight it may return a different one than before, since the search is no longer a one-directional Dijkstra.

`algo.steinerTree` also picks the cheapest of several parallel edges for the tree it reports, rather than the first
one the adjacency listed.

**Embedded API.** The `protected static final float MIN` constant of `SQLFunctionHeuristicPathFinderAbstract` is gone:
it was the 0 weight of an edge without one. A custom subclass that referenced it reads the shared rule from
`com.arcadedb.graph.EdgeWeight` instead (`MISSING`, `of(Object)`, `isWalkable(double)`).

## Fixes

- **`vector.sum`, `vector.avg`, `vector.min` and `vector.max` aggregate when written with the dotted name (#9768).** In SQL
  they answered one NULL row per record (`vectorSum(...)` and `` `vector.sum`(...) `` were fine) and in openCypher
  (`RETURN vector.sum(d.v)`) they had no working spelling at all. A dotted call is now a function call exactly when the
  function is registered, in both languages, so the next namespace somebody adds cannot drift again. A property named like
  a namespace keeps its own methods (`vector.size()` on a `vector` column), while a registered name (`vector.max()`) wins
  over the method of a property called `vector`, as it already did at run time; back-tick the property to address it.
- **A map with a non-String key renders as JSON (#9805).** A `MAP` property accepts an Integer key and a Gremlin
  `elementMap()` is keyed by the `T` enum, but `Result.toJSON()`, `convert.toJson()`, `asJSON()` and the Postgres wire threw
  `ClassCastException` for such a row (and a `null` key an `IllegalArgumentException`). The member name is now the text of
  the key, a `null` key being named `"null"`, as the HTTP serializer already did. `JSONObject(Map)` takes any `Map<?, ?>`.
- **Redis `HDEL` replies the number of records it removed, and a multi-key `HDEL` / `HSET` is one unit of work (#9837).** A
  delete a `BeforeRecordDeleteListener` vetoed was counted as a deletion on every branch. On the Redis protocol, `HDEL` by
  index key left the first keys deleted behind an error reply and was not retried on a conflict, and the transient `HSET`
  left the first JSON objects stored behind an error reply. A new `DatabaseInternal.deleteRecordReportingVeto(Record)` keeps
  the answer `Record.delete()` throws away, for any caller that counts deletions.
