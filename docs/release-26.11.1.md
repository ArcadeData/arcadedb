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

### `BACKUP DATABASE` applies the retention policy (#9844)

The SQL `BACKUP DATABASE` statement now prunes the database's archive directory after a successful backup, the way the
control plane's `trigger backup` has since #7472 and the scheduler always has. It writes into the same
`<backup directory>/<database>` they prune, and retention works off what is in the directory rather than what is
registered with the scheduler, so a database that is absent from the auto-backup configuration is pruned by the server
level defaults. Before, repeated statements against such a database grew the directory without bound.

**Who sees a different result.** An installation with auto-backup enabled that runs `BACKUP DATABASE` for a database
beyond the `maxFiles` (or tiered) retention of its effective policy now keeps only that many archives, and the older ones
are deleted after the statement. The archive just written is always kept. Archives that do not follow the
`<db>-backup-<timestamp>.zip` convention, such as one named on purpose with `BACKUP DATABASE 'before-the-upgrade.zip'`,
are invisible to retention and are never deleted. With auto-backup disabled nothing is pruned, as before.

### A MANDATORY or default-valued property cannot reach a LIGHTWEIGHT edge type (#9846)

A lightweight edge allocates no record, so nothing validates it: a `MANDATORY` property was never required and a default
never applied, on a subtype that inherited them from its super type. The type's own `CREATE PROPERTY` already refused any
property on a lightweight type; the same rule now covers the properties that arrive from a super type. These are refused
with a `SchemaException` naming the property and the type:

- `CREATE EDGE TYPE Child EXTENDS Parent LIGHTWEIGHT`, and `ALTER TYPE Child SUPERTYPE +Parent`, when `Parent` declares or
  inherits a `MANDATORY` or default-valued property (a refused `CREATE` leaves no type behind);
- declaring a property `MANDATORY` or giving it a default on a type that has a lightweight edge type below it;
- `ALTER TYPE Child WITH lightweight = true` on a type that declares or inherits such a property.

Optional properties, and properties that only constrain a value that is present (`NOT NULL`, `MIN`, `MAX`, `REGEXP`),
stay legal under a lightweight subtype. A schema written before this check that already holds the combination still
opens; it is only the DDL that is refused now.

### `values()` on a sub-query row lists exactly the values of `keys()` (#9843)

On a `Result` receiver (a sub-query row, a `LET` variable) `values()` used to answer the record's `@cat`, `@type` and
`@rid` (and an edge's `@in` and `@out`) as unlabelled values, so a two-property document had 2 keys and 5 values, and on a
row that overrode a property of its record it answered the record's value instead of the row's. It now answers one value
per key, in the order of `keys()`, and the row's own value where the row has one.
