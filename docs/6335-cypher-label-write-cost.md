# #6335: a Cypher label write is O(degree) and changes RIDs

Issue: https://github.com/ArcadeData/arcadedb/issues/6335

## What happens

A Cypher label is an ArcadeDB type, and a record's type comes from the bucket it lives in. There is no
in-place retype, so `SET n:Label` / `REMOVE n:Label` cannot edit metadata - it has to move the record.
`LabelReplacements.replace()`:

1. creates a new vertex under the composite type for the new label set,
2. copies every property,
3. re-creates **every incident edge**, in both directions, each of which also mutates the neighbour's
   edge list,
4. deletes the original.

Two consequences follow, and neither is visible in the query text:

**Cost.** `SET n:Label` on a vertex with a million edges rewrites a million edge records and touches a
million neighbour edge lists. On a supernode that is the difference between a millisecond and a stall.
It also works against the striping and append-merge paths: a label write undoes the locality they build.

**Identity.** The vertex's RID changes, and so does every edge's. Anything holding a RID across the write
- an application-side reference, a stored link from a non-graph type, an external index, a client that
cached an earlier result - is left pointing at a deleted record. #6326 makes the query's *own* row follow
the move (`LabelReplacements.redirect`); nothing can make an outside reference follow it.

## Why it is not simply fixed

The three options are:

- **Model extra labels as a property** rather than as a type, so the record never moves. This is the real
  fix and the large one: every place that resolves a label to a type - `Labels.ensureCompositeType`,
  `NodeByLabelScan` and the label-scan planning, the composite-type naming, `labels()` itself - has to
  learn a second source, and existing databases carry composite types that must keep working. It is its
  own epic, not a line in this one.
- **Make the rewrite cheaper.** Edges are re-created one at a time through the public API. A bulk path
  that moved the edge lists wholesale would cut the constant but not the complexity: an edge record
  stores the RIDs of both endpoints, and each neighbour's edge-list entry stores the edge's RID and the
  other endpoint's, so every one of them has to be rewritten whichever way the write is spelled. It would
  not help the RID churn at all.
- **Be honest about it.** The rewrite is slow, not wrong. What was actually broken is that it was
  *silent*.

## What was done

The third, with the second and first left open as their own work:

- `LabelReplacements.replace()` counts the edges it migrates - free, since it walks them anyway - and
  logs a `WARNING` naming the node, the two types and the edge count when the count reaches
  `arcadedb.opencypher.labelWriteDegreeWarning` (default 10000, `0` disables). A stall that has already
  happened now has a line in the log that explains it.
- `arcadedb.opencypher.labelWriteDegreeLimit` (default `0`, disabled) refuses the write above a degree,
  with a message that says what the write would have cost and what to do instead. It is checked *before*
  any record moves, at the cost of one extra edge-list walk - which is why it is opt-in rather than on:
  a query that is merely slow should not start failing because of an upgrade.
- The cost and the RID change are spelled out in the javadoc of `LabelReplacements.replace()`, which is
  where the next person tracing a label write will be.

## Affected components

- `engine/src/main/java/com/arcadedb/query/opencypher/executor/LabelReplacements.java`
- `engine/src/main/java/com/arcadedb/GlobalConfiguration.java`
  (`OPENCYPHER_LABEL_WRITE_DEGREE_WARNING`, `OPENCYPHER_LABEL_WRITE_DEGREE_LIMIT`)

## Tests

`engine/src/test/java/com/arcadedb/query/opencypher/CypherLabelWriteDegreeGuardIssue6335Test.java`:
the limit is off by default, a write above it is refused with nothing moved, `REMOVE` is guarded the
same way, a write at the limit is allowed, and crossing the warning threshold reports without refusing.
