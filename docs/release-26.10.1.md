# ArcadeDB v.26.10.1 Release Highlights

This is a living document: fixes, improvements, new features, and breaking changes are collected here as
they land during the 26.10.1 development cycle, so the release notes are ready at tag time.

## Breaking Changes (migration notes)

### Embedded API: `MultiColumnAggregationRequest.columnIndex` is a ROW index (#8140, #8191)

`MultiColumnAggregationRequest.columnIndex` is the position of the value in the ENGINE ROW - `0` is the
timestamp, `1 +` the ordinal among the non-TIMESTAMP columns in schema order is a value column - which is what
its javadoc has always stated. Until #8140 the two halves of the aggregation push-down disagreed about it: the
mutable half read it as a row index and the sealed half as a SCHEMA index, so the same samples answered one
number before compaction and another after it. Both halves now read the row index.

**Who has to change anything.** Only an EMBEDDED caller that builds a `MultiColumnAggregationRequest` by hand.
The record's arity and its field types are unchanged, so nothing fails to compile and nothing warns: a caller
that passed the SCHEMA index - which is what every in-tree producer did, and what the bug was - now silently
aggregates a different column, or is refused, on a type whose TIMESTAMP column is not declared first.

| declaration | before #8140 | after #8140 |
|---|---|---|
| TIMESTAMP declared first (every type written before #7702) | schema index == row index | unchanged, no migration needed |
| TIMESTAMP declared anywhere else | answered inconsistently (mutable vs sealed) | the caller must pass the ROW index |

**The conversion** is `TimeSeriesGateway.aggregationRowIndex(columns, schemaIndex)`, which is public for exactly
this reason. No change is needed for a TIMESERIES type whose TIMESTAMP column is declared first, where the two
numbers are equal.

**Unaffected:** the HTTP, Grafana, gRPC and SQL surfaces. All four resolve the aggregated field by NAME and
apply the conversion themselves.

Two further changes follow from the same convention:

- The single-column `TimeSeriesEngine.aggregate` and `TimeSeriesSealedStore.aggregate` are **removed** (#8189).
  They carried a second, incompatible convention - the ordinal among non-TIMESTAMP columns, with no `+1` - and
  had no caller in `src/main`: the three wire protocols and the SQL push-down all go through `aggregateMulti`,
  which subsumes them. An embedded caller using them moves to `aggregateMulti` with a one-element request list
  and a ROW index.
- A `columnIndex` that names no value column - row position `0`, the timestamp, or a position past the last
  column - is now **refused** with an `IllegalArgumentException` rather than answered as a silent gap, and the
  refusal reaches the caller as itself whatever the type's shard count (#8190). It used to be wrapped as
  `IOException("Parallel shard aggregation failed", ...)` on a multi-shard type, because only a multi-shard
  aggregation fans the sealed reads out over the shard executor - so the identical caller mistake was a
  400 / `INVALID_ARGUMENT` on a 1-shard type and a 500 / `INTERNAL` on a 4-shard one.

### A TIMESERIES read that crosses a DOWNSAMPLE now raises (#8166)

A row walk - `EXPORT DATABASE`, a PromQL range read, a Grafana panel - holds no lock across the sealed blocks
it reads, so a maintenance pass can rewrite the sealed file underneath it. Since #8043 a block the rewrite
merely MOVED keeps its identity and the walk finds it again, and a block a retention `truncateBefore` DELETED
is counted as vanished and stepped over, which is the correct answer: those rows are gone.

Downsampling is neither. It replaces a block's rows with coarser ones, so a walk that crosses it has already
emitted FINE rows and would go on to emit their COARSE replacements - an answer at two resolutions, in which the
bucket at the crossing point is counted twice or not at all. Such a walk now raises
`TimeSeriesWalkCoarsenedException` instead of returning a silently short answer. Re-running the read gives a
whole answer at one resolution; downsampling a series is a maintenance event rather than a per-request one, so
the retry succeeds.

`EXPORT DATABASE` also reports sealed blocks that retention removed from under it, as
`vanishedTimeSeriesBlocks` in the export statistics and as a WARNING naming the type. That count is reported
and never fatal, unlike `skippedRecords`: those samples are genuinely gone rather than somewhere else. It used
to be invisible - the engine counted them, but the export passed no `AggregationMetrics` and the only reader of
the count anywhere was the PromQL/HTTP metrics surface.

## Improvements

### HA: a replicated TimeSeries sealed payload this node cannot install refuses the Raft entry (#8172)

Follow-up to #8070. A sealed blob or slice sequence naming a type this node does not have, or a type that
exists and is not a TIMESERIES one, was logged at SEVERE and stepped over - so the entry was checkpointed as
applied while the payload was consumed, and since a Raft entry is applied once and never re-shipped, that node's
sealed store for the type stayed permanently behind the leader's with one log line as the whole of the evidence.
Both arms now accumulate into the same refusal #8070 introduced for a failed engine repair, so the entry is
refused, the database is quarantined and a resync from the leader is triggered. Every other payload in the same
entry is still installed first.

Both arms are guarded by a Raft-ordering invariant - the type-creation entry carries a lower index and is
applied first - so neither is expected to fire; the change is about what it costs if it ever does, for instance
during a rolling upgrade shipping a type this node's build cannot construct.
