# Design: Extend Cypher write-counter (QueryStatistics) surfacing

Issue: [#5015](https://github.com/ArcadeData/arcadedb/issues/5015)
Part of [#4890](https://github.com/ArcadeData/arcadedb/issues/4890) / epic [#4882](https://github.com/ArcadeData/arcadedb/issues/4882).
Builds on the `QueryStatistics` / `ResultSet.getStatistics()` infrastructure merged in [#5016](https://github.com/ArcadeData/arcadedb/pull/5016) (closes #5000).

## Problem

PR #5016 added a reusable engine capability: a `QueryStatistics` accumulator on `CommandContext`, surfaced through `ResultSet.getStatistics()` and consumed by the Bolt layer, which emits a Neo4j-hyphenated `stats` map on the terminal SUCCESS for write queries. Three extensions were deliberately left out to keep that PR's blast radius contained:

1. The counters are surfaced only over Bolt. HTTP `POST /command` and the gRPC service both hold a `ResultSet` and can surface the same counters cheaply, but do not.
2. `CypherExecutionPlan` attaches the accumulator for top-level mutation steps only. Two shapes execute their writes correctly but return zero counters, and worse, misreport `containsUpdates() == false` (`type: "w"` yet no updates), a misreport rather than merely a zero:
   - **Top-level UNION writes** (`executeUnion()`): each branch runs its own step chain; counts are not summed into the returned result set.
   - **`CALL { ... }` write subqueries** (`executeWithSeedRow()`): the inner query builds its own `CommandContext`; its counts are not propagated to the outer context.
3. Constraint-kind counting (`NOT_NULL` / `KEY` / `TYPED`) is implemented but only `UNIQUE` is test-covered. Two additional constraint/delete counter-fidelity bugs surfaced in review (see below).

## Goals

- Surface write counters over HTTP `POST /command` (ArcadeDB-native camelCase shape) and over gRPC (`grpcw` result), omitting them for read queries.
- Aggregate write counters across top-level `UNION` writes and `CALL { ... }` write subqueries so `CALL { CREATE ... }` and `CREATE ... UNION CREATE ...` report accurate, non-zero counters (and correct `containsUpdates()`) over every surface.
- Close two counter-fidelity correctness bugs and add missing constraint-kind test coverage.
- Single-source the duplicated replace-map removed-property counting rule.

## Non-goals

- No change to the Bolt `stats` shape or the engine accumulator's counter set (both established by #5016).
- No new counters beyond the existing eleven.
- No change to SQL-engine semantics (SQL never populates `QueryStatistics`).

## Current state (verified against `main`)

- `QueryStatistics` (`engine/.../query/sql/executor/QueryStatistics.java`) - eleven primitive `int` counters + `containsUpdates()`, `copy()`, `restore()`. No merge/aggregate method.
- `BasicCommandContext.getStatistics()` - lazily allocates a **local** accumulator; does **not** delegate to `parent` (unlike `getDatabase()` / `getInputParameters()` / `getVariableFromParentHierarchy()`, which do). `copy()` shares the same `statistics` reference.
- `ResultSet.getStatistics()` - `default Optional.empty()`; `IteratorResultSet` / `InternalResultSet` store and return it. `CypherExecutionPlan.execute()` and `OpenCypherQueryEngine.executeDDL()` attach it.
- `CypherExecutionPlan.executeUnion()` and `executeWithSeedRow()` build fresh child contexts and never attach/aggregate stats (documented in-code limitations).
- Bolt: `BoltNetworkExecutor.handlePull/handleDiscard` read `getStatistics()` and emit `BoltResultStats.toStatsMap(...)` (Neo4j hyphenated keys) when `containsUpdates()`.
- HTTP: `PostCommandHandler` -> `AbstractQueryHandler.serializeResultSet(...)` consumes and closes the `ResultSet`; no `getStatistics()` call anywhere in HTTP handlers.
- gRPC: `arcadedb-server.proto` `ExecuteCommandResponse` has no stats field; `ArcadeDbGrpcService.executeCommandInternal` drains the `ResultSet` for an affected-count but never reads `getStatistics()`.
- Duplicated idiom: `!prop.startsWith("@") && map.get(prop) == null` in `SetStep.applyReplaceMap` and the `MergeStep` REPLACE_MAP branch.

## Design

### 1. UNION / CALL aggregation (engine)

Add `QueryStatistics.add(QueryStatistics other)` - a null-safe field-wise sum used to merge branch/subquery accumulators. `getStatistics()` semantics are left unchanged (lower blast radius than the parent-delegation idea originally sketched; code inspection showed the CALL subquery context has no link to the outer context and top-level UNION branches run as independent root plans, so delegation would require threading anyway).

**CALL subqueries** (`executeWithSeedRow` / `SubqueryStep.executeInnerQuery`): thread the outer `CommandContext` into `executeWithSeedRow(...)` and have the inner context **share the outer's `QueryStatistics` instance** via a new `CommandContext.setStatistics(QueryStatistics)`. Inner mutation steps then accumulate directly into the outer accumulator - the same instance-sharing rationale `BasicCommandContext.copy()` already documents. No post-hoc merge, no wrapper, no change to `getStatistics()`. The "read query allocates nothing" regression from #5016 still holds because sharing only happens once the outer context has already allocated its accumulator for a write, and a pure-read CALL never triggers a mutation step.

**Top-level UNION** (`executeUnion` / `UnionStep`): each branch runs as an independent root `CypherExecutionPlan.execute()` whose stats are attached to its own `ResultSet`. `UnionStep` accumulates each exhausted branch's `getStatistics()` into an internal aggregate via `add(...)` and exposes it (`getAggregatedStatistics()`). The read path stays lazy and unchanged; for a write UNION (`!statement.isReadOnly()`), `executeUnion()` materializes the union result (mirroring the non-union write path), then attaches the aggregate to the returned `IteratorResultSet` only when `containsUpdates()`.

Blast radius is bounded: only Cypher mutation steps populate the accumulator, and only Bolt/HTTP/gRPC read it back off a `ResultSet`; SQL result sets never carry statistics.

### 2. HTTP `POST /command` surfacing

Add a `QueryStatistics.toJSON()` helper (engine, `com.arcadedb.serializer.json.JSONObject`) producing the ArcadeDB-native camelCase shape:

```json
"stats": {
  "nodesCreated": 1,
  "relationshipsCreated": 1,
  "propertiesSet": 2,
  "nodesDeleted": 0,
  "relationshipsDeleted": 0,
  "labelsAdded": 0,
  "labelsRemoved": 0,
  "indexesAdded": 0,
  "indexesRemoved": 0,
  "constraintsAdded": 0,
  "constraintsRemoved": 0,
  "containsUpdates": true
}
```

In `PostCommandHandler`, read `qResult.getStatistics()` **before** `serializeResultSet(...)` consumes/closes the result set (Cypher writes are materialized during `execute()`, so the accumulator is already populated). When present and `containsUpdates()`, put `stats` on the response `JSONObject`. Reads omit it. Zero-valued counters are included for a stable object shape when updates occurred; `containsUpdates` disambiguates.

### 3. gRPC surfacing

Add to `grpc/src/main/proto/arcadedb-server.proto`:

```proto
message QueryUpdateStats {
  int32 nodes_created = 1;
  int32 nodes_deleted = 2;
  int32 relationships_created = 3;
  int32 relationships_deleted = 4;
  int32 properties_set = 5;
  int32 labels_added = 6;
  int32 labels_removed = 7;
  int32 indexes_added = 8;
  int32 indexes_removed = 9;
  int32 constraints_added = 10;
  int32 constraints_removed = 11;
  bool contains_updates = 12;
}
```

Add an optional `QueryUpdateStats stats = <next free field number>;` to `ExecuteCommandResponse`. In `ArcadeDbGrpcService.executeCommandInternal`, after the drain loop read `rs.getStatistics()` and set the message only when `containsUpdates()`. proto3 message has-semantics mean the field is absent for reads.

### 4. Fidelity fixes

- **Constraint-vs-plain-index miscount** - in `OpenCypherQueryEngine.executeCreateConstraint` (UNIQUE / KEY), `existedBefore` derives from `indexExistsOnProperties(...)`, which is true for *any* index. Adding a UNIQUE/KEY constraint over properties already covered by a **non-unique** index therefore fails to increment `constraints-added`. Narrow the pre-existence check to unique-index / constraint existence so a genuine constraint addition is counted.
- **DETACH DELETE dedup** - `deleteAllEdges()` / `deleteVertex()` do not consult the batch `deleted` set that `deleteObjectStatic()` uses, so an edge removed both by `DETACH DELETE` and by a separate explicit match/delete can be counted twice, where Neo4j dedups. Consult the shared `deleted` set before incrementing `relationshipsDeleted`.
- **Single-source the replace-map rule** - extract the `!prop.startsWith("@") && map.get(prop) == null` removed-property count into one shared static helper used by both `SetStep.applyReplaceMap` and the `MergeStep` REPLACE_MAP branch, so the subtle semantics cannot drift.

## Testing

- **Engine unit** - `QueryStatistics.add()` merge; extend `CypherQueryStatisticsTest`:
  - `CALL { CREATE ... }` reports non-zero counters and `containsUpdates()`.
  - `CREATE ... UNION CREATE ...` sums both branches.
  - `DETACH DELETE` + explicit edge delete counts the shared edge once.
  - Constraint kinds `NOT_NULL` / `KEY` / `TYPED` each increment `constraintsAdded`.
  - UNIQUE/KEY constraint over properties already covered by a plain index increments `constraintsAdded`.
- **HTTP** - new handler test: `POST /command` write returns the camelCase `stats` object with correct values; a read query omits `stats`.
- **gRPC** - new `grpcw` test: `ExecuteCommandResponse.stats` populated and correct for a write; absent for a read.
- **Bolt** - extend `Bolt5000ResultCountersIT` (or a sibling IT) to assert non-zero, correct `summary.counters()` for `CALL { CREATE ... }` and a UNION write over the real `neo4j-java-driver`.

## Rollout

Single PR on branch `feat/5015-cypher-write-counter-surfacing`. TDD: failing tests first per component, then implementation, compile and run affected engine/server/grpcw/bolt tests until green. After the PR is opened, run the code-review response loop (Claude + Gemini) for at least four rounds, answering each via the `receiving-code-review` skill.

## Open risks

- `getStatistics()` is left unchanged; the only new context API is `setStatistics(...)`, used solely to share the outer accumulator into a CALL subquery context. A read-allocates-nothing regression test guards the hot path.
- Making `executeUnion()` materialize the union result for **write** UNIONs (to force branch writes and aggregate counters) changes read behavior for none - the read path stays lazy. Large write-UNION result sets are materialized, matching the existing non-union write path.
- Proto field additions are backward compatible (new field numbers only); no existing field is renumbered.
