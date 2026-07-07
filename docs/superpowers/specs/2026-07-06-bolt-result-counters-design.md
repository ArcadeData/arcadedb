# Bolt ResultSummary counters for write queries (#5000)

- Issue: [#5000](https://github.com/ArcadeData/arcadedb/issues/5000)
- Tracking: [#4890](https://github.com/ArcadeData/arcadedb/issues/4890) (Group C protocol/type-fidelity gaps)
- Epic: [#4882](https://github.com/ArcadeData/arcadedb/issues/4882) (Bolt Driver Compatibility Certification)
- Conformance scenario: `RESULT-004` in `bolt/conformance/spec.yaml` (`expected-fail` -> `passing`)

## Problem

`ResultSummary` counters (`nodesCreated`, `relationshipsCreated`, `propertiesSet`, etc.) are
never populated for Bolt write queries. Two layers are missing:

1. `BoltNetworkExecutor`'s `handleRun`/`handlePull`/`handleDiscard` build no `stats` map in the
   SUCCESS response metadata.
2. The underlying Cypher engine has no CRUD-counter tracking. The mutation steps
   (`CreateStep`/`SetStep`/`DeleteStep`/`MergeStep`/`RemoveStep`) do not aggregate counts, so even
   a Bolt-layer fix would have nothing to report. `CreateStep` keeps `vertexCount`/`edgeCount`
   fields but they are used only for `prettyPrint` profiling output, never surfaced.

The DDL path (`CREATE`/`DROP INDEX`, `CREATE`/`DROP CONSTRAINT`) executes directly in
`OpenCypherQueryEngine.executeDDL()`, outside the step pipeline, so index/constraint counts need
their own small instrumentation point.

This is the largest of the #4890 gaps because it is cross-cutting into the query engine, not
writer-only.

## Goals

- Populate the Bolt terminal-PULL/DISCARD SUCCESS metadata with a `stats` map so official Neo4j
  drivers report accurate `summary.counters()`.
- Track the full Neo4j counter set that maps to ArcadeDB's Cypher mutation and DDL paths.
- Add no measurable overhead and no allocation to read queries.
- Build the accumulator as a reusable engine capability so HTTP and gRPC can surface the same
  counters later at low cost (follow-up issue).

## Non-goals

- Wiring HTTP `/command` or gRPC responses in this PR. A follow-up issue tracks that; the engine
  channel is designed so it is cheap.
- SQL-engine write statistics beyond the existing scalar affected-row `count` row.
- Neo4j `contains-system-updates` semantics (system database of users/roles) - ArcadeDB has no
  equivalent; it is reported as absent/false.

## Design

### Data flow

```
Cypher mutation steps ── increment ─▶ QueryStatistics (held on CommandContext)
        │                                      │
        │  DDL path: executeDDL() increments a QueryStatistics directly
        ▼                                      ▼
CypherExecutionPlan.execute() / executeDDL() ── attach ─▶ ResultSet.getStatistics()
                                                              │
BoltNetworkExecutor.handlePull / handleDiscard ── read ──────┘
        │
        ▼
   terminal SUCCESS metadata { ..., "stats": { "nodes-created": 2, ... } }
```

One accumulator object threaded through the shared `CommandContext` that every execution step
already carries, read once at ResultSet assembly, surfaced through a new `ResultSet` channel,
consumed by the Bolt layer.

### Component: `QueryStatistics`

New value object in `com.arcadedb.query.sql.executor` (alongside `ResultSet`), primitive `int`
counters only (allocation-light, GC-friendly):

- `nodesCreated`, `nodesDeleted`
- `relationshipsCreated`, `relationshipsDeleted`
- `propertiesSet`
- `labelsAdded`, `labelsRemoved`
- `indexesAdded`, `indexesRemoved`
- `constraintsAdded`, `constraintsRemoved`

Plus `boolean containsUpdates()` (true if any counter > 0) and increment methods. The object is
engine-neutral: it does not know about Bolt wire keys. Mapping to hyphenated Bolt keys lives in the
Bolt module.

### Engine accumulator

- `CommandContext`: add `QueryStatistics getStatistics()`. Implemented on `BasicCommandContext` as
  a first-class nullable field, lazily created on first mutation. Read queries never touch it, so it
  stays `null` and allocates nothing.
- Instrument the five mutation steps (all extend `AbstractExecutionStep`, so `context` is in hand):
  - `CreateStep`: `+nodesCreated` per vertex, `+relationshipsCreated` per edge, `+propertiesSet`
    per inline property assigned at creation, `+labelsAdded` per label. Reuses the existing
    `vertexCount`/`edgeCount` increment sites.
  - `SetStep`: `+propertiesSet` per property write; `+labelsAdded` for `SET n:Label`.
  - `RemoveStep`: `+propertiesSet` per property removed (Neo4j counts property removals under
    `propertiesSet`); `+labelsRemoved` per label removed.
  - `DeleteStep`: `+nodesDeleted` / `+relationshipsDeleted` per deleted record.
  - `MergeStep`: on-create routes through the same create/set counting.
- DDL path in `OpenCypherQueryEngine`: `+indexesAdded`/`+indexesRemoved`,
  `+constraintsAdded`/`+constraintsRemoved` in `executeCreateIndex`/`executeDropIndex`/
  `executeCreateConstraint`/`executeDropConstraint`, guarded so `IF NOT EXISTS` / `IF EXISTS`
  no-ops do not over-count.

### Surfacing channel: `ResultSet.getStatistics()`

Add to the `ResultSet` interface:

```java
default Optional<QueryStatistics> getStatistics() {
  return Optional.empty();
}
```

This mirrors the existing `getExecutionPlan()` default exactly. Because it is a default method, the
blast radius on the many `ResultSet` implementations is zero; only the Cypher result path overrides
it. Populated:

- in `CypherExecutionPlan.execute()`, read from `context.getStatistics()` after the write path has
  been materialized (writes are fully materialized before `execute()` returns);
- in `OpenCypherQueryEngine.executeDDL()`, from the `QueryStatistics` incremented inline.

This is the reusable channel HTTP and gRPC will consume in the follow-up issue - both hold a
`ResultSet`, so surfacing there becomes a few lines.

### Bolt consumer

In `BoltNetworkExecutor`:

- `handlePull`, terminal `!hasMore` block (alongside `type` / `t_last` / plan metadata): read
  `currentResultSet.getStatistics()`; if present and `containsUpdates()`, put a `stats` map into the
  SUCCESS metadata.
- `handleDiscard`: same, before the final `sendSuccess(metadata)`.

The `stats` map uses Neo4j Bolt hyphenated keys and includes only non-zero counters plus
`contains-updates: true`:

`nodes-created`, `nodes-deleted`, `relationships-created`, `relationships-deleted`,
`properties-set`, `labels-added`, `labels-removed`, `indexes-added`, `indexes-removed`,
`constraints-added`, `constraints-removed`, `contains-updates`.

Read queries produce no `stats` key, matching Neo4j behavior.

## Testing

Test-first (TDD), server-side code covered by test cases per project convention.

- Engine unit test (`CypherQueryStatisticsTest` in `engine`): assert counters for CREATE, SET,
  DELETE, MERGE (match vs create), REMOVE, `SET n:Label`, `REMOVE n:Label`, `CREATE INDEX`,
  `CREATE CONSTRAINT`, `DROP INDEX`, `DROP CONSTRAINT`, including the RESULT-004 shape
  `CREATE (:Beer {name})-[:BREWED_BY]->(:Brewery {name})` -> 2 nodes, 1 rel, 2 properties.
- Read-query regression: a `MATCH ... RETURN` leaves `getStatistics()` empty and allocates no
  `QueryStatistics`.
- Bolt IT (`bolt` module or `RemoteBoltDatabaseIT`): via the real `neo4j-java-driver`, assert
  `summary.counters().nodesCreated()`, `relationshipsCreated()`, `propertiesSet()` for a write; and
  that a read reports no updates (`containsUpdates() == false`).
- Conformance: flip `RESULT-004` `current_status` `expected-fail` -> `passing` in
  `bolt/conformance/spec.yaml`; keep the spec-validator unit test green.

## Verification of done

- `RESULT-004` passes across the five per-language e2e suites; `summary.counters()` reflects actual
  writes.
- Read queries carry no `stats` key and allocate no `QueryStatistics` (no measurable overhead).
- No regression in existing write-query behavior.
- Follow-up issue opened for HTTP `/command` (and gRPC) to surface the same `QueryStatistics`.

## Follow-up

- New issue (child of #4890 or a fresh tracking item): surface `QueryStatistics` in the HTTP
  `/command` JSON response and in the gRPC result summary, consuming the `ResultSet.getStatistics()`
  channel added here.
