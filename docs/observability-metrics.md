# ArcadeDB Observability: Metrics Catalogue

This document describes the always-on Micrometer metrics emitted by ArcadeDB.
All series are additive and backward-compatible: enabling server metrics does not
remove or rename any pre-existing Prometheus series.

---

## `arcadedb.query.duration` - Query/command RED timer

**Prometheus name:** `arcadedb_query_duration_seconds_{count,sum,bucket,max}`

Records the end-to-end execution duration of every query and command that passes
through the ArcadeDB query engine. The timer is registered with a per-percentile
histogram so Prometheus can compute arbitrary quantiles server-side.

### Tags

| Tag        | Values                                                                           | Notes |
|------------|---------------------------------------------------------------------------------|-------|
| `protocol` | `http`, `bolt`, `postgres`, `mongo`, `grpc`, `redis`, `internal`                | Originating wire protocol. `internal` = engine-internal or embedded-API callers. |
| `db`        | database name                                                                   | Taken from the active database at execution time. |
| `language`  | `sql`, `opencypher`, `gremlin`, `graphql`, `mongo`, ...                         | Query language as declared by the caller. |
| `type`      | `query`, `command`                                                               | `query` for read-only SELECT / MATCH; `command` for DDL and write statements. |

The `protocol` tag cardinality is bounded to the seven values above. Tag values
never contain query text; each tag carries only a short symbolic constant.

### Filtering for wire RED (excluding embedded callers)

To compute the RED rate for external wire-protocol traffic only, exclude
`protocol="internal"`:

```promql
-- Request rate (requests per second, all wire protocols)
sum(rate(arcadedb_query_duration_seconds_count{protocol!="internal"}[1m])) by (protocol)

-- p95 latency per protocol
histogram_quantile(0.95,
  sum(rate(arcadedb_query_duration_seconds_bucket{protocol!="internal"}[5m]))
  by (le, protocol))
```

### Sample PromQL - p95 latency by protocol

```promql
histogram_quantile(0.95,
  sum(rate(arcadedb_query_duration_seconds_bucket{protocol!="internal"}[5m]))
  by (le, protocol))
```

---

## Pillar-1 siblings

### `arcadedb.http.requests` - HTTP RED timer

Records every request dispatched by the Undertow handler chain.

Tags: `method` (GET / POST / ...), `path` (Undertow path template, e.g.
`/query/{database}`), `status` (HTTP status code as a string, e.g. `200`, `401`),
`db` (database path-parameter when present, otherwise absent).

The `path` tag uses Undertow's matched path template so the raw database name never
appears in the tag value, keeping cardinality bounded.

### `arcadedb.engine.*` - Engine gauges

`EngineMetricsBinder` exposes live engine counters as Micrometer gauges, scraped
on demand from the internal `Profiler` singleton:

| Metric name                         | Description |
|-------------------------------------|-------------|
| `arcadedb.engine.pageCacheHits`     | Cumulative page cache hits |
| `arcadedb.engine.pageCacheMiss`     | Cumulative page cache misses |
| `arcadedb.engine.walBytesWritten`   | Cumulative WAL bytes written |
| `arcadedb.engine.walTotalFiles`     | Current number of open WAL files |
| `arcadedb.engine.queries`           | Cumulative query executions (engine-wide) |
| `arcadedb.engine.commands`          | Cumulative command executions (engine-wide) |
| `arcadedb.engine.readTx`            | Cumulative read transactions |
| `arcadedb.engine.writeTx`           | Cumulative write transactions |

All gauges are engine-wide aggregates (not per-database). A per-database breakdown
would require a periodically-refreshed `MultiGauge`; this is deferred as a follow-up.

---

## Coverage and known limitations

- **Postgres:** Both the simple query protocol and the extended/prepared-statement
  protocol are covered. `ProtocolContext` is set to `postgres` in both code paths
  before the query reaches the engine, and cleared in the handler's finally block.

- **MongoDB:** Explicit `find` / `runCommand` requests set `protocol=mongo` before
  dispatch. However, unfiltered collection scans that use the engine's internal
  iterate path (rather than going through `query`/`command`) are not timed by this
  metric.

- **gRPC:** The unary `Query` RPC, the server-streaming `StreamQuery` RPC, and the
  unary `BulkInsert` RPC all set `protocol=grpc`. The client-streaming `insertStream`
  RPC's per-message callbacks are NOT yet tagged - each message arrives on a separate
  callback invocation and the per-stream context wiring is tracked as a follow-up.

- **Redis:** The Redis wire-protocol adapter does not route through the ArcadeDB
  query/command engine. It handles key-value operations directly at the protocol
  layer, so no `arcadedb.query.duration` observations are emitted for Redis clients.
  The `redis` value in the `protocol` tag's bounded set is reserved defensively; any
  future engine-routed Redis commands would appear there automatically.

- **Bolt:** All Cypher queries sent over the Neo4j Bolt protocol are covered.

- **HTTP:** All POST `/query/{db}` and POST `/command/{db}` requests are covered.
  The `http` protocol tag also appears on GET `/query/{db}` requests (URL-encoded
  query parameter form).

- **Internal / embedded:** Engine-internal callers and applications using the
  embedded Java API default to `protocol=internal`. These are excluded from wire RED
  dashboards by filtering `protocol!="internal"`.
