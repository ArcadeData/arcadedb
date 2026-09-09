# #7305 — gRPC proto has no time-series API

Type: **feature** (`enhancement`, `grpc`, `server`, `timeseries`, `network`).
Branch: `feat/7305-grpc-proto-has-no-time-series-api`.

## Goal

Give gRPC a first-class time-series surface (typed ingest + query), and close the matching gap on the
Java HTTP client, which reaches none of the `/ts/*` endpoints today either.

## Finding ledger (as read from the issue body)

- [x] 1. `TimeSeriesWrite` (unary) RPC — added, served by `ArcadeDbGrpcService`, tested
- [x] 2. `TimeSeriesWriteStream` (client-streaming) RPC — added, tested across a chunk boundary
- [x] 3. `TimeSeriesQuery` (server-streaming) RPC — added, raw and aggregated, tested
- [x] 4. `TimeSeriesLatest` (unary) RPC — added, tested
- [x] 5. `RemoteGrpcDatabase` — all four, as overrides of the inherited HTTP implementations
- [x] 6. `RemoteDatabase` (HTTP client) — all four, over the existing `/ts/*` endpoints
- [x] 7. OpenAPI — no new HTTP route, so no spec change; `OpenApiSpecGenerationIT` green (18/18)
- [x] 8. Grafana / Prometheus / PromQL over gRPC — out of scope, argued below with the reporter's own reasoning

## Analysis

### Current state, verified

```
$ grep -c -iE "timeseries|time_series|downsample|sealed" grpc/src/main/proto/arcadedb-server.proto
0
$ grep -n '"/ts/' server/src/main/java/com/arcadedb/server/http/HttpServer.java | wc -l
13
$ grep -c -iE "timeseries|\"ts/|/ts/" network/src/main/java/com/arcadedb/remote/RemoteDatabase.java
0
$ grep -c -iE "timeseries" grpc-client/src/main/java/com/arcadedb/remote/grpc/RemoteGrpcDatabase.java
0
$ grep -rn -e '"ts/' -e '/ts/' --include='*.java' network/src/main grpc-client/src/main console/src/main
(no output)
```

So all four of the issue's claims hold: the proto is silent on time series, the HTTP server has 13
`/ts/*` routes, and neither Java client reaches any of them.

### Writers of time-series samples on a request path

```
$ grep -rn -e 'appendBatch(' -e 'appendSamples(' --include='*.java' server/src/main grpcw/src/main
server/.../PostTimeSeriesWriteHandler.java:247:  engine.appendBatch(timestamps, columnValues);
server/.../PostPrometheusWriteHandler.java:176:  engine.appendBatch(timestamps, columnValues);
```

`PostTimeSeriesWriteHandler` is the one that implements the generic contract (line protocol ->
grouped-by-measurement batch append, with the drop sets and the per-type ACL). `PostPrometheusWriteHandler`
implements the Prometheus remote-write contract on top of `getOrCreateType`, which is a different
contract and stays where it is.

### Readers, and the per-type ACL

```
$ grep -rn -e 'requireEngine(' -e 'getEngine(' --include='*.java' server/src/main | grep -c handler
9
```

Every HTTP reader goes through `LocalTimeSeriesType.getEngine(ACCESS.READ_RECORD)` /
`requireEngine(ACCESS.CREATE_RECORD)`. A TimeSeries type owns no record bucket, so that type-name check
is the *only* authorization standing between a denied user and the samples: a new gRPC path that
called `getEngine()` (the unchecked accessor) would be ungated. This is the single most important
invariant for this change.

## Invariant

> Every protocol that reaches time-series samples - HTTP today, gRPC from this change - resolves the
> type, applies the per-type ACL, groups by measurement and appends through **one** shared
> implementation, so a sample written through one protocol and read through the other yields the same
> rows and the same authorization answer.

The structural form of that (one implementation, not two that agree today) is what keeps a later fix
to one protocol from silently missing the other.

## Design

A protocol-neutral core in the **engine** module, `com.arcadedb.engine.timeseries.TimeSeriesGateway`,
carrying what the HTTP handlers do today:

- `write(DatabaseInternal, List<Sample>)` -> `WriteReport` (written / unknown / non-TS / unavailable types),
  including the grouping, the per-type `CREATE_RECORD` ACL, and the batch append;
- `resolveForRead(DatabaseInternal, String)` -> a `TypeResolution` carrying the engine or a typed failure,
  applying the `READ_RECORD` ACL before it looks at engine availability;
- `buildTagFilter` / `resolveColumnIndices` / `findColumnIndex` over plain Java values.

`PostTimeSeriesWriteHandler`, `PostTimeSeriesQueryHandler`, `GetTimeSeriesLatestHandler` and
`TimeSeriesHandlerUtils` delegate to it; `ArcadeDbGrpcService`'s four new RPCs call the same methods.

A shared **client** model in `network` (`com.arcadedb.remote.timeseries`) plus four methods on
`RemoteDatabase` (HTTP) that `RemoteGrpcDatabase` overrides with the gRPC RPCs. Since
`RemoteGrpcDatabase extends RemoteDatabase`, the equivalence test is literally the same code run
against two objects.

`LineProtocolWriter` (engine, next to `LineProtocolParser`) serializes the client point model to line
protocol for the HTTP path, and is round-trip tested against the parser.

## Completeness

### Entry-point coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| gRPC `TimeSeriesWrite` (unary) | yes | yes |
| gRPC `TimeSeriesWriteStream` (client-streaming, crosses a chunk boundary) | yes | yes |
| gRPC `TimeSeriesQuery`, raw rows | yes | yes |
| gRPC `TimeSeriesQuery`, aggregated buckets | yes | yes |
| gRPC `TimeSeriesLatest` | yes | yes |
| gRPC read of a **sealed** (`.ts.sealed`) shard | yes | yes |
| gRPC per-type ACL denial on write and on read | yes | yes |
| `RemoteGrpcDatabase` client methods (all four) | yes | yes |
| `RemoteDatabase` HTTP client methods (all four) | yes | yes |
| HTTP `/ts/write` handler still on the shared core | yes | yes (existing suite + equivalence) |
| HTTP `/ts/query`, `/ts/latest` handlers still on the shared core | yes | yes (existing suite + equivalence) |
| OpenAPI `TimeSeriesApiSpec` | argued (see below) | yes (`OpenApiSpecGenerationIT`) |
| gRPC Grafana / Prometheus / PromQL | argued (see below) | n/a |
| `RemoteDatabase` Grafana / Prometheus client methods | argued (see below) | n/a |

### Argued rows

- **OpenAPI.** This change adds no HTTP route: `RemoteDatabase` now *calls* `/ts/write`, `/ts/query`
  and `/ts/latest`, all three of which `TimeSeriesApiSpec` already documents. gRPC is not described by
  OpenAPI at all - the proto is its contract. The spec therefore needs no new path; that it stays
  correct is verified by running `OpenApiSpecGenerationIT`, whose result is recorded below.
- **Grafana / Prometheus / PromQL over gRPC.** Deliberately out of scope, in the reporter's own words:
  those endpoints exist to satisfy Grafana's datasource protocol and Prometheus' remote read/write,
  both of which speak HTTP, so a gRPC port "would produce a surface with no client". No follow-up issue
  is filed because the issue itself sets the condition for one - "a separate issue with a named
  consumer" - and there is no named consumer.
- **`RemoteDatabase` Grafana / Prometheus methods.** Same argument: a Java application does not consume
  its own database through Grafana's datasource protocol.

## Bug found and fixed on the way

The field projection was resolved with the wrong index convention, and had been since the endpoint
existed. `TimeSeriesBucket.readRow` always prepends the timestamp and then tests each **non-timestamp**
column's own ordinal against `columnIndices`; `TagFilter.matchesMapped` reads it the same way. But
`TimeSeriesHandlerUtils.resolveColumnIndices` returned **full-schema** indices, counting the timestamp
column as 0 and shifting every field by one. `POST /ts/{db}/query` with `fields` therefore answered the
neighbouring column's values under the requested column's name, with a trailing null:

```
columns: ["ts", "temperature"]
rows:    [[1000, "us-east", null]]
```

`PostGrafanaQueryHandler` had the same defect one step further along, mapping the indices back with
`columns.get(idx)`.

It survived because the existing tests assert on the column NAMES, which looked right
(`TimeSeriesQueryHandlerIT.queryWithFieldProjection`), and `GrafanaTimeSeriesHandlerIT` never projects
at all. The new gRPC surface would have inherited it, so it is fixed here rather than filed:
`TimeSeriesGateway.resolveColumnIndices` now returns ascending, de-duplicated non-timestamp indices, and
`selectedColumns`/`columnNames` map them back. Three tests pin it, one per protocol
(`TimeSeriesGatewayProjectionTest`, `Issue7305RemoteDatabaseTimeSeriesIT`,
`Issue7305GrafanaFieldProjectionIT`, `Issue7305TimeSeriesGrpcIT`), each asserting the VALUES and not only
the names.

## Adversarial pass

The skill's Phase 1.5 spawns an isolated `general-purpose` subagent for this. **No `Task` tool is
available in this environment**, so no ignorant reviewer could be spawned; the pass was run by the author
against the staged diff instead, which is weaker and is recorded as such. What it found:

| Finding | Disposition |
|---|---|
| `timeSeriesWriteStream` re-mapped an already-mapped failure, flattening every streaming error to `gRPC error: UNKNOWN` | **Real, fixed here.** `callAsyncDuplex`'s `wrapObserver` already runs `handleGrpcException`; mapping again lost the status. Caught by `Issue7305TimeSeriesGrpcAclIT`, which went from red to green on the fix |
| `TS_DEFAULT_WRITE_CHUNK_SIZE` was declared and then made unreachable by the `chunkSize <= 0` guard above it | **Real, fixed here.** Constant and the dead ternary removed |
| `TimeSeriesGateway.write` called `database.begin()` unconditionally; gRPC reuses pool threads, so it could nest inside a transaction the call does not own and then commit it | **Real, fixed here.** `beganHere` guard, the same pattern `createRecordInternal` uses. Costs nothing when skipped: `TimeSeriesShard.appendSamples` commits its own shard transaction either way |
| The whole streaming write runs under one gRPC deadline (`getTimeout()`, default 30 s), which is a trap on the RPC that exists for large ingest | **Real, documented here.** Named on the client method with the remedy (`setTimeout`, or split the batch). Not changed silently: a client-chosen deadline is the client's to choose |
| `LineProtocolWriter` writes a `BigDecimal` bare, so it reads back as a double | **Not a defect.** `ColumnDefinition.isStorableType` refuses DECIMAL, so no TimeSeries column can hold one and the value is destined for a FLOAT/DOUBLE column; quoting it would send text to a numeric column. Javadoc now says so |
| Write stream ignores a later chunk's `database`, unlike `insertStream` (#6597) | **Not a defect.** The proto states it (`REQUIRED on the first chunk; ignored on later ones`) and the alternative is a security lookup on the per-chunk hot path of a single authenticated call |

## Residual risk

What this change does **not** cover, in plain language:

- **Grafana, Prometheus remote read/write and PromQL are not on gRPC**, and neither client exposes them.
  Deliberate, in the reporter's words: those endpoints satisfy Grafana's datasource protocol and
  Prometheus' remote read/write, both of which speak HTTP, so porting them "would produce a surface with
  no client". The issue itself sets the bar for revisiting - "a separate issue with a named consumer" -
  and there is no named consumer, so no follow-up was filed.
- **A database-scoped user still cannot use gRPC at all** (#7320). `RemoteGrpcServer` sends no database
  header, so `GrpcAuthInterceptor` authenticates every call against the literal name `"default"`. This
  predates the change and affects every RPC; `Issue7305TimeSeriesGrpcAclIT` works around it by granting
  the scoped user `"*"` databases, and says so, so the test still proves the per-TYPE ACL it is about.
- **`latest` takes one tag over HTTP** (#7321), so the shared client method takes one predicate even
  though the gRPC RPC accepts a filter map. Closing it means changing a contract
  `TimeSeriesApiSpecTest` pins, which this PR must not edit to make its own change pass.
- **`latest` is O(series)** (#7322): it scans the whole range and takes the last row.
  `queryDescending(..., limit 1)` already exists and is O(shards x blocks), but switching changes the
  tie-break among equal maximum timestamps, and this PR needed both protocols to answer identically
  against the existing baseline.
- **HTTP row values keep the JSON parser's boxing.** The two protocols carry the same values, not the
  same boxed types - gRPC ships a DOUBLE as a `double`, the HTTP answer is JSON text whose numbers the
  parser boxes as whatever fits. The equivalence tests compare numerically and say why.

## Verification

| Suite | Result |
|---|---|
| `engine`, `com.arcadedb.engine.timeseries.**` (incl. the two new unit tests) | 593/593 |
| `server` `Issue7305*IT` | 9/9 |
| `grpc-client` `Issue7305*IT` (incl. the ACL IT) | 13/13 |
| `grpcw` unit tests | 211/211 |
| `network` unit tests | 478/478 |
| `server` `OpenApiSpecGenerationIT` | 18/18 |

**Not verifiable on this machine:** `TimeSeriesQueryHandlerIT`, `GrafanaTimeSeriesHandlerIT`,
`Issue7043AbsentMinMaxOverHttpIT`, `PromQLHttpHandlerIT` and `Issue4562RollbackDeleteTest` hard-code
`127.0.0.1:2480`, and a developer's own ArcadeDB 26.9.1 (homebrew, up since Sep 4) holds that port, so
they reach the wrong server and fail as 401/500/503 - the collision `CLAUDE.md` documents. Confirmed:
`curl http://127.0.0.1:2480/api/v1/server` answers 401 from that instance, and every test that resolves
the bound port with `getServer(0).getHttpServer().getPort()` passes. Every new test in this PR resolves
the port. CI runs the hard-coded ones on a free port.

**Proof the new tests can fail.** Two of them were observed red before the code that makes them green:
`Issue7305RemoteDatabaseTimeSeriesIT.aTagFilterAndAFieldProjectionNarrowTheAnswer` (`[1000, "us-east",
null]`, which is how the projection bug was found) and `Issue7305TimeSeriesGrpcAclIT` (streaming write
answering `gRPC error: UNKNOWN`). `Issue7305GrafanaFieldProjectionIT` was falsified deliberately by
restoring the old `columns.get(idx)` mapping - it failed with "expected 2 fields but was 1" - and the fix
was then restored and re-run green.

## PR

https://github.com/ArcadeData/arcadedb/pull/7323

## Review cycles

### Cycle 1 — `2b02d75d` — claude review, three findings

| Finding | Verified? | Disposition |
|---|---|---|
| `GET /ts/{db}/latest` does not render a non-finite sample as null, so it disagrees with its new gRPC twin | **Yes, reproduced** | **Fixed.** `putSampleValue` in the loop |
| `PostTimeSeriesQueryHandler.executeAggregation` hand-rolls the column-index scan the new shared helper does | Yes, read the code | **Fixed.** One-line swap to `TimeSeriesHandlerUtils.findColumnIndex` |
| `AggregationType.valueOf` is unguarded on both HTTP aggregation paths, unlike the gRPC one | Yes, read the code | **Filed as #7325**, on the reviewer's own framing ("pre-existing... a good candidate for a quick follow-up") |

**One correction to the review, recorded because the difference matters.** The review predicted the
endpoint "likely 500s ... `NaN is not a valid double value as per JSON specification`". It does not.
`JSONArray.put(Object)` routes through `JSONObject.objectToElement`, which reaches
`new JsonPrimitive(number)`, and what actually came back over the wire was the **token** `NaN`, which the
client read as the string `"NaN"`:

```
[HTTP renders a non-finite sample as null too]
expected: null
 but was: "NaN"
```

The conclusion the review drew from it is exactly right - the two protocols disagreed on the one input
this change is about, and a string where every sibling path answers `null` is arguably worse than a 500,
because it is silent. Only the predicted failure mode was wrong. The fix is the one the review suggested.

The regression test is `Issue7305TimeSeriesGrpcIT.aNonFiniteLatestSampleIsNullOnBothProtocols`, which
stores the NaN through the typed gRPC write (the path that can actually produce one - `LineProtocolWriter`
refuses it on the HTTP side) and then asserts both protocols answer `null`. It was observed red before the
fix, with the output above.

Nothing was deferred: every finding is fixed here or filed.
