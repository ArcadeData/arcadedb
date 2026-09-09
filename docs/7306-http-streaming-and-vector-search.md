# #7306 — HTTP API has no streaming query and no vector search endpoint

Issue: https://github.com/ArcadeData/arcadedb/issues/7306
Type: feature (`enhancement`, `grpc`, `server`, `network`, `vector`)
Branch: `feat/7306-http-streaming-and-vector-search`

## Finding ledger

- [x] 1. **No streaming.** `PostQueryHandler`/`GetQueryHandler`/`PostCommandHandler` materialize the whole result
      set into a `JSONObject` before the first byte reaches the client.
- [x] 2. **No vector search endpoint.** Vector search has no HTTP route and no gRPC RPC; the only structured
      access is MCP.
- [x] 3. Contract surface: proto (`grpc/src/main/proto/arcadedb-server.proto`).
- [x] 4. Contract surface: OpenAPI (`CoreApiSpec` streaming negotiation, new `VectorApiSpec`).
- [x] 5. Contract surface: client (`RemoteDatabase`, `RemoteGrpcDatabase`).

## Analysis

### Streaming

`AbstractQueryHandler.serializeResultSet` walks the `ResultSet` and appends every row into a `JSONArray` held by
the response `JSONObject`; `AbstractServerHttpHandler` then hands the finished string to
`ExecutionResponse.send()`. Nothing is written to the socket until the last row is serialized.

The codebase already streams in two places, and both are the template for the split:

- `PostBatchHandler` reads an `application/x-ndjson` request body incrementally and writes an NDJSON
  response, returning `null` from `execute()` to mean "response already sent".
- `AiChatHandler` (`POST /api/v1/ai/chat/stream`) sets the content type, calls `exchange.startBlocking()`,
  writes to `exchange.getOutputStream()` and flushes per event.

So the mechanism exists; what is missing is a streaming serializer for a `ResultSet` and the `Accept`
negotiation that selects it.

### Vector search

`MCPVectorLeg`, `VectorSearchTool`, `HybridSearchTool` and `FullTextSearchTool` live in `mcp`, which depends on
`server` in `provided` scope. `server` therefore cannot see them, so an HTTP handler could not reuse the bounds
the issue asks it to reuse — it would have to re-derive them, which is exactly the divergence the issue names.

Everything in those four classes below the MCP boundary (`config.isAllowReads()` +
`MCPToolUtils.resolveDatabase`) touches only engine types: `Database`, `TypeIndex`, `FullTextSearch`,
`QueryEngine`, `JsonSerializer`, `JSONObject`. So the search core moves down into `engine`, under
`com.arcadedb.query.search`, where `server`, `mcp` and `grpcw` can all reach one implementation.

## Completeness

### Invariant 1 (streaming)

> A caller that sends `Accept: application/x-ndjson` to a query or command endpoint receives one JSON result
> per line, written and flushed as each row is produced; every other caller receives byte-identical
> `application/json` to today.

### Invariant 2 (vector search)

> kNN, hybrid and full-text search are reachable over HTTP and over gRPC, and every surface validates
> `efSearch`, `k` and `limit` through the same code the MCP tools validate them with.

### Sweep commands

```
$ grep -rn "serializeResultSetBounded\|serializeResultSet(" --include='*.java' server/src/main mcp/src/main
server/.../GetQueryHandler.java:86        serializeResultSetBounded(...)
server/.../PostCommandHandler.java:268    serializeResultSetBounded(...)   # non-EXPLAIN path
server/.../PostCommandHandler.java:289    serializeResultSetBounded(...)   # EXPLAIN path
server/.../AbstractQueryHandler.java:248  declaration
server/.../AbstractQueryHandler.java:265  declaration

$ grep -rln "extends AbstractQueryHandler\|extends PostCommandHandler" --include='*.java' server/src/main
server/.../GetQueryHandler.java
server/.../PostCommandHandler.java
server/.../PostQueryHandler.java      # extends PostCommandHandler, overrides only executeCommand()

$ for f in proto HttpServer RemoteDatabase RemoteGrpcDatabase; do grep -ic vector ...; done
grpc/src/main/proto/arcadedb-server.proto            vector=0
server/.../http/HttpServer.java                      vector=0
network/.../remote/RemoteDatabase.java               vector=0
grpc-client/.../remote/grpc/RemoteGrpcDatabase.java  vector=0

$ grep -rln 'x-ndjson' --include='*.java' . | grep -v /target/
network/.../RemoteDatabase.java            # request body of sendBatch, not a response
server/.../PostBatchHandler.java           # request body + NDJSON response of /batch
server/.../openapi/CoreApiSpec.java        # /batch request body media type
(+ 8 test classes, all about /batch)

$ grep -rn "MCPVectorLeg" --include='*.java' . | grep -v /target/ | cut -d: -f1 | sort | uniq -c
   1 mcp/.../FullTextSearchTool.java
  15 mcp/.../HybridSearchTool.java
   2 mcp/.../MCPVectorLeg.java
  10 mcp/.../VectorSearchTool.java
   2 mcp/.../HybridSearchCapsTest.java
   3 mcp/.../MCPServerPluginTest.java
   2 mcp/.../Issue6837ToolInputBoundsFollowUpTest.java
```

### Coverage table

| # | Entry point | Covered by fix? | Covered by a test? |
|---|---|---|---|
| 1 | `POST /api/v1/query/{database}` → NDJSON | yes | yes |
| 2 | `POST /api/v1/command/{database}` → NDJSON | yes | yes |
| 3 | `GET /api/v1/query/{database}/{language}/{command}` → NDJSON | yes | yes |
| 4 | `POST /api/v1/command/{database}` EXPLAIN path → still buffered JSON | yes (argued: an execution plan is one object, not a row stream) | yes |
| 5 | default `application/json` on all three, byte-shape unchanged | yes | yes |
| 6 | `RemoteDatabase` streaming query client | yes | yes |
| 7 | `POST /api/v1/vector/{database}/search` | yes | yes |
| 8 | `POST /api/v1/vector/{database}/hybrid` | yes | yes |
| 9 | `POST /api/v1/vector/{database}/fulltext` | yes | yes |
| 10 | MCP `vector_search` / `hybrid_search` / `full_text_search` — same bounds, one implementation | yes | yes (existing MCP suites re-run against the delegating tools) |
| 11 | gRPC `VectorSearch` / `HybridSearch` / `FullTextSearch` | yes | yes |
| 12 | `RemoteDatabase` vector client methods | yes | yes |
| 13 | `RemoteGrpcDatabase` vector client methods | yes | yes |
| 14 | OpenAPI: NDJSON response media type + `VectorApiSpec` | yes | yes |
| 15 | HTTP counterpart of gRPC `InsertStream` / `InsertBidirectional` / `GraphBatchLoad` | no — filed | n/a |
| 16 | Studio UI surface for the new vector routes | no — filed | n/a |
| 17 | gRPC search inside a client transaction | no — filed | n/a |
| 18 | `POST /command` streaming a WRITE, inside the auto-commit wrapper | yes — refused with 400 | yes |
| 19 | Streamed client calls under HA `READ_YOUR_WRITES` | yes | yes |


### Follow-ups filed before the PR opened

| Row | Gap | Issue |
|---|---|---|
| 15 | HTTP has no counterpart for the gRPC client-streaming insert RPCs (`InsertStream`, `InsertBidirectional`, `GraphBatchLoad`) | [#7311](https://github.com/ArcadeData/arcadedb/issues/7311) |
| 16 | Studio has no UI for the new vector search endpoints | [#7312](https://github.com/ArcadeData/arcadedb/issues/7312) |
| 17 | The gRPC search RPCs carry no `TransactionContext`, so they cannot run inside a client transaction the way the HTTP routes run inside a session (found by the adversarial pass) | [#7326](https://github.com/ArcadeData/arcadedb/issues/7326) |

### Residual risk

What this change does **not** cover:

- **The other three gRPC streaming capabilities.** `POST /api/v1/batch/{database}` already reads an NDJSON
  request body incrementally, so the ingress half of client-streaming exists; what has no HTTP counterpart is a
  per-chunk *response*. Tracked as #7311.
- **Studio.** The routes are in the OpenAPI document and reachable from both drivers, but nothing in the webapp
  calls them. Tracked as #7312.
- **The `graph` and `studio` serializers cannot stream, and say so.** They build response-level `vertices` and
  `edges` arrays de-duplicated across the whole result, so no row can be emitted before the last is read. A
  request that combines them with `Accept: application/x-ndjson` is refused with 400 rather than silently
  answered with a buffered body. Making them streamable would need a different wire shape (per-row element
  deltas), which is a design decision, not an omission.
- **The hard row ceiling behaves differently on the streamed path, deliberately.** `SERVER_HTTP_QUERY_MAX_RESULT_ROWS`
  refuses a buffered response with 413 because a buffered response is fully resident before its size is known
  (#5719). A streamed response is never resident, so the ceiling caps the stream and is reported through the
  summary's `truncated` flag instead of failing a response whose first rows the client may already have consumed.
  `Issue7306StreamingQueryIT.theCeilingThatRefusesABufferedResponseOnlyCapsAStreamedOne` pins both halves.
- **The streamed response carries no execution plan.** `explain` / `explainPlan` / `stats` / `profile` are
  response-level fields with nowhere to go in a row stream, except EXPLAIN itself, whose whole payload is the plan
  and which streams it on the summary line. A caller that wants a plan alongside rows asks for the buffered
  response, which is unchanged.

## Reachability

Every changed path is reached by something on a live path, verified by command rather than by reading:

- The three handlers are constructed in `HttpServer.setupRoutes()` (`grep -n 'new PostVector'`), and
  `OpenApiSpecGenerationIT.coreOperationsMatchTheServersActualRegisteredRoutes` compares the served document
  against the server's actual registered routes, so a route present in one and not the other fails the build.
- `VectorApiSpec` is in `OpenApiSpecGenerator.CONTRIBUTORS`; a contributor absent from that list is silently
  omitted from the served document, which is why the same IT is the gate on it.
- The three gRPC RPCs are `@Override`s of the generated `ArcadeDbServiceImplBase`, so an unimplemented one would
  answer UNIMPLEMENTED rather than silently doing nothing - and `Issue7306GrpcSearchIT` drives all three over a
  real channel.
- The shared operations are reached from all three surfaces: `VectorSearchTool`/`HybridSearchTool`/
  `FullTextSearchTool` (MCP), the three HTTP handlers, and the three RPCs. The MCP suites re-run unchanged against
  the delegating tools, which is what proves the delegation preserved behavior rather than merely compiling.

## Notes on existing tests that had to change

`OpenApiSpecGenerationIT` holds a hand-maintained inventory of every documented operation and asserts the exact
count. Three new routes make that inventory wrong by construction, so `EXPECTED_CORE_OPERATIONS` gained three
entries and the count moved 65 -> 68 (the test method's name carries the number, so it moved too). This is the
maintenance the anti-drift test exists to force; no assertion was weakened and nothing was removed.

`CoreApiSpecStreamingTest` caught a real defect while being written: the first attempt at adding the streaming
`Accept` parameter landed on `POST /api/v1/server` instead of `POST /api/v1/command/{database}`, because the two
share a request-body line. `POST /api/v1/server` streams Server-Sent Events for restore/import progress, which is
a different body in a different media type, so it now takes `createCommandResponses(false)` and declares no NDJSON
alternative.

## Changes

### Shared search core (`engine`)

New package `com.arcadedb.query.search`, extracted from the MCP tools so `server`, `mcp` and `grpcw` can all
reach one implementation - `mcp` depends on `server` in `provided` scope, so an HTTP handler could not otherwise
have seen them and would have had to re-derive the bounds, which is exactly the divergence the issue named.

| Class | From | Public entry points |
|---|---|---|
| `VectorSearchLeg` | `MCPVectorLeg` | `validateArguments`, `build`, `toRID`, `DEFAULT_K`/`MAX_K`/`MAX_EF_SEARCH` |
| `VectorSearchOperation` | `VectorSearchTool` | `execute(Database, JSONObject[, indexNameField])`, `requireK` |
| `HybridSearchOperation` | `HybridSearchTool` | `validateArguments`, `execute`, `legLimit`, `collectSeeds` |
| `FullTextSearchOperation` | `FullTextSearchTool` | `validateArguments`, `execute`, `DEFAULT_LIMIT`/`MAX_LIMIT` |

The MCP tools keep their JSON-Schema definitions and their `execute(server, user, args, config)` signatures, and
now do only the MCP-side work - the `isAllowReads()` gate and `resolveDatabase` - before delegating. Their
published caps are read from the operations rather than restated, so the advertised schema cannot drift from what
the search enforces. `MCPVectorLeg` survives as a constants shim because the MCP test suite reads its bounds.

### Streaming (`server`, `network`)

- `AbstractQueryHandler`: `wantsNdjson`, `supportsStreaming`, `streamResultSetAsNdjson`, and the
  `http.query.stream.rows` counter.
- `GetQueryHandler` and `PostCommandHandler` (so `PostQueryHandler` too) take the streamed path when the caller
  negotiates it, and return `null` to mean "the response has already been written".
- `GetQueryHandler.handleRequest` dispatches a streaming GET to a worker thread - `startBlocking()` is illegal on
  an Undertow I/O thread - and only a streaming GET, so a buffered one is unaffected.
- `RemoteDatabase.queryStreaming(...)` plus `NdjsonResultSet`, a lazy `ResultSet` over the response body.

### Vector search (`server`, `network`, `grpc`, `grpcw`, `grpc-client`)

- `AbstractVectorSearchHandler` + `PostVectorSearchHandler` / `PostVectorHybridHandler` /
  `PostVectorFullTextHandler`, registered at `/api/v1/vector/{database}/{search,hybrid,fulltext}`.
- `VectorApiSpec`, registered in `OpenApiSpecGenerator.CONTRIBUTORS`, with a new `Vector` root tag.
- Proto: `VectorSearch`, `HybridSearch`, `FullTextSearch` RPCs and their messages, plus the shared `SearchHit`.
- `ArcadeDbGrpcService`: the three handlers, translating to and from the shared operations' JSON.
- `RemoteDatabase.vectorSearch/hybridSearch/fullTextSearch` and
  `RemoteGrpcDatabase.vectorSearch/hybridSearch/fullTextSearch`.

## Test results

| Suite | Result |
|---|---|
| `Issue7306VectorSearchEndpointsIT` (server, new) | 17/17 |
| `Issue7306StreamingQueryIT` (server, new) | 12/12 |
| `Issue7306RemoteClientIT` (server, new) | 9/9 |
| `Issue7306GrpcSearchIT` (grpcw, new) | 13/13 |
| `Issue7306GrpcClientSearchIT` (grpc-client, new) | 5/5 |
| `VectorApiSpecTest` + `CoreApiSpecStreamingTest` (server, new) | 13/13 |
| `mcp` module, whole suite | 319 unit + 5 IT, 0 failures |
| `server` module, unit | 965, 0 failures |
| `server` module, `OpenApiSpecGenerationIT` | 18/18 |
| `network` + `grpc` + `grpcw` + `grpc-client`, whole suites | 307, 0 failures |

### The streaming test was falsified before it was trusted

`theFirstRowReachesTheClientBeforeTheLastOneIsProduced` reads one line and then asks the server how many rows it
had produced by then. To confirm it can fail, `streamResultSetAsNdjson` was temporarily changed to accumulate
every line into a `ByteArrayOutputStream` and write it at the end - a buffered implementation with identical
output - and the test went red with "the server had produced the whole result set before the client saw its first
row". The patch was reverted and the test is green again. Without that check the test would have proven only that
the response is newline-framed.

### Environment note on the full `server` IT run

The full `server` integration run reported 22 failures on this machine. Every one is the port-2480 conflict
CLAUDE.md documents: this machine has a brew-installed ArcadeDB 26.9.1 listening on 2480 (five days uptime) and
another agent's test server on 2481, while `BaseGraphServerTest` builds its URLs as `127.0.0.1:248<serverIndex>`.
The signatures are the documented ones - `403 Too many failed authentication attempts`, `FileNotFound
http://localhost:2480/...`, `503`, `Socket Unexpected end of file` - and `StudioProductionModeIT` is the clearest
case of all: it asserts `GET /` answers 404 in production mode and got 200, because the request reached the brew
server, which serves Studio.

Every one of those 22 was re-run in isolation and passed. The two new server IT classes read the port from
`getServer(0).getHttpServer().getPort()` rather than hard-coding it, precisely so they cannot produce this class
of false red.

## Adversarial pass

The skill's Phase 1.5 spawns an independent subagent for this. No `Task` tool is exposed in this environment, so
the pass was run by the same agent that wrote the patch - which is weaker, because that agent is already persuaded,
and it is recorded here as such rather than as an independent review. It still found two real defects and one gap.

### 1. A streamed write on `POST /command` was unsound — REAL, fixed here

`PostCommandHandler` does not override `requiresTransaction()`, so `DatabaseAbstractHandler` runs it through
`executeInTransaction(...)`: the transaction commits **after** `execute()` returns, and
`LocalDatabase.transaction(..., retries)` re-runs the lambda on `NeedRetryException | DuplicatedKeyException`.

The first draft streamed the response from inside that wrapper, which is wrong twice over:

- the client received `200` and every row **before** the commit that could still fail, so a write could be
  acknowledged with a result the commit never made durable;
- a retry would call `execute()` again on an exchange whose body had already gone out, writing a second copy of
  the whole stream - two concatenated result sets and two summary lines, which no client could detect.

Fixed by refusing to stream anything but a read-only command: `refuseStreamingIfNotReadOnly` asks the engine's own
parser (`getQueryEngine(language).analyze(command).isIdempotent()`), the same source SQL uses to decide whether a
statement may go through `query()` at all, and answers 400 naming the buffered alternative otherwise. A read-only
transaction has no commit that can conflict and nothing to retry, so both failure modes are gone rather than
narrowed. `beginNdjsonResponse` also refuses outright if `exchange.isResponseStarted()`, so any future path that
reaches it twice fails loudly instead of corrupting a body.

Covered by `aWriteCommandIsRefusedRatherThanStreamedAheadOfItsCommit` (which also asserts the refused write left
no row behind), `theSameWriteStillSucceedsOnTheBufferedPath` - without which the refusal could be passing because
the write broke rather than because it was refused - and `aDdlCommandIsRefusedToo`.

### 2. The new client calls dropped the HA read-consistency headers — REAL, fixed here

`RemoteHttpComponent.httpCommand` injects `X-ArcadeDB-Read-Consistency` and `X-ArcadeDB-Read-After` on every
request that goes through it, and `RemoteDatabase` captures `X-ArcadeDB-Commit-Index` off the response. Both new
client paths build their own requests - `queryStreaming` because it needs the body as a stream, the vector calls
because their response is not a `ResultSet` - and so bypassed all three. A client that had declared
`READ_YOUR_WRITES` would have kept it everywhere except on exactly these endpoints, and a stale answer from a
lagging follower looks exactly like a complete one.

Fixed with `addReadConsistencyHeaders`, applied to both, plus `captureCommitIndexHeader` on both responses
(captured before the status is examined, as `sendBatch` does: the bookmark describes the server that answered and
is as valid on a refusal as on a success). `captureCommitIndexHeader` was widened from `HttpResponse<String>` to
`HttpResponse<?>` so the streamed response can use it. Covered by
`aStreamedReadUnderReadYourWritesSeesTheWriteThatPrecededIt`.

### 3. gRPC search cannot join a client transaction — REAL, out of scope, filed as #7326

The three new request messages carry no `TransactionContext`, unlike `LookupByRidRequest` and
`ExecuteQueryRequest`, so a search issued inside an open gRPC transaction runs outside it. HTTP does not have this
gap: `AbstractVectorSearchHandler` extends `DatabaseAbstractHandler`, which runs the handler inside the session's
transaction when the request carries `arcadedb-session-id`. Closing it means a proto field on each request plus
the `resolveAuthorizedTransaction` / `submitToActiveTransaction` plumbing `lookupByRid` already has, which is its
own change rather than a line in this one.

### Considered and dismissed, with the evidence

- **"The vector `filter` inlines caller SQL into a generated statement."** It does, and that is unchanged from the
  MCP tool this was extracted from: the generated statement is checked with `analyze(...).isIdempotent()` before
  it runs, and the HTTP caller reaching it has already passed `canAccessToDatabase`. The same caller can send
  arbitrary SQL to `POST /query`, so the route grants no capability it did not already have.
- **"`NdjsonResultSet.hasNext()` throws on a server-side error line."** Deliberate. The 200 status was sent with
  the first row, so the error line is the only channel a mid-stream failure has; ending the iteration quietly
  instead would hand the caller a silently short result set, which is precisely what cannot be detected.
- **"The gRPC handlers re-read each hit by RID rather than reusing the JSON the operation already built."** They
  do, and it costs one cached lookup per hit. Every other RPC of that service answers with typed `GrpcValue`s,
  and the JSON form has already flattened a date or a decimal to a string, so reusing it would make a search hit
  the one record shape on that wire that loses its types.
