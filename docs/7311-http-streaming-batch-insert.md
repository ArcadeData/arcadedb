# #7311 - HTTP has no counterpart for the gRPC client-streaming insert RPCs

Split out of #7306. gRPC exposes `InsertStream`, `InsertBidirectional` and `GraphBatchLoad`;
`POST /api/v1/batch/{database}` already reads an `application/x-ndjson` request body incrementally, so the
*ingress* half of client-streaming exists over HTTP. What has no counterpart is the **per-chunk response**: a
`/batch` caller learns nothing until the whole body has been consumed and the single summary object is written.

## The two decisions the issue asked to settle first

1. **Does the bidirectional shape belong on HTTP at all, or on `/ws`?**
   The half of `InsertBidirectional` that carries information is `BatchAck`: the outcome of chunk *n* reaching the
   client while chunk *n+1* is still being sent. HTTP/1.1 expresses exactly that - a server may begin its response
   before the request body is complete - and Undertow's blocking exchange lets one worker thread read the request
   stream and write the response stream in turn. So the *acknowledgement* half is expressible here and is what
   this issue implements.
   What is **not** expressible is the other half: `Start`/`Commit` control frames that let the client change the
   shape of the session mid-stream, and a server that pushes an unsolicited message. Those need a real duplex
   channel and stay on `/ws`, which is why OpenAPI 3.0 cannot describe them and why `/ws` is deliberately absent
   from the generated document. This PR does not add them.

2. **Is an incremental `/batch` response additive?**
   Only if negotiated. Today's response is one JSON object; turning it into a stream unconditionally is a
   breaking change. So it is selected with `Accept: application/x-ndjson`, exactly the way #7306 negotiated the
   streaming query, and a request that does not ask for it receives the byte-identical body it received before.

## Invariant

> A `POST /api/v1/batch/{database}` that sends `Accept: application/x-ndjson` receives a newline-delimited JSON
> body whose `progress` lines are flushed to the client while the request body is still being read, terminated
> by exactly one `summary` or `error` line carrying the same object the unary encoding would have sent; a
> request that does not negotiate that encoding receives the same status and the same bytes it did before.

## Completeness

### Every path that produces a `/batch` response

```
$ grep -n "new ExecutionResponse\|return null" server/src/main/java/com/arcadedb/server/http/handler/PostBatchHandler.java
212:    return null;                                    # parseRequestPayload, not a response
221:    return new ExecutionResponse(400, ...)           # missing database parameter
426:    return new ExecutionResponse(400, error.toString())      # client-input failure, streamRecords
485:    return new ExecutionResponse(200, result.toString())     # success, streamRecords
664:    return new ExecutionResponse(status, error.toString())   # partialPayloadResponse: 408 / 400 / 500
1135..1195: forwardBatchToLeader                       # follower relay: 400/401/503 + leader's own answer
```

### Every consumer of the `Accept` negotiation added by #7306

```
$ grep -rn "isNdJsonRequested" --include="*.java" . | grep -v /target/
server/.../GetQueryHandler.java:69     server/.../GetQueryHandler.java:160
server/.../PostCommandHandler.java:176
server/.../AbstractQueryHandler.java:283   (the definition)
```

`isNdJsonRequested` lived on `AbstractQueryHandler`, which `PostBatchHandler` does not extend. It is moved
verbatim to `AbstractServerHttpHandler` (with `isRejectedByQValue`, the two `Accept` patterns and
`X-Accel-Buffering`) so both hierarchies negotiate identically rather than through a second copy of the parser.

### Callers of the forwarding builder, which must keep compiling unchanged

```
$ grep -rn "buildForwardRequest" --include="*.java" . | grep -v /target/
server/src/test/java/.../PostBatchHandlerForwardRequestTest.java:53,66,79,95,113
server/src/main/java/.../PostBatchHandler.java:1175,1222
```

Five existing tests pin the 6-argument signature, so the `Accept` header is added through an **overload** and
the old signature delegates to it. No existing test is touched.

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `POST /batch` on a standalone/leader node, `Accept: application/x-ndjson` | yes | yes - `PostBatchStreamingIT.progressReachesTheClientBeforeTheBodyHasBeenFullySent` |
| Same, terminal `summary` line == the unary 200 body | yes | yes - `theSummaryLineCarriesTheSameObjectTheUnaryEncodingWouldHaveSent` |
| Same, client-input failure after the stream started (400 body) | yes | yes - `aClientInputFailureAfterTheStreamStartedIsReportedInBand` |
| Same, truncated upload after the stream started (408 body) | yes | yes - `aTruncatedUploadAfterTheStreamStartedIsReportedInBand` |
| Same, failure **before** the stream started | yes (rethrown, standard status mapping) | yes - `aFailureBeforeTheStreamStartedKeepsItsRealStatusCode` |
| `POST /batch` with no `Accept`, or any other type | unchanged | yes - `theUnaryEncodingIsByteIdenticalWhenTheStreamIsNotNegotiated` |
| `Accept: application/x-ndjson;q=0` | unchanged (the q=0 rule) | yes - `PostBatchStreamingNegotiationTest` |
| `X-ArcadeDB-Commit-Index` read-your-writes bookmark (#5862) | yes - moved in band | yes - `theCommitIndexBookmarkTravelsInTheTerminalLine` (HA), unit-covered by `commitIndexIsOmittedOnAStandaloneDatabase` |
| Idempotency replay (`X-Request-Id`) handing a buffered body to a streaming caller | yes - streaming requests do not participate | yes - `AbstractServerHttpHandlerNdJsonIdempotencyTest` |
| `POST /batch` on an HA **follower** (relayed to the leader) | yes - `Accept` forwarded, leader's stream relayed | yes - `RaftBatchStreamingForwardIT` |
| CSV request body (`text/csv`) with `Accept: application/x-ndjson` | yes - same code path, format only affects the parser | yes - `aCsvPayloadStreamsProgressToo` |
| OpenAPI document | yes | yes - `PostBatchStreamingOpenApiTest` |
| `RemoteDatabase` / `RemoteGraphBatch` (Java driver) | yes - `sendBatch(..., onProgress)` + `withProgressListener` | yes - `RemoteGraphBatchProgressIT` |
| Idempotency key of the BUFFERED `/batch` encoding not covering the payload | **no** - filed as #7381 (pre-existing, found by this sweep) | n/a |
| gRPC `InsertBidirectional` control frames (`Start` / `Commit`) over HTTP | **no** - argued: needs a duplex channel, belongs to `/ws` (#7382) | n/a |
| `/ws` streaming batch surface (`Start`/`Commit` control frames) | **no** - filed as #7382 | n/a |

## Residual risk

- A progress line is an **upper bound on what is durable**, exactly like the `verticesCreated` /
  `edgesCreated` counters the existing 400/408 bodies carry: vertices are committed at every vertex flush, but
  edges are buffered by `GraphBatch` and written at `close()`, so an edge-phase progress line reports records
  *accepted*, not records committed. This is stated in the javadoc, in the OpenAPI description and in the
  handler.
- Writing the response while reading the request is full duplex over one socket. A client that never reads
  its response and whose socket buffers fill would deadlock, in principle. The response is orders of magnitude
  smaller than the request it describes (one ~200-byte line per `vertexBatchSize` records, default 10,000), so
  the response cannot fill a socket buffer before the request drains it. No timeout was added for it.
- Once a 200 is on the wire the status code cannot be taken back, so a failure raised after the first line is
  reported in band with its intended status in the `error` object's `status` field. The distinction a client
  keys on (400 client input vs 408 truncation vs 500) survives; the HTTP status line does not.

## Changes

| File | What |
|---|---|
| `server/.../AbstractServerHttpHandler.java` | `isNdJsonRequested`, `isRejectedByQValue`, the two `Accept` patterns and `X-Accel-Buffering` moved up from `AbstractQueryHandler` so both hierarchies negotiate through one parser. A request that negotiated the stream no longer takes part in the idempotency replay cache |
| `server/.../AbstractQueryHandler.java` | the same members removed; behaviour identical, they are inherited now |
| `server/.../NdJsonResultStream.java` | `writeEvent(kind, body, forceFlush)` and `hasStarted()`, so a second surface can use the same writer and flush discipline instead of a second copy of it |
| `server/.../PostBatchHandler.java` | `Accept: application/x-ndjson` selects a streamed answer: `BatchProgressSink` at every commit boundary, `NdJsonBatchResponse` opening the response lazily so a failure that happens first keeps its real status, the terminal line built from the very `ExecutionResponse` the buffered encoding returns, and the follower relay passing the leader's stream through |
| `server/.../openapi/CoreApiSpec.java` | the `Accept` parameter, the second 200 content type and the `NdJsonBatchEvent` schema |
| `network/.../RemoteDatabase.java` | `sendBatch(content, params, onProgress)` negotiates the encoding, dispatches the `progress` lines and returns the `summary`; a failure line becomes a `DatabaseOperationException` carrying the in-band status |
| `network/.../RemoteGraphBatch.java` | `Builder.withProgressListener(...)` |

## Test results

```
PostBatchStreamingIT                    10/10   (server)
BatchStreamingApiSpecTest                4/4    (server, unit)
RemoteGraphBatchProgressIT               3/3    (server)
RaftBatchStreamingForwardIT              1/1    (ha-raft, @Tag("slow"))
NdJsonResultStreamTest                   7/7    regression
CoreApiSpecTest                         23/23   regression
PostBatchHandlerIT                      26/26   regression *
Issue5470BatchErrorDeliveryIT           12/12   regression
Issue5618BatchLineAccountingIT           6/6    regression
RemoteGraphBatchIT                      15/15   regression
Issue7306HttpStreamingQueryIT           18/18   regression
Issue7306RemoteDatabaseStreamingAndVectorIT 9/9 regression
network module (surefire)              478/478 regression
```

`*` On this developer machine a foreign ArcadeDB server (6 days old, different credentials) was listening on
port 2480 for the whole session, which the older ITs address by hard-coded literal. Three `PostBatchHandlerIT`
methods and both `Issue5023IdempotencyKeyReplayTest` methods answered 403/503 from that server when run
together with others; every one of them passes when re-run on its own, and none of them touches the code this
branch changes. The new ITs read the port the server actually bound
(`getServer(0).getHttpServer().getPort()`) and are immune to it.

### Proof the tests can fail

Mutating `PostBatchHandler.execute`'s `streaming` flag to `false`: 9 of the 10 `PostBatchStreamingIT` methods
fail, `progressReachesTheClientBeforeTheBodyHasBeenFullySent` with a read timeout - the server waiting for the
whole body, which is the defect. The tenth,
`theUnaryEncodingIsUnchangedWhenTheStreamIsNotNegotiated`, keeps passing, which is what it is for.

Mutating the relayed `Accept` to `null`: `RaftBatchStreamingForwardIT` fails on "a follower must not answer a
negotiated stream with the buffered encoding".

## Adversarial pass

The orchestrator's Phase 1.5 wants a subagent that has not been convinced by the author's reasoning. No
subagent-spawning tool was available in this environment, so the pass was run by the author against the staged
diff instead - which is weaker, and is recorded as such. Four findings, all fixed in this branch before the PR
opened:

1. **A failed RESPONSE write was reported as a truncated REQUEST body.** `streamRecords` catches `IOException`
   and answers it with `truncatedBody(...)` - "only N of the M announced bytes were received", with the counts
   a client resumes from. The progress sink writes to the response from inside that `try`, so a client that
   stopped reading would have been told, precisely and machine-parsably, that its upload was cut short. A
   write failure now travels as `BatchResponseWriteException`, which that catch cannot see.
2. **`close()` on a response that opened but never wrote would have committed an empty 200.** The catch that
   rethrows a pre-stream failure - so it keeps its real status - ran with a `finally` that closed the output
   stream, which is what commits the response. `NdJsonBatchResponse.close()` is now a no-op until a line has
   actually been written, and the rethrow branch says why.
3. **The idempotency skip was keyed on the header alone.** `Accept: application/x-ndjson` on a route that
   cannot stream - `POST /api/v1/user`, `POST /api/v1/server` - would have dropped that route's replay
   protection while giving nothing back, since it answers the same buffered body either way. The skip is now
   gated on `supportsNdJsonEncoding()`, which only `PostBatchHandler` and `PostCommandHandler` override.
4. **A cross-flush edge with a progress listener was untested.** `RemoteGraphBatch` resolves an edge against a
   vertex sent in an earlier request through the temporary-id mapping that request returned, which on this
   encoding rides in the terminal line. A driver that read the progress lines and stopped there would drop
   every cross-flush edge, silently.
   `RemoteGraphBatchProgressIT.anEdgeAcrossTwoFlushesStillResolvesWithAListener` now pins it.

Considered and left alone, with the reason:

- `RemoteDatabase.readStreamedBatch`'s buffered fallback raises a `DatabaseOperationException` carrying the raw
  body rather than going through `manageException`, which the negotiated-buffered path uses. It is reachable
  only against a server too old to know the encoding, and the body is the same text `manageException` would
  have parsed. Not worth a second code path on a compatibility branch.
- Writing the response while reading the request can deadlock in principle if a client never reads. See
  **Residual risk**: the response is smaller than the request by orders of magnitude, so it cannot fill a
  socket buffer before the request drains it.
