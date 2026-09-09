# #7306 - HTTP API has no streaming query and no vector search endpoint

Issue: https://github.com/ArcadeData/arcadedb/issues/7306
Branch: `feat/7306-http-streaming-and-vector-endpoints`

## Finding ledger

- [ ] 1. **No streaming.** `PostQueryHandler`/`PostCommandHandler`/`GetQueryHandler` materialize the whole result
      set into a `JSONObject` before the first byte leaves the server.
- [ ] 2. **No vector search endpoint.** Neither HTTP nor gRPC exposes vector/hybrid/full-text search; the only
      structured surface is MCP.
- [ ] 3. Proto must gain the vector RPCs in the same PR.
- [ ] 4. OpenAPI must document the streaming content-type negotiation and the new vector routes.
- [ ] 5. `RemoteDatabase` (and `RemoteGrpcDatabase` for vector) must expose both features.

## Analysis

### Streaming

`AbstractQueryHandler.serializeResultSet` builds a `JSONArray` of every row and puts it under `result` in a
`JSONObject`; `ExecutionResponse` then hands the finished `String` to Undertow's `Sender`. There is no seam
where a row can leave before the last one is produced.

There is precedent for writing the response directly from a handler: `PostServerCommandHandler` starts an SSE
stream, writes to `exchange.getOutputStream()` and returns `null`, and
`AbstractServerHttpHandler.handleRequest` already treats a `null` `ExecutionResponse` as "the handler sent the
response itself" (`if (response != null) response.send(exchange);`). The streaming path therefore needs no
change to the request pipeline.

### Vector search

The MCP tools already contain the whole search implementation, including the bounds the issue asks the HTTP
surface to reuse (`MCPVectorLeg.MAX_K`, `MAX_EF_SEARCH`, `MAX_FILTER_CANDIDATES`, `FullTextSearchTool.MAX_LIMIT`,
`HybridSearchTool.MAX_SEEDS`/`MAX_EXPANSION`/`MAX_DEPTH`). They live in the `mcp` module, which depends on
`arcadedb-server` in `provided` scope, so `server` cannot depend on them - re-deriving the bounds in an HTTP
handler is exactly the divergence the issue warns about.

The fix is therefore a **move, not a copy**: the search core moves down into `arcadedb-server`
(`com.arcadedb.server.vector`), where every surface that needs it - HTTP handlers, the gRPC wire module
(`grpcw`, also `provided` on server) and the MCP tools - can reach it. The MCP tool classes keep their public
API and delegate.

## Completeness

### Invariant

1. **Streaming:** a query answered over HTTP emits its first row before its last row has been produced, and the
   `application/json` response body for the same request is byte-identical to what it was before this change.
2. **Vector:** every protocol surface that exposes vector, hybrid or full-text search validates its arguments
   through one implementation, so no two surfaces can disagree about what is a legal request.

### Enumeration

```
$ grep -rn "extends AbstractQueryHandler\|extends PostCommandHandler\|extends PostQueryHandler" --include='*.java' . | grep -v '/test/'
server/.../GetQueryHandler.java:36:public class GetQueryHandler extends AbstractQueryHandler {
server/.../PostQueryHandler.java:27:public class PostQueryHandler extends PostCommandHandler {
server/.../PostCommandHandler.java:45:public class PostCommandHandler extends AbstractQueryHandler {
```

Three query entry points, no others.

```
$ grep -rn "serializeResultSetBounded\|serializeResultSet(" --include='*.java' . | grep -v '/test/'
server/.../GetQueryHandler.java:86
server/.../PostCommandHandler.java:268, 289
(+ the definitions in AbstractQueryHandler, + an unrelated private method in engine JsonSerializer)
```

```
$ grep -rn "VectorSearchTool.execute\|HybridSearchTool.execute\|FullTextSearchTool.execute" --include='*.java' . | grep -v '/test/'
mcp/.../MCPDispatcher.java:373  case "vector_search"    -> VectorSearchTool.execute(...)
mcp/.../MCPDispatcher.java:374  case "full_text_search" -> FullTextSearchTool.execute(...)
mcp/.../MCPDispatcher.java:375  case "hybrid_search"    -> HybridSearchTool.execute(...)
```

MCP is the only caller today - which is the gap the issue reports.

```
$ grep -ic vector grpc/src/main/proto/arcadedb-server.proto      -> 0
$ grep -c  vector server/src/main/java/com/arcadedb/server/http/HttpServer.java -> 0
```

Both of the issue's claims reproduce exactly.

### Coverage table

| Entry point | Covered by change? | Covered by a test? |
|---|---|---|
| `POST /api/v1/query/{db}` - NDJSON streaming | yes | yes |
| `POST /api/v1/command/{db}` - NDJSON streaming | yes | yes |
| `GET /api/v1/query/{db}/{lang}/{cmd}` - NDJSON streaming | yes | yes |
| `POST /api/v1/query|command` - `application/json` unchanged | yes | yes (byte-shape regression test) |
| `POST /api/v1/vector/{db}/search` | yes | yes |
| `POST /api/v1/vector/{db}/hybrid` | yes | yes |
| `POST /api/v1/vector/{db}/fulltext` | yes | yes |
| MCP `vector_search` / `hybrid_search` / `full_text_search` - same bounds after the move | yes | yes (existing mcp suite + equivalence test) |
| gRPC `VectorSearch` / `HybridSearch` / `FullTextSearch` | yes | yes |
| OpenAPI `VectorApiSpec` + ndjson media type on the query operations | yes | yes (`OpenApiSpecGenerationIT`) |
| `RemoteDatabase` streaming query + vector search | yes | yes |
| `RemoteGrpcDatabase` vector search | yes | yes |
| `serializer=graph` / `serializer=studio` under NDJSON | **no - argued** | n/a |

## Residual risk

See the end of this document, filled in at PR time.
