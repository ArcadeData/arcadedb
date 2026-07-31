# Clients: complete the OpenAPI spec to match the registered HTTP route surface (#4895)

**Issue:** [#4895](https://github.com/ArcadeData/arcadedb/issues/4895)
**Epic:** [#4894 - Spec-Driven Client Generation (multi-language)](https://github.com/ArcadeData/arcadedb/issues/4894) (Milestone 0, first issue)
**Date:** 2026-07-30
**Status:** approved

## Problem

`OpenApiSpecGenerator` documents **22 operations across 13 path items**. The server registers **65** HTTP API operations - plus the `/ws` WebSocket upgrade and the Studio static-content fallback, which are not HTTP API operations and are counted separately throughout this document. Every downstream issue in the Epic generates clients from this spec, so each one inherits the gap: a generated TypeScript, Python, Go, or C# client would expose no batch insert, no login, no time-series, no PromQL, no AI, no MCP, and no cluster management at all.

The spec also declares a `bearerAuth` security scheme (`OpenApiSpecGenerator.java:121-125`) that is never attached to any operation, because `createSecurityRequirement()` (line 494-498) only ever emits `basicAuth`. Meanwhile `AbstractServerHttpHandler:257-279` accepts `Authorization: Bearer` for *every* authenticated route. A generated client following the spec literally cannot authenticate with an `at-` API token.

This issue is the Epic's gate: nothing else in #4894 ships until it lands.

## Corrections to the issue's own enumeration

The issue was drafted from an audit that this design re-verified against source. Four deltas:

1. **`GET /api/v1/progress/{database}`** (`HttpServer.java:228`) is registered and undocumented, and the issue's missing-list omits it.
2. **MCP is 3 operations, not 2.** `MCPHttpHandler` accepts POST only (`MCPHttpHandler.java:80`); `MCPConfigHandler` accepts both GET and POST (`MCPConfigHandler.java:50,53`). So: `POST /api/v1/mcp`, `GET /api/v1/mcp/config`, `POST /api/v1/mcp/config`.
3. **Plugin routes are 12, not 10.** `RaftHAPlugin.registerAPI()` (`ha-raft/.../RaftHAPlugin.java:177-196`) registers 10 route entries, but `/api/v1/ha/snapshot/` is a prefix handler serving **two** distinct operations - `/{database}` and `/{database}/checksums` (`SnapshotHttpHandler.java:151-158`). Plus `GET /prometheus` from `PrometheusMetricsPlugin.registerAPI()` (`PrometheusMetricsPlugin.java:64`). Total: 1 + 9 + 2 = 12.
4. **`/ws` needs an explicit exclusion rationale.** The WebSocket route (`HttpServer.java:220`) is registered but is not expressible in OpenAPI 3.0 under any encoding. The issue's exclusion list covers only `openapi.json` and `docs`.

Final target: **63 documented operations** (51 declared in `HttpServer.setupRoutes()` plus 12 plugin-contributed), **2 deliberately excluded** (`openapi.json`, `docs`), totalling the 65 registered HTTP API operations. `/ws` and the static-content fallback are out of scope and outside that count.

## Constraint the issue does not state

`ha-raft` and `metrics` both declare `arcadedb-server` at `provided` scope (`ha-raft/pom.xml`, `metrics/pom.xml`). Provided-scope dependencies are not transitive, so swagger's `OpenAPI`/`PathItem`/`Schema` types are **not** on either module's compile classpath. Any SPI that asks a plugin to hand back swagger objects requires adding `swagger-models` as a new compile dependency to both modules. This constraint drives the plugin-route decision below.

## Design

### Operation inventory

63 operations, source-verified. Grouped by owning contributor class (see Structure).

**Already documented (22) - retained, gain `bearerAuth` via root-level security:**

| Path | Methods |
|---|---|
| `/api/v1/server` | GET, POST |
| `/api/v1/ready` | GET |
| `/api/v1/health` | GET |
| `/api/v1/databases` | GET |
| `/api/v1/exists/{database}` | GET |
| `/api/v1/query/{database}/{language}/{command}` | GET |
| `/api/v1/query/{database}` | POST |
| `/api/v1/command/{database}` | POST |
| `/api/v1/begin/{database}` | POST |
| `/api/v1/commit/{database}` | POST |
| `/api/v1/rollback/{database}` | POST |
| `/api/v1/server/users` | GET, POST, PUT, DELETE |
| `/api/v1/server/groups` | GET, POST, DELETE |
| `/api/v1/server/api-tokens` | GET, POST, DELETE |

**New (41):**

| Group | Ops | Paths |
|---|---|---|
| Core gaps | 2 | `POST /api/v1/batch/{database}`, `GET /api/v1/progress/{database}` |
| Auth | 3 | `POST /api/v1/login`, `POST /api/v1/logout`, `GET /api/v1/sessions` |
| Time-series | 3 | `POST /api/v1/ts/{database}/write`, `POST /api/v1/ts/{database}/query`, `GET /api/v1/ts/{database}/latest` |
| Grafana | 3 | `GET .../grafana/health`, `GET .../grafana/metadata`, `POST .../grafana/query` |
| Prometheus remote | 2 | `POST .../prom/write`, `POST .../prom/read` |
| PromQL | 5 | `GET .../prom/api/v1/query`, `query_range`, `labels`, `label/{name}/values`, `series` |
| MCP | 3 | `POST /api/v1/mcp`, `GET /api/v1/mcp/config`, `POST /api/v1/mcp/config` |
| AI | 8 | `GET /ai/config`, `POST /ai/activate`, `POST /ai/chat`, `POST /ai/analyze-profiler`, `GET /ai/chats`, `GET/PUT/DELETE /ai/chats/{id}` |
| Plugin | 12 | `GET /prometheus`; `GET /api/v1/cluster`; `POST /api/v1/cluster/peer`; `DELETE /api/v1/cluster/peer/{peerId}`; `POST /api/v1/cluster/leader`; `POST /api/v1/cluster/stepdown`; `POST /api/v1/cluster/leave`; `POST /api/v1/cluster/verify/{database}`; `POST /api/v1/cluster/resync/{database}`; `POST /api/v1/cluster/bootstrap-state`; `GET /api/v1/ha/snapshot/{database}`; `GET /api/v1/ha/snapshot/{database}/checksums` |

**Excluded, each with an in-code comment stating why - not a silent omission:**

| Route | Rationale recorded in code |
|---|---|
| `GET /api/v1/openapi.json` | Self-documentation; a spec describing its own retrieval adds no client capability. |
| `GET /api/v1/docs` | Swagger UI HTML, not an API operation. |
| `/ws` (prefix) | WebSocket; not expressible in OpenAPI 3.0. AsyncAPI would be the correct IDL. |
| `/` (fallback) | Studio static assets, not an API. Registered only outside production mode or under `STUDIO_ENABLED`. |

### Plugin-route strategy: static, unconditional, in the server module

`PluginApiSpec` (in the `server` module) declares all 12 plugin operations **unconditionally**, each description naming the plugin that must be active for the route to answer (`RaftHAPlugin` / `PrometheusMetricsPlugin`).

Rationale, recorded in the `PluginApiSpec` class Javadoc:

- **Determinism is required by the Epic.** #4902 generates clients in CI from a live server's `/api/v1/openapi.json`. A default CI server runs neither HA nor the Prometheus plugin, so a config-conditional spec would produce clients with zero cluster-management and zero scrape-endpoint methods - the exact failure this Epic exists to prevent.
- **No new dependency.** `ha-raft` and `metrics` keep `arcadedb-server` at `provided` scope and gain no `swagger-models` dependency.
- **The drift risk is named and delegated.** Declaring plugin routes across a module boundary means `RaftHAPlugin` can add a route without touching the spec. Closing that hole is #4896's job; the Javadoc says so and points at the issue. The natural shape - anti-drift tests living in `ha-raft` and `metrics` that assert every route their own `registerAPI()` declares appears in the server's generated spec - is noted there as the recommended approach but is **not** built here.

Rejected alternatives: a `ServerPlugin` SPI returning swagger path items (needs the new dependency in two modules, and makes the spec config-dependent); the same SPI returning `JSONObject` fragments (avoids the dependency but loses compile-time typing and is still config-dependent); excluding plugin routes entirely (contradicts the Epic's success criterion).

### Security model

Root-level `security: [{basicAuth: []}, {bearerAuth: []}]` replaces the per-operation `createSecurityRequirement()` boilerplate. Every operation inherits both schemes; overrides are the exception:

| Override | Operations | Reason |
|---|---|---|
| `security: []` | `GET /api/v1/health`, `GET /api/v1/ready` | The only two handlers returning `isRequireAuthentication() == false` (`GetHealthHandler`, `GetReadyHandler`). |
| `security: [{basicAuth: []}]` | `GET /api/v1/ha/snapshot/{database}`, `.../checksums` | `SnapshotHttpHandler implements HttpHandler` directly and parses Basic itself (`SnapshotHttpHandler.java:289-293`); it never reaches the bearer branch in `AbstractServerHttpHandler`. |

`GET /prometheus` inherits both schemes, with a description noting that `arcadedb.serverMetrics.prometheus.requireAuthentication=false` makes it public (`PrometheusMetricsPlugin.java:63`).

That leaves `bearerAuth` on 59 of the 63 operations, which is the honest reflection of `AbstractServerHttpHandler:257-279` handling bearer tokens for every authenticated route.

**One `bearerAuth` scheme, two token kinds.** `at-` API tokens and `AU-` session tokens (from `POST /api/v1/login`) are both `type: http, scheme: bearer`; OpenAPI 3.0 cannot distinguish them, so a single scheme carries a description naming both and pointing at `POST /api/v1/login` for the session-token flow.

**`X-ArcadeDB-Cluster-Token` is not declared.** A code comment states why: it is cluster-internal peer-to-peer auth (`AbstractServerHttpHandler:231-242`), paired with `X-ArcadeDB-Forwarded-User`, and exposing it as a client-facing scheme would invite misuse.

### Structure

`OpenApiSpecGenerator` is 811 lines documenting 22 operations. Tripling the operation count in place produces a ~3000-line class that cannot be reviewed coherently. New package `com.arcadedb.server.http.handler.openapi`:

| Class | Responsibility |
|---|---|
| `OpenApiContributor` | Interface: `void contribute(OpenAPI openAPI)`. One purpose - add this domain's paths and component schemas. |
| `SpecBuilders` | Shared static helpers: path/query parameters, JSON request bodies, standard response sets, error responses. Extracted from the existing private methods in `OpenApiSpecGenerator`. |
| `CoreApiSpec` | server, health, ready, databases, exists, query GET/POST, command, batch, progress, begin/commit/rollback |
| `AuthApiSpec` | login, logout, sessions |
| `SecurityAdminApiSpec` | users, groups, api-tokens |
| `TimeSeriesApiSpec` | write, query, latest |
| `GrafanaApiSpec` | health, metadata, query |
| `PrometheusApiSpec` | remote write/read, 5 PromQL operations |
| `AiApiSpec` | 8 AI operations |
| `McpApiSpec` | 3 MCP operations |
| `PluginApiSpec` | 12 plugin operations, plus the strategy Javadoc |

`OpenApiSpecGenerator` becomes a thin composer over an ordered contributor list, retaining `generateSpec()` and the root info/servers/security/tags setup. Its public surface is unchanged, so `GetOpenApiHandler` and its double-checked spec cache (`GetOpenApiHandler.java:83-94`) need no modification.

Each contributor is independently constructible and testable without a running server: contributors take no `HttpServer` (the existing generator stores it but never reads it).

### Naming is a public contract

`openapi-generator` (#4898) derives generated API class names from an operation's **first tag** and generated method names from **`operationId`**. Both are therefore public naming decisions, not decoration:

- Every operation gets a unique, collision-free camelCase `operationId`. A test asserts uniqueness across the whole spec.
- A fixed 17-tag vocabulary is declared as root-level `tags` with descriptions, so generated clients group predictably: `Server`, `Health`, `Database`, `Query`, `Command`, `Transaction`, `Batch`, `Auth`, `Security`, `TimeSeries`, `Grafana`, `Prometheus`, `PromQL`, `AI`, `MCP`, `Cluster`, `Metrics`.

The existing 22 operations keep their current `operationId`s and tags; renaming them would churn every downstream client for no gain.

### Schema fidelity, tiered by client value

**Named component schemas** for bodies a generated client will actually construct or destructure:

`BatchResponse`, `ProgressResponse`, `LoginResponse`, `SessionList`, `TimeSeriesQueryRequest`, `TimeSeriesRawResponse`, `TimeSeriesAggregatedResponse`, `TimeSeriesLatestResponse`, `TimeSeriesWriteError`, `GrafanaHealth`, `GrafanaMetadata`, `GrafanaQueryRequest`, `GrafanaQueryResponse`, `PromQLDataResponse`, `PromQLLabelsResponse`, `PromQLSeriesResponse`, `PromQLErrorResponse`, `AiConfig`, `AiActivateRequest`, `AiChatRequest`, `AiChatResponse`, `AiAnalyzeProfilerRequest`, `AiAnalyzeProfilerResponse`, `AiChatList`, `AiChat`, `McpConfig`, `ClusterStatus`, `AddPeerRequest`, `TransferLeaderRequest`, `ClusterActionResponse`, `VerifyDatabaseResponse`, `BootstrapStateResponse`.

`TimeSeriesQueryRequest` models the real payload read by `PostTimeSeriesQueryHandler`: `type` (required, line 69-72), `from`/`to` (line 85-86), `tags` (line 198), `fields` (line 204-206), and nested `aggregation` with `bucketInterval` plus a `requests[]` array of `{field, type, alias}` (line 144-154). Its response is a **`oneOf`**: the raw shape `{type, columns, rows, count}` (line 131-134) and the aggregated shape `{type, aggregations, buckets, count}` (line 189-192) are structurally different and selected by the presence of `aggregation` in the request.

PromQL is **four** schemas, not one, because `PromQLResponseFormatter` emits four envelopes: `{status, data:{resultType, result}}` for query/query_range (line 43-62), `{status, data:[string]}` for labels and label values (line 98-104), `{status, data:[labelMap]}` for series (line 108-114), and `{status, errorType, error}` for failures (line 118-122).

**Bodies that are not JSON at all.** Three operations were mis-modelled in an earlier draft of this spec; the handlers are authoritative:

| Operation | Actual request encoding |
|---|---|
| `POST /api/v1/batch/{database}` | Streaming **JSONL** (`application/x-ndjson`, `application/jsonl`) or **CSV** (`text/csv`) - not JSON. Body is never buffered. Behaviour is driven by ~19 query parameters documented in the `PostBatchHandler` class Javadoc (`batchSize`, `refMode`, `commitEvery`, `vertexBatchSize`, `expectedRecords`, `ordinalBase`, `idMapping`, ...). There is no `BatchRequest` schema; there is a `BatchResponse`. |
| `POST /api/v1/login` | **No request body.** The base handler authenticates from the `Authorization` header and `PostLoginHandler` only mints a session (`PostLoginHandler.java:52-69`). There is no `LoginRequest` schema; the response is `{token, user}`. |
| `POST /api/v1/ts/{database}/write` | **InfluxDB Line Protocol** text, optionally `Content-Encoding: gzip` (`PostTimeSeriesWriteHandler.java:104-112`), with a `precision` query parameter (`ns\|us\|ms\|s`). Success is **204 with no body**; only the 400 path carries `{error, requestId, written, dropped, unknownTypes, nonTimeSeriesTypes}`. There is no `TimeSeriesWriteRequest` schema. |

**Accurate-but-opaque**, where a JSON schema would be fiction:

| Operation | Encoding | Description carries |
|---|---|---|
| `POST .../prom/write`, `POST .../prom/read` | `application/x-protobuf`, `format: binary` | Snappy-compressed protobuf `WriteRequest`/`ReadRequest` framing (`PostPrometheusWriteHandler.java:75-83`), pointing at the Prometheus remote-write spec |
| `GET /api/v1/ha/snapshot/**` | `application/zip`, `format: binary` | Streamed database snapshot with the completeness-manifest trailer header |
| `GET /prometheus` | `text/plain` | Prometheus text exposition format |
| `POST /api/v1/mcp` | `application/json`, `type: object` | JSON-RPC 2.0 envelope per the MCP specification |

Grafana is **not** the SimpleJSON contract, as an earlier draft of this spec claimed. `PostGrafanaQueryHandler` emits the Grafana **DataFrame** format: `{results: {<refId>: {frames: [{schema: {fields: [{name, type}]}, data: {values: [[...]]}}]}}}` (`buildFrame`, line 236-250). It is fully modelable and gets named schemas.

Every operation - including the opaque ones - carries correct parameters, security, tags, `operationId`, and status codes. Only body *depth* varies.

### Cleanup in code being restructured

`OpenApiHandler` (33 lines) and `OpenApiDocsHandler` (32 lines) are dead. Neither is registered in `HttpServer.setupRoutes()`, which uses `GetOpenApiHandler` and `GetApiDocsHandler`. They survive only because two reflection tests assert their existence: `OpenApiSpecGenerationIT:309-320` and `OpenApiDocsEndpointIT:336-347`.

Both classes and both tests are deleted. A test whose only function is keeping dead code alive is worse than no test, and this is the package being restructured.

## Verification

Per `feedback_prove_a_test_can_fail_before_trusting_it`, every new assertion group is run against the pre-change generator and confirmed to **fail** before the corresponding contributor lands. A green new test on unchanged code means the test asserts nothing.

**Unit tests, one per contributor** (`server/src/test/java/com/arcadedb/server/http/handler/openapi/`) - no server needed:
- Every declared path and method is present.
- Every operation has a non-blank `operationId`, at least one tag, and a success response.
- Security overrides land where specified (`security: []` on health/ready; basic-only on snapshot).
- Named component schemas are registered and their required fields match the handler's reads.

**`OpenApiSpecGenerationIT`** (extended):
- Enumerated assertion over all 63 operations as `(path, method)` pairs, driven by a single constant list so a missing operation names itself in the failure.
- `operationId` uniqueness across the whole spec.
- `OpenAPIV3Parser` with `setResolve(true)` reports zero messages, proving every `$ref` resolves.
- Root-level security declares both schemes; `bearerAuth` resolves.
- **Acceptance criterion #2 as a live round trip:** create an API token via `POST /api/v1/server/api-tokens`, call `GET /api/v1/databases` with `Authorization: Bearer at-...`, assert 200, and assert the spec's effective security for that operation includes `bearerAuth`. This proves the declared scheme matches what the server accepts, rather than asserting the string exists.
- The two dead-class reflection tests are removed.

**`OpenApiDocsEndpointIT`** (extended): Swagger UI renders the new path set without error - automated, replacing the issue's "manual check".

**Regression scope** (`mvn -pl server -am verify -Dit.test=...`, per `reference_shared_m2_poisons_single_module_test_runs`): `OpenApiSpecGenerationIT`, `OpenApiDocsEndpointIT`, plus the new contributor unit tests. No handler behaviour changes, so no handler test is affected; the full `server` module test run is the backstop.

## Out of scope

- **Anti-drift enforcement (#4896).** This issue makes the decision on plugin-route scope and records it in code comments, as #4896 requires. The structural derivation and the drift tests - including per-module tests in `ha-raft`/`metrics` - are #4896's.
- **Client generation.** #4897 onward.
- **Handler behaviour.** This issue changes no request handling. If documenting an endpoint surfaces a defect in it, that becomes its own issue.
- **`/ws` documentation.** Would require adopting AsyncAPI; not proposed.
- **Renaming existing `operationId`s or tags.** Would churn downstream clients for no gain.

## Acceptance criteria mapping

| Issue AC | How this design satisfies it |
|---|---|
| All `HttpServer`-declared plus plugin-contributed operations present | 63 documented (51 `HttpServer`-declared + 12 plugin), with 4 exclusions each carrying an in-code rationale comment. The issue's figure of 61 is superseded per the Corrections section. |
| `bearerAuth` attached, with a test proving `Bearer at-...` validates against the declared scheme | Root-level security puts `bearerAuth` on 59 of the 63 operations (all but health, ready, and the two snapshot operations); the live round-trip test creates a real token and asserts both the 200 and the spec's declared scheme. |
| `OpenApiSpecGenerationIT` passes and asserts the new paths/schemas | Enumerated 63-operation assertion, `operationId` uniqueness, full `$ref` resolution, plus per-contributor unit tests. |
| Swagger UI renders the new paths | Automated in `OpenApiDocsEndpointIT` rather than checked by hand. |
