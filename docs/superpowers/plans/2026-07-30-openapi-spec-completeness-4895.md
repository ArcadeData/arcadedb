# OpenAPI Spec Completeness (#4895) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Raise the generated OpenAPI spec at `/api/v1/openapi.json` from 22 to 63 documented operations, attach the dead `bearerAuth` scheme to the 59 operations that actually accept bearer tokens, and split the 811-line generator into per-domain contributors.

**Architecture:** `OpenApiSpecGenerator` becomes a thin composer over an ordered list of `OpenApiContributor` implementations, one per API domain, sharing a `SpecBuilders` helper. Security moves from per-operation `basicAuth` requirements to a root-level `[{basicAuth},{bearerAuth}]` declaration with three narrow overrides. Plugin-contributed routes (Raft/HA, Prometheus) are declared statically and unconditionally in the server module, because CI generates clients from a default non-HA server and a config-conditional spec would ship clients with no cluster methods.

**Tech Stack:** Java 21, swagger-core / swagger-models 2.2.52, swagger-parser 2.1.45 (test), JUnit 5, AssertJ, Undertow, Maven.

**Spec:** `docs/superpowers/specs/2026-07-30-openapi-spec-completeness-4895-design.md`

## Global Constraints

- Never use the em dash character in any output, comment, or Javadoc. Use a normal dash, a comma, or rephrase.
- Use `final` on variables and parameters wherever possible.
- Single-statement `if` bodies take no curly braces.
- Import classes; never use fully qualified names inline.
- JSON goes through `com.arcadedb.serializer.json.JSONObject` / `JSONArray`, using the default-value getter overloads.
- Javadoc states invariants only. **No issue numbers in Javadoc.** (Ordinary `//` comments may cite an issue where it aids archaeology.)
- Do not add Claude as author on any source file.
- Tests assert in the form `assertThat(x.isMandatory()).isTrue();`.
- **Never write a fully qualified class name inline, including in test code.** Add the import instead. Task 1's review caught an inline `java.util.List.of` that this plan had introduced.
- **Import exactly what the code uses, and nothing more.** Work out the import set by reading your own finished code, not from any list given to you: Task 5 shipped an unused `java.util.List` import because its dispatch named three imports while the test only needed two. `.toList()` is a `Stream` method and needs no import; `List.of(...)` does.
- Do not run `git commit` unless the plan step says to; the maintainer reviews before committing. **Every "Commit" step in this plan is authorized** because the plan is approved.
- Compile after every Java change: `mvn -q -pl server -am -DskipTests compile`.
- Run single-module tests with `-am`, never bare `-pl`, or Maven may load a stale `arcadedb-server` jar from the shared `~/.m2`.
- Surefire `-Dtest=` takes a **comma**-separated list. `'A+B'` silently runs zero tests.
- No new third-party dependency. `swagger-models` must NOT be added to `ha-raft` or `metrics`.
- Before trusting any new test, stub its subject to a no-op and confirm the test FAILS.
- **Any task that creates a new `OpenApiContributor` must also register it** in `OpenApiSpecGenerator.CONTRIBUTORS`, adding both the import and the list entry in the same commit, keeping the order documented in the comment above that list: `CoreApiSpec, AuthApiSpec, SecurityAdminApiSpec, TimeSeriesApiSpec, GrafanaApiSpec, PrometheusApiSpec, AiApiSpec, McpApiSpec, PluginApiSpec`. An unregistered contributor contributes nothing and its unit test still passes, so this is the one step no test in its own task can catch.

## Reference: verified handler facts

Every schema in this plan was read off the handler. Do not re-derive; do not guess.

| Operation | Request | Success response |
|---|---|---|
| `POST /batch/{database}` | `application/x-ndjson`, `application/jsonl`, or `text/csv` streaming body. ~19 query params. **No JSON schema.** | 200 `{verticesCreated, edgesCreated, elapsedMs, bytesRead}` plus `idMapping` or `idMappingOmitted`+`idMappingSize` |
| `GET /progress/{database}` | none | 200 `{result: [{id, database, operation, stepName, stepIndex, totalSteps, done, total, percentage, startedOn, elapsedMs}]}` |
| `POST /login` | **no body**; `Authorization` header only | 200 `{token, user}` |
| `POST /logout` | none | **204, empty body** |
| `GET /sessions` | none | 200 `{result: [{token, user, createdAt, lastUpdate, elapsedMs, sourceIp, userAgent, country, city}], count}` |
| `POST /ts/{db}/write` | InfluxDB Line Protocol text, optional `Content-Encoding: gzip`; `precision` query param (`ns\|us\|ms\|s`) | **204, empty body**. 400 carries `{error, requestId, written, dropped, unknownTypes, nonTimeSeriesTypes}` |
| `POST /ts/{db}/query` | `{type*, from, to, tags, fields, aggregation:{bucketInterval, requests:[{field, type, alias}]}}` | `oneOf`: raw `{type, columns, rows, count}` or aggregated `{type, aggregations, buckets:[{timestamp, values}], count}` |
| `GET /ts/{db}/latest` | `type` (required), `tag` query params | 200 `{type, columns, latest}` (`latest` nullable) |
| `GET /ts/{db}/grafana/health` | none | 200 `{status, database}` |
| `GET /ts/{db}/grafana/metadata` | none | 200 `{types: [{name, fields:[{name, dataType}], tags}], aggregationTypes}` |
| `POST /ts/{db}/grafana/query` | `{targets*: [{refId, type, tags, aggregation}], range, maxDataPoints}` | 200 `{results: {<refId>: {frames: [{schema:{fields:[{name,type}]}, data:{values}}]}}}` |
| `POST /ts/{db}/prom/write` `/read` | `application/x-protobuf` Snappy+protobuf binary | write 204; read protobuf |
| `GET .../prom/api/v1/query` | `query*`, `time`, `lookback_delta` | `{status, data:{resultType, result}}` |
| `GET .../prom/api/v1/query_range` | `query*`, `start*`, `end*`, `step*`, `lookback_delta` | `{status, data:{resultType, result}}` |
| `GET .../prom/api/v1/labels` | none | `{status, data:[string]}` |
| `GET .../prom/api/v1/label/{name}/values` | `name` path | `{status, data:[string]}` |
| `GET .../prom/api/v1/series` | `match[]*`, `start`, `end` | `{status, data:[{__name__, ...labels}]}` |
| PromQL error (any) | | 400 `{status:"error", errorType, error}` |
| `POST /mcp` | JSON-RPC 2.0 object | 200 JSON-RPC object |
| `GET`/`POST /mcp/config` | POST: partial `McpConfig` | 200 `{enabled, allowReads, allowInsert, allowUpdate, allowDelete, allowSchemaChange, allowAdmin, profile, allowedUsers, allowedOrigins, principalProfiles?, databases?}` |
| `GET /ai/config` | none | 200 `{configured, gatewayUrl, currentProtocolVersion, supportedProtocolVersions}` |
| `POST /ai/activate` | `{subscriptionKey*}` | 200 `{activated: true}` |
| `POST /ai/chat` | `{database*, message*, chatId, mode, protocolVersion}` | 200 `{chatId, response, commands?, toolCalls?}`; 400 protocol mismatch adds `{code, currentProtocolVersion, supportedProtocolVersions}` |
| `POST /ai/analyze-profiler` | `{profilerData*, schemas}` | 200 `{response, commands?}` |
| `GET /ai/chats` | none | 200 `{chats: []}` |
| `GET/PUT/DELETE /ai/chats/{id}` | PUT: `{messages}` | GET/PUT 200 chat object; DELETE 200 `{deleted: true}` |
| `GET /prometheus` | none | 200 `text/plain` exposition format |
| `GET /api/v1/cluster` | none | 200 `{implementation, clusterName, localPeerId, raftState, isLeader, leaderReady, leaderId, leaderHttpAddress, electionCount, lastElectionTime, uptime, peers:[...], databases:[...], databasePresence?, alerts}`; 503 when Raft not started |
| `POST /cluster/peer` | `{peerId*, address*, name}` | 200 `{result}` |
| `DELETE /cluster/peer/{peerId}` | none | 200 `{result}`; 409 on conflict |
| `POST /cluster/leader` | `{peerId}` | 200 `{result, leaderId}` |
| `POST /cluster/stepdown`, `/leave` | none | 200 `{result}`; leave 409 |
| `POST /cluster/verify/{database}` | none | 200 `{localChecksums, files:[{name, checksum, size, type}], localServer}` |
| `POST /cluster/resync/{database}` | none | 200 `{result, database, localServer}`; 503 |
| `POST /cluster/bootstrap-state` | none | 200 `{databases:[{name, fingerprint, lastTxId, error?}], peerId}` |
| `GET /ha/snapshot/{database}` | none | 200 `application/zip`; 503 concurrency cap |
| `GET /ha/snapshot/{database}/checksums` | none | 200 JSON checksum map |

`AbstractServerHttpHandler` maps `IllegalArgumentException` to 400 and `SecurityException` to 403 for every handler.

## File Structure

**Create** (all under `server/src/main/java/com/arcadedb/server/http/handler/openapi/`):

| File | Responsibility |
|---|---|
| `OpenApiContributor.java` | Single-method interface: contribute one domain's paths and schemas to an `OpenAPI`. |
| `SpecBuilders.java` | Shared static builders: parameters, request bodies, response sets, error responses, schema primitives. No domain knowledge. |
| `CoreApiSpec.java` | server GET/POST, ready, health, databases, exists, query GET/POST, command, batch, progress, begin/commit/rollback. 14 ops. |
| `AuthApiSpec.java` | login, logout, sessions. 3 ops. |
| `SecurityAdminApiSpec.java` | users, groups, api-tokens. 10 ops. |
| `TimeSeriesApiSpec.java` | ts write, query, latest. 3 ops. |
| `GrafanaApiSpec.java` | grafana health, metadata, query. 3 ops. |
| `PrometheusApiSpec.java` | prom remote write/read, 5 PromQL. 7 ops. |
| `AiApiSpec.java` | 8 AI ops. |
| `McpApiSpec.java` | 3 MCP ops. |
| `PluginApiSpec.java` | 12 plugin ops plus the static-declaration rationale Javadoc. |

**Modify:**

| File | Change |
|---|---|
| `handler/OpenApiSpecGenerator.java` | Reduce to composer: info, servers, root security, root tags, contributor loop. All `create*Path`/`create*Schema` bodies move out. |
| `server/src/test/java/com/arcadedb/server/http/OpenApiSpecGenerationIT.java` | Add 63-operation inventory assertion, operationId uniqueness, `$ref` resolution, live bearer round trip. Remove the dead-class reflection test. |
| `server/src/test/java/com/arcadedb/server/http/OpenApiDocsEndpointIT.java` | Assert Swagger UI renders new paths. Remove the dead-class reflection test. |

**Delete:** `handler/OpenApiHandler.java`, `handler/OpenApiDocsHandler.java` (never registered; kept alive only by reflection tests).

**Create tests** under `server/src/test/java/com/arcadedb/server/http/handler/openapi/`: one `*Test` per contributor, plus `SpecInventoryTest`.

---

### Task 1: Contributor seam, `SpecBuilders`, and root security

Behaviour-preserving extraction plus the one real behaviour change: root-level security. After this task the spec still has 22 operations, but `bearerAuth` is live and the composition seam exists.

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/http/handler/openapi/OpenApiContributor.java`
- Create: `server/src/main/java/com/arcadedb/server/http/handler/openapi/SpecBuilders.java`
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/OpenApiSpecGenerator.java`
- Delete: `server/src/main/java/com/arcadedb/server/http/handler/OpenApiHandler.java`
- Delete: `server/src/main/java/com/arcadedb/server/http/handler/OpenApiDocsHandler.java`
- Test: `server/src/test/java/com/arcadedb/server/http/handler/openapi/SpecBuildersTest.java`
- Modify test: `server/src/test/java/com/arcadedb/server/http/OpenApiSpecGenerationIT.java`
- Modify test: `server/src/test/java/com/arcadedb/server/http/OpenApiDocsEndpointIT.java`

**Interfaces:**
- Produces: `OpenApiContributor` with `void contribute(OpenAPI openAPI)`. Every later task implements it.
- Produces `SpecBuilders` statics, used by every later task:
  - `Parameter pathParam(String name, String description)`
  - `Parameter pathParam(String name, String description, List<String> enumValues)`
  - `Parameter queryParam(String name, String description, boolean required)`
  - `Parameter queryParam(String name, String description, boolean required, String type)`
  - `Schema<Object> object(String description)`
  - `Schema<String> string(String description)`
  - `Schema<Number> integer(String description)`
  - `Schema<Boolean> bool(String description)`
  - `Schema<?> arrayOf(Schema<?> items, String description)`
  - `Schema<?> ref(String componentName)`
  - `RequestBody jsonBody(String description, String componentName, boolean required)`
  - `RequestBody rawBody(String description, String mediaType, String format)`
  - `ApiResponse jsonResponse(String description, String componentName)`
  - `ApiResponse emptyResponse(String description)`
  - `ApiResponse errorResponse(String description)`
  - `ApiResponses standardResponses(String successCode, ApiResponse success, String... extraCodes)`
  - `Operation operation(String operationId, String tag, String summary, String description)`
  - `void publicOperation(Operation op)` sets `security` to an empty list
  - `void basicAuthOnly(Operation op)` sets `security` to `basicAuth` alone
- Produces on `OpenApiSpecGenerator`: unchanged public `OpenAPI generateSpec()`; new package-private `List<OpenApiContributor> contributors()` so unit tests can compose a spec without a server.

- [ ] **Step 1: Write the failing test for `SpecBuilders`**

Create `server/src/test/java/com/arcadedb/server/http/handler/openapi/SpecBuildersTest.java`:

```java
/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SpecBuildersTest {
  @Test
  void pathParameterIsRequiredAndTyped() {
    final Parameter param = SpecBuilders.pathParam("database", "Database name");
    assertThat(param.getIn()).isEqualTo("path");
    assertThat(param.getRequired()).isTrue();
    assertThat(param.getSchema().getType()).isEqualTo("string");
  }

  @Test
  void pathParameterCarriesEnumValues() {
    final Parameter param = SpecBuilders.pathParam("language", "Query language", List.of("sql", "cypher"));
    assertThat(param.getSchema().getEnum()).containsExactly("sql", "cypher");
  }

  @Test
  void optionalQueryParameterIsNotRequired() {
    final Parameter param = SpecBuilders.queryParam("precision", "Timestamp precision", false);
    assertThat(param.getIn()).isEqualTo("query");
    assertThat(param.getRequired()).isFalse();
  }

  @Test
  void rawBodyDeclaresMediaTypeAndFormat() {
    final RequestBody body = SpecBuilders.rawBody("Snappy protobuf", "application/x-protobuf", "binary");
    assertThat(body.getContent()).containsKey("application/x-protobuf");
    assertThat(body.getContent().get("application/x-protobuf").getSchema().getFormat()).isEqualTo("binary");
  }

  @Test
  void jsonResponseReferencesComponentSchema() {
    final ApiResponse response = SpecBuilders.jsonResponse("OK", "BatchResponse");
    assertThat(response.getContent().get("application/json").getSchema().get$ref())
        .isEqualTo("#/components/schemas/BatchResponse");
  }

  @Test
  void emptyResponseCarriesNoContent() {
    assertThat(SpecBuilders.emptyResponse("No content").getContent()).isNull();
  }

  @Test
  void operationCarriesIdSummaryAndSingleTag() {
    final Operation op = SpecBuilders.operation("listDatabases", "Database", "List databases", "Lists them");
    assertThat(op.getOperationId()).isEqualTo("listDatabases");
    assertThat(op.getTags()).containsExactly("Database");
  }

  @Test
  void publicOperationClearsSecurityToEmptyList() {
    final Operation op = SpecBuilders.operation("checkHealth", "Health", "Liveness", "Liveness probe");
    SpecBuilders.publicOperation(op);
    assertThat(op.getSecurity()).isNotNull().isEmpty();
  }

  @Test
  void basicAuthOnlyExcludesBearer() {
    final Operation op = SpecBuilders.operation("downloadSnapshot", "Cluster", "Snapshot", "Streams a snapshot");
    SpecBuilders.basicAuthOnly(op);
    assertThat(op.getSecurity()).hasSize(1);
    assertThat(op.getSecurity().getFirst()).containsOnlyKeys("basicAuth");
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
mvn -q -pl server -am -Dtest=SpecBuildersTest test
```

Expected: compilation failure, `cannot find symbol: class SpecBuilders`.

- [ ] **Step 3: Create `OpenApiContributor`**

```java
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.OpenAPI;

/**
 * Contributes the path items and component schemas of one API domain to a specification under
 * construction. Implementations are stateless and hold no server reference, so they can be
 * composed and asserted on without a running server.
 */
public interface OpenApiContributor {
  /**
   * Adds this domain's path items and component schemas to the given specification. Implementations
   * must not replace the specification's existing paths, components, or security declarations.
   */
  void contribute(OpenAPI openAPI);
}
```

- [ ] **Step 4: Create `SpecBuilders`**

```java
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.RequestBody;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import io.swagger.v3.oas.models.security.SecurityRequirement;

import java.util.List;

/**
 * Shared builders for the OpenAPI contributors. Carries no domain knowledge: every method turns a
 * few literals into one swagger model object, so a contributor reads as a declaration of its
 * endpoints rather than as model plumbing.
 */
public final class SpecBuilders {
  public static final String JSON     = "application/json";
  public static final String ERROR_REF = "ErrorResponse";

  private SpecBuilders() {
  }

  public static Parameter pathParam(final String name, final String description) {
    return pathParam(name, description, null);
  }

  public static Parameter pathParam(final String name, final String description, final List<String> enumValues) {
    final Parameter param = new Parameter();
    param.setName(name);
    param.setIn("path");
    param.setRequired(true);
    param.setDescription(description);
    final Schema<String> schema = new Schema<>();
    schema.setType("string");
    if (enumValues != null)
      schema.setEnum(enumValues);
    param.setSchema(schema);
    return param;
  }

  public static Parameter queryParam(final String name, final String description, final boolean required) {
    return queryParam(name, description, required, "string");
  }

  public static Parameter queryParam(final String name, final String description, final boolean required,
      final String type) {
    final Parameter param = new Parameter();
    param.setName(name);
    param.setIn("query");
    param.setRequired(required);
    param.setDescription(description);
    param.setSchema(new Schema<>().type(type));
    return param;
  }

  public static Schema<Object> object(final String description) {
    final Schema<Object> schema = new Schema<>();
    schema.setType("object");
    schema.setDescription(description);
    return schema;
  }

  public static Schema<String> string(final String description) {
    final Schema<String> schema = new Schema<>();
    schema.setType("string");
    schema.setDescription(description);
    return schema;
  }

  public static Schema<Number> integer(final String description) {
    final Schema<Number> schema = new Schema<>();
    schema.setType("integer");
    schema.setDescription(description);
    return schema;
  }

  public static Schema<Boolean> bool(final String description) {
    final Schema<Boolean> schema = new Schema<>();
    schema.setType("boolean");
    schema.setDescription(description);
    return schema;
  }

  public static Schema<?> arrayOf(final Schema<?> items, final String description) {
    return new Schema<>().type("array").items(items).description(description);
  }

  public static Schema<?> ref(final String componentName) {
    return new Schema<>().$ref("#/components/schemas/" + componentName);
  }

  public static RequestBody jsonBody(final String description, final String componentName, final boolean required) {
    final RequestBody body = new RequestBody();
    body.setDescription(description);
    body.setRequired(required);
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(componentName == null ? new Schema<>().type("object") : ref(componentName));
    body.setContent(new Content().addMediaType(JSON, mediaType));
    return body;
  }

  public static RequestBody rawBody(final String description, final String mediaType, final String format) {
    final RequestBody body = new RequestBody();
    body.setDescription(description);
    body.setRequired(true);
    final MediaType media = new MediaType();
    media.setSchema(new Schema<>().type("string").format(format));
    body.setContent(new Content().addMediaType(mediaType, media));
    return body;
  }

  public static ApiResponse jsonResponse(final String description, final String componentName) {
    final ApiResponse response = new ApiResponse();
    response.setDescription(description);
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(componentName == null ? new Schema<>().type("object") : ref(componentName));
    response.setContent(new Content().addMediaType(JSON, mediaType));
    return response;
  }

  public static ApiResponse emptyResponse(final String description) {
    final ApiResponse response = new ApiResponse();
    response.setDescription(description);
    return response;
  }

  public static ApiResponse errorResponse(final String description) {
    return jsonResponse(description, ERROR_REF);
  }

  /**
   * Builds a response set from one success entry plus the error codes named in {@code extraCodes},
   * each mapped to the standard error body with a description derived from the code.
   */
  public static ApiResponses standardResponses(final String successCode, final ApiResponse success,
      final String... extraCodes) {
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse(successCode, success);
    for (final String code : extraCodes)
      responses.addApiResponse(code, errorResponse(describeStatus(code)));
    return responses;
  }

  private static String describeStatus(final String code) {
    return switch (code) {
      case "400" -> "Bad request";
      case "401" -> "Unauthorized";
      case "403" -> "Forbidden";
      case "404" -> "Not found";
      case "405" -> "Method not allowed";
      case "408" -> "Request timeout: the body ended before it was fully consumed";
      case "409" -> "Conflict";
      case "413" -> "Request body too large";
      case "500" -> "Internal server error";
      case "503" -> "Service unavailable";
      case "504" -> "Gateway timeout";
      default -> "Error " + code;
    };
  }

  public static Operation operation(final String operationId, final String tag, final String summary,
      final String description) {
    final Operation op = new Operation();
    op.setOperationId(operationId);
    op.addTagsItem(tag);
    op.setSummary(summary);
    op.setDescription(description);
    return op;
  }

  /** Marks an operation as reachable without credentials, overriding the root security declaration. */
  public static void publicOperation(final Operation op) {
    op.setSecurity(List.of());
  }

  /** Restricts an operation to HTTP Basic, for handlers that never reach the bearer-token branch. */
  public static void basicAuthOnly(final Operation op) {
    op.setSecurity(List.of(new SecurityRequirement().addList("basicAuth")));
  }
}
```

- [ ] **Step 5: Run the test to verify it passes**

```bash
mvn -q -pl server -am -Dtest=SpecBuildersTest test
```

Expected: PASS, 9 tests.

- [ ] **Step 6: Prove the test can fail**

Temporarily change `publicOperation` to `op.setSecurity(null);` and re-run. Expected: `publicOperationClearsSecurityToEmptyList` FAILS. Revert.

- [ ] **Step 7: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/openapi/OpenApiContributor.java \
        server/src/main/java/com/arcadedb/server/http/handler/openapi/SpecBuilders.java \
        server/src/test/java/com/arcadedb/server/http/handler/openapi/SpecBuildersTest.java
git commit -m "refactor(server) #4895: add OpenAPI contributor seam and shared spec builders"
```

- [ ] **Step 8: Write the failing test for root-level security**

Append to `OpenApiSpecGenerationIT`:

```java
  @Test
  void rootSecurityDeclaresBasicAndBearer() throws Exception {
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();

    assertThat(openAPI.getSecurity())
        .as("root security should offer basicAuth and bearerAuth as alternatives")
        .hasSize(2);
    assertThat(openAPI.getSecurity().stream().flatMap(r -> r.keySet().stream()).toList())
        .containsExactlyInAnyOrder("basicAuth", "bearerAuth");
  }

  @Test
  void livenessAndReadinessAreDeclaredPublic() throws Exception {
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();

    for (final String path : List.of("/api/v1/health", "/api/v1/ready")) {
      assertThat(openAPI.getPaths().get(path).getGet().getSecurity())
          .as("%s requires no authentication, so it must override root security with an empty list", path)
          .isNotNull()
          .isEmpty();
    }
  }
```

- [ ] **Step 9: Run to verify both fail**

```bash
mvn -q -pl server -am -Dit.test=OpenApiSpecGenerationIT verify
```

Expected: `rootSecurityDeclaresBasicAndBearer` FAILS (`getSecurity()` is null), `livenessAndReadinessAreDeclaredPublic` FAILS (security is null, not empty).

- [ ] **Step 10: Move the existing 22 operations into `CoreApiSpec` and `SecurityAdminApiSpec` shells**

Create `CoreApiSpec` and `SecurityAdminApiSpec` implementing `OpenApiContributor`. Move verbatim, converting `createSecurityRequirement()` calls to nothing (root security now covers them) and rewriting the private helpers to `SpecBuilders` calls:

- `CoreApiSpec` takes: `/api/v1/server` (GET `getServerInfo`, POST `executeServerCommand`), `/api/v1/ready` (`checkReady`, public), `/api/v1/health` (`checkHealth`, public), `/api/v1/databases` (`listDatabases`), `/api/v1/exists/{database}` (`checkDatabaseExists`), `/api/v1/query/{database}/{language}/{command}` (`executeQueryGet`), `/api/v1/query/{database}` (`executeQueryPost`), `/api/v1/command/{database}` (`executeCommand`), `/api/v1/begin/{database}` (`beginTransaction`), `/api/v1/commit/{database}` (`commitTransaction`), `/api/v1/rollback/{database}` (`rollbackTransaction`). Plus the schemas `QueryRequest`, `QueryResponse`, `CommandRequest`, `ErrorResponse`, `ServerInfo`, `DatabaseList`.
- `SecurityAdminApiSpec` takes `/api/v1/server/users` (GET/POST/PUT/DELETE), `/api/v1/server/groups` (GET/POST/DELETE), `/api/v1/server/api-tokens` (GET/POST/DELETE), keeping every existing `operationId` and the `Security` tag unchanged.

Keep every existing `operationId`, summary, description, and tag byte-for-byte. Renaming them would churn downstream generated clients.

Both classes take this shape, so the target is unambiguous:

```java
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;

/**
 * Documents the endpoints every deployment exposes: server information and administration, the
 * probes, database enumeration, the query and command data plane, streaming bulk ingestion, and the
 * explicit transaction lifecycle.
 */
public class CoreApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/server", createServerPath());
    openAPI.getPaths().addPathItem("/api/v1/ready", createReadyPath());
    openAPI.getPaths().addPathItem("/api/v1/health", createHealthPath());
    openAPI.getPaths().addPathItem("/api/v1/databases", createDatabasesPath());
    openAPI.getPaths().addPathItem("/api/v1/exists/{database}", createExistsPath());
    openAPI.getPaths().addPathItem("/api/v1/query/{database}/{language}/{command}", createGetQueryPath());
    openAPI.getPaths().addPathItem("/api/v1/query/{database}", createPostQueryPath());
    openAPI.getPaths().addPathItem("/api/v1/command/{database}", createCommandPath());
    openAPI.getPaths().addPathItem("/api/v1/begin/{database}", createBeginPath());
    openAPI.getPaths().addPathItem("/api/v1/commit/{database}", createCommitPath());
    openAPI.getPaths().addPathItem("/api/v1/rollback/{database}", createRollbackPath());

    openAPI.getComponents().addSchemas("QueryRequest", createQueryRequestSchema());
    openAPI.getComponents().addSchemas("QueryResponse", createQueryResponseSchema());
    openAPI.getComponents().addSchemas("CommandRequest", createCommandRequestSchema());
    openAPI.getComponents().addSchemas("ErrorResponse", createErrorResponseSchema());
    openAPI.getComponents().addSchemas("ServerInfo", createServerInfoSchema());
    openAPI.getComponents().addSchemas("DatabaseList", createDatabaseListSchema());
  }

  // The create*Path() and create*Schema() bodies move here verbatim from OpenApiSpecGenerator,
  // with two mechanical edits: every 'setSecurity(Arrays.asList(createSecurityRequirement()))'
  // call is deleted, because root security now covers it, and the private parameter, request-body,
  // and response helpers are replaced by their SpecBuilders equivalents. Readiness and liveness
  // additionally gain a SpecBuilders.publicOperation(op) call.
}
```

```java
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;

/**
 * Documents the root-only administration of users, groups, and API tokens.
 */
public class SecurityAdminApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/server/users", createUsersPath());
    openAPI.getPaths().addPathItem("/api/v1/server/groups", createGroupsPath());
    openAPI.getPaths().addPathItem("/api/v1/server/api-tokens", createApiTokensPath());
  }

  // createUsersPath(), createGroupsPath(), and createApiTokensPath() move here verbatim from
  // OpenApiSpecGenerator, with the same two mechanical edits described in CoreApiSpec.
}
```

- [ ] **Step 10b: Record why the four excluded routes are excluded**

The issue requires the exclusions be deliberate and documented rather than silent. Add this comment immediately above the `for (final OpenApiContributor contributor : CONTRIBUTORS)` loop in `OpenApiSpecGenerator.generateSpec()`:

```java
    // Four registered routes are deliberately absent from this document.
    //
    // GET /api/v1/openapi.json serves this document. Describing its own retrieval gives a
    //   generated client no capability it does not already have by construction.
    // GET /api/v1/docs serves the Swagger UI page. It is HTML for a human, not an API operation.
    // /ws is a WebSocket upgrade. OpenAPI 3.0 cannot express a bidirectional stream under any
    //   encoding; AsyncAPI is the IDL that would, and adopting it is not in scope here.
    // / is the Studio static-content fallback, registered only outside production mode or when
    //   STUDIO_ENABLED is set. Assets, not an API.
    //
    // Adding a route to this list is a decision, not a shortcut: OpenApiSpecGenerationIT asserts
    // the exact operation inventory, so an accidental omission fails the build instead.
```

- [ ] **Step 11: Reduce `OpenApiSpecGenerator` to a composer**

Replace the body of `OpenApiSpecGenerator` with:

```java
package com.arcadedb.server.http.handler;

import com.arcadedb.server.http.HttpServer;
import com.arcadedb.server.http.handler.openapi.CoreApiSpec;
import com.arcadedb.server.http.handler.openapi.OpenApiContributor;
import com.arcadedb.server.http.handler.openapi.SecurityAdminApiSpec;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Paths;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import io.swagger.v3.oas.models.tags.Tag;

import java.util.List;

/**
 * Generates the OpenAPI 3.0 specification for the ArcadeDB HTTP API. The document is assembled by
 * a fixed list of {@link OpenApiContributor}s, one per API domain: this class owns only the
 * document-level parts, so adding endpoints never grows it.
 * <p>
 * Security is declared once at the root as basicAuth or bearerAuth, because
 * {@code AbstractServerHttpHandler} accepts both on every authenticated route. Operations that
 * deviate override it themselves.
 */
public class OpenApiSpecGenerator {

  // Each task of this plan appends its own contributor here, in this order:
  //   CoreApiSpec, AuthApiSpec, SecurityAdminApiSpec, TimeSeriesApiSpec, GrafanaApiSpec,
  //   PrometheusApiSpec, AiApiSpec, McpApiSpec, PluginApiSpec.
  // Order affects nothing but the key order of the emitted paths object.
  private static final List<OpenApiContributor> CONTRIBUTORS = List.of(//
      new CoreApiSpec(), //
      new SecurityAdminApiSpec());

  private final HttpServer httpServer;

  public OpenApiSpecGenerator(final HttpServer httpServer) {
    this.httpServer = httpServer;
  }

  public OpenAPI generateSpec() {
    final OpenAPI openAPI = new OpenAPI();
    openAPI.setOpenapi("3.0.3");
    openAPI.setInfo(createApiInfo());
    openAPI.setServers(createServers());
    openAPI.setTags(createTags());
    openAPI.setComponents(createComponents());
    openAPI.setPaths(new Paths());
    openAPI.setSecurity(List.of(//
        new SecurityRequirement().addList("basicAuth"), //
        new SecurityRequirement().addList("bearerAuth")));

    for (final OpenApiContributor contributor : CONTRIBUTORS)
      contributor.contribute(openAPI);

    return openAPI;
  }

  /** Exposed for tests that compose a specification without a running server. */
  static List<OpenApiContributor> contributors() {
    return CONTRIBUTORS;
  }

  private Info createApiInfo() {
    final Info info = new Info();
    info.setTitle("ArcadeDB HTTP API");
    info.setDescription(
        "Multi-Model DBMS HTTP API for Graph, Document, Key/Value, Search Engine, Time Series, and Vector Embedding operations");
    info.setVersion("1.0.0");

    final Contact contact = new Contact();
    contact.setName("Arcade Data Ltd");
    contact.setEmail("info@arcadedata.com");
    contact.setUrl("https://arcadedb.com");
    info.setContact(contact);

    final License license = new License();
    license.setName("Apache 2.0");
    license.setUrl("https://www.apache.org/licenses/LICENSE-2.0");
    info.setLicense(license);

    return info;
  }

  private List<Server> createServers() {
    final Server server = new Server();
    server.setUrl("http://localhost:2480");
    server.setDescription("ArcadeDB Server");
    return List.of(server);
  }

  /**
   * The tag vocabulary is a public contract: client generators derive an API class per tag, so a
   * renamed or invented tag renames a generated class. Keep this list closed.
   */
  private List<Tag> createTags() {
    return List.of(//
        tag("Server", "Server information and administrative commands"), //
        tag("Health", "Liveness and readiness probes"), //
        tag("Database", "Database existence and enumeration"), //
        tag("Query", "Read-only query execution"), //
        tag("Command", "Data and schema modification"), //
        tag("Transaction", "Explicit transaction lifecycle"), //
        tag("Batch", "Streaming bulk ingestion"), //
        tag("Auth", "Session login, logout, and enumeration"), //
        tag("Security", "Users, groups, and API tokens"), //
        tag("TimeSeries", "Time-series ingestion and querying"), //
        tag("Grafana", "Grafana data source endpoints"), //
        tag("Prometheus", "Prometheus remote read and write"), //
        tag("PromQL", "Prometheus-compatible query API"), //
        tag("AI", "AI assistant configuration and chat"), //
        tag("MCP", "Model Context Protocol endpoint and configuration"), //
        tag("Cluster", "Raft high-availability cluster management"), //
        tag("Metrics", "Metrics scrape endpoints"));
  }

  private Tag tag(final String name, final String description) {
    final Tag tag = new Tag();
    tag.setName(name);
    tag.setDescription(description);
    return tag;
  }

  private Components createComponents() {
    final Components components = new Components();

    final SecurityScheme basicAuth = new SecurityScheme();
    basicAuth.setType(SecurityScheme.Type.HTTP);
    basicAuth.setScheme("basic");
    basicAuth.setDescription("Basic HTTP authentication with username and password");
    components.addSecuritySchemes("basicAuth", basicAuth);

    // One scheme covers both token kinds: an API token ('at-' prefix, minted at
    // POST /api/v1/server/api-tokens) and a session token ('AU-' prefix, minted at
    // POST /api/v1/login). OpenAPI cannot distinguish two http/bearer schemes, so the
    // distinction lives in the description.
    //
    // X-ArcadeDB-Cluster-Token is deliberately not declared. It is cluster-internal
    // peer-to-peer authentication paired with X-ArcadeDB-Forwarded-User, not a scheme a
    // client should ever present.
    final SecurityScheme bearerAuth = new SecurityScheme();
    bearerAuth.setType(SecurityScheme.Type.HTTP);
    bearerAuth.setScheme("bearer");
    bearerAuth.setDescription("""
        Token authentication. Accepts an API token prefixed 'at-', created at \
        POST /api/v1/server/api-tokens, or a session token prefixed 'AU-', returned by \
        POST /api/v1/login.""");
    components.addSecuritySchemes("bearerAuth", bearerAuth);

    return components;
  }
}
```

Note: `httpServer` stays as a field so the constructor signature and `GetOpenApiHandler` are untouched. Component schemas now come from contributors, so `createComponents()` only declares security schemes.

- [ ] **Step 12: Delete the dead handlers and their reflection tests**

```bash
git rm server/src/main/java/com/arcadedb/server/http/handler/OpenApiHandler.java \
       server/src/main/java/com/arcadedb/server/http/handler/OpenApiDocsHandler.java
```

Delete `openApiHandlerClassExists()` from `OpenApiSpecGenerationIT` (lines 308-320) and the equivalent `OpenApiDocsHandler` reflection test from `OpenApiDocsEndpointIT` (lines 336-347). Also delete the now-unused `fail` import from `OpenApiDocsEndpointIT` if nothing else uses it.

- [ ] **Step 13: Compile and run**

```bash
mvn -q -pl server -am -DskipTests compile
mvn -q -pl server -am -Dtest=SpecBuildersTest test
mvn -q -pl server -am -Dit.test='OpenApiSpecGenerationIT,OpenApiDocsEndpointIT' verify
```

Expected: all PASS. `rootSecurityDeclaresBasicAndBearer` and `livenessAndReadinessAreDeclaredPublic` now pass; the pre-existing endpoint and model tests still pass, proving the extraction was behaviour-preserving.

- [ ] **Step 14: Commit**

```bash
git add -A server/src/main/java/com/arcadedb/server/http/handler server/src/test/java/com/arcadedb/server/http
git commit -m "refactor(server) #4895: compose the OpenAPI spec from per-domain contributors, attach bearerAuth at the root

Security moves from a per-operation basicAuth requirement to a root-level
basicAuth-or-bearerAuth declaration, which is what AbstractServerHttpHandler
actually accepts on every authenticated route. Liveness and readiness override
it with an empty requirement.

Removes OpenApiHandler and OpenApiDocsHandler: neither was ever registered in
HttpServer, and both survived only because two tests asserted their existence
by reflection."
```

---

### Task 2: Batch and progress operations

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/openapi/CoreApiSpec.java`
- Test: `server/src/test/java/com/arcadedb/server/http/handler/openapi/CoreApiSpecTest.java`

**Interfaces:**
- Consumes: `SpecBuilders` statics and `OpenApiContributor` from Task 1; `CoreApiSpec` as created in Task 1 Step 10.
- Produces: component schemas `BatchResponse`, `BatchError`, `ProgressResponse`; path items `/api/v1/batch/{database}` (operationId `executeBatch`, tag `Batch`) and `/api/v1/progress/{database}` (operationId `getOperationProgress`, tag `Server`).

- [ ] **Step 1: Write the failing test**

Create `CoreApiSpecTest.java` with the standard Apache license header (copy it from `SpecBuildersTest.java`), package `com.arcadedb.server.http.handler.openapi`:

```java
class CoreApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new CoreApiSpec().contribute(openAPI);
  }

  @Test
  void batchDeclaresStreamingBodyMediaTypes() {
    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    assertThat(post.getOperationId()).isEqualTo("executeBatch");
    assertThat(post.getTags()).containsExactly("Batch");
    assertThat(post.getRequestBody().getContent().keySet())
        .as("the body is streamed JSONL or CSV and is never parsed as JSON")
        .containsExactlyInAnyOrder("application/x-ndjson", "application/jsonl", "text/csv");
  }

  @Test
  void batchDeclaresTheDocumentedQueryParameters() {
    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    assertThat(post.getParameters().stream().map(Parameter::getName).toList())
        .contains("database", "batchSize", "refMode", "commitEvery", "vertexBatchSize",
            "expectedRecords", "ordinalBase", "idMapping", "lightEdges", "wal", "parallelFlush",
            "preAllocateEdgeChunks", "edgeListInitialSize", "bidirectional", "expectedEdgeCount",
            "commitRetries", "commitRetryDelayMs", "expectedVertexCount");
  }

  @Test
  void batchDeclaresTruncationTimeout() {
    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    assertThat(post.getResponses().keySet())
        .as("a body that ends early is answered 408 with the partial-commit counts, never 200")
        .contains("200", "400", "408");
  }

  @Test
  void batchResponseCarriesTheCountsAndIdMapping() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("BatchResponse");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "verticesCreated", "edgesCreated", "elapsedMs", "bytesRead",
        "idMapping", "idMappingOmitted", "idMappingSize");
  }

  @Test
  void batchFailuresCarryThePartialCommitCountsNotTheGenericError() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("BatchError");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "error", "exception", "requestId", "verticesCreated", "edgesCreated", "partialCommit");

    final Operation post = openAPI.getPaths().get("/api/v1/batch/{database}").getPost();
    for (final String code : List.of("400", "408")) {
      assertThat(post.getResponses().get(code).getContent().get("application/json")
          .getSchema().get$ref())
          .as("a client cannot reconcile after a failed load without these counts, so %s "
              + "must not point at the generic error body", code)
          .isEqualTo("#/components/schemas/BatchError");
    }
  }

  @Test
  void progressResponseMatchesOperationProgressJson() {
    final Operation get = openAPI.getPaths().get("/api/v1/progress/{database}").getGet();
    assertThat(get.getOperationId()).isEqualTo("getOperationProgress");

    final Schema<?> entry = openAPI.getComponents().getSchemas().get("ProgressResponse");
    assertThat(entry.getProperties()).containsKey("result");
    final Schema<?> item = entry.getProperties().get("result").getItems();
    assertThat(item.getProperties().keySet()).containsExactlyInAnyOrder(
        "id", "database", "operation", "stepName", "stepIndex", "totalSteps",
        "done", "total", "percentage", "startedOn", "elapsedMs");
  }

  @Test
  void existingOperationIdsAreUnchanged() {
    assertThat(openAPI.getPaths().get("/api/v1/query/{database}").getPost().getOperationId())
        .isEqualTo("executeQueryPost");
    assertThat(openAPI.getPaths().get("/api/v1/server").getGet().getOperationId())
        .isEqualTo("getServerInfo");
  }
}
```

Imports needed: `io.swagger.v3.oas.models.{Components, OpenAPI, Operation, Paths}`, `io.swagger.v3.oas.models.media.Schema`, `io.swagger.v3.oas.models.parameters.Parameter`, `org.junit.jupiter.api.{BeforeEach, Test}`, `static org.assertj.core.api.Assertions.assertThat`.

- [ ] **Step 2: Run the test to verify it fails**

```bash
mvn -q -pl server -am -Dtest=CoreApiSpecTest test
```

Expected: FAIL with `NullPointerException` on `getPaths().get("/api/v1/batch/{database}")` returning null. `existingOperationIdsAreUnchanged` passes already, which is the point: it guards the Task 1 extraction.

- [ ] **Step 3: Add the batch and progress operations to `CoreApiSpec`**

Add to `CoreApiSpec.contribute()`:

```java
    openAPI.getPaths().addPathItem("/api/v1/batch/{database}", createBatchPath());
    openAPI.getPaths().addPathItem("/api/v1/progress/{database}", createProgressPath());
    openAPI.getComponents().addSchemas("BatchResponse", createBatchResponseSchema());
    openAPI.getComponents().addSchemas("BatchError", createBatchErrorSchema());
    openAPI.getComponents().addSchemas("ProgressResponse", createProgressResponseSchema());
```

And the methods:

```java
  private PathItem createBatchPath() {
    final Operation post = SpecBuilders.operation("executeBatch", "Batch",
        "Bulk-load vertices and edges",
        """
            Streams vertices and then edges into the database using the GraphBatch API. The body is \
            never buffered, so a load is bounded by the server's memory only through the batching \
            parameters below.

            A batch is NOT atomic: GraphBatch commits every 'commitEvery' records, so a failure \
            mid-stream leaves earlier chunks durably committed. A client-input failure answers 400 \
            with 'verticesCreated', 'edgesCreated', and a 'partialCommit' flag; those counts are the \
            records attempted before the failure and are an upper bound on what is durable. Because \
            temporary ids are not keys, retrying the whole payload duplicates already-committed \
            vertices.

            A body that ends before its announced length answers 408 with the same partial-commit \
            counts, never a 200 with a truncated count. Compare the returned 'bytesRead' against the \
            bytes sent to verify a chunked upload arrived whole.""");

    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.addParametersItem(SpecBuilders.queryParam("batchSize",
        "Records buffered per GraphBatch flush. Default 100000.", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("lightEdges",
        "Create edges without their own record. Default false.", false, "boolean"));
    post.addParametersItem(SpecBuilders.queryParam("wal",
        "Write through the WAL. Default false.", false, "boolean"));
    post.addParametersItem(SpecBuilders.queryParam("parallelFlush",
        "Flush buckets in parallel. Default true.", false, "boolean"));
    post.addParametersItem(SpecBuilders.queryParam("preAllocateEdgeChunks",
        "Pre-allocate edge chunks. Default true.", false, "boolean"));
    post.addParametersItem(SpecBuilders.queryParam("edgeListInitialSize",
        "Initial edge-list size in bytes. Default 2048.", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("bidirectional",
        "Create edges in both directions. Default true.", false, "boolean"));
    post.addParametersItem(SpecBuilders.queryParam("commitEvery",
        """
            Records written per transaction during the edge flush. On a replicated database that \
            transaction becomes one Raft entry, so lower this when the server warns that an entry \
            approaches the maximum entry size. Default 50000.""", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("expectedEdgeCount",
        "Hint for the number of edges in the payload. Default 0.", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("commitRetries",
        """
            Retries of a vertex-creation commit that fails with a transient retryable error, such as \
            a Raft leader re-election, so a cluster hiccup does not abort a streaming load. \
            Default 10.""", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("commitRetryDelayMs",
        "Initial back-off before the first commit retry. Default 1000.", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("vertexBatchSize",
        """
            Vertices accumulated before they are created and committed in one transaction. On a \
            replicated database that transaction becomes one Raft entry. Default 10000.""",
        false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("expectedVertexCount",
        """
            Hint used to pre-size the vertex references, saving the copies of their growth. Only a \
            hint: the payload may carry more. Default 0.""", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("expectedRecords",
        """
            How many records, vertices plus edges, the payload carries. When given, a load that ends \
            with a different count is reported as incomplete instead of successful. This is the only \
            way to catch a chunked upload that stopped early, since a chunked body announces no \
            length.""", false, "integer"));
    post.addParametersItem(SpecBuilders.queryParam("ordinalBase",
        """
            With refMode=ordinal, the position of the first vertex of this payload. A client that \
            splits one load across several requests keeps a single counter across all of them. \
            Positions below the base belong to an earlier request and must be referenced by RID. \
            Default 0.""", false, "integer"));

    final Parameter idMapping = SpecBuilders.queryParam("idMapping",
        """
            Whether the response echoes the temporary-id to RID mapping. 'auto' echoes it only below \
            10000 entries, because a larger mapping would hold a second full copy of the map as JSON \
            in one string.""", false);
    idMapping.getSchema().setEnum(List.of("auto", "true", "false"));
    post.addParametersItem(idMapping);

    final Parameter refMode = SpecBuilders.queryParam("refMode",
        """
            How edges name the vertices they connect. 'id' resolves @from and @to against the @id each \
            vertex declared, costing the id plus a hash slot per vertex. 'ordinal' resolves them \
            against the 0-based position of the vertex in the payload, storing no id at all.""",
        false);
    refMode.getSchema().setEnum(List.of("id", "ordinal"));
    post.addParametersItem(refMode);

    final RequestBody body = new RequestBody();
    body.setDescription("""
        Vertices first, then edges. JSONL sends one JSON record per line; CSV sends a header row \
        followed by data rows. Vertices may declare a temporary '@id' that edges reference through \
        '@from' and '@to', or be referenced by position when refMode=ordinal. Edges may also \
        reference existing RIDs in #bucket:position form.""");
    body.setRequired(true);
    final Content content = new Content();
    final MediaType jsonl = new MediaType();
    jsonl.setSchema(new Schema<>().type("string").description("One JSON object per line"));
    content.addMediaType("application/x-ndjson", jsonl);
    content.addMediaType("application/jsonl", jsonl);
    final MediaType csv = new MediaType();
    csv.setSchema(new Schema<>().type("string").description("Header row followed by data rows"));
    content.addMediaType("text/csv", csv);
    body.setContent(content);
    post.setRequestBody(body);

    // 400 and 408 carry the partial-commit counts rather than the generic error body: a batch is
    // not atomic, so a client that cannot read how much was committed cannot reconcile before
    // retrying. Every other failure keeps the base handler's standard error shape.
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse("Load completed", "BatchResponse"));
    responses.addApiResponse("400", SpecBuilders.jsonResponse(
        "Client-input failure, with the counts attempted before it", "BatchError"));
    responses.addApiResponse("408", SpecBuilders.jsonResponse(
        "The body ended before it was fully consumed, with the counts attempted before that",
        "BatchError"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("409", SpecBuilders.errorResponse(
        "Concurrent modification: a page the load touched changed underneath it"));
    responses.addApiResponse("413", SpecBuilders.errorResponse("Request body too large"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    responses.addApiResponse("503", SpecBuilders.errorResponse(
        "Service unavailable: on a replicated database, no leader was reachable"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createProgressPath() {
    final Operation get = SpecBuilders.operation("getOperationProgress", "Server",
        "List in-progress maintenance operations",
        """
            Returns the long-running maintenance operations currently running on this server for one \
            database, with their step-by-step progress. Reads a lock-free snapshot: no database \
            access and no transaction, so polling at any frequency is safe and cannot interfere with \
            the operation being watched.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Progress snapshot", "ProgressResponse"),
        "400", "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private Schema<?> createBatchResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Result of a bulk load");
    schema.addProperty("verticesCreated", SpecBuilders.integer("Vertices created"));
    schema.addProperty("edgesCreated", SpecBuilders.integer("Edges created"));
    schema.addProperty("elapsedMs", SpecBuilders.integer("Elapsed time in milliseconds"));
    schema.addProperty("bytesRead", SpecBuilders.integer(
        "Bytes of the upload the server consumed, so a client can verify its whole file arrived"));
    schema.addProperty("idMapping", SpecBuilders.object(
        "Temporary id to RID mapping, present only when temporary ids were used and the mapping was small enough to echo"));
    schema.addProperty("idMappingOmitted", SpecBuilders.bool(
        "True when the mapping was too large to return"));
    schema.addProperty("idMappingSize", SpecBuilders.integer(
        "Number of entries in the omitted mapping"));
    return schema;
  }

  private Schema<?> createBatchErrorSchema() {
    final Schema<Object> schema = SpecBuilders.object("""
        Failed bulk load. Carries how much of the payload was attempted, because a batch is not \
        atomic and the caller has to reconcile before retrying.""");
    schema.addProperty("error", SpecBuilders.string("""
        Why the load failed. Carries the offending location, such as a line number or a temporary id, \
        because a batch failure echoes client input rather than engine internals."""));
    schema.addProperty("exception", SpecBuilders.string(
        "Exception class name, for distinguishing failure classes programmatically"));
    schema.addProperty("requestId", SpecBuilders.string("""
        Correlation id echoing X-Request-Id, for cross-referencing the failure against the server \
        log. Absent when the request carried no correlation id."""));
    schema.addProperty("verticesCreated", SpecBuilders.integer("""
        Vertices attempted before the failure. An upper bound on what is durable: records handled \
        since the last commit boundary were rolled back."""));
    schema.addProperty("edgesCreated", SpecBuilders.integer("""
        Edges attempted before the failure, with the same upper-bound caveat as \
        'verticesCreated'."""));
    schema.addProperty("partialCommit", SpecBuilders.bool("""
        True when earlier chunks are durably committed. Retrying the whole payload then duplicates \
        the already-committed vertices, because temporary ids are not keys."""));
    return schema;
  }

  private Schema<?> createProgressResponseSchema() {
    final Schema<Object> operation = SpecBuilders.object("One in-progress operation");
    operation.addProperty("id", SpecBuilders.integer("Operation identifier"));
    operation.addProperty("database", SpecBuilders.string("Database the operation runs on"));
    operation.addProperty("operation", SpecBuilders.string("Operation name, for example CHECK DATABASE"));
    operation.addProperty("stepName", SpecBuilders.string("Current step name"));
    operation.addProperty("stepIndex", SpecBuilders.integer("Current step, 0-based"));
    operation.addProperty("totalSteps", SpecBuilders.integer("Total number of steps"));
    operation.addProperty("done", SpecBuilders.integer("Units completed in the current step"));
    operation.addProperty("total", SpecBuilders.integer("Units in the current step, -1 when unknown"));
    operation.addProperty("percentage", SpecBuilders.integer(
        "Completion percentage of the current step, -1 when the total is unknown"));
    operation.addProperty("startedOn", SpecBuilders.integer("Start time as epoch milliseconds"));
    operation.addProperty("elapsedMs", SpecBuilders.integer("Elapsed time in milliseconds"));

    final Schema<Object> schema = SpecBuilders.object("In-progress maintenance operations");
    schema.addProperty("result", SpecBuilders.arrayOf(operation, "In-progress operations"));
    return schema;
  }
```

Add imports to `CoreApiSpec`: `io.swagger.v3.oas.models.media.{Content, MediaType, Schema}`, `io.swagger.v3.oas.models.parameters.{Parameter, RequestBody}`, `io.swagger.v3.oas.models.responses.ApiResponses`, `io.swagger.v3.oas.models.{Operation, PathItem}`, `java.util.List`.

- [ ] **Step 4: Run the test to verify it passes**

```bash
mvn -q -pl server -am -Dtest=CoreApiSpecTest test
```

Expected: PASS, 6 tests.

- [ ] **Step 5: Prove the test can fail**

Temporarily drop `content.addMediaType("text/csv", csv);`. Expected: `batchDeclaresStreamingBodyMediaTypes` FAILS. Revert.

- [ ] **Step 6: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/openapi/CoreApiSpec.java \
        server/src/test/java/com/arcadedb/server/http/handler/openapi/CoreApiSpecTest.java
git commit -m "feat(server) #4895: document the batch and progress operations

The batch body is streamed JSONL or CSV rather than JSON, so it is declared as
three media types carrying a string body plus the eighteen query parameters that
drive GraphBatch, not as a request schema."
```

---

### Task 3: Auth operations and the live bearer-token round trip

This task closes acceptance criterion 2 with a real request rather than a string assertion.

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/http/handler/openapi/AuthApiSpec.java`
- Test: `server/src/test/java/com/arcadedb/server/http/handler/openapi/AuthApiSpecTest.java`
- Modify test: `server/src/test/java/com/arcadedb/server/http/OpenApiSpecGenerationIT.java`

**Interfaces:**
- Consumes: `SpecBuilders`, `OpenApiContributor` from Task 1.
- Produces: `AuthApiSpec implements OpenApiContributor`; component schemas `LoginResponse`, `SessionList`; path items `/api/v1/login` (`login`), `/api/v1/logout` (`logout`), `/api/v1/sessions` (`listSessions`), all tagged `Auth`.

- [ ] **Step 1: Write the failing unit test**

Create `AuthApiSpecTest.java`, same header and package as Task 2:

```java
class AuthApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new AuthApiSpec().contribute(openAPI);
  }

  @Test
  void loginTakesNoRequestBody() {
    final Operation post = openAPI.getPaths().get("/api/v1/login").getPost();
    assertThat(post.getOperationId()).isEqualTo("login");
    assertThat(post.getRequestBody())
        .as("login authenticates from the Authorization header and reads no body")
        .isNull();
  }

  @Test
  void loginResponseCarriesTokenAndUser() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("LoginResponse");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder("token", "user");
  }

  @Test
  void logoutReturnsNoContentAndDeclaresNoSuccessBody() {
    final Operation post = openAPI.getPaths().get("/api/v1/logout").getPost();
    assertThat(post.getOperationId()).isEqualTo("logout");
    assertThat(post.getResponses()).containsKey("204");
    assertThat(post.getResponses().get("204").getContent()).isNull();
    assertThat(post.getResponses().get("200"))
        .as("logout never answers 200")
        .isNull();
  }

  @Test
  void sessionListCarriesEveryTrackedField() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("SessionList");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder("result", "count");
    assertThat(schema.getProperties().get("result").getItems().getProperties().keySet())
        .containsExactlyInAnyOrder("token", "user", "createdAt", "lastUpdate", "elapsedMs",
            "sourceIp", "userAgent", "country", "city");
  }

  @Test
  void everyAuthOperationIsTaggedAuth() {
    for (final String path : List.of("/api/v1/login", "/api/v1/logout", "/api/v1/sessions")) {
      final PathItem item = openAPI.getPaths().get(path);
      final Operation op = item.getPost() != null ? item.getPost() : item.getGet();
      assertThat(op.getTags()).as("%s", path).containsExactly("Auth");
    }
  }
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
mvn -q -pl server -am -Dtest=AuthApiSpecTest test
```

Expected: compilation failure, `cannot find symbol: class AuthApiSpec`.

- [ ] **Step 3: Create `AuthApiSpec`**

```java
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;

/**
 * Documents the session endpoints. Login mints a bearer session token from credentials already
 * validated by the handler chain, so it declares no request body of its own.
 */
public class AuthApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/login", createLoginPath());
    openAPI.getPaths().addPathItem("/api/v1/logout", createLogoutPath());
    openAPI.getPaths().addPathItem("/api/v1/sessions", createSessionsPath());

    openAPI.getComponents().addSchemas("LoginResponse", createLoginResponseSchema());
    openAPI.getComponents().addSchemas("SessionList", createSessionListSchema());
  }

  private PathItem createLoginPath() {
    final Operation post = SpecBuilders.operation("login", "Auth",
        "Create an authentication session",
        """
            Exchanges the credentials on the Authorization header for a session token prefixed 'AU-'. \
            The token is then presented as a bearer token on subsequent requests. This operation \
            takes no request body: the credentials travel on the header, and geolocation metadata is \
            read from the CF-IPCountry, CF-IPCity, CF-Connecting-IP, X-Forwarded-For, and User-Agent \
            headers when a proxy supplies them.""");
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Session created", "LoginResponse"),
        "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createLogoutPath() {
    final Operation post = SpecBuilders.operation("logout", "Auth",
        "Invalidate the current authentication session",
        "Invalidates the session token presented on the Authorization header. Answers 204 with no body.");
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("204", SpecBuilders.emptyResponse("Session invalidated"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createSessionsPath() {
    final Operation get = SpecBuilders.operation("listSessions", "Auth",
        "List active authentication sessions",
        "Lists the authentication sessions currently held by this server, with their client metadata.");
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Active sessions", "SessionList"),
        "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private Schema<?> createLoginResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Newly created session");
    schema.addProperty("token", SpecBuilders.string(
        "Session token prefixed 'AU-', presented as a bearer token"));
    schema.addProperty("user", SpecBuilders.string("Authenticated user name"));
    return schema;
  }

  private Schema<?> createSessionListSchema() {
    final Schema<Object> session = SpecBuilders.object("One active session");
    session.addProperty("token", SpecBuilders.string("Session token"));
    session.addProperty("user", SpecBuilders.string("User the session belongs to"));
    session.addProperty("createdAt", SpecBuilders.integer("Creation time as epoch milliseconds"));
    session.addProperty("lastUpdate", SpecBuilders.integer("Last use as epoch milliseconds"));
    session.addProperty("elapsedMs", SpecBuilders.integer("Milliseconds since last use"));
    session.addProperty("sourceIp", SpecBuilders.string("Client address"));
    session.addProperty("userAgent", SpecBuilders.string("Client user agent"));
    session.addProperty("country", SpecBuilders.string("Country reported by the proxy, when available"));
    session.addProperty("city", SpecBuilders.string("City reported by the proxy, when available"));

    final Schema<Object> schema = SpecBuilders.object("Active authentication sessions");
    schema.addProperty("result", SpecBuilders.arrayOf(session, "Active sessions"));
    schema.addProperty("count", SpecBuilders.integer("Number of active sessions"));
    return schema;
  }
}
```

Add the import `io.swagger.v3.oas.models.responses.ApiResponses`.

- [ ] **Step 4: Run to verify it passes**

```bash
mvn -q -pl server -am -Dtest=AuthApiSpecTest test
```

Expected: PASS, 5 tests.

- [ ] **Step 5: Write the failing acceptance test for bearer authentication**

Append to `OpenApiSpecGenerationIT`. This is acceptance criterion 2: it proves the declared scheme matches what the server accepts.

```java
  @Test
  void apiTokenAuthenticatesAgainstTheDeclaredBearerScheme() throws Exception {
    // Mint a real API token through the documented endpoint.
    final HttpRequest createToken = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/server/api-tokens"))
        .header("Content-Type", "application/json")
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .POST(HttpRequest.BodyPublishers.ofString("{\"name\":\"openapi-bearer-test\"}"))
        .build();

    final HttpResponse<String> created = client.send(createToken, BodyHandlers.ofString());
    assertThat(created.statusCode())
        .as("token creation should succeed: %s", created.body())
        .isIn(200, 201);

    final String token = new JSONObject(created.body()).getString("token");
    assertThat(token).startsWith("at-");

    // The server accepts it as a bearer token on an ordinary documented operation.
    final HttpRequest listDatabases = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/databases"))
        .GET()
        .setHeader("Authorization", "Bearer " + token)
        .build();

    final HttpResponse<String> listed = client.send(listDatabases, BodyHandlers.ofString());
    assertThat(listed.statusCode())
        .as("an 'at-' API token must authenticate as a bearer token: %s", listed.body())
        .isEqualTo(200);

    // And the spec says so: root security offers bearerAuth, and this operation does not override it.
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();
    assertThat(openAPI.getComponents().getSecuritySchemes().get("bearerAuth").getScheme())
        .isEqualTo("bearer");
    assertThat(openAPI.getPaths().get("/api/v1/databases").getGet().getSecurity())
        .as("listDatabases must inherit root security rather than override it")
        .isNull();
    assertThat(openAPI.getSecurity().stream().flatMap(r -> r.keySet().stream()).toList())
        .contains("bearerAuth");
  }
```

- [ ] **Step 6: Run to verify it passes, then prove it can fail**

```bash
mvn -q -pl server -am -Dit.test=OpenApiSpecGenerationIT verify
```

Expected: PASS. Then temporarily remove the `bearerAuth` `SecurityRequirement` from `OpenApiSpecGenerator.generateSpec()` and re-run: the final assertion FAILS, proving the test binds the spec to the server's real behaviour. Revert.

- [ ] **Step 7: Register `AuthApiSpec`, compile, commit**

Add the import and the list entry to `OpenApiSpecGenerator`, keeping the documented order:

```java
import com.arcadedb.server.http.handler.openapi.AuthApiSpec;
```

```java
  private static final List<OpenApiContributor> CONTRIBUTORS = List.of(//
      new CoreApiSpec(), //
      new AuthApiSpec(), //
      new SecurityAdminApiSpec());
```

Verify:

```bash
mvn -q -pl server -am -DskipTests compile
mvn -q -pl server -am -Dtest='SpecBuildersTest,CoreApiSpecTest,AuthApiSpecTest' test
```

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/openapi/AuthApiSpec.java \
        server/src/test/java/com/arcadedb/server/http/handler/openapi/AuthApiSpecTest.java \
        server/src/test/java/com/arcadedb/server/http/OpenApiSpecGenerationIT.java
git commit -m "feat(server) #4895: document login, logout, and sessions, and prove bearer auth end to end

The new integration test mints a real 'at-' API token, authenticates an ordinary
operation with it, and asserts the spec declares the scheme that accepted it.
Asserting the string 'bearerAuth' appears in the document would have passed
against the previous spec, which declared the scheme and attached it to nothing."
```

---

### Task 4: Extract `SecurityAdminApiSpec` assertions

Task 1 moved these ten operations; this task locks them behind their own test so a later refactor cannot silently drop one.

**Files:**
- Test: `server/src/test/java/com/arcadedb/server/http/handler/openapi/SecurityAdminApiSpecTest.java`
- Modify if the test finds gaps: `server/src/main/java/com/arcadedb/server/http/handler/openapi/SecurityAdminApiSpec.java`

**Interfaces:**
- Consumes: `SecurityAdminApiSpec` from Task 1 Step 10.
- Produces: no new production interface.

- [ ] **Step 1: Write the test**

```java
class SecurityAdminApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new SecurityAdminApiSpec().contribute(openAPI);
  }

  @Test
  void usersPathKeepsAllFourMethodsAndOperationIds() {
    final PathItem users = openAPI.getPaths().get("/api/v1/server/users");
    assertThat(users.getGet().getOperationId()).isEqualTo("listUsers");
    assertThat(users.getPost().getOperationId()).isEqualTo("createUser");
    assertThat(users.getPut().getOperationId()).isEqualTo("updateUser");
    assertThat(users.getDelete().getOperationId()).isEqualTo("deleteUser");
  }

  @Test
  void groupsPathKeepsThreeMethods() {
    final PathItem groups = openAPI.getPaths().get("/api/v1/server/groups");
    assertThat(groups.getGet().getOperationId()).isEqualTo("listGroups");
    assertThat(groups.getPost().getOperationId()).isEqualTo("createOrUpdateGroup");
    assertThat(groups.getDelete().getOperationId()).isEqualTo("deleteGroup");
    assertThat(groups.getPut()).isNull();
  }

  @Test
  void apiTokensPathKeepsThreeMethods() {
    final PathItem tokens = openAPI.getPaths().get("/api/v1/server/api-tokens");
    assertThat(tokens.getGet().getOperationId()).isEqualTo("listApiTokens");
    assertThat(tokens.getPost().getOperationId()).isEqualTo("createApiToken");
    assertThat(tokens.getDelete().getOperationId()).isEqualTo("deleteApiToken");
  }

  @Test
  void createUserAnswers201() {
    assertThat(openAPI.getPaths().get("/api/v1/server/users").getPost().getResponses())
        .containsKey("201");
  }

  @Test
  void createApiTokenAnswers201() {
    assertThat(openAPI.getPaths().get("/api/v1/server/api-tokens").getPost().getResponses())
        .containsKey("201");
  }

  @Test
  void deleteOperationsDeclareTheirQueryParameters() {
    assertThat(openAPI.getPaths().get("/api/v1/server/users").getDelete().getParameters()
        .stream().map(Parameter::getName).toList()).containsExactly("name");
    assertThat(openAPI.getPaths().get("/api/v1/server/groups").getDelete().getParameters()
        .stream().map(Parameter::getName).toList()).containsExactlyInAnyOrder("database", "name");
    assertThat(openAPI.getPaths().get("/api/v1/server/api-tokens").getDelete().getParameters()
        .stream().map(Parameter::getName).toList()).containsExactly("token");
  }

  @Test
  void everyOperationIsTaggedSecurity() {
    for (final String path : List.of("/api/v1/server/users", "/api/v1/server/groups",
        "/api/v1/server/api-tokens")) {
      final PathItem item = openAPI.getPaths().get(path);
      assertThat(item.readOperations()).allSatisfy(op ->
          assertThat(op.getTags()).as("%s", path).containsExactly("Security"));
    }
  }
}
```

- [ ] **Step 2: Run it**

```bash
mvn -q -pl server -am -Dtest=SecurityAdminApiSpecTest test
```

Expected: PASS, 7 tests. If any fail, the Task 1 extraction dropped something; fix `SecurityAdminApiSpec` to match the original `OpenApiSpecGenerator` behaviour rather than adjusting the test.

- [ ] **Step 3: Prove the test can fail**

Temporarily comment out `pathItem.setPut(putOp);` in `SecurityAdminApiSpec`. Expected: `usersPathKeepsAllFourMethodsAndOperationIds` FAILS with a null-pointer on `getPut()`. Revert.

- [ ] **Step 4: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/http/handler/openapi/SecurityAdminApiSpecTest.java
git commit -m "test(server) #4895: lock the security-admin operations behind their own contributor test"
```

---

### Task 5: Time-series operations

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/http/handler/openapi/TimeSeriesApiSpec.java`
- Test: `server/src/test/java/com/arcadedb/server/http/handler/openapi/TimeSeriesApiSpecTest.java`

**Interfaces:**
- Consumes: `SpecBuilders`, `OpenApiContributor` from Task 1.
- Produces: `TimeSeriesApiSpec implements OpenApiContributor`; schemas `TimeSeriesQueryRequest`, `TimeSeriesRawResponse`, `TimeSeriesAggregatedResponse`, `TimeSeriesLatestResponse`, `TimeSeriesWriteError`; path items `/api/v1/ts/{database}/write` (`writeTimeSeries`), `/query` (`queryTimeSeries`), `/latest` (`getTimeSeriesLatest`), all tagged `TimeSeries`.

- [ ] **Step 1: Write the failing test**

```java
class TimeSeriesApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new TimeSeriesApiSpec().contribute(openAPI);
  }

  @Test
  void writeTakesLineProtocolTextNotJson() {
    final Operation post = openAPI.getPaths().get("/api/v1/ts/{database}/write").getPost();
    assertThat(post.getOperationId()).isEqualTo("writeTimeSeries");
    assertThat(post.getRequestBody().getContent().keySet())
        .as("the body is InfluxDB Line Protocol text, not JSON")
        .containsExactly("text/plain");
  }

  @Test
  void writeDeclaresPrecisionEnum() {
    final Operation post = openAPI.getPaths().get("/api/v1/ts/{database}/write").getPost();
    final Parameter precision = post.getParameters().stream()
        .filter(p -> "precision".equals(p.getName())).findFirst().orElseThrow();
    assertThat(precision.getSchema().getEnum()).containsExactly("ns", "us", "ms", "s");
    assertThat(precision.getRequired()).isFalse();
  }

  @Test
  void writeSucceedsWith204AndNeverWith200() {
    final Operation post = openAPI.getPaths().get("/api/v1/ts/{database}/write").getPost();
    assertThat(post.getResponses()).containsKey("204");
    assertThat(post.getResponses().get("204").getContent()).isNull();
    assertThat(post.getResponses().get("200")).isNull();
  }

  @Test
  void writeErrorBodyCarriesTheIngestionCounts() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("TimeSeriesWriteError");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "error", "requestId", "written", "dropped", "unknownTypes", "nonTimeSeriesTypes");
    assertThat(openAPI.getPaths().get("/api/v1/ts/{database}/write").getPost()
        .getResponses().get("400").getContent().get("application/json").getSchema().get$ref())
        .isEqualTo("#/components/schemas/TimeSeriesWriteError");
  }

  @Test
  void queryRequestModelsTheNestedAggregation() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("TimeSeriesQueryRequest");
    assertThat(schema.getRequired()).containsExactly("type");
    assertThat(schema.getProperties().keySet())
        .containsExactlyInAnyOrder("type", "from", "to", "tags", "fields", "aggregation");

    final Schema<?> aggregation = schema.getProperties().get("aggregation");
    assertThat(aggregation.getProperties().keySet())
        .containsExactlyInAnyOrder("bucketInterval", "requests");
    assertThat(aggregation.getProperties().get("requests").getItems().getProperties().keySet())
        .containsExactlyInAnyOrder("field", "type", "alias");
  }

  @Test
  void queryResponseIsAOneOfOverTheRawAndAggregatedShapes() {
    final Operation post = openAPI.getPaths().get("/api/v1/ts/{database}/query").getPost();
    final Schema<?> success = post.getResponses().get("200")
        .getContent().get("application/json").getSchema();
    assertThat(success.getOneOf())
        .as("the raw and aggregated shapes are structurally different")
        .hasSize(2);
    assertThat(success.getOneOf().stream().map(Schema::get$ref).toList())
        .containsExactlyInAnyOrder(
            "#/components/schemas/TimeSeriesRawResponse",
            "#/components/schemas/TimeSeriesAggregatedResponse");
  }

  @Test
  void rawAndAggregatedResponsesHaveTheirDistinctFields() {
    assertThat(openAPI.getComponents().getSchemas().get("TimeSeriesRawResponse")
        .getProperties().keySet()).containsExactlyInAnyOrder("type", "columns", "rows", "count");
    final Schema<?> aggregated = openAPI.getComponents().getSchemas()
        .get("TimeSeriesAggregatedResponse");
    assertThat(aggregated.getProperties().keySet())
        .containsExactlyInAnyOrder("type", "aggregations", "buckets", "count");
    assertThat(aggregated.getProperties().get("buckets").getItems().getProperties().keySet())
        .containsExactlyInAnyOrder("timestamp", "values");
  }

  @Test
  void latestDeclaresTypeRequiredAndTagOptional() {
    final Operation get = openAPI.getPaths().get("/api/v1/ts/{database}/latest").getGet();
    assertThat(get.getOperationId()).isEqualTo("getTimeSeriesLatest");
    final Map<String, Boolean> required = get.getParameters().stream()
        .collect(Collectors.toMap(Parameter::getName, Parameter::getRequired));
    assertThat(required).containsEntry("type", true).containsEntry("tag", false);
  }

  @Test
  void latestResponseAllowsANullLatestSample() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("TimeSeriesLatestResponse");
    assertThat(schema.getProperties().keySet())
        .containsExactlyInAnyOrder("type", "columns", "latest");
    assertThat(schema.getProperties().get("latest").getNullable())
        .as("latest is JSON null when the series is empty")
        .isTrue();
  }
}
```

Add imports `java.util.Map`.

- [ ] **Step 2: Run to verify it fails**

```bash
mvn -q -pl server -am -Dtest=TimeSeriesApiSpecTest test
```

Expected: compilation failure, `cannot find symbol: class TimeSeriesApiSpec`.

- [ ] **Step 3: Create `TimeSeriesApiSpec`**

```java
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.util.List;

/**
 * Documents time-series ingestion and querying. Ingestion speaks InfluxDB Line Protocol rather than
 * JSON, and the query response takes one of two structurally different shapes depending on whether
 * the request asked for aggregation.
 */
public class TimeSeriesApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/ts/{database}/write", createWritePath());
    openAPI.getPaths().addPathItem("/api/v1/ts/{database}/query", createQueryPath());
    openAPI.getPaths().addPathItem("/api/v1/ts/{database}/latest", createLatestPath());

    openAPI.getComponents().addSchemas("TimeSeriesQueryRequest", createQueryRequestSchema());
    openAPI.getComponents().addSchemas("TimeSeriesRawResponse", createRawResponseSchema());
    openAPI.getComponents().addSchemas("TimeSeriesAggregatedResponse", createAggregatedResponseSchema());
    openAPI.getComponents().addSchemas("TimeSeriesLatestResponse", createLatestResponseSchema());
    openAPI.getComponents().addSchemas("TimeSeriesWriteError", createWriteErrorSchema());
  }

  private PathItem createWritePath() {
    final Operation post = SpecBuilders.operation("writeTimeSeries", "TimeSeries",
        "Ingest samples in InfluxDB Line Protocol",
        """
            Ingests one or more samples expressed in InfluxDB Line Protocol. The measurement name \
            selects the time-series type, tags select the series, and fields carry the values.

            The body may be gzip-compressed by sending Content-Encoding: gzip. A fully accepted \
            request answers 204 with no body; a request whose samples could not all be applied \
            answers 400 with the counts of what was written and dropped, so a client can tell a total \
            rejection from a partial one.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));

    final Parameter precision = SpecBuilders.queryParam("precision",
        "Unit of the timestamps in the body. Defaults to the server's configured precision.", false);
    precision.getSchema().setEnum(List.of("ns", "us", "ms", "s"));
    post.addParametersItem(precision);

    post.setRequestBody(SpecBuilders.rawBody(
        "InfluxDB Line Protocol text, one measurement per line. Optionally gzip-compressed.",
        "text/plain", null));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("204", SpecBuilders.emptyResponse("All samples ingested"));
    responses.addApiResponse("400",
        SpecBuilders.jsonResponse("Samples rejected, with the counts written and dropped",
            "TimeSeriesWriteError"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createQueryPath() {
    final Operation post = SpecBuilders.operation("queryTimeSeries", "TimeSeries",
        "Query samples, optionally aggregated into buckets",
        """
            Reads samples from a time-series type over a timestamp range, optionally filtered by tag \
            and projected to a subset of fields.

            The response shape depends on the request: without 'aggregation' it carries raw rows \
            under 'rows'; with 'aggregation' it carries fixed-interval buckets under 'buckets' and \
            names the computed aggregations under 'aggregations'.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setRequestBody(SpecBuilders.jsonBody("Query definition", "TimeSeriesQueryRequest", true));

    final ApiResponse success = new ApiResponse();
    success.setDescription("Samples, raw or aggregated according to the request");
    final Schema<?> oneOf = new Schema<>();
    oneOf.setOneOf(List.of(//
        SpecBuilders.ref("TimeSeriesRawResponse"), //
        SpecBuilders.ref("TimeSeriesAggregatedResponse")));
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(oneOf);
    success.setContent(new Content().addMediaType(SpecBuilders.JSON, mediaType));

    post.setResponses(SpecBuilders.standardResponses("200", success,
        "400", "401", "403", "404", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createLatestPath() {
    final Operation get = SpecBuilders.operation("getTimeSeriesLatest", "TimeSeries",
        "Read the most recent sample of a series",
        """
            Returns the most recent sample of a time-series type, optionally narrowed to one series \
            by tag. 'latest' is null when the type or the selected series holds no sample.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.addParametersItem(SpecBuilders.queryParam("type", "Time-series type name", true));
    get.addParametersItem(SpecBuilders.queryParam("tag",
        "Tag filter in name=value form, repeatable to narrow to one series", false));
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Most recent sample", "TimeSeriesLatestResponse"),
        "400", "401", "403", "404", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private Schema<?> createQueryRequestSchema() {
    final Schema<Object> request = SpecBuilders.object("One aggregation to compute over a bucket");
    request.addProperty("field", SpecBuilders.string("Field name to aggregate"));
    request.addProperty("type", SpecBuilders.string(
        "Aggregation function, for example AVG, SUM, MIN, MAX, COUNT"));
    request.addProperty("alias", SpecBuilders.string(
        "Output name. Defaults to the field name suffixed with the lower-cased aggregation type."));

    final Schema<Object> aggregation = SpecBuilders.object(
        "Bucketed aggregation. Present only when the caller wants buckets rather than raw rows.");
    aggregation.addProperty("bucketInterval", SpecBuilders.integer(
        "Bucket width in the same unit as the timestamps"));
    aggregation.addProperty("requests", SpecBuilders.arrayOf(request, "Aggregations to compute"));

    final Schema<Object> schema = SpecBuilders.object("Time-series query definition");
    schema.addProperty("type", SpecBuilders.string("Time-series type name"));
    schema.addProperty("from", SpecBuilders.integer(
        "Inclusive lower bound of the timestamp range. Unbounded when omitted."));
    schema.addProperty("to", SpecBuilders.integer(
        "Inclusive upper bound of the timestamp range. Unbounded when omitted."));
    schema.addProperty("tags", SpecBuilders.object(
        "Tag filter as name to value pairs. All pairs must match."));
    schema.addProperty("fields", SpecBuilders.arrayOf(
        SpecBuilders.string("Field name"), "Fields to project. All fields when omitted."));
    schema.addProperty("aggregation", aggregation);
    schema.setRequired(List.of("type"));
    return schema;
  }

  private Schema<?> createRawResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Raw samples");
    schema.addProperty("type", SpecBuilders.string("Time-series type name"));
    schema.addProperty("columns", SpecBuilders.arrayOf(
        SpecBuilders.string("Column name"), "Column names, in the order the row values appear"));
    schema.addProperty("rows", SpecBuilders.arrayOf(
        SpecBuilders.arrayOf(SpecBuilders.object("Column value"), "One row"),
        "Rows, each positionally aligned with 'columns'"));
    schema.addProperty("count", SpecBuilders.integer("Number of rows returned"));
    return schema;
  }

  private Schema<?> createAggregatedResponseSchema() {
    final Schema<Object> bucket = SpecBuilders.object("One aggregation bucket");
    bucket.addProperty("timestamp", SpecBuilders.integer("Bucket start timestamp"));
    bucket.addProperty("values", SpecBuilders.arrayOf(
        SpecBuilders.object("Aggregated value"),
        "Aggregated values, positionally aligned with 'aggregations'"));

    final Schema<Object> schema = SpecBuilders.object("Aggregated samples");
    schema.addProperty("type", SpecBuilders.string("Time-series type name"));
    schema.addProperty("aggregations", SpecBuilders.arrayOf(
        SpecBuilders.string("Aggregation alias"),
        "Aliases of the computed aggregations, in bucket value order"));
    schema.addProperty("buckets", SpecBuilders.arrayOf(bucket, "Buckets, ordered by timestamp"));
    schema.addProperty("count", SpecBuilders.integer("Number of buckets returned"));
    return schema;
  }

  private Schema<?> createLatestResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Most recent sample of a series");
    schema.addProperty("type", SpecBuilders.string("Time-series type name"));
    schema.addProperty("columns", SpecBuilders.arrayOf(
        SpecBuilders.string("Column name"), "Column names, in sample value order"));
    final Schema<?> latest = SpecBuilders.arrayOf(SpecBuilders.object("Column value"),
        "Most recent sample, positionally aligned with 'columns'. Null when the series is empty.");
    latest.setNullable(true);
    schema.addProperty("latest", latest);
    return schema;
  }

  private Schema<?> createWriteErrorSchema() {
    final Schema<Object> schema = SpecBuilders.object("Rejected ingestion, with partial counts");
    schema.addProperty("error", SpecBuilders.string("Why the request was rejected"));
    schema.addProperty("requestId", SpecBuilders.string(
        "Correlation id echoing X-Request-Id, for matching against server logs"));
    schema.addProperty("written", SpecBuilders.integer("Samples successfully ingested"));
    schema.addProperty("dropped", SpecBuilders.integer("Samples discarded"));
    schema.addProperty("unknownTypes", SpecBuilders.arrayOf(
        SpecBuilders.string("Measurement name"),
        "Measurements naming a type that does not exist"));
    schema.addProperty("nonTimeSeriesTypes", SpecBuilders.arrayOf(
        SpecBuilders.string("Type name"),
        "Measurements naming a type that exists but is not a time-series type"));
    return schema;
  }
}
```

Add imports `io.swagger.v3.oas.models.media.{Content, MediaType}`.

- [ ] **Step 4: Run to verify it passes**

```bash
mvn -q -pl server -am -Dtest=TimeSeriesApiSpecTest test
```

Expected: PASS, 9 tests.

- [ ] **Step 5: Prove the test can fail**

Temporarily change `createWritePath` to add a `200` response alongside the `204`. Expected: `writeSucceedsWith204AndNeverWith200` FAILS. Revert.

- [ ] **Step 6: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/openapi/TimeSeriesApiSpec.java \
        server/src/test/java/com/arcadedb/server/http/handler/openapi/TimeSeriesApiSpecTest.java
git commit -m "feat(server) #4895: document the time-series write, query, and latest operations

Ingestion is Line Protocol text with a precision parameter, answering 204 on
success, so it carries no request schema and no success body. The query response
is a oneOf: the raw and aggregated shapes share no fields beyond type and count."
```

---

### Task 6: Grafana operations

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/http/handler/openapi/GrafanaApiSpec.java`
- Test: `server/src/test/java/com/arcadedb/server/http/handler/openapi/GrafanaApiSpecTest.java`

**Interfaces:**
- Consumes: `SpecBuilders`, `OpenApiContributor` from Task 1.
- Produces: `GrafanaApiSpec implements OpenApiContributor`; schemas `GrafanaHealth`, `GrafanaMetadata`, `GrafanaQueryRequest`, `GrafanaQueryResponse`; path items `/api/v1/ts/{database}/grafana/health` (`checkGrafanaHealth`), `/grafana/metadata` (`getGrafanaMetadata`), `/grafana/query` (`queryGrafana`), all tagged `Grafana`.

- [ ] **Step 1: Write the failing test**

```java
class GrafanaApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new GrafanaApiSpec().contribute(openAPI);
  }

  @Test
  void healthReportsStatusAndDatabase() {
    assertThat(openAPI.getPaths().get("/api/v1/ts/{database}/grafana/health").getGet()
        .getOperationId()).isEqualTo("checkGrafanaHealth");
    assertThat(openAPI.getComponents().getSchemas().get("GrafanaHealth").getProperties().keySet())
        .containsExactlyInAnyOrder("status", "database");
  }

  @Test
  void metadataDescribesTypesFieldsAndTags() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("GrafanaMetadata");
    assertThat(schema.getProperties().keySet())
        .containsExactlyInAnyOrder("types", "aggregationTypes");
    final Schema<?> type = schema.getProperties().get("types").getItems();
    assertThat(type.getProperties().keySet())
        .containsExactlyInAnyOrder("name", "fields", "tags");
    assertThat(type.getProperties().get("fields").getItems().getProperties().keySet())
        .containsExactlyInAnyOrder("name", "dataType");
  }

  @Test
  void queryRequestRequiresTargets() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("GrafanaQueryRequest");
    assertThat(schema.getRequired()).containsExactly("targets");
    assertThat(schema.getProperties().get("targets").getItems().getProperties().keySet())
        .containsExactlyInAnyOrder("refId", "type", "tags", "aggregation");
  }

  @Test
  void queryResponseIsTheGrafanaDataFrameEnvelopeNotSimpleJson() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("GrafanaQueryResponse");
    assertThat(schema.getProperties().keySet()).containsExactly("results");

    final Schema<?> perRefId = schema.getProperties().get("results").getAdditionalPropertiesSchema();
    assertThat(perRefId)
        .as("results is keyed by refId, so it must be modelled with additionalProperties")
        .isNotNull();
    assertThat(perRefId.getProperties().keySet()).containsExactly("frames");

    final Schema<?> frame = perRefId.getProperties().get("frames").getItems();
    assertThat(frame.getProperties().keySet()).containsExactlyInAnyOrder("schema", "data");
    assertThat(frame.getProperties().get("schema").getProperties().get("fields").getItems()
        .getProperties().keySet()).containsExactlyInAnyOrder("name", "type");
    assertThat(frame.getProperties().get("data").getProperties().keySet()).containsExactly("values");
  }

  @Test
  void everyGrafanaOperationIsTaggedGrafanaAndTakesTheDatabasePath() {
    for (final String suffix : List.of("health", "metadata", "query")) {
      final PathItem item = openAPI.getPaths().get("/api/v1/ts/{database}/grafana/" + suffix);
      final Operation op = item.getGet() != null ? item.getGet() : item.getPost();
      assertThat(op.getTags()).as("%s", suffix).containsExactly("Grafana");
      assertThat(op.getParameters().stream().map(Parameter::getName).toList())
          .as("%s", suffix).contains("database");
    }
  }
}
```

Note: the accessor is `getAdditionalPropertiesSchema()` in swagger-models 2.2.x when `additionalProperties` holds a `Schema`. If the compiler rejects it, use `(Schema<?>) schema.getProperties().get("results").getAdditionalProperties()` with a cast.

- [ ] **Step 2: Run to verify it fails**

```bash
mvn -q -pl server -am -Dtest=GrafanaApiSpecTest test
```

Expected: compilation failure, `cannot find symbol: class GrafanaApiSpec`.

- [ ] **Step 3: Create `GrafanaApiSpec`**

```java
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;

import java.util.List;

/**
 * Documents the Grafana data source endpoints. Query results use the Grafana DataFrame envelope:
 * one entry per request refId, each holding frames whose schema names the fields and whose data
 * holds one column-major array per field.
 */
public class GrafanaApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/ts/{database}/grafana/health", createHealthPath());
    openAPI.getPaths().addPathItem("/api/v1/ts/{database}/grafana/metadata", createMetadataPath());
    openAPI.getPaths().addPathItem("/api/v1/ts/{database}/grafana/query", createQueryPath());

    openAPI.getComponents().addSchemas("GrafanaHealth", createHealthSchema());
    openAPI.getComponents().addSchemas("GrafanaMetadata", createMetadataSchema());
    openAPI.getComponents().addSchemas("GrafanaQueryRequest", createQueryRequestSchema());
    openAPI.getComponents().addSchemas("GrafanaQueryResponse", createQueryResponseSchema());
  }

  private PathItem createHealthPath() {
    final Operation get = SpecBuilders.operation("checkGrafanaHealth", "Grafana",
        "Test the data source connection",
        "Answers the Grafana data source health check for one database.");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Data source reachable", "GrafanaHealth"),
        "400", "401", "403", "404", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createMetadataPath() {
    final Operation get = SpecBuilders.operation("getGrafanaMetadata", "Grafana",
        "List queryable types, fields, and tags",
        """
            Describes what a Grafana panel can query: the time-series types in the database, each \
            with its value fields and their data types, its tag names, and the aggregation functions \
            the server supports.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Queryable metadata", "GrafanaMetadata"),
        "400", "401", "403", "404", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createQueryPath() {
    final Operation post = SpecBuilders.operation("queryGrafana", "Grafana",
        "Execute panel queries and return DataFrames",
        """
            Executes one query per entry in 'targets' and returns the results keyed by each target's \
            refId, in the Grafana DataFrame format. A target carrying 'aggregation' produces bucketed \
            values; a target without it produces raw samples. 'maxDataPoints' bounds the points \
            returned per frame.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setRequestBody(SpecBuilders.jsonBody("Grafana panel query", "GrafanaQueryRequest", true));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("DataFrames keyed by target refId", "GrafanaQueryResponse"),
        "400", "401", "403", "404", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private Schema<?> createHealthSchema() {
    final Schema<Object> schema = SpecBuilders.object("Data source health");
    schema.addProperty("status", SpecBuilders.string("Always 'ok' when the database is reachable"));
    schema.addProperty("database", SpecBuilders.string("Database the check ran against"));
    return schema;
  }

  private Schema<?> createMetadataSchema() {
    final Schema<Object> field = SpecBuilders.object("One value field");
    field.addProperty("name", SpecBuilders.string("Field name"));
    field.addProperty("dataType", SpecBuilders.string("ArcadeDB column data type"));

    final Schema<Object> type = SpecBuilders.object("One queryable time-series type");
    type.addProperty("name", SpecBuilders.string("Type name"));
    type.addProperty("fields", SpecBuilders.arrayOf(field, "Value fields"));
    type.addProperty("tags", SpecBuilders.arrayOf(
        SpecBuilders.string("Tag name"), "Tag names available as filters"));

    final Schema<Object> schema = SpecBuilders.object("Queryable metadata");
    schema.addProperty("types", SpecBuilders.arrayOf(type, "Queryable time-series types"));
    schema.addProperty("aggregationTypes", SpecBuilders.arrayOf(
        SpecBuilders.string("Aggregation function name"), "Supported aggregation functions"));
    return schema;
  }

  private Schema<?> createQueryRequestSchema() {
    final Schema<Object> aggregationRequest = SpecBuilders.object("One aggregation to compute");
    aggregationRequest.addProperty("field", SpecBuilders.string("Field name to aggregate"));
    aggregationRequest.addProperty("type", SpecBuilders.string("Aggregation function"));
    aggregationRequest.addProperty("alias", SpecBuilders.string(
        "Output field name. Defaults to the field name suffixed with the aggregation type."));

    final Schema<Object> aggregation = SpecBuilders.object(
        "Bucketed aggregation. Omit for raw samples.");
    aggregation.addProperty("bucketInterval", SpecBuilders.integer("Bucket width in milliseconds"));
    aggregation.addProperty("requests", SpecBuilders.arrayOf(
        aggregationRequest, "Aggregations to compute"));

    final Schema<Object> target = SpecBuilders.object("One panel query");
    target.addProperty("refId", SpecBuilders.string(
        "Identifier echoed back as the result key. Defaults to 'A'."));
    target.addProperty("type", SpecBuilders.string("Time-series type name"));
    target.addProperty("tags", SpecBuilders.object("Tag filter as name to value pairs"));
    target.addProperty("aggregation", aggregation);

    final Schema<Object> range = SpecBuilders.object("Panel time range");
    range.addProperty("from", SpecBuilders.string("Inclusive lower bound"));
    range.addProperty("to", SpecBuilders.string("Inclusive upper bound"));

    final Schema<Object> schema = SpecBuilders.object("Grafana panel query");
    schema.addProperty("targets", SpecBuilders.arrayOf(target, "Queries to execute"));
    schema.addProperty("range", range);
    schema.addProperty("maxDataPoints", SpecBuilders.integer(
        "Upper bound on the points returned per frame"));
    schema.setRequired(List.of("targets"));
    return schema;
  }

  private Schema<?> createQueryResponseSchema() {
    final Schema<Object> frameField = SpecBuilders.object("One frame field");
    frameField.addProperty("name", SpecBuilders.string("Field name, 'time' for the time column"));
    frameField.addProperty("type", SpecBuilders.string("Grafana field type, for example time or number"));

    final Schema<Object> frameSchema = SpecBuilders.object("Frame schema");
    frameSchema.addProperty("fields", SpecBuilders.arrayOf(
        frameField, "Fields, positionally aligned with the value arrays"));

    final Schema<Object> frameData = SpecBuilders.object("Frame data");
    frameData.addProperty("values", SpecBuilders.arrayOf(
        SpecBuilders.arrayOf(SpecBuilders.object("Value"), "One column of values"),
        "Column-major values, one array per field"));

    final Schema<Object> frame = SpecBuilders.object("One DataFrame");
    frame.addProperty("schema", frameSchema);
    frame.addProperty("data", frameData);

    final Schema<Object> perTarget = SpecBuilders.object("Result for one target");
    perTarget.addProperty("frames", SpecBuilders.arrayOf(frame, "Frames produced by the target"));

    final Schema<Object> results = SpecBuilders.object("Results keyed by target refId");
    results.setAdditionalProperties(perTarget);

    final Schema<Object> schema = SpecBuilders.object("Grafana DataFrame response");
    schema.addProperty("results", results);
    return schema;
  }
}
```

- [ ] **Step 4: Run to verify it passes**

```bash
mvn -q -pl server -am -Dtest=GrafanaApiSpecTest test
```

Expected: PASS, 5 tests.

- [ ] **Step 5: Prove the test can fail**

Temporarily replace `results.setAdditionalProperties(perTarget);` with nothing. Expected: `queryResponseIsTheGrafanaDataFrameEnvelopeNotSimpleJson` FAILS on the null `additionalProperties`. Revert.

- [ ] **Step 6: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/openapi/GrafanaApiSpec.java \
        server/src/test/java/com/arcadedb/server/http/handler/openapi/GrafanaApiSpecTest.java
git commit -m "feat(server) #4895: document the Grafana data source operations

The query response is the Grafana DataFrame envelope, keyed by target refId and
carrying column-major values, not the SimpleJSON contract."
```

---

### Task 7: Prometheus remote read/write and the PromQL API

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/http/handler/openapi/PrometheusApiSpec.java`
- Test: `server/src/test/java/com/arcadedb/server/http/handler/openapi/PrometheusApiSpecTest.java`

**Interfaces:**
- Consumes: `SpecBuilders`, `OpenApiContributor` from Task 1.
- Produces: `PrometheusApiSpec implements OpenApiContributor`; schemas `PromQLDataResponse`, `PromQLLabelsResponse`, `PromQLSeriesResponse`, `PromQLErrorResponse`; 7 path items. Remote read/write are tagged `Prometheus`; the 5 PromQL operations are tagged `PromQL`.

- [ ] **Step 1: Write the failing test**

```java
class PrometheusApiSpecTest {
  private static final String BASE = "/api/v1/ts/{database}/prom";

  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new PrometheusApiSpec().contribute(openAPI);
  }

  @Test
  void remoteWriteAndReadDeclareProtobufBinaryBodies() {
    for (final String suffix : List.of("write", "read")) {
      final Operation post = openAPI.getPaths().get(BASE + "/" + suffix).getPost();
      assertThat(post.getRequestBody().getContent())
          .as("%s", suffix).containsKey("application/x-protobuf");
      assertThat(post.getRequestBody().getContent().get("application/x-protobuf")
          .getSchema().getFormat()).as("%s", suffix).isEqualTo("binary");
      assertThat(post.getTags()).as("%s", suffix).containsExactly("Prometheus");
    }
  }

  @Test
  void remoteWriteSucceedsWith204() {
    final Operation post = openAPI.getPaths().get(BASE + "/write").getPost();
    assertThat(post.getOperationId()).isEqualTo("prometheusRemoteWrite");
    assertThat(post.getResponses()).containsKey("204");
    assertThat(post.getResponses().get("204").getContent()).isNull();
  }

  @Test
  void remoteReadReturnsProtobuf() {
    final Operation post = openAPI.getPaths().get(BASE + "/read").getPost();
    assertThat(post.getOperationId()).isEqualTo("prometheusRemoteRead");
    assertThat(post.getResponses().get("200").getContent())
        .as("the read response is a protobuf ReadResponse, not JSON")
        .containsKey("application/x-protobuf");
  }

  @Test
  void instantQueryDeclaresItsParameters() {
    final Operation get = openAPI.getPaths().get(BASE + "/api/v1/query").getGet();
    assertThat(get.getOperationId()).isEqualTo("promQLQuery");
    assertThat(get.getTags()).containsExactly("PromQL");
    final Map<String, Boolean> required = get.getParameters().stream()
        .collect(Collectors.toMap(Parameter::getName, Parameter::getRequired));
    assertThat(required)
        .containsEntry("query", true)
        .containsEntry("time", false)
        .containsEntry("lookback_delta", false);
  }

  @Test
  void rangeQueryRequiresStartEndAndStep() {
    final Operation get = openAPI.getPaths().get(BASE + "/api/v1/query_range").getGet();
    assertThat(get.getOperationId()).isEqualTo("promQLQueryRange");
    final Map<String, Boolean> required = get.getParameters().stream()
        .collect(Collectors.toMap(Parameter::getName, Parameter::getRequired));
    assertThat(required)
        .containsEntry("query", true)
        .containsEntry("start", true)
        .containsEntry("end", true)
        .containsEntry("step", true)
        .containsEntry("lookback_delta", false);
  }

  @Test
  void labelsAndLabelValuesShareTheStringDataEnvelope() {
    assertThat(openAPI.getPaths().get(BASE + "/api/v1/labels").getGet().getOperationId())
        .isEqualTo("promQLLabels");
    final Operation values = openAPI.getPaths()
        .get(BASE + "/api/v1/label/{name}/values").getGet();
    assertThat(values.getOperationId()).isEqualTo("promQLLabelValues");
    assertThat(values.getParameters().stream().map(Parameter::getName).toList())
        .contains("database", "name");

    for (final String path : List.of(BASE + "/api/v1/labels", BASE + "/api/v1/label/{name}/values")) {
      assertThat(openAPI.getPaths().get(path).getGet().getResponses().get("200")
          .getContent().get("application/json").getSchema().get$ref())
          .as("%s", path).isEqualTo("#/components/schemas/PromQLLabelsResponse");
    }

    final Schema<?> schema = openAPI.getComponents().getSchemas().get("PromQLLabelsResponse");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder("status", "data");
    assertThat(schema.getProperties().get("data").getItems().getType()).isEqualTo("string");
  }

  @Test
  void seriesRequiresMatchAndReturnsLabelMaps() {
    final Operation get = openAPI.getPaths().get(BASE + "/api/v1/series").getGet();
    assertThat(get.getOperationId()).isEqualTo("promQLSeries");
    final Parameter match = get.getParameters().stream()
        .filter(p -> "match[]".equals(p.getName())).findFirst().orElseThrow();
    assertThat(match.getRequired()).isTrue();

    final Schema<?> schema = openAPI.getComponents().getSchemas().get("PromQLSeriesResponse");
    assertThat(schema.getProperties().get("data").getItems().getType()).isEqualTo("object");
  }

  @Test
  void dataResponseCarriesResultTypeAndResult() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("PromQLDataResponse");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder("status", "data");
    assertThat(schema.getProperties().get("data").getProperties().keySet())
        .containsExactlyInAnyOrder("resultType", "result");
    assertThat(schema.getProperties().get("data").getProperties().get("resultType").getEnum())
        .containsExactlyInAnyOrder("vector", "matrix", "scalar");
  }

  @Test
  void everyPromQlOperationUsesTheErrorEnvelopeNotTheGenericOne() {
    final Schema<?> error = openAPI.getComponents().getSchemas().get("PromQLErrorResponse");
    assertThat(error.getProperties().keySet())
        .containsExactlyInAnyOrder("status", "errorType", "error");

    for (final String path : List.of(BASE + "/api/v1/query", BASE + "/api/v1/query_range",
        BASE + "/api/v1/labels", BASE + "/api/v1/label/{name}/values", BASE + "/api/v1/series")) {
      assertThat(openAPI.getPaths().get(path).getGet().getResponses().get("400")
          .getContent().get("application/json").getSchema().get$ref())
          .as("%s must report errors in the Prometheus envelope", path)
          .isEqualTo("#/components/schemas/PromQLErrorResponse");
    }
  }
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
mvn -q -pl server -am -Dtest=PrometheusApiSpecTest test
```

Expected: compilation failure, `cannot find symbol: class PrometheusApiSpec`.

- [ ] **Step 3: Create `PrometheusApiSpec`**

```java
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.util.List;

/**
 * Documents the Prometheus remote read and write endpoints and the Prometheus-compatible query API.
 * <p>
 * Remote read and write exchange Snappy-compressed protobuf messages, which no JSON schema can
 * describe, so their bodies are declared as opaque binary and the framing lives in the description.
 * The query API answers in the Prometheus HTTP API envelope, which takes four distinct shapes.
 */
public class PrometheusApiSpec implements OpenApiContributor {

  private static final String BASE     = "/api/v1/ts/{database}/prom";
  private static final String PROTOBUF = "application/x-protobuf";

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem(BASE + "/write", createRemoteWritePath());
    openAPI.getPaths().addPathItem(BASE + "/read", createRemoteReadPath());
    openAPI.getPaths().addPathItem(BASE + "/api/v1/query", createInstantQueryPath());
    openAPI.getPaths().addPathItem(BASE + "/api/v1/query_range", createRangeQueryPath());
    openAPI.getPaths().addPathItem(BASE + "/api/v1/labels", createLabelsPath());
    openAPI.getPaths().addPathItem(BASE + "/api/v1/label/{name}/values", createLabelValuesPath());
    openAPI.getPaths().addPathItem(BASE + "/api/v1/series", createSeriesPath());

    openAPI.getComponents().addSchemas("PromQLDataResponse", createDataResponseSchema());
    openAPI.getComponents().addSchemas("PromQLLabelsResponse", createLabelsResponseSchema());
    openAPI.getComponents().addSchemas("PromQLSeriesResponse", createSeriesResponseSchema());
    openAPI.getComponents().addSchemas("PromQLErrorResponse", createErrorResponseSchema());
  }

  private PathItem createRemoteWritePath() {
    final Operation post = SpecBuilders.operation("prometheusRemoteWrite", "Prometheus",
        "Ingest samples through Prometheus remote write",
        """
            Accepts a Prometheus remote-write request: a protobuf WriteRequest message compressed \
            with Snappy block format. Configure this endpoint as a remote_write target in \
            prometheus.yml. Answers 204 with no body once the samples are applied.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setRequestBody(SpecBuilders.rawBody(
        "Snappy-compressed protobuf WriteRequest, per the Prometheus remote-write specification",
        PROTOBUF, "binary"));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("204", SpecBuilders.emptyResponse("Samples ingested"));
    responses.addApiResponse("400", SpecBuilders.errorResponse(
        "Bad request: body empty, not Snappy-compressed, or not a valid WriteRequest"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createRemoteReadPath() {
    final Operation post = SpecBuilders.operation("prometheusRemoteRead", "Prometheus",
        "Read samples through Prometheus remote read",
        """
            Accepts a Prometheus remote-read request: a protobuf ReadRequest message compressed with \
            Snappy block format. Answers with a Snappy-compressed protobuf ReadResponse. Configure \
            this endpoint as a remote_read target in prometheus.yml.""");
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setRequestBody(SpecBuilders.rawBody(
        "Snappy-compressed protobuf ReadRequest, per the Prometheus remote-read specification",
        PROTOBUF, "binary"));

    final ApiResponse success = new ApiResponse();
    success.setDescription("Snappy-compressed protobuf ReadResponse");
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(new Schema<>().type("string").format("binary"));
    success.setContent(new Content().addMediaType(PROTOBUF, mediaType));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", success);
    responses.addApiResponse("400", SpecBuilders.errorResponse(
        "Bad request: body empty, not Snappy-compressed, or not a valid ReadRequest"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createInstantQueryPath() {
    final Operation get = SpecBuilders.operation("promQLQuery", "PromQL",
        "Evaluate a PromQL expression at a single instant",
        """
            Evaluates a PromQL expression at one point in time. Compatible with the Prometheus \
            /api/v1/query endpoint, so Grafana's Prometheus data source and promtool can target it \
            directly.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.addParametersItem(SpecBuilders.queryParam("query", "PromQL expression", true));
    get.addParametersItem(SpecBuilders.queryParam("time",
        "Evaluation instant as an RFC 3339 timestamp or a Unix timestamp. Defaults to now.", false));
    get.addParametersItem(SpecBuilders.queryParam("lookback_delta",
        "How far back to look for a sample, as a duration. Defaults to the server setting.", false));
    get.setResponses(promQlResponses("Evaluation result", "PromQLDataResponse"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createRangeQueryPath() {
    final Operation get = SpecBuilders.operation("promQLQueryRange", "PromQL",
        "Evaluate a PromQL expression over a time range",
        """
            Evaluates a PromQL expression at every step across a range. Compatible with the \
            Prometheus /api/v1/query_range endpoint. 'step' must be positive.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.addParametersItem(SpecBuilders.queryParam("query", "PromQL expression", true));
    get.addParametersItem(SpecBuilders.queryParam("start",
        "Inclusive range start as an RFC 3339 or Unix timestamp", true));
    get.addParametersItem(SpecBuilders.queryParam("end",
        "Inclusive range end as an RFC 3339 or Unix timestamp", true));
    get.addParametersItem(SpecBuilders.queryParam("step",
        "Evaluation interval as a duration or a number of seconds. Must be positive.", true));
    get.addParametersItem(SpecBuilders.queryParam("lookback_delta",
        "How far back to look for a sample, as a duration. Defaults to the server setting.", false));
    get.setResponses(promQlResponses("Evaluation result", "PromQLDataResponse"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createLabelsPath() {
    final Operation get = SpecBuilders.operation("promQLLabels", "PromQL",
        "List label names",
        "Lists every label name present in the database, sorted. Compatible with the Prometheus /api/v1/labels endpoint.");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.setResponses(promQlResponses("Sorted label names", "PromQLLabelsResponse"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createLabelValuesPath() {
    final Operation get = SpecBuilders.operation("promQLLabelValues", "PromQL",
        "List the values of one label",
        "Lists every value of one label name, sorted. Compatible with the Prometheus /api/v1/label/{name}/values endpoint.");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    get.addParametersItem(SpecBuilders.pathParam("name", "Label name"));
    get.setResponses(promQlResponses("Sorted label values", "PromQLLabelsResponse"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createSeriesPath() {
    final Operation get = SpecBuilders.operation("promQLSeries", "PromQL",
        "Find series matching selectors",
        """
            Returns the label sets of the series matching the given selectors. Compatible with the \
            Prometheus /api/v1/series endpoint. Each returned object is a label map including the \
            '__name__' label.""");
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    final Parameter match = SpecBuilders.queryParam("match[]",
        "Series selector, repeatable to union several selectors", true);
    get.addParametersItem(match);
    get.addParametersItem(SpecBuilders.queryParam("start",
        "Inclusive range start as an RFC 3339 or Unix timestamp", false));
    get.addParametersItem(SpecBuilders.queryParam("end",
        "Inclusive range end as an RFC 3339 or Unix timestamp", false));
    get.setResponses(promQlResponses("Matching series label sets", "PromQLSeriesResponse"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  /**
   * The Prometheus API reports failures in its own envelope rather than the ArcadeDB error body, so
   * a client written against the Prometheus API can parse both outcomes with one reader.
   */
  private ApiResponses promQlResponses(final String successDescription, final String successSchema) {
    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse(successDescription, successSchema));
    responses.addApiResponse("400",
        SpecBuilders.jsonResponse("Bad request, in the Prometheus error envelope", "PromQLErrorResponse"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    return responses;
  }

  private Schema<?> createDataResponseSchema() {
    final Schema<String> resultType = SpecBuilders.string(
        "Shape of 'result': a vector of instant samples, a matrix of range samples, or a scalar");
    resultType.setEnum(List.of("vector", "matrix", "scalar"));

    final Schema<Object> data = SpecBuilders.object("Evaluation result");
    data.addProperty("resultType", resultType);
    data.addProperty("result", SpecBuilders.arrayOf(
        SpecBuilders.object(
            "A vector entry carries 'metric' and a single 'value'; a matrix entry carries 'metric' and 'values'"),
        "Result entries. A scalar result is a two-element array of timestamp and value."));

    final Schema<Object> schema = SpecBuilders.object("Prometheus query response");
    schema.addProperty("status", SpecBuilders.string("Always 'success' on a 200"));
    schema.addProperty("data", data);
    return schema;
  }

  private Schema<?> createLabelsResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Prometheus label response");
    schema.addProperty("status", SpecBuilders.string("Always 'success' on a 200"));
    schema.addProperty("data", SpecBuilders.arrayOf(
        SpecBuilders.string("Label name or value"), "Sorted names or values"));
    return schema;
  }

  private Schema<?> createSeriesResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Prometheus series response");
    schema.addProperty("status", SpecBuilders.string("Always 'success' on a 200"));
    schema.addProperty("data", SpecBuilders.arrayOf(
        SpecBuilders.object("One series as a label map, including the '__name__' label"),
        "Matching series"));
    return schema;
  }

  private Schema<?> createErrorResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Prometheus error envelope");
    schema.addProperty("status", SpecBuilders.string("Always 'error'"));
    schema.addProperty("errorType", SpecBuilders.string(
        "Prometheus error class, for example 'bad_data'"));
    schema.addProperty("error", SpecBuilders.string("Human-readable message"));
    return schema;
  }
}
```

- [ ] **Step 4: Run to verify it passes**

```bash
mvn -q -pl server -am -Dtest=PrometheusApiSpecTest test
```

Expected: PASS, 9 tests.

- [ ] **Step 5: Prove the test can fail**

Temporarily change `promQlResponses` to map `400` to `SpecBuilders.errorResponse("Bad request")`. Expected: `everyPromQlOperationUsesTheErrorEnvelopeNotTheGenericOne` FAILS for all five paths. Revert.

- [ ] **Step 6: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/openapi/PrometheusApiSpec.java \
        server/src/test/java/com/arcadedb/server/http/handler/openapi/PrometheusApiSpecTest.java
git commit -m "feat(server) #4895: document Prometheus remote read/write and the PromQL query API

Remote read and write exchange Snappy-compressed protobuf, so their bodies are
declared as opaque binary with the framing in the description. The query API
reports failures in the Prometheus error envelope rather than the ArcadeDB error
body, so its 400 responses reference PromQLErrorResponse."
```

---

### Task 8: AI assistant operations

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/http/handler/openapi/AiApiSpec.java`
- Test: `server/src/test/java/com/arcadedb/server/http/handler/openapi/AiApiSpecTest.java`

**Interfaces:**
- Consumes: `SpecBuilders`, `OpenApiContributor` from Task 1.
- Produces: `AiApiSpec implements OpenApiContributor`; schemas `AiConfig`, `AiActivateRequest`, `AiActivateResponse`, `AiChatRequest`, `AiChatResponse`, `AiProtocolError`, `AiAnalyzeProfilerRequest`, `AiAnalyzeProfilerResponse`, `AiChatList`, `AiChat`, `AiChatDeleted`; path items `/api/v1/ai/config` (`getAiConfig`), `/ai/activate` (`activateAi`), `/ai/chat` (`chatWithAi`), `/ai/analyze-profiler` (`analyzeProfilerWithAi`), `/ai/chats` (`listAiChats`), `/ai/chats/{id}` (`getAiChat`, `updateAiChat`, `deleteAiChat`), all tagged `AI`.

- [ ] **Step 1: Write the failing test**

```java
class AiApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new AiApiSpec().contribute(openAPI);
  }

  @Test
  void allEightOperationsArePresentAndTaggedAi() {
    final Map<String, String> expected = Map.of(
        "/api/v1/ai/config", "getAiConfig",
        "/api/v1/ai/activate", "activateAi",
        "/api/v1/ai/chat", "chatWithAi",
        "/api/v1/ai/analyze-profiler", "analyzeProfilerWithAi",
        "/api/v1/ai/chats", "listAiChats");
    expected.forEach((path, operationId) -> {
      final PathItem item = openAPI.getPaths().get(path);
      final Operation op = item.getGet() != null ? item.getGet() : item.getPost();
      assertThat(op.getOperationId()).as("%s", path).isEqualTo(operationId);
      assertThat(op.getTags()).as("%s", path).containsExactly("AI");
    });

    final PathItem chat = openAPI.getPaths().get("/api/v1/ai/chats/{id}");
    assertThat(chat.getGet().getOperationId()).isEqualTo("getAiChat");
    assertThat(chat.getPut().getOperationId()).isEqualTo("updateAiChat");
    assertThat(chat.getDelete().getOperationId()).isEqualTo("deleteAiChat");
    assertThat(chat.getPost()).as("the chats resource exposes no POST").isNull();
  }

  @Test
  void configReportsProtocolNegotiationFields() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AiConfig");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "configured", "gatewayUrl", "currentProtocolVersion", "supportedProtocolVersions");
  }

  @Test
  void activateRequiresASubscriptionKey() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AiActivateRequest");
    assertThat(schema.getRequired()).containsExactly("subscriptionKey");
  }

  @Test
  void chatRequiresDatabaseAndMessage() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AiChatRequest");
    assertThat(schema.getRequired()).containsExactlyInAnyOrder("database", "message");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "database", "message", "chatId", "mode", "protocolVersion");
  }

  @Test
  void chatResponseCarriesChatIdAndOptionalCommands() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AiChatResponse");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "chatId", "response", "commands", "toolCalls");
  }

  @Test
  void chatDeclaresTheGatewayFailureStatuses() {
    final Operation post = openAPI.getPaths().get("/api/v1/ai/chat").getPost();
    assertThat(post.getResponses().keySet())
        .as("the gateway is a remote dependency, so 503 and 504 are part of the contract")
        .contains("200", "400", "403", "404", "503", "504");
  }

  @Test
  void protocolMismatchIsItsOwnSchemaNotTheGenericError() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AiProtocolError");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "error", "code", "currentProtocolVersion", "supportedProtocolVersions");
    assertThat(openAPI.getPaths().get("/api/v1/ai/chat").getPost().getResponses().get("400")
        .getContent().get("application/json").getSchema().get$ref())
        .isEqualTo("#/components/schemas/AiProtocolError");
  }

  @Test
  void analyzeProfilerRequiresProfilerData() {
    final Schema<?> request = openAPI.getComponents().getSchemas()
        .get("AiAnalyzeProfilerRequest");
    assertThat(request.getRequired()).containsExactly("profilerData");
    assertThat(request.getProperties().keySet())
        .containsExactlyInAnyOrder("profilerData", "schemas");
    assertThat(openAPI.getComponents().getSchemas().get("AiAnalyzeProfilerResponse")
        .getProperties().keySet()).containsExactlyInAnyOrder("response", "commands");
  }

  @Test
  void chatsByIdTakeThePathParameter() {
    final PathItem item = openAPI.getPaths().get("/api/v1/ai/chats/{id}");
    for (final Operation op : item.readOperations()) {
      assertThat(op.getParameters().stream().map(Parameter::getName).toList())
          .as("%s", op.getOperationId()).contains("id");
    }
  }

  @Test
  void deleteAnswersADeletedFlag() {
    assertThat(openAPI.getComponents().getSchemas().get("AiChatDeleted")
        .getProperties().keySet()).containsExactly("deleted");
  }
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
mvn -q -pl server -am -Dtest=AiApiSpecTest test
```

Expected: compilation failure, `cannot find symbol: class AiApiSpec`.

- [ ] **Step 3: Create `AiApiSpec`**

```java
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.util.List;

/**
 * Documents the AI assistant endpoints. Chat and profiler analysis proxy a remote gateway, so their
 * contract includes the gateway's unreachable and timeout outcomes as well as the local validation
 * failures. Chat additionally negotiates a protocol version and reports a mismatch in its own body
 * shape, carrying the versions the server accepts.
 */
public class AiApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/ai/config", createConfigPath());
    openAPI.getPaths().addPathItem("/api/v1/ai/activate", createActivatePath());
    openAPI.getPaths().addPathItem("/api/v1/ai/chat", createChatPath());
    openAPI.getPaths().addPathItem("/api/v1/ai/analyze-profiler", createAnalyzeProfilerPath());
    openAPI.getPaths().addPathItem("/api/v1/ai/chats", createChatsPath());
    openAPI.getPaths().addPathItem("/api/v1/ai/chats/{id}", createChatByIdPath());

    openAPI.getComponents().addSchemas("AiConfig", createConfigSchema());
    openAPI.getComponents().addSchemas("AiActivateRequest", createActivateRequestSchema());
    openAPI.getComponents().addSchemas("AiActivateResponse", createActivateResponseSchema());
    openAPI.getComponents().addSchemas("AiChatRequest", createChatRequestSchema());
    openAPI.getComponents().addSchemas("AiChatResponse", createChatResponseSchema());
    openAPI.getComponents().addSchemas("AiProtocolError", createProtocolErrorSchema());
    openAPI.getComponents().addSchemas("AiAnalyzeProfilerRequest", createAnalyzeProfilerRequestSchema());
    openAPI.getComponents().addSchemas("AiAnalyzeProfilerResponse", createAnalyzeProfilerResponseSchema());
    openAPI.getComponents().addSchemas("AiChatList", createChatListSchema());
    openAPI.getComponents().addSchemas("AiChat", createChatSchema());
    openAPI.getComponents().addSchemas("AiChatDeleted", createChatDeletedSchema());
  }

  private PathItem createConfigPath() {
    final Operation get = SpecBuilders.operation("getAiConfig", "AI",
        "Read the AI assistant configuration",
        """
            Reports whether the AI assistant is configured and which protocol versions this server \
            speaks. A client reads 'currentProtocolVersion' at start-up and either matches it or \
            picks the highest version it shares with 'supportedProtocolVersions'.""");
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Assistant configuration", "AiConfig"),
        "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createActivatePath() {
    final Operation post = SpecBuilders.operation("activateAi", "AI",
        "Activate the AI assistant with a subscription key",
        """
            Exchanges a subscription key for an activation held by the server. The server sends its \
            version and a derived hardware id to the gateway as part of the exchange.""");
    post.setRequestBody(SpecBuilders.jsonBody("Subscription key", "AiActivateRequest", true));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Assistant activated", "AiActivateResponse"),
        "400", "401", "403", "500", "503"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createChatPath() {
    final Operation post = SpecBuilders.operation("chatWithAi", "AI",
        "Send a message to the AI assistant",
        """
            Sends one message in the context of a database, optionally continuing an existing chat by \
            'chatId'. The server supplies the database schema and, in the richer modes, server \
            metrics to the gateway. The reply may carry SQL commands the assistant proposes and the \
            tool calls it made.

            The assistant is a remote dependency: 503 means the gateway was unreachable and 504 that \
            it did not answer in time. Both are retryable.""");
    post.setRequestBody(SpecBuilders.jsonBody("Chat message", "AiChatRequest", true));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse("Assistant reply", "AiChatResponse"));
    responses.addApiResponse("400", SpecBuilders.jsonResponse(
        """
            Bad request. When the requested protocol version is unsupported the body carries 'code' \
            set to 'protocol_unsupported' plus the versions this server accepts.""",
        "AiProtocolError"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse(
        "Forbidden: the user cannot access the requested database"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Chat not found"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    responses.addApiResponse("503", SpecBuilders.errorResponse(
        "AI gateway unreachable, reported with code 'gateway_unreachable'"));
    responses.addApiResponse("504", SpecBuilders.errorResponse(
        "AI gateway timed out, reported with code 'gateway_timeout'"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createAnalyzeProfilerPath() {
    final Operation post = SpecBuilders.operation("analyzeProfilerWithAi", "AI",
        "Analyse a profiler snapshot",
        """
            Submits a profiler snapshot, optionally with database schemas, and returns the \
            assistant's analysis plus any SQL commands it proposes.""");
    post.setRequestBody(SpecBuilders.jsonBody(
        "Profiler snapshot", "AiAnalyzeProfilerRequest", true));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", SpecBuilders.jsonResponse(
        "Analysis", "AiAnalyzeProfilerResponse"));
    responses.addApiResponse("400", SpecBuilders.errorResponse(
        "Bad request, or the assistant is not configured"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("403", SpecBuilders.errorResponse("Forbidden"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    responses.addApiResponse("503", SpecBuilders.errorResponse(
        "AI gateway unreachable, reported with code 'gateway_unreachable'"));
    responses.addApiResponse("504", SpecBuilders.errorResponse(
        "AI gateway timed out, reported with code 'gateway_timeout'"));
    post.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createChatsPath() {
    final Operation get = SpecBuilders.operation("listAiChats", "AI",
        "List stored chats",
        "Lists the chat transcripts this server has stored.");
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Stored chats", "AiChatList"),
        "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createChatByIdPath() {
    final PathItem pathItem = new PathItem();

    final Operation get = SpecBuilders.operation("getAiChat", "AI",
        "Read one stored chat",
        "Returns one stored chat transcript with its messages.");
    get.addParametersItem(SpecBuilders.pathParam("id", "Chat identifier"));
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Chat transcript", "AiChat"),
        "401", "403", "404", "500"));
    pathItem.setGet(get);

    final Operation put = SpecBuilders.operation("updateAiChat", "AI",
        "Replace the messages of a stored chat",
        "Replaces the message list of a stored chat and stamps its update time.");
    put.addParametersItem(SpecBuilders.pathParam("id", "Chat identifier"));
    put.setRequestBody(SpecBuilders.jsonBody(
        "Replacement messages", "AiChat", true));
    put.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Updated chat", "AiChat"),
        "400", "401", "403", "404", "500"));
    pathItem.setPut(put);

    final Operation delete = SpecBuilders.operation("deleteAiChat", "AI",
        "Delete a stored chat",
        "Deletes one stored chat transcript.");
    delete.addParametersItem(SpecBuilders.pathParam("id", "Chat identifier"));
    delete.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Chat deleted", "AiChatDeleted"),
        "401", "403", "404", "500"));
    pathItem.setDelete(delete);

    return pathItem;
  }

  private Schema<?> createConfigSchema() {
    final Schema<Object> schema = SpecBuilders.object("AI assistant configuration");
    schema.addProperty("configured", SpecBuilders.bool(
        "True once a subscription has been activated"));
    schema.addProperty("gatewayUrl", SpecBuilders.string("AI gateway endpoint"));
    schema.addProperty("currentProtocolVersion", SpecBuilders.integer(
        "Protocol version this server prefers"));
    schema.addProperty("supportedProtocolVersions", SpecBuilders.arrayOf(
        SpecBuilders.integer("Protocol version"), "Every version this server accepts"));
    return schema;
  }

  private Schema<?> createActivateRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Activation request");
    schema.addProperty("subscriptionKey", SpecBuilders.string("Subscription key"));
    schema.setRequired(List.of("subscriptionKey"));
    return schema;
  }

  private Schema<?> createActivateResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Activation result");
    schema.addProperty("activated", SpecBuilders.bool("Always true on a 200"));
    return schema;
  }

  private Schema<?> createChatRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Chat message");
    schema.addProperty("database", SpecBuilders.string(
        "Database the question is about. The caller must be authorized on it."));
    schema.addProperty("message", SpecBuilders.string("User message"));
    schema.addProperty("chatId", SpecBuilders.string(
        "Existing chat to continue. A new chat is created when omitted."));
    schema.addProperty("mode", SpecBuilders.string(
        "How much context to send to the gateway. Defaults to 'auto'."));
    schema.addProperty("protocolVersion", SpecBuilders.integer(
        "Protocol version the client speaks. Rejected with 'protocol_unsupported' when unknown."));
    schema.setRequired(List.of("database", "message"));
    return schema;
  }

  private Schema<?> createChatResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Assistant reply");
    schema.addProperty("chatId", SpecBuilders.string(
        "Chat this exchange belongs to, for continuing the conversation"));
    schema.addProperty("response", SpecBuilders.string("Assistant message"));
    schema.addProperty("commands", SpecBuilders.arrayOf(
        SpecBuilders.object("A proposed command"),
        "SQL commands the assistant proposes. Absent when it proposes none."));
    schema.addProperty("toolCalls", SpecBuilders.arrayOf(
        SpecBuilders.object("A tool invocation"),
        "Tools the assistant invoked while answering. Absent when it invoked none."));
    return schema;
  }

  private Schema<?> createProtocolErrorSchema() {
    final Schema<Object> schema = SpecBuilders.object(
        "Rejected chat request. Carries the negotiation fields when the protocol version is at fault.");
    schema.addProperty("error", SpecBuilders.string("Why the request was rejected"));
    schema.addProperty("code", SpecBuilders.string(
        "Machine-readable cause, 'protocol_unsupported' for a version mismatch"));
    schema.addProperty("currentProtocolVersion", SpecBuilders.integer(
        "Protocol version this server prefers"));
    schema.addProperty("supportedProtocolVersions", SpecBuilders.arrayOf(
        SpecBuilders.integer("Protocol version"), "Every version this server accepts"));
    return schema;
  }

  private Schema<?> createAnalyzeProfilerRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Profiler analysis request");
    schema.addProperty("profilerData", SpecBuilders.object("Profiler snapshot to analyse"));
    schema.addProperty("schemas", SpecBuilders.object(
        "Database schemas to send alongside the snapshot, for a more specific analysis"));
    schema.setRequired(List.of("profilerData"));
    return schema;
  }

  private Schema<?> createAnalyzeProfilerResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Profiler analysis");
    schema.addProperty("response", SpecBuilders.string("Assistant analysis"));
    schema.addProperty("commands", SpecBuilders.arrayOf(
        SpecBuilders.object("A proposed command"),
        "Commands the assistant proposes. Absent when it proposes none."));
    return schema;
  }

  private Schema<?> createChatListSchema() {
    final Schema<Object> schema = SpecBuilders.object("Stored chats");
    schema.addProperty("chats", SpecBuilders.arrayOf(
        SpecBuilders.ref("AiChat"), "Stored chat transcripts"));
    return schema;
  }

  private Schema<?> createChatSchema() {
    final Schema<Object> message = SpecBuilders.object("One chat message");
    message.addProperty("role", SpecBuilders.string("'user' or the assistant role"));
    message.addProperty("content", SpecBuilders.string("Message text"));
    message.addProperty("timestamp", SpecBuilders.string("ISO-8601 instant"));

    final Schema<Object> schema = SpecBuilders.object("One chat transcript");
    schema.addProperty("id", SpecBuilders.string("Chat identifier"));
    schema.addProperty("messages", SpecBuilders.arrayOf(message, "Messages, oldest first"));
    schema.addProperty("updated", SpecBuilders.string("ISO-8601 instant of the last change"));
    return schema;
  }

  private Schema<?> createChatDeletedSchema() {
    final Schema<Object> schema = SpecBuilders.object("Deletion result");
    schema.addProperty("deleted", SpecBuilders.bool("Always true on a 200"));
    return schema;
  }
}
```

- [ ] **Step 4: Run to verify it passes**

```bash
mvn -q -pl server -am -Dtest=AiApiSpecTest test
```

Expected: PASS, 10 tests.

- [ ] **Step 5: Prove the test can fail**

Temporarily map the chat `400` to `SpecBuilders.errorResponse("Bad request")`. Expected: `protocolMismatchIsItsOwnSchemaNotTheGenericError` FAILS. Revert.

- [ ] **Step 6: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/openapi/AiApiSpec.java \
        server/src/test/java/com/arcadedb/server/http/handler/openapi/AiApiSpecTest.java
git commit -m "feat(server) #4895: document the AI assistant operations

Chat and profiler analysis proxy a remote gateway, so 503 unreachable and 504
timeout are part of their declared contract. A chat protocol mismatch answers 400
with the accepted versions, which is a different body from the generic error."
```

---

### Task 9: MCP operations

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/http/handler/openapi/McpApiSpec.java`
- Test: `server/src/test/java/com/arcadedb/server/http/handler/openapi/McpApiSpecTest.java`

**Interfaces:**
- Consumes: `SpecBuilders`, `OpenApiContributor` from Task 1.
- Produces: `McpApiSpec implements OpenApiContributor`; schemas `McpConfig`, `McpDatabaseOverride`; path items `/api/v1/mcp` (`invokeMcp`, POST only) and `/api/v1/mcp/config` (`getMcpConfig`, `updateMcpConfig`), tagged `MCP`.

- [ ] **Step 1: Write the failing test**

```java
class McpApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new McpApiSpec().contribute(openAPI);
  }

  @Test
  void mcpEndpointExposesPostOnly() {
    final PathItem item = openAPI.getPaths().get("/api/v1/mcp");
    assertThat(item.getPost().getOperationId()).isEqualTo("invokeMcp");
    assertThat(item.getPost().getTags()).containsExactly("MCP");
    assertThat(item.getGet()).as("the MCP handler rejects every method but POST").isNull();
    assertThat(item.getDelete()).isNull();
    assertThat(item.getPut()).isNull();
  }

  @Test
  void configExposesGetAndPostButNoOtherMethod() {
    final PathItem item = openAPI.getPaths().get("/api/v1/mcp/config");
    assertThat(item.getGet().getOperationId()).isEqualTo("getMcpConfig");
    assertThat(item.getPost().getOperationId()).isEqualTo("updateMcpConfig");
    assertThat(item.getPut()).isNull();
    assertThat(item.getDelete()).isNull();
  }

  @Test
  void configSchemaCarriesEveryPermissionFlagAndTheProfile() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("McpConfig");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "enabled", "allowReads", "allowInsert", "allowUpdate", "allowDelete",
        "allowSchemaChange", "allowAdmin", "profile", "allowedUsers", "allowedOrigins",
        "principalProfiles", "databases");
  }

  // As in GrafanaApiSpecTest: if the compiler rejects getAdditionalPropertiesSchema(), cast
  // getAdditionalProperties() to Schema<?> instead. The accessor name varies across swagger-models
  // 2.2.x patch releases.
  @Test
  void databaseOverridesAreKeyedByDatabaseName() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("McpConfig");
    assertThat(schema.getProperties().get("databases").getAdditionalPropertiesSchema())
        .as("databases is keyed by database name")
        .isNotNull();
    assertThat(openAPI.getComponents().getSchemas().get("McpDatabaseOverride")
        .getProperties().keySet()).containsExactlyInAnyOrder(
        "allowReads", "allowInsert", "allowUpdate", "allowDelete",
        "allowSchemaChange", "allowAdmin", "allowedUsers");
  }

  @Test
  void configUpdateIsPartialAndAllOrNothing() {
    final Operation post = openAPI.getPaths().get("/api/v1/mcp/config").getPost();
    assertThat(post.getDescription())
        .as("a rejected field must leave the whole configuration untouched, and the description must say so")
        .contains("partial")
        .contains("rejected");
    assertThat(post.getResponses()).containsKeys("200", "400");
  }

  @Test
  void bothPathsDeclare405() {
    for (final String path : List.of("/api/v1/mcp", "/api/v1/mcp/config")) {
      final PathItem item = openAPI.getPaths().get(path);
      assertThat(item.readOperations()).allSatisfy(op ->
          assertThat(op.getResponses()).as("%s %s", path, op.getOperationId()).containsKey("405"));
    }
  }
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
mvn -q -pl server -am -Dtest=McpApiSpecTest test
```

Expected: compilation failure, `cannot find symbol: class McpApiSpec`.

- [ ] **Step 3: Create `McpApiSpec`**

```java
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;

/**
 * Documents the Model Context Protocol endpoint and its configuration. The protocol endpoint carries
 * JSON-RPC 2.0 envelopes whose method set belongs to the MCP specification rather than to this API,
 * so its bodies stay opaque. The configuration resource is fully modelled, because a client edits it
 * field by field.
 */
public class McpApiSpec implements OpenApiContributor {

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/api/v1/mcp", createMcpPath());
    openAPI.getPaths().addPathItem("/api/v1/mcp/config", createConfigPath());

    openAPI.getComponents().addSchemas("McpConfig", createConfigSchema());
    openAPI.getComponents().addSchemas("McpDatabaseOverride", createDatabaseOverrideSchema());
  }

  private PathItem createMcpPath() {
    final Operation post = SpecBuilders.operation("invokeMcp", "MCP",
        "Exchange a JSON-RPC message with the MCP server",
        """
            Accepts a single JSON-RPC 2.0 request and answers with the corresponding response. The \
            method set, parameter shapes, and result shapes are defined by the Model Context Protocol \
            specification, not by this API, so they are not enumerated here.

            The route is always registered. When the MCP server is disabled the request is refused at \
            request time, which is what makes runtime toggling possible without restarting.""");
    post.setRequestBody(SpecBuilders.jsonBody("JSON-RPC 2.0 request", null, true));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("JSON-RPC 2.0 response", null),
        "400", "401", "403", "405", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createConfigPath() {
    final PathItem pathItem = new PathItem();

    final Operation get = SpecBuilders.operation("getMcpConfig", "MCP",
        "Read the MCP server configuration",
        "Returns the MCP server's enablement, permission flags, tool profile, and access lists.");
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Current configuration", "McpConfig"),
        "401", "403", "405", "500"));
    pathItem.setGet(get);

    final Operation post = SpecBuilders.operation("updateMcpConfig", "MCP",
        "Update the MCP server configuration",
        """
            Applies a partial configuration update: send only the fields to change. The update is \
            all-or-nothing. Every field is parsed and validated before the first one is assigned, so \
            a payload rejected on any field leaves the configuration exactly as it was, and a 400 \
            never leaves a partially applied prefix behind.

            Answers with the full configuration as it stands after the update.""");
    post.setRequestBody(SpecBuilders.jsonBody(
        "Partial configuration. Omitted fields keep their current value.", "McpConfig", true));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Configuration after the update", "McpConfig"),
        "400", "401", "403", "405", "500"));
    pathItem.setPost(post);

    return pathItem;
  }

  private Schema<?> createConfigSchema() {
    final Schema<Object> databases = SpecBuilders.object(
        "Per-database overrides, keyed by database name. Absent when none are set.");
    databases.setAdditionalProperties(SpecBuilders.ref("McpDatabaseOverride"));

    final Schema<Object> principalProfiles = SpecBuilders.object(
        "Tool profile per principal, keyed by user name. Absent when none are set.");
    principalProfiles.setAdditionalProperties(SpecBuilders.string("Tool profile name"));

    final Schema<Object> schema = SpecBuilders.object("MCP server configuration");
    schema.addProperty("enabled", SpecBuilders.bool("Whether the MCP server answers requests"));
    schema.addProperty("allowReads", SpecBuilders.bool("Permit read operations"));
    schema.addProperty("allowInsert", SpecBuilders.bool("Permit inserts"));
    schema.addProperty("allowUpdate", SpecBuilders.bool("Permit updates"));
    schema.addProperty("allowDelete", SpecBuilders.bool("Permit deletes"));
    schema.addProperty("allowSchemaChange", SpecBuilders.bool("Permit schema changes"));
    schema.addProperty("allowAdmin", SpecBuilders.bool("Permit administrative operations"));
    schema.addProperty("profile", SpecBuilders.string("Active tool profile name"));
    schema.addProperty("allowedUsers", SpecBuilders.arrayOf(
        SpecBuilders.string("User name"), "Users permitted to reach the MCP server"));
    schema.addProperty("allowedOrigins", SpecBuilders.arrayOf(
        SpecBuilders.string("Origin"), "Origins permitted by the CORS check"));
    schema.addProperty("principalProfiles", principalProfiles);
    schema.addProperty("databases", databases);
    return schema;
  }

  private Schema<?> createDatabaseOverrideSchema() {
    final Schema<Object> schema = SpecBuilders.object(
        "Per-database override. Every field is optional; an omitted field inherits the server-wide value.");
    schema.addProperty("allowReads", SpecBuilders.bool("Permit read operations"));
    schema.addProperty("allowInsert", SpecBuilders.bool("Permit inserts"));
    schema.addProperty("allowUpdate", SpecBuilders.bool("Permit updates"));
    schema.addProperty("allowDelete", SpecBuilders.bool("Permit deletes"));
    schema.addProperty("allowSchemaChange", SpecBuilders.bool("Permit schema changes"));
    schema.addProperty("allowAdmin", SpecBuilders.bool("Permit administrative operations"));
    schema.addProperty("allowedUsers", SpecBuilders.arrayOf(
        SpecBuilders.string("User name"), "Users permitted on this database"));
    return schema;
  }
}
```

- [ ] **Step 4: Run to verify it passes**

```bash
mvn -q -pl server -am -Dtest=McpApiSpecTest test
```

Expected: PASS, 6 tests.

- [ ] **Step 5: Prove the test can fail**

Temporarily add `pathItem.setGet(...)` to `createMcpPath()` reusing the config GET operation. Expected: `mcpEndpointExposesPostOnly` FAILS. Revert.

- [ ] **Step 6: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/openapi/McpApiSpec.java \
        server/src/test/java/com/arcadedb/server/http/handler/openapi/McpApiSpecTest.java
git commit -m "feat(server) #4895: document the MCP endpoint and its configuration resource

The protocol endpoint takes POST only and its JSON-RPC bodies stay opaque,
because the method set belongs to the MCP specification. The configuration
resource is fully modelled, including the per-database and per-principal maps."
```

---

### Task 10: Plugin-contributed operations

**Files:**
- Create: `server/src/main/java/com/arcadedb/server/http/handler/openapi/PluginApiSpec.java`
- Test: `server/src/test/java/com/arcadedb/server/http/handler/openapi/PluginApiSpecTest.java`

**Interfaces:**
- Consumes: `SpecBuilders`, `OpenApiContributor` from Task 1.
- Produces: `PluginApiSpec implements OpenApiContributor`; schemas `ClusterStatus`, `AddPeerRequest`, `TransferLeaderRequest`, `ClusterActionResponse`, `VerifyDatabaseResponse`, `BootstrapStateResponse`; 12 path items. `/prometheus` is tagged `Metrics`; the other 11 are tagged `Cluster`.

- [ ] **Step 1: Write the failing test**

```java
class PluginApiSpecTest {
  private final OpenAPI openAPI = new OpenAPI();

  @BeforeEach
  void contribute() {
    openAPI.setPaths(new Paths());
    openAPI.setComponents(new Components());
    new PluginApiSpec().contribute(openAPI);
  }

  @Test
  void allTwelvePluginOperationsAreDeclared() {
    assertThat(openAPI.getPaths().keySet()).containsExactlyInAnyOrder(
        "/prometheus",
        "/api/v1/cluster",
        "/api/v1/cluster/peer",
        "/api/v1/cluster/peer/{peerId}",
        "/api/v1/cluster/leader",
        "/api/v1/cluster/stepdown",
        "/api/v1/cluster/leave",
        "/api/v1/cluster/verify/{database}",
        "/api/v1/cluster/resync/{database}",
        "/api/v1/cluster/bootstrap-state",
        "/api/v1/ha/snapshot/{database}",
        "/api/v1/ha/snapshot/{database}/checksums");

    final long operations = openAPI.getPaths().values().stream()
        .mapToLong(item -> item.readOperations().size()).sum();
    assertThat(operations).isEqualTo(12);
  }

  @Test
  void everyOperationNamesTheRequiredPlugin() {
    for (final Map.Entry<String, PathItem> entry : openAPI.getPaths().entrySet()) {
      for (final Operation op : entry.getValue().readOperations()) {
        assertThat(op.getDescription())
            .as("%s must tell a client which plugin has to be active", entry.getKey())
            .containsAnyOf("RaftHAPlugin", "PrometheusMetricsPlugin");
      }
    }
  }

  @Test
  void prometheusScrapeReturnsTextNotJson() {
    final Operation get = openAPI.getPaths().get("/prometheus").getGet();
    assertThat(get.getOperationId()).isEqualTo("scrapePrometheusMetrics");
    assertThat(get.getTags()).containsExactly("Metrics");
    assertThat(get.getResponses().get("200").getContent()).containsKey("text/plain");
  }

  @Test
  void clusterStatusDeclaresTheUnstartedOutcome() {
    final Operation get = openAPI.getPaths().get("/api/v1/cluster").getGet();
    assertThat(get.getOperationId()).isEqualTo("getClusterStatus");
    assertThat(get.getResponses())
        .as("the endpoint is registered before Raft starts and answers 503 until it has")
        .containsKey("503");
  }

  @Test
  void clusterStatusSchemaCarriesTheLeadershipAndPeerFields() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("ClusterStatus");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder(
        "implementation", "clusterName", "localPeerId", "raftState", "isLeader", "leaderReady",
        "leaderId", "leaderHttpAddress", "electionCount", "lastElectionTime", "uptime",
        "peers", "databases", "databasePresence", "alerts");
    assertThat(schema.getProperties().get("peers").getItems().getProperties().keySet())
        .contains("id", "address", "role", "matchIndex", "nextIndex", "replicationLag",
            "lastContactMs", "replicaStatus", "laggingForMs", "lagging");
  }

  @Test
  void addPeerRequiresPeerIdAndAddress() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("AddPeerRequest");
    assertThat(schema.getRequired()).containsExactlyInAnyOrder("peerId", "address");
    assertThat(schema.getProperties().keySet())
        .containsExactlyInAnyOrder("peerId", "address", "name");
  }

  @Test
  void peerRemovalAndLeaveDeclareConflict() {
    assertThat(openAPI.getPaths().get("/api/v1/cluster/peer/{peerId}").getDelete()
        .getResponses()).containsKey("409");
    assertThat(openAPI.getPaths().get("/api/v1/cluster/leave").getPost()
        .getResponses()).containsKey("409");
  }

  @Test
  void transferLeaderReportsTheResultingLeader() {
    final Operation post = openAPI.getPaths().get("/api/v1/cluster/leader").getPost();
    assertThat(post.getOperationId()).isEqualTo("transferClusterLeadership");
    assertThat(openAPI.getComponents().getSchemas().get("ClusterActionResponse")
        .getProperties().keySet()).contains("result", "leaderId");
  }

  @Test
  void verifyReportsPerFileChecksums() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("VerifyDatabaseResponse");
    assertThat(schema.getProperties().keySet())
        .containsExactlyInAnyOrder("localChecksums", "files", "localServer");
    assertThat(schema.getProperties().get("files").getItems().getProperties().keySet())
        .containsExactlyInAnyOrder("name", "checksum", "size", "type");
  }

  @Test
  void bootstrapStateReportsPerDatabaseFingerprints() {
    final Schema<?> schema = openAPI.getComponents().getSchemas().get("BootstrapStateResponse");
    assertThat(schema.getProperties().keySet()).containsExactlyInAnyOrder("databases", "peerId");
    assertThat(schema.getProperties().get("databases").getItems().getProperties().keySet())
        .containsExactlyInAnyOrder("name", "fingerprint", "lastTxId", "error");
  }

  @Test
  void snapshotOperationsAreBasicAuthOnlyAndStreamZip() {
    final Operation snapshot = openAPI.getPaths().get("/api/v1/ha/snapshot/{database}").getGet();
    assertThat(snapshot.getOperationId()).isEqualTo("downloadDatabaseSnapshot");
    assertThat(snapshot.getResponses().get("200").getContent()).containsKey("application/zip");
    assertThat(snapshot.getResponses())
        .as("the handler caps concurrent snapshots and refuses beyond the cap")
        .containsKey("503");
    assertThat(snapshot.getSecurity())
        .as("SnapshotHttpHandler parses Basic itself and never reaches the bearer branch")
        .hasSize(1);
    assertThat(snapshot.getSecurity().getFirst()).containsOnlyKeys("basicAuth");

    final Operation checksums = openAPI.getPaths()
        .get("/api/v1/ha/snapshot/{database}/checksums").getGet();
    assertThat(checksums.getOperationId()).isEqualTo("getDatabaseSnapshotChecksums");
    assertThat(checksums.getSecurity().getFirst()).containsOnlyKeys("basicAuth");
  }

  @Test
  void noClusterOperationIsMarkedPublic() {
    for (final Map.Entry<String, PathItem> entry : openAPI.getPaths().entrySet()) {
      for (final Operation op : entry.getValue().readOperations()) {
        assertThat(op.getSecurity())
            .as("%s must not opt out of authentication", entry.getKey())
            .satisfiesAnyOf(
                security -> assertThat(security).isNull(),
                security -> assertThat(security).isNotEmpty());
      }
    }
  }
}
```

- [ ] **Step 2: Run to verify it fails**

```bash
mvn -q -pl server -am -Dtest=PluginApiSpecTest test
```

Expected: compilation failure, `cannot find symbol: class PluginApiSpec`.

- [ ] **Step 3: Create `PluginApiSpec`**

The class Javadoc carries the strategy decision. Write it exactly as given: #4896 is required to read it.

```java
package com.arcadedb.server.http.handler.openapi;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;

import java.util.List;

/**
 * Documents the routes contributed by server plugins rather than by {@code HttpServer} itself: the
 * Prometheus scrape endpoint and the Raft high-availability cluster management surface.
 * <p>
 * These operations are declared here, in the server module, and declared unconditionally. Two
 * constraints force that shape.
 * <p>
 * First, the specification has to be deterministic. Client generation runs against a live server's
 * spec, and a default server runs neither the HA plugin nor the metrics plugin. A specification that
 * only listed the routes of the currently active plugins would therefore generate clients with no
 * cluster management and no scrape endpoint at all, which is the opposite of what a complete spec is
 * for. Every operation below instead names the plugin a deployment must run for the route to answer,
 * so a client always has the method and a reader always knows the precondition.
 * <p>
 * Second, the plugins cannot declare their own. The {@code ha-raft} and {@code metrics} modules hold
 * {@code arcadedb-server} at provided scope, so the swagger model classes are absent from their
 * compile classpath. Asking a plugin to return path items would mean adding a swagger dependency to
 * both modules.
 * <p>
 * The cost is that a plugin can add a route without touching this class. Closing that gap is the
 * anti-drift work tracked separately: the natural shape is a test inside each plugin module that
 * asserts every route the module's own {@code registerAPI} declares appears in the generated
 * specification, which needs no new dependency because the assertion can compare plain path strings.
 */
public class PluginApiSpec implements OpenApiContributor {

  private static final String RAFT_REQUIRED =
      "Requires RaftHAPlugin: the route is registered on every server, but answers only where high availability is configured.";
  private static final String METRICS_REQUIRED =
      "Requires PrometheusMetricsPlugin: absent unless the metrics plugin is enabled.";

  @Override
  public void contribute(final OpenAPI openAPI) {
    openAPI.getPaths().addPathItem("/prometheus", createScrapePath());
    openAPI.getPaths().addPathItem("/api/v1/cluster", createClusterStatusPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/peer", createAddPeerPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/peer/{peerId}", createRemovePeerPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/leader", createTransferLeaderPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/stepdown", createStepDownPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/leave", createLeavePath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/verify/{database}", createVerifyPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/resync/{database}", createResyncPath());
    openAPI.getPaths().addPathItem("/api/v1/cluster/bootstrap-state", createBootstrapStatePath());
    openAPI.getPaths().addPathItem("/api/v1/ha/snapshot/{database}", createSnapshotPath());
    openAPI.getPaths().addPathItem("/api/v1/ha/snapshot/{database}/checksums", createChecksumsPath());

    openAPI.getComponents().addSchemas("ClusterStatus", createClusterStatusSchema());
    openAPI.getComponents().addSchemas("AddPeerRequest", createAddPeerRequestSchema());
    openAPI.getComponents().addSchemas("TransferLeaderRequest", createTransferLeaderRequestSchema());
    openAPI.getComponents().addSchemas("ClusterActionResponse", createClusterActionResponseSchema());
    openAPI.getComponents().addSchemas("VerifyDatabaseResponse", createVerifyResponseSchema());
    openAPI.getComponents().addSchemas("BootstrapStateResponse", createBootstrapStateResponseSchema());
  }

  private PathItem createScrapePath() {
    final Operation get = SpecBuilders.operation("scrapePrometheusMetrics", "Metrics",
        "Scrape server metrics",
        """
            Exposes the server's metrics in the Prometheus text exposition format, for a Prometheus \
            scrape_config to poll.

            Authentication can be turned off for this route with \
            arcadedb.serverMetrics.prometheus.requireAuthentication=false, which is how most scrape \
            setups run it. """ + METRICS_REQUIRED);

    final ApiResponse success = new ApiResponse();
    success.setDescription("Metrics in the Prometheus text exposition format");
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(SpecBuilders.string("Prometheus text exposition format"));
    success.setContent(new Content().addMediaType("text/plain", mediaType));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", success);
    responses.addApiResponse("401", SpecBuilders.errorResponse(
        "Unauthorized: returned only when the plugin requires authentication"));
    responses.addApiResponse("500", SpecBuilders.errorResponse("Internal server error"));
    get.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createClusterStatusPath() {
    final Operation get = SpecBuilders.operation("getClusterStatus", "Cluster",
        "Read cluster and replication status",
        """
            Reports this server's Raft role, the current leader, and per-peer replication health \
            including match and next index, lag, and round-trip latency. Answers 503 until Raft has \
            started, because the route is registered before the Raft server comes up. """
            + RAFT_REQUIRED);
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Cluster status", "ClusterStatus"),
        "401", "403", "500", "503"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createAddPeerPath() {
    final Operation post = SpecBuilders.operation("addClusterPeer", "Cluster",
        "Add a peer to the cluster",
        "Adds a peer to the Raft configuration. " + RAFT_REQUIRED);
    post.setRequestBody(SpecBuilders.jsonBody("Peer to add", "AddPeerRequest", true));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Peer added", "ClusterActionResponse"),
        "400", "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createRemovePeerPath() {
    final Operation delete = SpecBuilders.operation("removeClusterPeer", "Cluster",
        "Remove a peer from the cluster",
        """
            Removes a peer from the Raft configuration. Answers 409 when the removal would break \
            quorum or the configuration is already changing. """ + RAFT_REQUIRED);
    delete.addParametersItem(SpecBuilders.pathParam("peerId", "Peer identifier"));
    delete.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Peer removed", "ClusterActionResponse"),
        "400", "401", "403", "409", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setDelete(delete);
    return pathItem;
  }

  private PathItem createTransferLeaderPath() {
    final Operation post = SpecBuilders.operation("transferClusterLeadership", "Cluster",
        "Transfer leadership",
        """
            Transfers Raft leadership, to the named peer when 'peerId' is given and to whichever peer \
            Raft selects otherwise. Unknown fields in the body are rejected. """ + RAFT_REQUIRED);
    post.setRequestBody(SpecBuilders.jsonBody(
        "Transfer target", "TransferLeaderRequest", true));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Leadership transferred", "ClusterActionResponse"),
        "400", "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createStepDownPath() {
    final Operation post = SpecBuilders.operation("stepDownClusterLeader", "Cluster",
        "Step down from leadership",
        "Asks this server to give up leadership, triggering an election. " + RAFT_REQUIRED);
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Step-down initiated", "ClusterActionResponse"),
        "400", "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createLeavePath() {
    final Operation post = SpecBuilders.operation("leaveCluster", "Cluster",
        "Leave the cluster",
        """
            Removes this server from the Raft configuration. Answers 409 when leaving would break \
            quorum. """ + RAFT_REQUIRED);
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Leaving the cluster", "ClusterActionResponse"),
        "400", "401", "403", "409", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createVerifyPath() {
    final Operation post = SpecBuilders.operation("verifyClusterDatabase", "Cluster",
        "Checksum a database's files for comparison across peers",
        """
            Computes a per-file checksum of one database on this server, so an operator can compare \
            the same call across peers and find a diverged replica. """ + RAFT_REQUIRED);
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Per-file checksums", "VerifyDatabaseResponse"),
        "400", "401", "403", "404", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createResyncPath() {
    final Operation post = SpecBuilders.operation("resyncClusterDatabase", "Cluster",
        "Re-fetch a database from the leader",
        """
            Discards this server's copy of one database and installs a fresh snapshot from the \
            leader. Answers 503 when no leader is currently reachable. """ + RAFT_REQUIRED);
    post.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Database resynced", "ClusterActionResponse"),
        "400", "401", "403", "404", "500", "503"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createBootstrapStatePath() {
    final Operation post = SpecBuilders.operation("getClusterBootstrapState", "Cluster",
        "Report per-database bootstrap state",
        """
            Reports this peer's fingerprint and last transaction id for every database. Used by the \
            bootstrap leader at first cluster formation to decide which copy of each database wins. \
            A database this peer cannot read is reported with an 'error' and a last transaction id of \
            -1 rather than omitted. """ + RAFT_REQUIRED);
    post.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Bootstrap state", "BootstrapStateResponse"),
        "400", "401", "403", "500"));

    final PathItem pathItem = new PathItem();
    pathItem.setPost(post);
    return pathItem;
  }

  private PathItem createSnapshotPath() {
    final Operation get = SpecBuilders.operation("downloadDatabaseSnapshot", "Cluster",
        "Download a database snapshot",
        """
            Streams a consistent snapshot of one database as a ZIP archive, for a follower installing \
            a fresh copy. The stream ends with a completeness manifest, advertised by a response \
            header, so a consumer can tell a complete download from one truncated at an archive entry \
            boundary. Answers 503 when the server's concurrent-snapshot limit is already reached.

            This route accepts HTTP Basic only: it is served by a handler outside the standard chain \
            and never reads a bearer token. """ + RAFT_REQUIRED);
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    SpecBuilders.basicAuthOnly(get);

    final ApiResponse success = new ApiResponse();
    success.setDescription("ZIP archive of the database, ending with a completeness manifest");
    final MediaType mediaType = new MediaType();
    mediaType.setSchema(new Schema<>().type("string").format("binary"));
    success.setContent(new Content().addMediaType("application/zip", mediaType));

    final ApiResponses responses = new ApiResponses();
    responses.addApiResponse("200", success);
    responses.addApiResponse("400", SpecBuilders.errorResponse(
        "Missing or invalid database name"));
    responses.addApiResponse("401", SpecBuilders.errorResponse("Unauthorized"));
    responses.addApiResponse("404", SpecBuilders.errorResponse("Database not found"));
    responses.addApiResponse("503", SpecBuilders.errorResponse(
        "Too many concurrent snapshots"));
    get.setResponses(responses);

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private PathItem createChecksumsPath() {
    final Operation get = SpecBuilders.operation("getDatabaseSnapshotChecksums", "Cluster",
        "Read the checksums of a snapshot's files",
        """
            Returns the per-file checksums of the database a snapshot download would produce, so a \
            follower can decide whether it needs the full transfer.

            This route accepts HTTP Basic only, for the same reason as the snapshot download. """
            + RAFT_REQUIRED);
    get.addParametersItem(SpecBuilders.pathParam("database", "Database name"));
    SpecBuilders.basicAuthOnly(get);
    get.setResponses(SpecBuilders.standardResponses("200",
        SpecBuilders.jsonResponse("Per-file checksums", null),
        "400", "401", "404", "503"));

    final PathItem pathItem = new PathItem();
    pathItem.setGet(get);
    return pathItem;
  }

  private Schema<?> createClusterStatusSchema() {
    final Schema<Object> peer = SpecBuilders.object("One peer's replication health");
    peer.addProperty("id", SpecBuilders.string("Peer identifier"));
    peer.addProperty("address", SpecBuilders.string("Peer address"));
    peer.addProperty("role", SpecBuilders.string("LEADER or FOLLOWER"));
    peer.addProperty("matchIndex", SpecBuilders.integer("Highest log entry known replicated"));
    peer.addProperty("nextIndex", SpecBuilders.integer("Next log entry to send"));
    peer.addProperty("replicationLag", SpecBuilders.integer("Entries behind the leader"));
    peer.addProperty("lastContactMs", SpecBuilders.integer("Milliseconds since last contact"));
    peer.addProperty("replicaStatus", SpecBuilders.string("Replica health status"));
    peer.addProperty("laggingForMs", SpecBuilders.integer(
        "How long this peer has been lagging, in milliseconds"));
    peer.addProperty("lagging", SpecBuilders.bool(
        "True when the lag exceeds the configured warning threshold"));
    peer.addProperty("replicationRttMs", SpecBuilders.integer(
        "Mean replication round-trip time. Absent when no sample exists."));
    peer.addProperty("replicationRttP99Ms", SpecBuilders.integer(
        "99th percentile replication round-trip time. Absent when no sample exists."));

    final Schema<Object> database = SpecBuilders.object("One database's cluster state");
    database.addProperty("name", SpecBuilders.string("Database name"));
    database.addProperty("bootstrapLastTxId", SpecBuilders.integer(
        "Last transaction id recorded at bootstrap. Absent when no baseline exists."));
    database.addProperty("bootstrapFingerprint", SpecBuilders.string(
        "Fingerprint recorded at bootstrap. Absent when no baseline exists."));
    database.addProperty("acquireStatus", SpecBuilders.string(
        "State of the last acquisition attempt. Absent when none was made."));
    database.addProperty("acquireTimestamp", SpecBuilders.integer(
        "When the last acquisition attempt ran, as epoch milliseconds"));
    database.addProperty("acquireError", SpecBuilders.string(
        "Why the last acquisition failed. Absent on success."));

    final Schema<Object> schema = SpecBuilders.object("Cluster and replication status");
    schema.addProperty("implementation", SpecBuilders.string("Always 'raft'"));
    schema.addProperty("clusterName", SpecBuilders.string("Configured cluster name"));
    schema.addProperty("localPeerId", SpecBuilders.string("This server's peer identifier"));
    schema.addProperty("raftState", SpecBuilders.string("Raft lifecycle state"));
    schema.addProperty("isLeader", SpecBuilders.bool("True when this server is the leader"));
    schema.addProperty("leaderReady", SpecBuilders.bool(
        "True when the leader has finished the work that makes it safe to serve writes"));
    final Schema<String> leaderId = SpecBuilders.string("Current leader, null when unknown");
    leaderId.setNullable(true);
    schema.addProperty("leaderId", leaderId);
    final Schema<String> leaderAddress = SpecBuilders.string(
        "Leader HTTP address, null when unknown");
    leaderAddress.setNullable(true);
    schema.addProperty("leaderHttpAddress", leaderAddress);
    schema.addProperty("electionCount", SpecBuilders.integer("Elections observed since start"));
    schema.addProperty("lastElectionTime", SpecBuilders.integer(
        "Last election as epoch milliseconds"));
    schema.addProperty("uptime", SpecBuilders.integer("Milliseconds since the Raft server started"));
    schema.addProperty("peers", SpecBuilders.arrayOf(peer, "Known peers"));
    schema.addProperty("databases", SpecBuilders.arrayOf(database, "Replicated databases"));
    schema.addProperty("databasePresence", SpecBuilders.object(
        "Which peer holds which database. Present only on the leader."));
    schema.addProperty("alerts", SpecBuilders.arrayOf(
        SpecBuilders.object("One cluster alert"), "Conditions worth an operator's attention"));
    return schema;
  }

  private Schema<?> createAddPeerRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object("Peer to add");
    schema.addProperty("peerId", SpecBuilders.string("Peer identifier"));
    schema.addProperty("address", SpecBuilders.string("Peer address"));
    schema.addProperty("name", SpecBuilders.string("Optional display name"));
    schema.setRequired(List.of("peerId", "address"));
    return schema;
  }

  private Schema<?> createTransferLeaderRequestSchema() {
    final Schema<Object> schema = SpecBuilders.object(
        "Transfer target. Send an empty object to let Raft choose. Unknown fields are rejected.");
    schema.addProperty("peerId", SpecBuilders.string(
        "Peer to make leader. Raft chooses when omitted."));
    return schema;
  }

  private Schema<?> createClusterActionResponseSchema() {
    final Schema<Object> schema = SpecBuilders.object("Outcome of a cluster management action");
    schema.addProperty("result", SpecBuilders.string("Human-readable outcome"));
    schema.addProperty("leaderId", SpecBuilders.string(
        "Leader after the action. Present on leadership transfer."));
    schema.addProperty("database", SpecBuilders.string(
        "Database the action applied to. Present on resync."));
    schema.addProperty("localServer", SpecBuilders.string(
        "Server that performed the action. Present on resync."));
    return schema;
  }

  private Schema<?> createVerifyResponseSchema() {
    final Schema<Object> file = SpecBuilders.object("One database file");
    file.addProperty("name", SpecBuilders.string("File name"));
    file.addProperty("checksum", SpecBuilders.integer("CRC of the file's contents"));
    file.addProperty("size", SpecBuilders.integer("File size in bytes"));
    file.addProperty("type", SpecBuilders.string("File category"));

    final Schema<Object> schema = SpecBuilders.object("Per-file checksums of one database");
    schema.addProperty("localChecksums", SpecBuilders.object(
        "File name to checksum map, for a quick cross-peer comparison"));
    schema.addProperty("files", SpecBuilders.arrayOf(file, "Files with size and category"));
    schema.addProperty("localServer", SpecBuilders.string("Server the checksums were taken on"));
    return schema;
  }

  private Schema<?> createBootstrapStateResponseSchema() {
    final Schema<Object> database = SpecBuilders.object("One database's bootstrap state");
    database.addProperty("name", SpecBuilders.string("Database name"));
    database.addProperty("fingerprint", SpecBuilders.string(
        "Content fingerprint, empty when the database could not be read"));
    database.addProperty("lastTxId", SpecBuilders.integer(
        "Last transaction id, -1 when the database could not be read"));
    database.addProperty("error", SpecBuilders.string(
        "Why the database could not be read. Absent on success."));

    final Schema<Object> schema = SpecBuilders.object("Per-database bootstrap state of one peer");
    schema.addProperty("databases", SpecBuilders.arrayOf(database, "Databases on this peer"));
    schema.addProperty("peerId", SpecBuilders.string("Peer that reported the state"));
    return schema;
  }
}
```

- [ ] **Step 4: Run to verify it passes**

```bash
mvn -q -pl server -am -Dtest=PluginApiSpecTest test
```

Expected: PASS, 12 tests.

- [ ] **Step 5: Prove the test can fail**

Temporarily remove the `SpecBuilders.basicAuthOnly(get);` call from `createSnapshotPath()`. Expected: `snapshotOperationsAreBasicAuthOnlyAndStreamZip` FAILS on the null security. Revert.

- [ ] **Step 6: Commit**

```bash
git add server/src/main/java/com/arcadedb/server/http/handler/openapi/PluginApiSpec.java \
        server/src/test/java/com/arcadedb/server/http/handler/openapi/PluginApiSpecTest.java
git commit -m "feat(server) #4895: document the plugin-contributed Raft and Prometheus routes

Declared in the server module and declared unconditionally: client generation
runs against a default server that loads neither plugin, so a config-dependent
spec would generate clients with no cluster methods. Each operation names the
plugin a deployment must run. The class Javadoc records why the plugins cannot
declare their own routes and what the anti-drift follow-up should look like.

The two snapshot operations are Basic-only: SnapshotHttpHandler sits outside the
standard handler chain and never reads a bearer token."
```

---

### Task 11: Whole-spec inventory, uniqueness, and Swagger UI

The gate. Everything before this proves one contributor; this proves the assembled document.

**Files:**
- Modify test: `server/src/test/java/com/arcadedb/server/http/OpenApiSpecGenerationIT.java`
- Modify test: `server/src/test/java/com/arcadedb/server/http/OpenApiDocsEndpointIT.java`

**Interfaces:**
- Consumes: every contributor from Tasks 1 through 10; `OpenApiSpecGenerator.contributors()` from Task 1.
- Produces: no production interface.

- [ ] **Step 1: Write the failing inventory test**

Append to `OpenApiSpecGenerationIT`. The inventory is a single constant so a missing operation names itself.

```java
  /**
   * Every operation the specification must document, as "METHOD path". Deliberately exhaustive and
   * deliberately hand-written: a list derived from the specification under test would assert nothing.
   */
  private static final List<String> EXPECTED_OPERATIONS = List.of(
      // Core
      "GET /api/v1/server", "POST /api/v1/server",
      "GET /api/v1/ready", "GET /api/v1/health",
      "GET /api/v1/databases", "GET /api/v1/exists/{database}",
      "GET /api/v1/query/{database}/{language}/{command}", "POST /api/v1/query/{database}",
      "POST /api/v1/command/{database}", "POST /api/v1/batch/{database}",
      "GET /api/v1/progress/{database}",
      "POST /api/v1/begin/{database}", "POST /api/v1/commit/{database}",
      "POST /api/v1/rollback/{database}",
      // Auth
      "POST /api/v1/login", "POST /api/v1/logout", "GET /api/v1/sessions",
      // Security admin
      "GET /api/v1/server/users", "POST /api/v1/server/users",
      "PUT /api/v1/server/users", "DELETE /api/v1/server/users",
      "GET /api/v1/server/groups", "POST /api/v1/server/groups", "DELETE /api/v1/server/groups",
      "GET /api/v1/server/api-tokens", "POST /api/v1/server/api-tokens",
      "DELETE /api/v1/server/api-tokens",
      // Time-series
      "POST /api/v1/ts/{database}/write", "POST /api/v1/ts/{database}/query",
      "GET /api/v1/ts/{database}/latest",
      // Grafana
      "GET /api/v1/ts/{database}/grafana/health", "GET /api/v1/ts/{database}/grafana/metadata",
      "POST /api/v1/ts/{database}/grafana/query",
      // Prometheus and PromQL
      "POST /api/v1/ts/{database}/prom/write", "POST /api/v1/ts/{database}/prom/read",
      "GET /api/v1/ts/{database}/prom/api/v1/query",
      "GET /api/v1/ts/{database}/prom/api/v1/query_range",
      "GET /api/v1/ts/{database}/prom/api/v1/labels",
      "GET /api/v1/ts/{database}/prom/api/v1/label/{name}/values",
      "GET /api/v1/ts/{database}/prom/api/v1/series",
      // MCP
      "POST /api/v1/mcp", "GET /api/v1/mcp/config", "POST /api/v1/mcp/config",
      // AI
      "GET /api/v1/ai/config", "POST /api/v1/ai/activate", "POST /api/v1/ai/chat",
      "POST /api/v1/ai/analyze-profiler", "GET /api/v1/ai/chats",
      "GET /api/v1/ai/chats/{id}", "PUT /api/v1/ai/chats/{id}", "DELETE /api/v1/ai/chats/{id}",
      // Plugin-contributed
      "GET /prometheus",
      "GET /api/v1/cluster", "POST /api/v1/cluster/peer", "DELETE /api/v1/cluster/peer/{peerId}",
      "POST /api/v1/cluster/leader", "POST /api/v1/cluster/stepdown", "POST /api/v1/cluster/leave",
      "POST /api/v1/cluster/verify/{database}", "POST /api/v1/cluster/resync/{database}",
      "POST /api/v1/cluster/bootstrap-state",
      "GET /api/v1/ha/snapshot/{database}", "GET /api/v1/ha/snapshot/{database}/checksums");

  private static List<String> declaredOperations(final OpenAPI openAPI) {
    final List<String> declared = new ArrayList<>();
    openAPI.getPaths().forEach((path, item) -> {
      if (item.getGet() != null)
        declared.add("GET " + path);
      if (item.getPost() != null)
        declared.add("POST " + path);
      if (item.getPut() != null)
        declared.add("PUT " + path);
      if (item.getDelete() != null)
        declared.add("DELETE " + path);
      if (item.getPatch() != null)
        declared.add("PATCH " + path);
      if (item.getHead() != null)
        declared.add("HEAD " + path);
      if (item.getOptions() != null)
        declared.add("OPTIONS " + path);
    });
    return declared;
  }

  @Test
  void specDocumentsExactlyTheExpectedSixtyThreeOperations() throws Exception {
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();
    final List<String> declared = declaredOperations(openAPI);

    assertThat(EXPECTED_OPERATIONS)
        .as("the inventory itself must hold no duplicate")
        .doesNotHaveDuplicates()
        .hasSize(63);

    assertThat(declared)
        .as("operations missing from the specification")
        .containsAll(EXPECTED_OPERATIONS);

    assertThat(declared)
        .as("operations in the specification with no registered handler: reverse drift")
        .containsExactlyInAnyOrderElementsOf(EXPECTED_OPERATIONS);
  }

  @Test
  void specDoesNotDocumentItselfOrTheWebSocketUpgrade() throws Exception {
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();
    assertThat(openAPI.getPaths().keySet())
        .as("self-documentation, the Swagger UI page, and the WebSocket upgrade stay out")
        .doesNotContain("/api/v1/openapi.json", "/api/v1/docs", "/ws");
  }

  @Test
  void everyOperationIdIsUniqueAcrossTheWholeSpec() throws Exception {
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();

    final List<String> ids = new ArrayList<>();
    openAPI.getPaths().values().forEach(item ->
        item.readOperations().forEach(op -> ids.add(op.getOperationId())));

    assertThat(ids)
        .as("client generators derive a method name per operationId, so a collision breaks codegen")
        .doesNotHaveDuplicates()
        .doesNotContainNull()
        .hasSize(63);
  }

  @Test
  void everyOperationCarriesExactlyOneDeclaredTag() throws Exception {
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();
    final Set<String> declaredTags = openAPI.getTags().stream()
        .map(Tag::getName).collect(Collectors.toSet());

    openAPI.getPaths().forEach((path, item) -> item.readOperations().forEach(op -> {
      assertThat(op.getTags())
          .as("%s %s: generators derive an API class from the first tag", path, op.getOperationId())
          .hasSize(1);
      assertThat(declaredTags)
          .as("%s uses tag %s, which is not in the root tag vocabulary", op.getOperationId(),
              op.getTags().getFirst())
          .contains(op.getTags().getFirst());
    }));
  }

  @Test
  void everyReferenceResolvesAndTheDocumentValidatesClean() throws Exception {
    final ParseOptions options = new ParseOptions();
    options.setResolve(true);
    options.setResolveFully(true);
    options.setValidateExternalRefs(false);

    final SwaggerParseResult result = new OpenAPIV3Parser().readContents(getOpenApiSpec(), null, options);

    assertThat(result.getMessages())
        .as("an unresolved $ref or a malformed schema shows up here: %s", result.getMessages())
        .isEmpty();
  }

  @Test
  void everyOperationDeclaresASuccessAndAnAuthFailureResponse() throws Exception {
    final OpenAPI openAPI = new OpenAPIV3Parser().readContents(getOpenApiSpec()).getOpenAPI();

    openAPI.getPaths().forEach((path, item) -> item.readOperations().forEach(op -> {
      assertThat(op.getResponses())
          .as("%s %s has no responses", path, op.getOperationId())
          .isNotNull().isNotEmpty();

      final boolean hasSuccess = op.getResponses().keySet().stream()
          .anyMatch(code -> code.startsWith("2"));
      assertThat(hasSuccess)
          .as("%s %s declares no 2xx response", path, op.getOperationId())
          .isTrue();

      // Health and readiness are declared public, so 401 would be a lie for them.
      final boolean isPublic = op.getSecurity() != null && op.getSecurity().isEmpty();
      if (!isPublic) {
        assertThat(op.getResponses().keySet())
            .as("%s %s is authenticated, so it must declare 401", path, op.getOperationId())
            .contains("401");
      }
    }));
  }
```

Add imports: `java.util.ArrayList`, `java.util.List`, `java.util.Set`.

- [ ] **Step 2: Run to verify it fails, then passes**

```bash
mvn -q -pl server -am -Dit.test=OpenApiSpecGenerationIT verify
```

If Tasks 2 through 10 all landed, these pass. Run them once before Task 10 lands to confirm `specDocumentsExactlyTheExpectedSixtyThreeOperations` reports the missing plugin operations by name; that failure message is the test's whole value.

- [ ] **Step 3: Prove the inventory test can fail**

Temporarily comment out `openAPI.getPaths().addPathItem("/api/v1/progress/{database}", createProgressPath());` in `CoreApiSpec`. Expected: the test FAILS naming `GET /api/v1/progress/{database}` as missing. Revert.

- [ ] **Step 4: Extend the Swagger UI test**

Append to `OpenApiDocsEndpointIT`, matching the retrieval helper that file already uses for the docs page:

```java
  @Test
  void swaggerUiPageLoadsAndPointsAtTheCompletedSpec() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/docs"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body())
        .as("the UI must load the spec this issue completed")
        .contains("/api/v1/openapi.json");
  }

  @Test
  void theServedSpecIsRenderableWithoutParserErrors() throws Exception {
    final HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI("http://localhost:2480/api/v1/openapi.json"))
        .GET()
        .setHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString(("root:" + DEFAULT_PASSWORD_FOR_TESTS).getBytes()))
        .build();

    final HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
    assertThat(response.statusCode()).isEqualTo(200);

    final ParseOptions options = new ParseOptions();
    options.setResolve(true);
    final SwaggerParseResult result = new OpenAPIV3Parser().readContents(response.body(), null, options);

    assertThat(result.getMessages())
        .as("Swagger UI renders what the parser accepts, so a clean parse is the render check: %s",
            result.getMessages())
        .isEmpty();
    assertThat(result.getOpenAPI().getPaths()).hasSizeGreaterThanOrEqualTo(40);
  }
```

Reuse the existing `client` field and `DEFAULT_PASSWORD_FOR_TESTS` constant in that file. Add the swagger-parser imports if absent.

- [ ] **Step 5: Full verification**

```bash
mvn -q -pl server -am -DskipTests compile
mvn -q -pl server -am -Dtest='SpecBuildersTest,CoreApiSpecTest,AuthApiSpecTest,SecurityAdminApiSpecTest,TimeSeriesApiSpecTest,GrafanaApiSpecTest,PrometheusApiSpecTest,AiApiSpecTest,McpApiSpecTest,PluginApiSpecTest' test
mvn -q -pl server -am -Dit.test='OpenApiSpecGenerationIT,OpenApiDocsEndpointIT' verify
```

Then the whole module, to catch anything the extraction disturbed:

```bash
mvn -pl server -am verify
```

Expected: green. Two tests are known-red on `main` independently of this work (`EdgeAppendRace` and the issue-4141 session test); confirm any failure you see is one of those by checking out `main` and re-running that test alone before treating it as a regression.

- [ ] **Step 6: Commit**

```bash
git add server/src/test/java/com/arcadedb/server/http/OpenApiSpecGenerationIT.java \
        server/src/test/java/com/arcadedb/server/http/OpenApiDocsEndpointIT.java
git commit -m "test(server) #4895: assert the whole 63-operation inventory, id uniqueness, and ref resolution

The inventory is hand-written rather than derived from the document under test,
so it fails by naming the missing operation. Also asserts the reverse direction:
a spec path with no registered handler fails too.

operationId uniqueness and the single-declared-tag rule are asserted because
client generators derive a method name per operationId and an API class per first
tag, so a collision or an undeclared tag breaks code generation downstream."
```

---

## Post-implementation

- [ ] Confirm 63 operations are served: start a server and count with `curl -su root:$PASSWORD http://localhost:2480/api/v1/openapi.json | jq '[.paths | to_entries[] | .value | keys[] | select(. == "get" or . == "post" or . == "put" or . == "delete")] | length'`.
- [ ] Open `http://localhost:2480/api/v1/docs` and confirm the new tag groups render: Batch, Auth, TimeSeries, Grafana, Prometheus, PromQL, AI, MCP, Cluster, Metrics.
- [ ] Update the issue with the corrected counts: 63 documented rather than 61, and the four enumeration deltas the audit found.
- [ ] #4896 can now start. Point it at the `PluginApiSpec` Javadoc, which records the plugin-route decision it must encode and the per-module test shape it should build.
