# Issue #7325 - HTTP time-series aggregation rejects an unknown `type` with an unguarded `IllegalArgumentException`

## Problem

Both HTTP aggregation paths read the aggregation function straight off the request payload:

```java
// server/.../PostTimeSeriesQueryHandler.java:199
final AggregationType aggType = AggregationType.valueOf(req.getString("type"));

// server/.../PostGrafanaQueryHandler.java:199
final AggregationType aggType = AggregationType.valueOf(reqJson.getString("type"));
```

`AggregationType.valueOf` accepts only the exact enum spellings `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`.
Anything else - a lower-cased `avg`, a typo, a Grafana panel built by hand - throws, and the throw
leaves the handler for the generic error mapper in `AbstractServerHttpHandler`.

Two consequences, both verified by reading the mapper:

1. **The message is useless outside development.** The `IllegalArgumentException` arm
   (`AbstractServerHttpHandler:824-828`) answers `400 "Cannot execute command"` and puts the JVM's own
   `No enum constant com.arcadedb.engine.timeseries.AggregationType.avg` into the `detail` field. But
   `buildErrorBody` conceals `detail` whenever the server is in production mode, so a production caller
   receives `{"error":"Cannot execute command","exception":"java.lang.IllegalArgumentException"}` - the
   field that was wrong is not named and the accepted values are not listed. Every other refusal in the
   same handler (`Type 'x' does not exist`, `Field 'x' not found in type`) is an explicit
   `ExecutionResponse(400, ...)` whose text survives production mode.
2. **On the Grafana endpoint it fails the whole request, not the target.** `PostGrafanaQueryHandler`
   answers per-target problems with an error *frame* keyed by `refId`, and keeps serving the other
   targets. The unguarded `valueOf` is the one refusal in that loop that escapes it, so one mistyped
   aggregation in panel B also blanks panels A and C.

The gRPC twin proposed in #7305 (PR #7323, still open at the time of writing - the class does not exist
on `main`) answers this cleanly with `INVALID_ARGUMENT` and a message that lists the accepted values.

## Invariant the fix establishes

An aggregation `type` that no `AggregationType` matches is refused with a message that names the field
and lists the accepted values, rendered on the surface that endpoint answers errors on - a `400` body
for `/ts/{db}/query`, a per-target error frame for `/ts/{db}/grafana/query` - and the message survives
production mode.

## Completeness

### Entry points reached by grep, not by memory

Every value of `AggregationType` that comes from a request payload:

```
$ grep -rn "AggregationType\.valueOf" --include="*.java" .
./server/src/main/java/com/arcadedb/server/http/handler/PostGrafanaQueryHandler.java:199
./server/src/main/java/com/arcadedb/server/http/handler/PostTimeSeriesQueryHandler.java:199
```

Same-shape sibling sweep - an `Enum.valueOf` applied to a value taken off the request payload, anywhere
in the HTTP handler package:

```
$ grep -rn "\.valueOf(.*getString\|\.valueOf(.*get(" --include="*.java" \
    server/src/main/java/com/arcadedb/server/http/handler/
server/src/main/java/com/arcadedb/server/http/handler/PostGrafanaQueryHandler.java:199
server/src/main/java/com/arcadedb/server/http/handler/PostTimeSeriesQueryHandler.java:199
```

Two hits, both the ones this issue names.

Every reader of `AggregationType` in main source, to find a path that could resolve one another way:

```
$ grep -rln "AggregationType" --include="*.java" . | grep "/src/main/"
server/src/main/java/com/arcadedb/server/http/handler/PostGrafanaQueryHandler.java
server/src/main/java/com/arcadedb/server/http/handler/PostTimeSeriesQueryHandler.java
server/src/main/java/com/arcadedb/server/http/handler/GetGrafanaMetadataHandler.java
engine/src/main/java/com/arcadedb/query/sql/executor/SelectExecutionPlanner.java
engine/src/main/java/com/arcadedb/engine/timeseries/{AggregationType,AggregationResult,
  MultiColumnAggregationRequest,MultiColumnAggregationResult,TimeSeriesEngine,TimeSeriesSealedStore}.java
```

Every reader of the `aggregation` payload object:

```
$ grep -rn '"aggregation"' --include="*.java" server/src/main/java
PostGrafanaQueryHandler.java:119, 183
PostTimeSeriesQueryHandler.java:108, 189
(+ the two OpenAPI spec builders, which describe the field rather than read it)
```

### Coverage table

| Entry point | Covered by fix? | Covered by a test? |
|---|---|---|
| `POST /api/v1/ts/{db}/query`, `aggregation.requests[].type` -> `PostTimeSeriesQueryHandler:199` | yes | yes - `aggregationRejectsUnknownTypeWithANamedError`, `aggregationTypeIsCaseInsensitive`, `aggregationTypeIsRequired` |
| `POST /api/v1/ts/{db}/grafana/query`, `targets[].aggregation.requests[].type` -> `PostGrafanaQueryHandler:199` | yes | yes - `grafanaAggregationRejectsUnknownTypeAsAnErrorFrame`, `grafanaAggregationTypeIsCaseInsensitive` |
| SQL push-down, `SelectExecutionPlanner:3087` | no - argued | n/a |
| `GET /api/v1/ts/{db}/grafana/metadata` -> `GetGrafanaMetadataHandler:95` | no - argued | n/a |
| gRPC `TimeSeriesAggregationRequest.type` | no - argued | n/a |

**Argued rows, with the evidence:**

- **SQL push-down.** `SelectExecutionPlanner:3087-3096` is a `switch` over the lower-cased SQL function
  name with `default -> null`, and `if (aggType == null) return false;` on the next line - an unknown
  function simply declines the push-down and the query runs through the normal execution path. No
  exception, nothing to guard.
- **Grafana metadata endpoint.** `GetGrafanaMetadataHandler:95` iterates `AggregationType.values()` to
  *publish* the accepted names. It reads nothing off the request, so it cannot carry a bad one.
- **gRPC.** `grep -rn "TS_AGG_UNSPECIFIED" --include="*.java" .` returns nothing and
  `find . -name "GrpcTimeSeriesSupport.java"` finds no file: the class the issue quotes lives on PR
  #7323, which `gh pr view 7323` reports as `OPEN`. There is no gRPC time-series aggregation path on
  `main` to fix, and the fix here does not touch `grpcw`.

### Residual risk

Neither handler validates the *other* required members of an aggregation request - `bucketInterval`,
`requests`, `field` - beyond what `JSONObject` itself throws, so a payload missing `field` still leaves
through `JSONException` and answers `400 "Invalid JSON payload"` with the specifics concealed in
production. That is a different defect (missing-required-property reporting, shared by every endpoint
that reads a payload) rather than the same one at another entry point, and it is deliberately not
widened into here. The two `Enum.valueOf`-on-payload sites the sweep found are both fixed.

## Decision: the resolver accepts a case-insensitive name

The issue names a lower-cased `avg` as "what a hand-written client is most likely to send". Answering it
with a better-worded refusal still refuses it, so the resolver upper-cases and trims before matching.
This is a strict superset of what was accepted before, so no request that worked stops working, and it
matches the SQL push-down path, which has always matched the same five functions case-insensitively
(`funcName.toLowerCase()` at `SelectExecutionPlanner:3086`). The OpenAPI description and the
`grafana/metadata` endpoint keep advertising the canonical upper-case spellings.

## Changes

- `TimeSeriesHandlerUtils.resolveAggregationType(JSONObject, int)` - new shared resolver. Trims and
  upper-cases before matching, and throws an `IllegalArgumentException` whose message names
  `aggregation.requests[<i>].type`, lists the accepted values built from `AggregationType.values()`, and
  echoes the received value truncated to 64 characters. It signals with an exception rather than choosing
  a rendering, because the two endpoints render errors differently.
- `PostTimeSeriesQueryHandler` - catches it and answers `new ExecutionResponse(400, {"error": ...})`, the
  same shape as the `Type '...' does not exist` and `Field '...' not found in type` refusals beside it, so
  the text survives production mode.
- `PostGrafanaQueryHandler` - catches it and answers `buildErrorFrame(...)` for that target, so the other
  targets in the request are still served.
- `TimeSeriesApiSpec` / `GrafanaApiSpec` - the `type` property description now states that the field is
  required, lists the five values, and says the match is case-insensitive; the Grafana one also states
  that a bad value is reported as an error frame on that target.

## Reachability

```
$ grep -rn "PostTimeSeriesQueryHandler\|PostGrafanaQueryHandler" --include="*.java" server/src/main/java
server/src/main/java/com/arcadedb/server/http/HttpServer.java:254  .post("/ts/{database}/query", new PostTimeSeriesQueryHandler(this))
server/src/main/java/com/arcadedb/server/http/HttpServer.java:258  .post("/ts/{database}/grafana/query", new PostGrafanaQueryHandler(this))
```

Both handlers are constructed unconditionally and bound to live routes; no flag gates either one.

## Test results

`mvn -o -pl server test -Dtest='Issue7325AggregationTypeResolverTest,TimeSeriesApiSpecTest,GrafanaApiSpecTest,OpenApiSpecGeneratorTest'`

```
Tests run: 27, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

The new tests were proved able to fail: with the resolver mutated to drop the upper-casing and to drop the
field name from the message, the same run reported `Tests run: 6, Failures: 2, Errors: 1` - the
case-insensitivity test errored with `bad type: SUM, AVG, MIN, MAX, COUNT: received 'sum'` and the two
message tests failed. The mutation was reverted and the suite is green again.

### The two endpoint ITs were not executed locally

`TimeSeriesQueryHandlerIT` and `GrafanaTimeSeriesHandlerIT` extend `BaseGraphServerTest`, whose HTTP
requests are addressed to a hard-coded `127.0.0.1:248<serverIndex>`. On this machine ports 2480 and 2481
were already held:

```
$ lsof -nP -iTCP:2480 -sTCP:LISTEN
java  93797  ... TCP *:2480 (LISTEN)   # com.arcadedb.server.ArcadeDBServer, homebrew 26.9.1, up 5 days
$ lsof -nP -iTCP:2481 -sTCP:LISTEN
java  45616  ... TCP *:2481 (LISTEN)
```

`SERVER_HTTP_INCOMING_PORT` defaults to the range `2480-2489`, so a test server started here binds 2482
while the test still addresses 2480 - the requests would reach the long-running server instead, fail
authentication, and count against its brute-force lockout. The ITs were therefore compiled but not run
locally (`mvn -o -pl server test` compiles the full test tree, which is what proved they compile); they
run in CI, where the ports are free. The resolver unit test covers the shared logic without a server.

## Adversarial pass

The orchestrator's Phase 1.5 spawns a subagent that has not seen this document. No `Task` tool was
available in this session, so the pass was run against the diff directly rather than by a second agent -
which is weaker, because the reader had already been convinced. Recorded here rather than skipped.

What it produced:

1. **The residual-risk paragraph deserves an issue, not just a paragraph.** `bucketInterval`, `requests`
   and `field` are read with bare `JSONObject` getters, whose `JSONException` the mapper answers as
   `400 "Invalid JSON payload"` with the specifics in the concealed `detail` field - and on the Grafana
   endpoint that throw escapes the per-target loop, which is exactly the second half of what this PR fixes
   for `type`. Verified by reading `JSONObject.getNotNullElement:728` and the mapper's `JSONException` arm
   at `AbstractServerHttpHandler:818-823`. **Filed as #7340**, out of scope here.
2. **The Grafana endpoint's status for a bad aggregation type changes from 400 to 200-with-an-error-frame.**
   Real, intended, and what the issue asked for ("its half needs to route through that instead of a plain
   400"). Checked that no existing test pins the old status: the only `postGrafanaQueryRaw(...) == 400`
   assertion in `GrafanaTimeSeriesHandlerIT` is `missingTargets`, which this does not touch, and
   `grep -rn "Cannot execute command" server/src/test/.../{TimeSeries,Grafana}*IT.java` returns nothing.
3. **"is required and must be one of ..." reads oddly when a value WAS supplied.** Not real enough to
   change: the wording deliberately mirrors the gRPC message the issue quotes, and the trailing
   `: received '<value>'` clause disambiguates the two cases.
4. **Echoing the caller's value into an error body.** Both renderings build the body with `JSONObject`, so
   the value is escaped, and the echo is truncated to 64 characters. Not a finding, but the truncation is
   pinned by a test so it stays that way.

## Pull request

https://github.com/ArcadeData/arcadedb/pull/7343

## Review cycles

### Cycle 1 - `0360f11`

The `claude` review traced the PR's claims against the code and confirmed each one (the mapper's
`IllegalArgumentException` arm and the production concealment of `detail`, the declaration order of
`AggregationType`, `opt` vs `getString` for the absent key, the tightly-scoped `try` blocks, the escaping of
the echoed value). It raised three observations and marked all three explicitly non-blocking:

1. `req.getString("field")` still throws unguarded, ahead of the new resolver call - the reviewer noted this
   is scoped out and filed as #7340, and called that a reasonable boundary. **No change**: it is #7340.
2. "is required and must be one of ..." reads oddly when a value was supplied - the reviewer wrote "not
   asking for a change". **No change**: the wording mirrors the gRPC message the issue quotes, and the
   `: received '<value>'` clause separates the two cases.
3. `unknownAggregationType` passes a `null` cause when the field is absent - "harmless ... not worth a
   change". **No change**: `IllegalArgumentException(message, null)` is equivalent to the single-argument
   constructor, and one call site keeps the real cause.

Nothing in the review was actionable, so no code changed in response to it. What did change in the follow-up
commit came from CI rather than from the review:

- **Codacy Static Code Analysis** reported `Issues Added 1` on this head. Scanning the added lines for the
  patterns Codacy's Java rule set flags found exactly one candidate,
  `expected.name().toLowerCase()` in the new unit test - a locale-less case conversion
  (`UseLocaleWithCaseConversions`). Fixed to `toLowerCase(Locale.ENGLISH)`, matching the
  `toUpperCase(Locale.ENGLISH)` the resolver itself already used.
- `MAX_ECHOED_VALUE_LENGTH` was declared between the private constructor and the first method; moved to the
  top of the class, where a field declaration belongs.

The reviewer also asked that CI be confirmed green before merge, since the two endpoint ITs run only there.
That is the developer's check at merge time, and it is the reason the "not run locally" note is in the PR
body rather than buried here.

## Final state

`clean-approval` - one review cycle, no actionable review items, no deferred items.
