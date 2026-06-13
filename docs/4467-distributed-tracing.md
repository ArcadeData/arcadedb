# Issue #4467 - Observability Pillar 2: Distributed Tracing (OpenTelemetry)

**Branch:** `feat/4467-distributed-tracing`
**Type:** feature
**Spec:** `docs/superpowers/specs/2026-06-03-observability-cloud-design.md`
**Plan:** `docs/superpowers/plans/2026-06-03-observability-p2-tracing.md`
**Parent:** #4463 (Cloud Observability Architecture). Depends on Pillar 1 (#4465, merged via PR #4532).

## Goal

Add a new optional Maven module `tracing` that, when enabled, registers an OpenTelemetry tracer
into Micrometer's `ObservationRegistry` so the HTTP instrumentation point produces OTLP-exported
spans - with the OTel SDK kept entirely off the core/server compile classpath.

## State of Pillar 1 on main (verified before starting)

- HTTP latency is recorded by a **direct `Timer.builder("arcadedb.http.requests")`** in
  `AbstractServerHttpHandler.handleRequest`'s `finally` block (tags: `method`, `path`, `status`,
  `db`; `pathTemplate(exchange)` + `databaseTag(exchange)` helpers). It is **not** an `Observation`.
- That timer is asserted by `server/.../monitor/HttpRedMetricsIT` and
  `metrics/.../prometheus/PrometheusEngineMetricsIT`.
- No `ObservationRegistry` exists on `ArcadeDBServer`.
- No `arcadedb.serverMetrics.tracing.*` keys in `GlobalConfiguration` (only `SERVER_METRICS`,
  `SERVER_METRICS_LOGGING`).
- `LogManager` has `setContext(String)`/`getContext()`; **no** `clearCorrelation()` (idiom is
  `setContext(null)`, already called in the handler's `finally`).
- `metrics/pom.xml` parent uses a **literal** `26.7.1-SNAPSHOT` (not `${revision}`); micrometer
  pinned by `${micrometer.version}=1.16.5`; no micrometer-bom import.
- SPI: `metrics/src/main/resources/META-INF/services/com.arcadedb.server.ServerPlugin` lists the
  two metrics plugins. `ServerPlugin` requires only `startService()`; `configure`, `stopService`
  are defaults.
- Builder: `SHADED_MODULES="gremlin redisw mongodbw postgresw grpcw metrics"`; `get_module_description()`
  case block lists each module.

## Key design decisions (deviations from the plan, justified)

1. **Do not register `DefaultMeterObservationHandler` on the server registry; do not remove or
   rename Pillar 1's `arcadedb.http.requests` timer.** The plan proposed a second
   `arcadedb.http.server.requests` meter + deleting/editing Pillar 1's test. That is rejected
   because (a) project constraint forbids modifying existing tests, and (b) it would add a new
   timer to `/prometheus` even when tracing is disabled (default), weakening the "default-off =
   byte-for-byte identical" guarantee. Instead the Observation is **span-only**: with no tracer
   attached the server's `ObservationRegistry` has zero handlers, so the wrap is a true no-op and
   metrics come solely from Pillar 1's untouched timer. When the tracing plugin attaches its
   tracing handler, the same wrapped code emits spans. Single instrumentation point preserved;
   no duplicate metrics.

2. **Raft outbound trace-context propagation is deferred** (per the plan's own self-review note):
   it requires Observations inside `ArcadeStateMachine.applyTransaction` plus trace-context
   serialization into the Raft log entry - a larger change beyond this pillar's minimum. Inbound
   HTTP `traceparent` continuation and in-process span nesting are delivered. Documented as a
   known limitation.

## How this change is verified

- `tracing` module unit tests with OTel `InMemorySpanExporter`: spans created for Observations;
  inbound `traceparent` continuation; **zero** spans when no tracer attached.
- `server` IT: `ObservationRegistry` exposed; HTTP request drives the Observation (probe handler).
- Classpath isolation: `mvn -pl server dependency:tree | grep opentelemetry` -> none.
- Retro-compat: existing `HttpRedMetricsIT` / `PrometheusEngineMetricsIT` stay green untouched.

## Files changed

- `pom.xml` - `micrometer-tracing` (1.6.5) + `opentelemetry` (1.55.0) version properties, two
  import-scope BOMs, `tracing` module registration.
- `tracing/` (new module) - `pom.xml`, SPI registration, `package-info`, `TracingPlugin`, tests
  (`TracingPluginTest`, `TraceparentPropagationTest`, `ServerTracingIT`).
- `engine/.../GlobalConfiguration.java` - `SERVER_METRICS_TRACING_ENABLED/ENDPOINT/SAMPLING_RATE`.
- `server/.../ArcadeDBServer.java` - shared `ObservationRegistry` + getter.
- `server/.../http/handler/AbstractServerHttpHandler.java` - span-only Observation wrapper +
  lazy `RequestReplyReceiverContext` for inbound `traceparent`. Pillar 1 timer untouched.
- `server/.../ObservationRegistryIT.java`, `.../http/HttpObservationIT.java` - new server tests.
- `package/arcadedb-builder.sh` - `tracing` in `SHADED_MODULES` + module description.
- `ATTRIBUTIONS.md`, `NOTICE` - OpenTelemetry + Micrometer Tracing (Apache-2.0; folded the
  micrometer-tracing NOTICE).

## Test results

- `tracing` module: `TracingPluginTest` (2), `TraceparentPropagationTest` (1), `ServerTracingIT` (1) - green (verify/failsafe).
- `server`: `ObservationRegistryIT` (1), `HttpObservationIT` (1) green; existing `HttpRedMetricsIT` (2)
  and 433 server unit tests green (no regressions).
- `metrics`: 5 tests green (retro-compat: `/prometheus` + OTLP unaffected).
- `engine` `ConfigurationTest`: green (new keys don't break config snapshot).
- Classpath isolation: `mvn -pl server dependency:tree | grep -i opentelemetry` -> none.
- Version alignment: tracing module resolves `micrometer-observation:1.16.5` (matches core).

## Self-review (1 cycle, code-reviewer agent)

Applied: BatchSpanProcessor in production (non-blocking export; tests use SimpleSpanProcessor seam);
sampling key `Float`-typed to avoid double->float precision loss; single `getTracer` local; single
`attachForTest` seam; `isActive()` returns `enabled`; endpoint null/blank guard; effective
endpoint+samplingRate logged at startup; micrometer-tracing pinned to 1.6.5 to align with core
1.16.5. Deferred (per plan): outbound Raft span propagation.

## Progress log

- T0: analysis complete; tracking doc created.
- T1-T10: module scaffolded, config keys, ObservationRegistry, HTTP Observation, TracingPlugin,
  traceparent continuation, builder flag, attributions, full build + isolation verified, self-review
  applied. Complete.
- Code review (Gemini bot, PR #4604): isolated optional-tracing cleanup from the core HTTP path
  (per-step try/catch) and added graceful endpoint-init failure handling in TracingPlugin.

## Multi-protocol tracing (extension beyond HTTP)

Goal: cover the other wire protocols (Postgres/Bolt/Redis/Mongo/gRPC), not just HTTP.

Design: a single **engine-boundary** instrumentation point rather than five handler passes, mirroring
the existing `QueryMetricsRecorder` decoupling (engine has Micrometer only at test scope):

- **`engine` `QueryTracer`** SPI (`com.arcadedb.database`): `beginQuery(protocol,db,language,type,query)`
  returns a `Span` (AutoCloseable); `Holder` is a volatile NO_OP-default registry; `Holder.begin(...)`
  reads the protocol from `ProtocolContext`. No Micrometer types in engine main.
- **`LocalDatabase.query()/command()`** (the 7 terminal overloads) wrap execution in
  `try (QueryTracer.Span span = QueryTracer.Holder.begin(...))`, alongside the existing metric recorder.
  Every protocol that funnels through `Database.query/command` (HTTP, Bolt, Postgres, Mongo, gRPC) is
  covered from this one point; spans nest under the HTTP request span and are root spans otherwise.
- **`server` `MicrometerQueryTracer`** opens a span-only `Observation` (`arcadedb.query`) on the shared
  registry: low-card tags `protocol/db/language/type`; query text as a high-card span attribute
  (`db.statement`). `isNoop()` short-circuit => zero allocation when tracing is off. Registered
  unconditionally in `ArcadeDBServer`. Defensive close (failure can't disturb the query path).
- **Redis**: `RedisNetworkExecutor.executeCommand` maps commands directly to engine ops (it does not
  call `Database.query/command`), so a `QueryTracer` span is opened there directly (protocol=redis,
  type=command, statement=command verb).
- **Deferred (per spec)**: gRPC inbound `traceparent` continuation (needs OTel on the gRPC
  interceptor + io.grpc.Metadata extraction). Query spans still give gRPC root spans.

Tests: `LocalDatabaseQueryTracerTest` (engine, fake tracer: protocol/language/type/query + span
close, and NO_OP default); `ServerTracingIT.httpCommandProducesQuerySpanNestedUnderTheHttpSpan`
(real server: HTTP POST /command -> `arcadedb.query` span tagged protocol=http, child of the HTTP
span). Regression: 157 SQL execution tests, redisw 32, server HTTP ITs - all green.
