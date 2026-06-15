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

## Packaging

The optional `tracing` module ships in the **full** distribution (`arcadedb-*.tar.gz` and the
`arcadedata/arcadedb` Docker image), mirroring `metrics`: `package/pom.xml` depends on
`arcadedb-tracing` (shaded classifier), and `package/src/main/assembly/full.xml` excludes it from the
wildcard `/lib` set then re-includes it in the plugin set. The lean flavors (`minimal`, `headless`,
`base`) exclude it (like `metrics`), so the OTel SDK is not carried there. The OTel SDK is bundled
inside the tracing shaded jar (not loose in `/lib`). The modular builder (`arcadedb-builder.sh`) also
exposes it via `SHADED_MODULES` for custom distributions.

Version alignment: `micrometer-tracing` tracks the `micrometer` line - with `micrometer` at `1.17.0`,
`micrometer-tracing` is `1.7.0` (OTel `1.62.0`). The tracing BOM must match, otherwise its
`micrometer-bom` import would pin the whole reactor's micrometer-observation to the older line.

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

Test coverage: `LocalDatabaseQueryTracerTest` (engine, fake tracer: protocol/language/type/query +
span close, and the NO_OP default); `ServerTracingIT.httpCommandProducesQuerySpanNestedUnderTheHttpSpan`
(real server: HTTP POST /command -> `arcadedb.query` span tagged `protocol=http`, child of the HTTP
span).
