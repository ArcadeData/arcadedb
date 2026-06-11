# Issue #4465 - Observability Pillar 1: Metrics Depth

**Branch:** `feat/4465-observability-pillar-1-metrics-depth`
**Type:** feature
**Parent:** #4463 (Cloud Observability Architecture)
**Spec:** `docs/superpowers/specs/2026-06-03-observability-cloud-design.md`
**Plan:** `docs/superpowers/plans/2026-06-03-observability-p1-metrics.md`

## Goal

Add always-on RED (Rate/Errors/Duration) latency timers on the HTTP and query paths,
bridge engine `Profiler` stats into Micrometer gauges (`EngineMetricsBinder`), and add an
optional OTLP metrics registry - all surfaced in the existing `/prometheus` endpoint without
changing its current series. Default-off / behavior-preserving.

## Verification strategy

TDD. JUnit 5 + AssertJ. Per task: write a failing test, implement, confirm green, then run
the broader `*Metrics*` suites in `server` and `metrics`. Retro-compat guarded by asserting
the pre-existing `system_cpu_usage` series still renders in `/prometheus`.

## Pre-implementation analysis (deviations from the plan)

The plan was followed structurally; the following facts were verified against the code and
required adjustments to the plan's literal snippets:

1. **`Profiler.toJSON()` nests every stat** as `{count|space|value: N}` (e.g.
   `pageCacheHits -> {"count": N}`, `walBytesWritten -> {"space": N}`); only `walTotalFiles`
   is a bare scalar. The plan's `json.getLong(key, 0L)` would throw on the nested object, so
   `EngineMetricsBinder` reads the inner numeric (`count`/`space`/`value`).
2. **Test root password** is `BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS`
   (`"DefaultPasswordForTests"`), not `root:root` as the plan's snippet hard-coded.
3. **HTTP `path` tag** uses Undertow's `PathTemplateMatch` matched template (e.g.
   `/command/{database}`) instead of the plan's naive last-segment collapse, which would have
   mangled fixed endpoints like `/ready`. A `db` tag is added from the `database` path param
   (issue asks for `method`, `path-template`, `status`, `db`).
4. **Per-database gauge tagging:** the metrics block binds at `ArcadeDBServer` startup
   *before* `loadDatabases()`, and the page-cache / WAL / MVCC counters are JVM-global
   singletons (`PageManager.INSTANCE`), not per-DB. `EngineMetricsBinder` therefore exposes
   engine-wide gauges read live from `Profiler.INSTANCE` on each scrape (the Profiler
   aggregates whatever databases are registered at scrape time). Per-database-tagged
   breakdown would require a scheduler-refreshed `MultiGauge`; deferred as a scoped follow-up
   (noted on the issue). DB-level counters (writeTx/readTx/queries/commands/records) are still
   exposed as aggregate gauges.

## Tasks

- [x] T1: HTTP RED timer `arcadedb.http.requests` (AbstractServerHttpHandler)
- [x] T2: Query/command RED timer `arcadedb.query.duration` (Post{Query,Command}Handler)
- [x] T3: `EngineMetricsBinder` Profiler->gauges
- [x] T4: Bind `EngineMetricsBinder` at server startup
- [x] T5: Add `micrometer-registry-otlp` dependency
- [x] T6: `OtlpMetricsPlugin` opt-in
- [x] T7: `/prometheus` retro-compat + engine gauges IT
- [x] T8: Run broader affected suites

## Decisions & notes

- **Micrometer weak-gauge trap (caught by T7).** `Gauge.builder(name, obj, fn)` keeps a *weak*
  reference to `obj`. `EngineMetricsBinder` is constructed inline in `ArcadeDBServer` and not
  stored, so the instance was garbage-collected between startup and the first scrape and its
  gauges silently went dead (the `/prometheus` body had the pool gauges but not the engine
  gauges). Switched to the `Gauge.builder(name, Supplier<Number>)` form, whose captured lambda
  strongly references the binder - mirroring how `PoolMetrics` survives by capturing long-lived
  singletons. This was a real production defect, not just a test artifact.
- **Config value coercion.** `ContextConfiguration.getValue(key, default)` infers its return type
  from the default; a `Boolean` stored via `setValue` would `ClassCastException` against a
  `String` default. `OtlpMetricsPlugin` reads with an `Object` default and coerces, accepting
  both `Boolean` (programmatic / tests) and `String` (config file / system property).
- **`SERVER_METRICS` defaults to `true`**, so "default-off" for OTLP is enforced by the
  `arcadedb.serverMetrics.otlp.enabled` flag (default false) plus the plugin not being listed in
  `SERVER_PLUGINS`. Verified the plugin is a no-op unless both flags are on.
- **Snapshot caching.** `Profiler.toJSON()` is heavy (JMX, free-disk, per-DB iteration). The
  binder memoizes the snapshot for 1s so a scrape rebuilds it at most once instead of once per
  gauge.

## Final summary

All 8 tasks implemented TDD-first and green. New always-on RED timers `arcadedb.http.requests`
(tagged `method`/`path-template`/`status`/`db`) and `arcadedb.query.duration` (tagged
`db`/`language`/`type`); `EngineMetricsBinder` exposes `arcadedb.engine.*` gauges read live from
`Profiler`; opt-in `OtlpMetricsPlugin` (default-off) pushes alongside the untouched `/prometheus`
scrape. Path tag uses Undertow's `PathTemplateMatch` so cardinality stays bounded (guarded by a
test asserting the raw database name never appears in a path tag).

**Test results:**
- New: `HttpRedMetricsIT`, `QueryRedMetricsIT` (x2), `EngineMetricsBinderTest` (x2),
  `EngineMetricsExposedIT`, `OtlpMetricsPluginTest` (x3), `PrometheusEngineMetricsIT` - all green.
- Regression: full `metrics` module (5, incl. both existing Prometheus tests) + 58 server HTTP
  handler tests (`HealthProbesIT`, `AutoCommitParameterTest`, `QueryEndpointReadOnlyTest`,
  `ApiTokenAuthenticationIT`, `HTTPAuthSessionIT`, etc.) - all green.

**Retro-compat:** `/prometheus` still serves and keeps `system_cpu_usage`; all new series are
additive; OTLP confined to the optional `metrics` module and off by default.

**Scoped follow-up (documented on the issue):** per-database-*tagged* gauge breakdown is deferred -
the metrics binder binds before `loadDatabases()` and the page-cache/WAL counters are JVM-global,
so a periodically-refreshed `MultiGauge` would be required. DB-level counters are exposed as
engine-wide aggregates here. Transaction-commit and Raft Observations are deferred to Pillar 2,
where they also yield spans (tx latency is captured at the HTTP boundary in this pillar).
