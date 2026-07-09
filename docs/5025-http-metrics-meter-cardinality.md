# Issue #5025 - HTTP metrics: unbounded Micrometer meter cardinality + per-request meter allocation

## Symptom
- Defect 1 (HIGH): `AbstractServerHttpHandler.pathTemplate` falls back to the raw client URI
  (`exchange.getRelativePath()`) when no route template matches. The Studio `/` fallback route and any
  unmatched/404 URI hit this branch, so every distinct client path permanently registers a new
  percentile-histogram `Timer` in `Metrics.globalRegistry` - unbounded heap growth driven by client input.
- Defect 2 (MEDIUM): every request/query rebuilds a `Timer.Builder`, tag strings, `Meter.Id` and does a
  registry hash lookup on the hottest paths (`AbstractServerHttpHandler` finally block,
  `MicrometerQueryMetricsRecorder.record`), producing steady per-request garbage.
- Defect 3 (LOW): `GetServerHandler` walks the whole registry per Studio poll; mostly resolved once
  Defect 1 bounds the meter count.

## Root cause
`pathTemplate` returns attacker-controlled URIs as a metric tag value, and the RED timer is (re)built
per request instead of resolved once per tag tuple.

## Fix
- `pathTemplate` returns the constant `"unmatched"` when there is no `PathTemplateMatch` (fixed
  `/api/v1/*` routes are registered through Undertow's `RoutingHandler`, which always attaches a match,
  so only prefix/fallback/404 traffic reaches this branch).
- Cache resolved timers per bounded tag tuple in a `ConcurrentHashMap`
  (`AbstractServerHttpHandler.httpRequestTimer`, `MicrometerQueryMetricsRecorder.queryTimer`).
- Registry backstop `MeterFilter.maximumAllowableTags("arcadedb.http.requests", "path", 100, deny())`
  installed when server metrics start.

## Tests
- `AbstractServerHttpHandlerMetricsTest` - unit: `pathTemplate` collapses unmatched URIs to `"unmatched"`,
  keeps the route template when matched, and `httpRequestTimer` returns the same cached instance per tuple.
- `MicrometerQueryMetricsRecorderTest` - unit: `queryTimer` cache-hit per tuple.
- `HttpRedMetricsIT.unmatchedUrisCollapseToBoundedPathTag` - IT: hammering 50 distinct unmatched URIs
  registers zero raw-path meters and one collapsed `"unmatched"` meter.

## Impact
Removes a client-driven OOM/DoS vector and per-request meter allocation on the HTTP + query hot paths.
No public API change; the `path` tag for non-templated routes changes from raw URI to `"unmatched"`.
