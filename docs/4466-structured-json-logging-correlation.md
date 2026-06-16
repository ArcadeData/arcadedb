# Issue #4466 - Observability Pillar 3: Structured (JSON) logging + correlation IDs

**Branch:** `feat/4466-structured-json-logging`
**Type:** feature (label `feature`)
**Parent:** #4463 (Cloud Observability Architecture). Pillars 1 (#4465), 2 (#4467), 4 already merged on `main`.

## Goal

Opt-in structured (JSON) logging plus a per-request correlation context (`requestId`, `db`, and
`traceId`/`spanId` when tracing is active) propagated through a thread-local and injected into log
lines, with byte-identical default text output preserved.

## Verification strategy

TDD. New tests first, then implement:
- `engine`: `LogCorrelationContextTest`, `JsonLogFormatterTest`, `LogFormatterTraceTagTest`,
  `DefaultFormatterUnchangedTest`; existing `LogFormatterMessageFormatTest` must stay green.
- `server`: `LogCorrelationLeakIT` (X-Request-Id echo + per-request clear).
- `tracing`: `TraceContextSupplierIT` (supplier yields a real traceId when a span is active).

## Design decisions

- **Correlation thread-local.** `LogManager` gains a parallel `ThreadLocal<Correlation>` (record:
  `requestId, database, traceId, spanId`) next to the existing `LogContext` string thread-local.
  The existing `getContext()/setContext(String)` are untouched.
- **traceId source.** Server core has NO OpenTelemetry/micrometer-tracing dependency (only the
  optional `tracing` plugin does). To populate `traceId`/`spanId` when tracing is active without a
  new core dependency, `LogManager` exposes a `TraceContextSupplier` SPI (default unset -> null).
  `TracingPlugin` registers a supplier reading `Tracer.currentSpan()` on `configure()` and clears it
  on `stopService()`. With no plugin: supplier null -> traceId/spanId null -> requestId still works.
- **JSON formatter.** `JsonLogFormatter extends LogFormatter` so the dual MessageFormat `{0}` /
  printf `%s` substitution (`formatMessage` + `SAFE_PRINTF_PATTERN`) is inherited unchanged. Built
  with in-tree `JSONObject` whose `toString()` is compact single-line. No new dependency.
- **Formatter selection.** `DefaultLogger.installCustomFormatter()` picks `JsonLogFormatter` when
  `arcadedb.server.logFormat=json`, else the existing `AnsiLogFormatter`.
- **Text-mode trace tag.** `LogFormatter`/`AnsiLogFormatter` append `[traceId=...]` only when
  `arcadedb.server.logIncludeTrace=true` AND a trace is active. The config read happens only when a
  traceId is present, so the default path (traceId null) is zero-overhead and byte-identical.

## Config keys

| Key | Default | Effect |
|---|---|---|
| `arcadedb.server.logFormat` | `text` | `json` selects `JsonLogFormatter` |
| `arcadedb.server.logIncludeTrace` | `false` | append `[traceId=...]` in text mode |

## Changes made

**engine**
- `GlobalConfiguration`: added `SERVER_LOG_FORMAT` (default `text`) and `SERVER_LOG_INCLUDE_TRACE`
  (default `false`).
- `LogManager`: added `Correlation` record, a parallel `ThreadLocal<Correlation>`, accessors
  (`setCorrelation/getCorrelation/clearCorrelation/getRequestId/getDatabaseContext/getTraceId/getSpanId`),
  and the `TraceContextSupplier` SPI (`setTraceContextSupplier`, `currentTraceContext`).
- `JsonLogFormatter` (new): one compact JSON line per record; reuses inherited `formatMessage` for the
  dual MessageFormat/printf substitution; includes correlation fields only when present; exception as a
  single JSON-escaped field.
- `LogFormatter`: added `appendTraceTag(StringBuilder)` (text-mode `[traceId=...]`, gated by config +
  active trace); removed a pre-existing duplicate `Formatter` import.
- `AnsiLogFormatter`: calls `appendTraceTag` (it overrides `customFormatMessage`).
- `DefaultLogger`: `selectConsoleFormatter()` picks `JsonLogFormatter` when `logFormat=json`, else
  `AnsiLogFormatter`; both install branches use it.

**server**
- `AbstractServerHttpHandler.handleRequest`: populates correlation per request (requestId from
  `X-Request-Id` or generated UUID, `db` from path template, traceId/spanId from the supplier), echoes
  `X-Request-Id` on the response, clears correlation in the `finally`.

**tracing**
- `TracingPlugin`: registers a `TraceContextSupplier` reading `Tracer.currentSpan()` (returns null for
  no/invalid span) on attach; clears it on `stopService()`.

## Test results (all green)

- engine logging/formatter sweep: 110 tests pass, incl. existing `LogFormatterMessageFormatTest` (8),
  `LoggerTest` (5), `DefaultLoggerLogDirTest` (4), and the 4 new test classes
  (`LogCorrelationContextTest` 6, `JsonLogFormatterTest` 6, `LogFormatterTraceTagTest` 3,
  `DefaultFormatterUnchangedTest` 4).
- tracing: `TracingPluginTest` 4 (incl. new supplier test), `TraceparentPropagationTest` 1,
  `ServerTracingIT` 2.
- server: `LogCorrelationIT` 3 (echo, generation, no-leak), `HttpObservationIT` 1, plus
  `QueryEndpointReadOnlyTest` 3 and `IdempotencyCacheTest` 8 as regression guards.

## Acceptance criteria

- [x] Per-request correlation context (`traceId`, `spanId`, `db`, `requestId`) populated and cleared safely.
- [x] `JsonLogFormatter` behind `arcadedb.server.logFormat=json`, built on `JSONObject`.
- [x] Dual-style substitution preserved; `LogFormatterMessageFormatTest` green.
- [x] Optional `[traceId=...]` text-mode tag.
- [x] Default text output unchanged; tests (incl. leakage + retro-compat) green.

## Impact / notes

- Default behavior unchanged: `logFormat=text`, `logIncludeTrace=false`, no tracer -> correlation
  fields null, text output byte-identical (the trace-tag path does no work when traceId is null).
- No new dependency: JSON via in-tree `JSONObject`; engine takes no OTel dependency (supplier SPI).
- `X-Request-Id` is now echoed on every HTTP response (generated when the client omits it). This is
  additive and verified not to disturb idempotency or existing handlers.
