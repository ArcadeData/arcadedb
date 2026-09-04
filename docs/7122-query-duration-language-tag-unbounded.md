# #7122 — `arcadedb.query.duration` registers a Timer per client-supplied `language` string

## Problem

`MicrometerQueryMetricsRecorder.queryTimer()` builds one Micrometer `Timer` per
`protocol|db|language|type` tuple and caches it in a static `ConcurrentHashMap`. The class comment
called the tuple "bounded", which holds for `protocol` (a fixed set of constants passed to
`ProtocolContext.set`) and `type` (`query`/`command`) but **not** for `language`: the value travels
straight from the caller — `db.query("<language>", …)`, the `{lang}` path segment of the HTTP API,
the Postgres portal language — and reaches the tag with no validation.

Every distinct string therefore registers a permanent percentile-histogram `Timer` in the static
`Metrics.globalRegistry` **and** a permanent entry in the recorder's own cache. Micrometer never
evicts, so heap and `/metrics` grow without bound. This is the shape #6805 fixed for the `db` tag of
`arcadedb.http.requests`, at a meter #6805 did not cover.

## Root cause

Three gaps, the same three #6805 had:

1. no validation of `language` before it becomes a tag value;
2. no `MeterFilter.maximumAllowableTags` backstop for `arcadedb.query.duration` in
   `ArcadeDBServer.startMetrics()` (the two existing filters cover only `arcadedb.http.requests`);
3. no ceiling on the recorder's own cache — a `MeterFilter` can deny the meter but cannot stop
   `computeIfAbsent` from retaining the key.

## Fix

- `QueryEngineManager.isLanguageRegistered(String)` — a lock-free lookup against the copy-on-write
  engine map, so "is this a real language?" is answered by the one component that owns the answer.
- `MicrometerQueryMetricsRecorder.languageTag()` lowercases the value (so case permutations of a
  real language cannot multiply the tag space) and collapses anything unregistered onto the constant
  `unknown`.
- `queryTimer()` looks the raw key up first (unchanged hot path: one concat, one `get`), and only on
  a miss bounds the tags — so a bogus language is never retained as a cache key. Past
  `MAX_QUERY_TIMERS` the `db` half collapses onto `other`, the remaining dimension that can grow
  through database create/drop churn.
- `ArcadeDBServer.startMetrics()` installs two more `MeterFilter`s, on the `language` and `db` tags
  of `arcadedb.query.duration`, sized from the recorder's own public constants so the registry-side
  and cache-side bounds stay one number rather than two independently chosen ones.
- `MicrometerQueryTracer` routes its `language` low-cardinality key through the same helper, so an
  attached observation handler that turns keys into meter tags inherits the same bound.

## Also in this sweep

`LoadCSVStep.openReader()` leaked the raw `InputStream` when the `.gz`/`.zip` wrapper threw: the
outer stream was already open and nothing closed it on that path. The empty-ZIP refusal also escaped
as `CommandExecutionException`, which the caller's `IOException` arm does not catch, so it surfaced
without the "Error opening CSV file: <url>" context every other open failure carries. Both fixed
with one try/catch and an `IOException`.

## Why the leak is reachable

`LocalDatabase.query()/command()` record in a `finally`, so the timer is registered even when
`getQueryEngine(language)` throws `Query engine '<language>' was not found`. An unauthenticated
`GET /api/v1/query/{db}/{lang}/{query}` that fails therefore still minted a permanent meter, which is
what makes the repro in the issue work.

## Verification

Every new test was run against the unfixed code first and fails there:

| Test | Unfixed result |
|---|---|
| `anUnregisteredLanguageCollapsesToABoundedConstant` | `expected: "unknown"` |
| `anUnregisteredLanguageIsNeverRetainedAsACacheKey` | `expected: 1` (500 entries retained) |
| `languageTagIsCaseNormalized` | two distinct timers |
| `aNullLanguageCollapsesInsteadOfBeingTaggedNull` | `NullPointerException` |
| `theDatabaseTagCollapsesOnceTheCacheCeilingIsReached` | `expected: "other"` |
| `aCorruptGzipHeaderDoesNotStrandTheRawStream` | stream left open |
| `anEmptyZipDoesNotStrandTheRawStream` | `CommandExecutionException`, stream left open |

Suites run green after the fix:

- `server`: `MicrometerQueryMetricsRecorderTest` (8), `ArcadeDBServerMetricsFiltersTest` (3),
  `ServerMetricsLifecycleTest` (7), `AbstractServerHttpHandlerMetricsTest` (6)
- `engine`: `LoadCSVStepOpenReaderTest` (3), `OpenCypherLoadCSVTest` (26),
  `OpenCypherLoadCsvFunctionsComprehensiveTest` (16), `CypherLoadCSVRowContextIssue6402Test` (5),
  `QueryEngineManagerLanguagesTest` (5), `QueryEngineManagerPoolTest` (4), `QueryMetricsRecorderTest` (3),
  `LocalDatabaseQueryMetricsTest` (1), `LocalDatabaseQueryTracerTest` (2), `ProtocolContextTest` (3),
  `DedicatedThreadPoolTest` (11)

## Performance

The hot path is unchanged for an already-seen tuple: one string concatenation and one
`ConcurrentHashMap.get`, exactly as before. `languageTag()` — a `toLowerCase` scan (which returns
`this` when nothing changes) plus one volatile read and one hash lookup — runs only on a cache miss,
and a miss already pays for a `Timer.Builder`, a `Tags` list and a registry registration.
