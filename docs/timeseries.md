# ArcadeDB TimeSeries Module — Research Report & Implementation Plan

## Implementation Progress (last updated: 2026-02-23)

### Completed
- **Phase 1: Core Storage Engine** — TimeSeries type, columnar storage with Gorilla/Delta-of-Delta/Simple-8b/Dictionary codecs, sealed bucket compaction, shard-per-core parallelism, Line Protocol ingestion (HTTP handler), retention policies, `CREATE TIMESERIES TYPE` SQL statement, `FetchFromTimeSeriesStep` query executor, basic SQL queries (`SELECT`, `WHERE`, `GROUP BY`, `ORDER BY`)
- **Phase 2: Analytical Functions** — All 9 timeseries SQL functions implemented and tested:
  - `ts.first(value, ts)` / `ts.last(value, ts)` — first/last value by timestamp
  - `ts.rate(value, ts [, counterResetDetection])` — per-second rate of change with optional counter reset detection (3rd param = `true` enables Prometheus-style reset handling for monotonic counters)
  - `ts.delta(value, ts)` — difference between first and last values
  - `ts.movingAvg(value, window)` — moving average with configurable window
  - `ts.interpolate(value, method [, timestamp])` — gap filling (zero/prev/linear/none methods; linear interpolation requires timestamp parameter)
  - `ts.correlate(a, b)` — Pearson correlation coefficient
  - `ts.timeBucket(interval, ts)` — time bucketing for GROUP BY aggregation
  - `ts.percentile(value, percentile)` — approximate percentile calculation (0.0-1.0, e.g. 0.95 for p95, 0.99 for p99) with sorted exact computation and linear rank interpolation
  - `ts.lag(value, offset, timestamp [, default])` — previous row value (window function)
  - `ts.lead(value, offset, timestamp [, default])` — next row value (window function)
  - `ts.rowNumber(timestamp)` — sequential 1-based row numbering (window function)
  - `ts.rank(value, timestamp)` — rank with ties, gaps after ties (window function)

- **Phase 3: Continuous Aggregates** — Watermark-based incremental refresh for pre-computed timeseries rollups:
  - `ContinuousAggregate` interface and `ContinuousAggregateImpl` with watermark tracking, atomic refresh guard, JSON persistence, metrics
  - `ContinuousAggregateRefresher` — incremental refresh: deletes stale buckets from watermark, re-runs filtered query, inserts results, advances watermark
  - `ContinuousAggregateBuilder` — fluent API with validation (source must be TimeSeries type, query must contain `ts.timeBucket()`, must have GROUP BY)
  - Schema integration: `LocalSchema` stores/persists CAs in JSON, protects source/backing types from drop, crash recovery (BUILDING→STALE on restart)
  - Post-commit trigger via `SaveElementStep.saveToTimeSeries()` → `TransactionContext.addAfterCommitCallbackIfAbsent()` schedules incremental refresh
  - SQL DDL: `CREATE CONTINUOUS AGGREGATE [IF NOT EXISTS] name AS select`, `DROP CONTINUOUS AGGREGATE [IF EXISTS] name`, `REFRESH CONTINUOUS AGGREGATE name`
  - Schema metadata: `SELECT FROM schema:continuousAggregates` returns name, query, sourceType, bucketColumn, bucketIntervalMs, watermarkTs, status, metrics
  - 19 tests (12 API + 7 SQL), all passing

- **Streaming Query Pipeline** — Full OOM fix for large dataset queries:
  - Lazy page-level iterators replacing materialized `List<Object[]>` throughout the query chain: `TimeSeriesBucket.iterateRange()` → `TimeSeriesSealedStore.iterateRange()` → `TimeSeriesShard.iterateRange()` → `TimeSeriesEngine.iterateQuery()`
  - Memory usage reduced from O(totalRows) to O(shardCount × blockSize) — constant memory regardless of dataset size
  - Merge-sort across shard iterators using `PriorityQueue<PeekableIterator>` min-heap sorted by timestamp
  - Binary search on sealed block directory for O(log B) block selection instead of linear scan
  - Binary search within blocks using `lowerBound()`/`upperBound()` on sorted timestamp arrays
  - Lazy column decompression: timestamps decoded first, value columns only if rows match time range
  - Early termination: stops scanning blocks once `minTimestamp > toTs`
  - Empty bucket short-circuit: `getSampleCount() == 0` skips scanning entirely (critical after compaction clears mutable pages)
  - Chunked compaction: writes 65K-row sealed blocks instead of one giant block per shard (configurable via `SEALED_BLOCK_SIZE`)
  - Sealed store directory persistence: inline block metadata (`BLOCK_MAGIC_VALUE = 0x5453424C`) enables cold queries after close/reopen without losing block index
  - Profiling integration: `FetchFromTimeSeriesStep` uses `context.isProfiling()` pattern with `cost` and `rowCount` accumulation, visible via `PROFILE SELECT ...`
  - `BitReader` optimization: byte-level batch reads instead of per-bit loop for faster codec decompression
- **Cold Open Persistence** — TimeSeries types and data survive database close/reopen:
  - `.tstb` file extension registered in `SUPPORTED_FILE_EXT` (FileManager) and `ComponentFactory` (schema loader)
  - `TimeSeriesBucket.PaginatedComponentFactoryHandler` creates stub buckets on load; columns set later via `setColumns()`
  - `TimeSeriesShard` constructor detects already-loaded buckets via `LocalSchema.getFileByName()` to avoid duplicate creation
  - `LocalSchema.readConfiguration()` calls `initEngine()` on `LocalTimeSeriesType` instances during deserialization
  - Sealed store block directory reconstructed from inline metadata on cold open (`loadDirectory()`)

- **Block-Level Aggregation Statistics** — Per-block min/max/sum statistics stored alongside compressed data:
  - `BlockEntry` stores `columnMins[]`, `columnMaxs[]`, `columnSums[]` for numeric columns
  - Fast path in `aggregateMultiBlocks()`: when entire block fits in a single time bucket, uses block stats directly — zero decompression
  - Stats section persisted in sealed store block header and reconstructed on cold open

- **Aggregation Performance Optimization** — 50M-row aggregation reduced from ~3,400ms to ~710ms (4.8x improvement):
  - `AggregationMetrics` instrumentation: timing breakdown per phase (I/O, timestamp decompression, value decompression, accumulation) with block category counters (fast/slow/skipped). Displayed in `PROFILE` output via `AggregateFromTimeSeriesStep`
  - Flat array accumulation in `MultiColumnAggregationResult`: pre-allocated `double[][]`/`long[][]` indexed by `(bucketTs - firstBucket) / interval`, eliminating 50M HashMap lookups and Long autoboxing. Data range computed from `getGlobalMinTimestamp()`/`getGlobalMaxTimestamp()` across shards
  - SIMD vectorized accumulation: slow path uses `TimeSeriesVectorOps.sum()/min()/max()` on contiguous timestamp segments within each block, turning 65,536 per-element operations into ~2 vectorized segment calls per block via binary search on bucket boundaries
  - Parallel shard processing: sealed stores processed concurrently via `CompletableFuture.supplyAsync()` per shard, results merged via `MultiColumnAggregationResult.mergeFrom()`. Mutable buckets processed sequentially on calling thread (requires database context)
  - Coalesced I/O: single `pread()` per block via `readBlockData()` reads all column data contiguously, then `sliceColumn()` extracts individual columns — halves syscall count
  - Reusable decode buffers: `long[65536]` and `double[65536]` allocated once per `aggregateMultiBlocks()` call, reused across all blocks. Buffer-reuse `decode()` overloads added to `DeltaOfDeltaCodec` and `GorillaXORCodec`
  - `BitReader` sliding-window register: pre-loaded 64-bit MSB-aligned window with lazy refill — `readBits(n)` extracts top n bits via single shift, refill amortized every ~7-8 bytes consumed. Eliminates per-call byte-assembly loop (decompVal 1305ms → 1224ms, ~6% improvement — JIT already optimized the old loop effectively)
  - Bucket-aligned compaction: `COMPACTION_INTERVAL` DDL option splits sealed blocks at time bucket boundaries during compaction, ensuring each block fits entirely within one bucket for 100% fast-path aggregation. SQL syntax: `CREATE TIMESERIES TYPE ... COMPACTION_INTERVAL 1 HOURS`. Config persisted in schema JSON and threaded through `TimeSeriesEngine` → `TimeSeriesShard`
  - 213 timeseries tests passing, zero regressions

- **Phase 4: Downsampling Policies** — Automatic resolution reduction for old data:
  - `DownsamplingTier` record: `afterMs` (age threshold) + `granularityMs` (target resolution), with validation
  - Schema persistence: `downsamplingTiers` field in `LocalTimeSeriesType` with JSON serialization/deserialization — backward-compatible with old schemas (null-safe `getJSONArray`)
  - Builder API: `TimeSeriesTypeBuilder.withDownsamplingTiers(List<DownsamplingTier>)` for programmatic type creation
  - DDL: `ALTER TIMESERIES TYPE <name> ADD DOWNSAMPLING POLICY AFTER <n> <unit> GRANULARITY <n> <unit> [AFTER ...]` and `ALTER TIMESERIES TYPE <name> DROP DOWNSAMPLING POLICY`
  - Grammar: 3 new lexer tokens (`DOWNSAMPLING`, `POLICY`, `GRANULARITY`), parser rules (`alterTimeSeriesTypeBody`, `downsamplingTierClause`, `tsTimeUnit`), soft-keyword registration
  - `AlterTimeSeriesTypeStatement` DDL statement with `SQLASTBuilder.visitAlterTimeSeriesTypeStmt()` visitor — time unit parsing reuses existing DAYS/HOURS/MINUTES tokens
  - `TimeSeriesEngine.applyDownsampling(tiers, nowMs)`: iterates tiers sorted by afterMs, identifies timestamp/tag/numeric column roles, delegates to sealed store per shard
  - `TimeSeriesSealedStore.downsampleBlocks()`: density-check idempotency (blocks already at target resolution are skipped), tag-grouped AVG aggregation per `(bucketTs, tagKey)`, atomic tmp-file rewrite with CRC32
  - Multi-tier behavior: tiers applied independently; density check naturally handles hierarchy (1min blocks pass 1min check but fail 1hr check when tier 2 cutoff reached)
  - 7 new tests: DDL add/drop with persistence across close/reopen, single-tier accuracy (AVG=30.5 for 1..60), multi-tier, idempotency, multi-tag grouping, retention interaction, empty engine no-op

- **Phase 5: HTTP API & Studio Integration** — Dedicated REST endpoints and web-based TimeSeries Explorer:
  - **REST Endpoints**: 3 dedicated timeseries HTTP handlers registered in `HttpServer`:
    - `POST /api/v1/ts/{database}/write` — InfluxDB Line Protocol ingestion with configurable precision (`ns`/`us`/`ms`/`s`), `requiresJsonPayload()=false` to avoid JSON parsing of plain-text body
    - `POST /api/v1/ts/{database}/query` — JSON query endpoint supporting raw queries (time range, field projection, tag filtering, limit) and aggregated queries (`AVG`/`SUM`/`MIN`/`MAX`/`COUNT` with configurable bucket intervals via `aggregateMulti()`)
    - `GET /api/v1/ts/{database}/latest?type=name&tag=key:value` — returns most recent data point with optional single-tag filter
  - **Studio TimeSeries Tab**: Full-featured explorer accessible from the main navigation sidebar:
    - Header bar: database selector (synced `.inputDatabase` class), TimeSeries type dropdown with live sample count, Create/Drop type buttons
    - **Query sub-tab**: single-row controls (Time Range, Aggregation, Bucket Interval, Field checkboxes), Query/Latest/Auto-refresh buttons, ApexCharts line/area chart with datetime x-axis and zoom, DataTable with pagination, chart/table toggle switches with per-database localStorage persistence, query execution time display
    - **Schema sub-tab**: detailed type introspection — TimeSeries Columns (with TIMESTAMP/TAG/FIELD role badges), Diagnostics cards (total samples, shards, time range), Configuration table, Downsampling Tiers, per-Shard Details (sealed/mutable block counts, timestamps)
    - **Ingestion sub-tab**: comprehensive documentation with 4 ingestion methods (SQL CREATE TYPE, InfluxDB Line Protocol with curl/Python examples, SQL INSERT, Java embedded API), method comparison table
  - **API Panel Integration**: TimeSeries section in HTTP API Reference with all 3 endpoints documented, including query parameter support, request/response examples, and working "Try It" playground (with `text/plain` content type handling for Line Protocol)
  - **Query tab layout**: "Connected as" bar moved above Database Info sidebar for uniform positioning across Query/Database/TimeSeries tabs
  - 10 integration tests in `TimeSeriesQueryHandlerIT`: raw query, aggregated query, tag filter, field projection, missing/invalid type errors, latest value, latest with tag, latest on empty type

- **Competitive Gap Closure (P0/P1)** — Critical features identified from gap analysis against top 10 TSDBs (InfluxDB 3, TimescaleDB, Prometheus, QuestDB, TDengine, ClickHouse, Kdb+, Apache IoTDB, VictoriaMetrics, Grafana Mimir):
  - **Counter Reset Detection in `ts.rate()`** — Optional 3rd parameter enables Prometheus-style counter reset handling. When `true`, detects value decreases and treats post-reset values as increments from 0. Default behavior (simple `(last-first)/timeDelta`) preserved for backward compatibility with gauge-type data
  - **Time Range Operators Beyond BETWEEN** — `SelectExecutionPlanner.extractTimeRange()` now handles `>`, `>=`, `<`, `<=`, `=` operators on the timestamp column, pushing them down to the TimeSeries engine. Multiple range conditions ANDed together (tightest bounds win). Previously only `BETWEEN` was pushed down; other operators caused full scans
  - **Multi-Tag Filtering** — `TagFilter` redesigned to support multiple tag conditions ANDed together via `and()` and `andIn()` chaining methods. HTTP query handler updated to iterate over all tags in the request JSON (previously only used the first tag). Backward compatible: existing `eq()` and `in()` factory methods still work
  - **Automatic Retention/Downsampling Scheduler** — `TimeSeriesMaintenanceScheduler` runs as a daemon thread (60s interval), automatically applying retention and downsampling policies. Follows the same pattern as `MaterializedViewScheduler`. Integrated into `LocalSchema` (lazy init, shutdown on close) and `TimeSeriesTypeBuilder` (scheduled on type creation). Previously required explicit `applyRetention()` / `applyDownsampling()` calls from application code
  - **Linear Interpolation** — `ts.interpolate(value, 'linear', timestamp)` added as a 4th method. Interpolates null values using linear interpolation between surrounding non-null values. Requires optional 3rd parameter for timestamps
  - **Approximate Percentiles** — New `ts.percentile(value, percentile)` function (registered as `SQLFunctionTsPercentile`). Works with GROUP BY for per-bucket percentile calculation (e.g., `ts.percentile(latency, 0.99)` for p99)
  - 19 new tests in `TimeSeriesGapAnalysisTest`

- **Block-Level Tag Metadata for Sealed Blocks** — Per-block distinct tag values stored in sealed block headers, enabling three-way block decision during tag-filtered queries:
  - **Block header tag metadata section**: after numeric stats, stores `tagColCount` (2 bytes) + per-TAG column `distinctCount` + UTF-8 encoded distinct values. Minimal overhead: a block with one tag column holding "TSLA" costs 10 bytes per block header
  - **Sealed format version upgrade**: `CURRENT_VERSION` bumped from 0 to 1. `loadDirectory()` reads tag metadata for version ≥ 1. Auto-migration via `upgradeFileToVersion1()` rewrites version-0 blocks with empty tag metadata on first `appendBlock()`
  - **Three-way block decision** (`BlockMatchResult` enum):
    - **SKIP** — filtered tag value not in block's distinct set → skip entire block (zero decompression)
    - **FAST_PATH** — block has exactly 1 distinct value for the filtered tag AND it matches → use block-level stats (min/max/sum/count) directly, zero decompression
    - **SLOW_PATH** — block has multiple distinct values including the filtered one → decompress tag columns via `DictionaryCodec.decode()`, filter rows inline
  - **Compaction integration**: `TimeSeriesShard.compact()` collects distinct tag values per chunk via `LinkedHashSet` and passes `String[][] tagDistinctValues` to `appendBlock()`
  - **Aggregation path**: `aggregateMultiBlocks()` accepts `TagFilter` parameter, applies three-way decision per block. Removed the `aggregateMultiWithTagFilter()` row-by-row fallback from `TimeSeriesEngine` — sealed store handles block-level skipping internally. Mutable bucket tag filtering remains row-level
  - **Query path**: `scanRange()` and `iterateRange()` accept `TagFilter` parameter, skip non-matching blocks entirely
  - **Downsampling integration**: `downsampleBlocks()` computes tag metadata for newly created blocks
  - CRC32 integrity covers tag metadata section
  - 3 new tests in `TimeSeriesGapAnalysisTest`: `testTagFilterBlockSkipping`, `testTagFilterAggregationAfterCompaction`, `testTagFilterNonexistentTag`
  - All 213 timeseries tests passing, zero regressions

- **Grafana Integration** — Grafana DataFrame-compatible HTTP endpoints for visualization via the Grafana Infinity datasource plugin (no custom plugin needed):
  - `GET /api/v1/ts/{database}/grafana/health` — datasource health check (verifies database exists)
  - `GET /api/v1/ts/{database}/grafana/metadata` — discovers TimeSeries types, fields, tags, and available aggregation types
  - `POST /api/v1/ts/{database}/grafana/query` — multi-target query returning Grafana DataFrame wire format (columnar arrays with schema metadata); supports raw queries, aggregated queries (SUM/AVG/MIN/MAX/COUNT), tag filtering, field projection, and automatic bucket interval calculation from `maxDataPoints`
  - Shared `TimeSeriesHandlerUtils` utility class extracted from `PostTimeSeriesQueryHandler` (tag filter building, column index resolution)
  - 8 integration tests in `GrafanaTimeSeriesHandlerIT`: health, metadata, raw query, aggregated query, multi-target, tag filter, auto maxDataPoints, missing targets

- **Phase 6: PromQL Query Language** — Native PromQL support for Prometheus-compatible observability:
  - **`PromQLParser`** — Hand-written recursive-descent parser covering: instant vector selectors (`metric{label=~"re"}`), range vector selectors (`metric[5m]`), binary expressions (`+`, `-`, `*`, `/`, `%`, `^`, comparison ops, logical `and`/`or`/`unless`), aggregations (`sum`, `min`, `max`, `avg`, `count`, `topk`, `bottomk`, `quantile` with `by`/`without` clauses), function calls (`rate`, `irate`, `increase`, `delta`, `idelta`, `avg_over_time`, `min_over_time`, `max_over_time`, `sum_over_time`, `count_over_time`, `stddev_over_time`, `absent`, `ceil`, `floor`, `round`, `abs`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `clamp_min`, `clamp_max`, `histogram_quantile`, `label_replace`, `label_join`, `vector`, `scalar`, `time`, `pi`), unary negation, and scalar literals. Duration parsing (`5m`, `1h`, `30s`, `1d`, `1w`, `1y`). `PromQLParser.parseDuration(str)` also available as utility
  - **`PromQLEvaluator`** — Two-phase evaluation: `evaluateInstant(expr, evalTimeMs)` for instant vector results, `evaluateRange(expr, startMs, endMs, stepMs)` for matrix results. Vector selector → 5-minute lookback window (configurable via `PromQLEvaluator(database, lookbackMs)` constructor). Binary op label matching, aggregation with group-by/without, range function window computation via `TimeSeriesEngine.iterateQuery()`. ReDoS-safe regex compilation with pattern cache (ConcurrentHashMap, 512-entry LRU-style eviction). Validates regex patterns against nested quantifier and alternation+quantifier patterns before compilation
  - **`PromQLResult`** — Sealed interface with three implementations: `InstantVector(List<VectorSample>)`, `MatrixResult(List<MatrixSeries>)`, `ScalarResult(double)`. `VectorSample` holds labels, value, and timestamp. `MatrixSeries` holds labels and a list of `(timestamp, value)` pairs
  - **`PromQLFunctions`** — All range functions implemented: `rate`/`irate`/`increase`/`delta`/`idelta` use first/last sample arithmetic; `avg_over_time` etc. compute window statistics. `absent()` inverts presence. Math functions delegate to `Math.*`
  - **HTTP API — PromQL-compatible endpoints** (base: `/ts/{database}/prom`):
    - `GET /ts/{database}/prom/api/v1/query?query=&time=` — instant query; `time` is Unix seconds (float); defaults to current time; optional `lookback_delta` override
    - `GET /ts/{database}/prom/api/v1/query_range?query=&start=&end=&step=` — range query; all timestamps in Unix seconds; `step` can be a duration string or seconds float
    - `GET /ts/{database}/prom/api/v1/labels` — lists all TimeSeries type names (metric names) and their tag column names as PromQL label names
    - `GET /ts/{database}/prom/api/v1/label/{name}/values` — returns distinct values for a given label across all TimeSeries types
    - `GET /ts/{database}/prom/api/v1/series?match[]=` — returns series matching the given selector(s); evaluates each selector against all TimeSeries types
    - All endpoints return standard Prometheus JSON `{status: "success", data: {...}}` format via `PromQLResponseFormatter`
  - **SQL function `promql(expr [, evalTimeMs])`** — Calls the PromQL evaluator from within SQL. Returns a result row per matched sample (each map's keys become row properties; `__value__` holds the numeric value):
    ```sql
    -- Instant query at explicit time
    RETURN promql('cpu_usage{host="srv1"}', 1700000000000)
    -- Scalar arithmetic
    RETURN promql('2 + 3 * 4', 1000)
    -- Current time (no second arg)
    RETURN promql('rate(http_requests_total[5m])')
    ```
    Registered as `promql` in `DefaultSQLFunctionFactory`; min 1 arg, max 2 args
  - **Studio — PromQL Explorer tab**: new "PromQL" sub-tab alongside Query/Schema/Ingestion in the TimeSeries Explorer; expression input, instant / range toggle, time controls, executes via `/prom/api/v1/query` or `/prom/api/v1/query_range`, renders results in DataTable and ApexCharts line chart
  - **14 integration tests in `PromQLHttpHandlerIT`**: instant query (scalar, instant vector, rate, sum by, binary op), range query, labels, label values, series, error cases (missing query param, invalid PromQL, zero step)

- **Prometheus Remote Write / Read Protocol** — Drop-in Prometheus remote storage backend:
  - **`POST /ts/{database}/prom/write`** (`PostPrometheusWriteHandler`) — accepts Prometheus `remote_write` Protobuf payload (snappy-compressed). Decodes `WriteRequest.timeseries[]` using hand-written `ProtobufDecoder`; each `TimeSeries` is mapped to an ArcadeDB TimeSeries type named after the `__name__` label. Tags become TimeSeries tag columns (via `CREATE TIMESERIES TYPE IF NOT EXISTS`). Samples bulk-appended via `DatabaseAsyncAppendSamples`. Returns HTTP 204 on success
  - **`POST /ts/{database}/prom/read`** (`PostPrometheusReadHandler`) — accepts Prometheus `remote_read` Protobuf query payload (snappy-compressed). Each `Query` has matchers, start/end timestamps. Evaluates against the named TimeSeries type; encodes the response as a `ReadResponse.QueryResult` containing `TimeSeries[]` Protobuf objects, snappy-compressed. Supports `=`, `!=`, `=~`, `!~` label matchers
  - **`ProtobufDecoder`** — Minimal hand-written protobuf wire-format decoder (no generated code, no protobuf library dependency): varint (wire type 0), fixed64 (wire type 1), length-delimited (wire type 2). Varint capped at 64 bits; length-delimited bounds-checked against remaining buffer. Used for both write and read request decoding
  - **`ProtobufEncoder`** — Minimal protobuf encoder for building `ReadResponse` messages: `writeTag()`, `writeVarint()`, `writeLengthDelimited()`, `writeFixed64AsDouble()`, `writeSInt64()`. Outputs to `ByteArrayOutputStream`
  - **`PrometheusTypes`** — Protobuf field constants for all message types in the `remote.proto` schema: `WriteRequest`, `TimeSeries`, `Label`, `Sample`, `ReadRequest`, `Query`, `LabelMatcher`, `QueryResult`, `ReadResponse`
  - **snappy-java dependency** added to `server/pom.xml` (Apache 2.0) for frame-format decompression/compression of all remote_write/read payloads. Recorded in `ATTRIBUTIONS.md`
  - **8 integration tests in `PrometheusRemoteWriteReadIT`**: write single series, write multi-series, read back written data, label matcher `=`/`!=`/`=~`/`!~`, empty result for unmatched matcher, round-trip write+read consistency

### In Progress / Not Yet Started

#### Competitive Gap Analysis — Prioritized Roadmap

Gap analysis comparing ArcadeDB's TimeSeries against top 10 TSDBs: InfluxDB 3, TimescaleDB, Prometheus, QuestDB, TDengine, ClickHouse, Kdb+, Apache IoTDB, VictoriaMetrics, Grafana Mimir.

**P0 — Table Stakes (standard in 7+/10 TSDBs):**
- ~~Counter reset handling in `ts.rate()`~~ — **DONE** (optional 3rd param)
- ~~Time range operators beyond BETWEEN~~ — **DONE** (`>`, `>=`, `<`, `<=`, `=` pushed down)
- ~~Multi-tag filtering~~ — **DONE** (ANDed multi-tag conditions)
- ~~Automatic retention/downsampling scheduler~~ — **DONE** (daemon thread, 60s interval)
- ~~PromQL / MetricsQL query language~~ — **DONE** (native `PromQLParser` + `PromQLEvaluator`; 5 HTTP endpoints under `/ts/{db}/prom/api/v1/`; `promql()` SQL function; Studio PromQL Explorer tab; 14 integration tests)
- ~~Grafana native datasource plugin~~ — **DONE** (Grafana DataFrame-compatible endpoints: `GET /ts/{db}/grafana/health`, `GET /ts/{db}/grafana/metadata`, `POST /ts/{db}/grafana/query` — works with Grafana Infinity datasource plugin, no custom plugin needed)
- ~~Prometheus `remote_write` / `remote_read` protocol~~ — **DONE** (`POST /ts/{db}/prom/write` + `POST /ts/{db}/prom/read`; hand-written protobuf decoder/encoder; snappy compression; 8 integration tests)
- **Alerting & recording rules** — Built-in alerting on metric thresholds with routing (email, Slack, PagerDuty). Recording rules pre-compute expensive queries (7+/10 TSDBs)

**P1 — Core Analytics (present in 4-6/10 TSDBs):**
- ~~Approximate percentiles (p50/p95/p99)~~ — **DONE** (`ts.percentile` function)
- ~~Linear interpolation in gap filling~~ — **DONE** (`ts.interpolate` 'linear' method)
- **OpenTelemetry (OTLP) ingestion** — CNCF standard for observability. OTLP (gRPC + HTTP) becoming the universal ingest protocol (5/10 and growing)
- ~~Window functions for TimeSeries queries~~ — **DONE** (`ts.lag`, `ts.lead`, `ts.rowNumber`, `ts.rank`)
- **Cardinality management & monitoring** — Tools to explore, limit, and alert on cardinality growth. The `DictionaryCodec` has a hard 65,535 limit per block that throws at runtime with no warning
- **Streaming / real-time aggregation at ingestion** — Pre-aggregate data at ingestion time to reduce storage and query cost (e.g., reduce 1s samples to 1min before storage)
- **ASOF JOIN / temporal joins** — Find closest timestamp match between two time series without exact alignment. Critical for correlating data from sensors with different sampling rates (3/10 TSDBs)

**P2 — Ecosystem & Advanced Features (present in 2-3/10 TSDBs):**
- **TimeSeries via PostgreSQL wire protocol** — ArcadeDB already has `postgresw` module. Enabling TS queries through it would unlock all PostgreSQL client libraries, BI tools (Tableau, Metabase, Superset), JDBC/ODBC connectivity
- **Native histogram support** — Modern way to capture latency distributions without pre-defined bucket boundaries (Prometheus 3.0, VictoriaMetrics 3.0)
- **Tiered storage (hot/warm/cold with object storage)** — Object storage (S3/GCS) backends reduce cost 10-100x for historical data (5/10 TSDBs)
- **Parquet/Arrow export/import** — Standard format for data lakes. Enables interop with Spark, Pandas, DuckDB
- **MQTT protocol support** — Dominant protocol for IoT devices. Native ingestion eliminates broker middleware (TDengine, Apache IoTDB)
- **Exemplars (trace-to-metrics linking)** — Attach trace IDs to metric samples for click-through from metric spike to distributed trace
- **Anomaly detection** — ML-based anomaly scoring on time series (emerging feature, VictoriaMetrics enterprise)

**Competitive Advantages (what ArcadeDB has that others don't):**
- Multi-model in one engine: Graph + Document + K/V + TimeSeries in the same database with cross-model queries
- Embeddable: can run as a Java library inside an application (no separate process)
- InfluxDB Line Protocol ingestion: already implemented (most TSDBs except InfluxDB don't have this natively)
- Continuous aggregates with auto-refresh on commit: post-INSERT trigger-based refresh is more immediate than TimescaleDB's policy-based refresh
- SIMD-vectorized aggregation: Java Vector API usage for aggregation is cutting-edge

- **Graph + TimeSeries Integration** — Cross-model queries (e.g., `MATCH {type: Device} -HAS_METRIC-> {type: Sensor} WHERE ts.rate(value, ts) > 100`)

---

## High Availability (HA) and Multi-Node Behaviour

### How TimeSeries data flows in an HA cluster

ArcadeDB's HA layer replicates data at the page level through the `PaginatedComponent` infrastructure. TimeSeries storage uses two layers:

| Layer | File extension | Storage mechanism | Replicated? |
|---|---|---|---|
| Mutable bucket | `.tstb` | `TimeSeriesBucket extends PaginatedComponent` | **Yes** — changes are page-replicated to all followers in real time |
| Sealed store | `.ts.sealed` | `TimeSeriesSealedStore` via `RandomAccessFile` / `FileChannel` | **No** — local-only files |

### Why sealed stores are not replicated

This is by design. The sealed store is a *derived* artefact: it is produced by compacting the mutable bucket. Since the mutable bucket data is already replicated, every HA node independently holds all the source data it needs to perform its own compaction. Replicating the sealed files as well would double the I/O cost for no benefit.

The `TimeSeriesMaintenanceScheduler` runs as a daemon thread on each node and periodically compacts mutable data into the local sealed store. Each node therefore converges to the same sealed state independently.

### Behaviour after a failover

Immediately after a follower is promoted to leader (or a new follower joins), it may not yet have compacted all mutable data into its sealed store. In that case:

- **Range queries** still return correct results: the engine queries both the sealed store and the mutable bucket for every time range.
- **Aggregation queries** also remain correct: `aggregateMulti()` processes sealed blocks (fast path) and then iterates the mutable bucket (slow path) for the same time window.
- **Compaction lag**: a follower that has not yet compacted may serve reads slightly more slowly until its maintenance scheduler runs the next compaction cycle (default interval: 60 seconds).

### Summary

| Concern | Answer |
|---|---|
| Is in-flight mutable data replicated? | Yes, via the normal page-replication protocol |
| Are sealed store files replicated? | No — each node compacts independently |
| Are reads consistent immediately after failover? | Yes — the mutable bucket covers the gap |
| Is there a performance impact after failover? | Queries may be slower until compaction catches up |

---

## Context

ArcadeDB users are requesting native TimeSeries support, with the key requirement being **fast range queries**. ArcadeDB is uniquely positioned as a multi-model database (Graph, Document, Key/Value, Search, Vector) to become the first production database that **natively unifies graph traversal with timeseries aggregation** in a single query engine — a gap confirmed by a January 2025 SIGMOD survey paper (arXiv:2601.00304).

This document presents: (1) a competitive landscape analysis, (2) the underlying technology that makes TSDBs fast, (3) how ArcadeDB's existing architecture compares, (4) graph+timeseries integration opportunities, (5) the query & ingestion interface (SQL, OpenCypher, HTTP Line Protocol, Java API), (6) the two-layer storage architecture with shard-per-core parallelism, and (7-8) a phased implementation plan.

---

## Part 1: Competitive Landscape — Top TimeSeries Databases

### 1.1 Open Source

| Database | Storage Engine | Compression | Fast Range Query Technique | Query Language | License |
|---|---|---|---|---|---|
| **InfluxDB 3.0** | Apache Arrow + Parquet (Rust rewrite) | Parquet native (Delta, Dict, Snappy/ZSTD) | Time-partitioned Parquet files + DataFusion vectorized execution + predicate pushdown | SQL + InfluxQL | MIT (core) |
| **TimescaleDB** | PostgreSQL extension: row-based "hypertable" chunks → columnar compression | 7 algorithms: Gorilla (floats), Delta-of-delta (timestamps), Simple-8b RLE, Dictionary, LZ4 | Chunk exclusion (prune time ranges), B-tree on time column per chunk, continuous aggregates | Full PostgreSQL SQL | Apache 2.0 (core) |
| **QuestDB** | Custom columnar, one memory-mapped file per column per partition (Java+C++) | ZFS-level + Parquet for cold tier | SIMD-accelerated scans (SSE2/AVX2), time partitions, zero-copy mmap, parallel partition execution | SQL (PG wire protocol) | Apache 2.0 |
| **ClickHouse** | MergeTree — columnar parts sorted by primary key | Composable codecs: DoubleDelta + Gorilla + T64 + LZ4/ZSTD | Sparse primary index (1 entry per 8192-row granule), partition pruning, vectorized SIMD execution | Full SQL | Apache 2.0 |
| **TDengine** | LSM-tree with SkipList/Red-Black Tree memtables | Delta + Gorilla + LZ4/ZSTD (two-level encoding) | Time-based partitioning, one sub-table per device, SkipList in-memory | TDengine SQL | **AGPL 3.0** |
| **VictoriaMetrics** | MergeTree-inspired (Go), column-oriented parts | Gorilla + ZSTD → **0.4 bytes/sample** (best in class) | Monthly partitions, MergeSet label index, bitmap series filtering | PromQL / MetricsQL | Apache 2.0 |
| **Prometheus** | Block-based chunks with inverted label index (Go) | Gorilla encoding → ~1.37 bytes/sample | Block-level time metadata pruning, posting list intersection for label matching | PromQL | Apache 2.0 |
| **Apache IoTDB** | LSM-tree + custom TsFile columnar format | Delta, RLE, Gorilla, Snappy/LZ4/ZSTD per chunk | Chunk-level min/max stats, device-level data grouping, in-memory index | SQL-like | Apache 2.0 |

### 1.2 Commercial / Cloud

| Database | Architecture | Key Differentiator |
|---|---|---|
| **Kdb+ (KX)** | In-memory RDB → Intraday IDB → Historical HDB (columnar flat files, mmap'd) | 30+ years in finance; q language is inherently vectorized; sub-millisecond on tick data |
| **Amazon Timestream** | Serverless; memory store (row) → magnetic store (columnar); **now deprecated in favor of InfluxDB 3** | Auto lifecycle management; but closed to new customers as of June 2025 |
| **Azure Data Explorer (Kusto)** | Distributed columnar extents; EngineV3 | Built-in ML: seasonality detection, anomaly detection, forecasting; KQL language |
| **Datadog Monocle** | Rust, shard-per-core LSM (one LSM instance per CPU core, lock-free writes) | 60x ingestion improvement; tag-hash-based sharding; zero-contention architecture |

---

## Part 2: Underlying Technology — What Makes TSDBs Fast

### 2.1 The Three Pillars of Fast Range Queries

**Pillar 1: Time-Based Partitioning (Eliminate I/O)**
Every top TSDB partitions by time. When you query `WHERE timestamp BETWEEN X AND Y`, partitions outside that range are **never touched** — no I/O at all. This is the single biggest speedup. Granularity varies: hours (Prometheus), days (QuestDB, Kdb+), weeks (TimescaleDB), months (VictoriaMetrics), or configurable.

**Pillar 2: Columnar Storage + Compression (Minimize I/O)**
Once you've narrowed to the right partitions, columnar storage ensures you only read the columns you need. `SELECT avg(temperature)` reads only the temperature column, not humidity, pressure, etc. This can reduce I/O by 10-100x for wide tables. Combined with timeseries-specific compression:

| Algorithm | Target | How It Works | Compression |
|---|---|---|---|
| **Delta-of-delta** | Timestamps | Regular intervals → delta is constant → delta-of-delta is 0 → 1 bit | 96% → 1 bit |
| **Gorilla XOR** | Float values | XOR consecutive IEEE 754 floats → many leading/trailing zeros → store only middle bits | 51% → 1 bit, avg 1.37 B/pair |
| **Simple-8b RLE** | Integers | Pack multiple small ints into 64-bit words with run-length encoding | 4-8x |
| **Dictionary** | Tags/labels | Map low-cardinality strings to integer IDs | 10-100x for tags |
| **T64** | Integers | Find minimum bit-width needed | 2-4x |

Combined result: **0.4 to 1.37 bytes per (timestamp, value) pair** vs. 16 bytes uncompressed.

**Pillar 3: Vectorized Execution + SIMD (Maximize CPU)**
Once data is in memory in columnar format, process it in batches using CPU SIMD instructions:
- QuestDB: AVX2 for filtering and aggregation
- ClickHouse: Processes 65,505-row blocks with SIMD
- Kdb+: Language-level vectorization (all operations work on arrays)
- InfluxDB 3: DataFusion's vectorized Arrow-based execution

### 2.2 Additional Key Techniques

- **Memory-mapped I/O**: QuestDB and Kdb+ mmap column files for zero-copy access
- **Sparse indexing**: ClickHouse stores 1 index entry per 8192 rows (vs. per-row), saving memory
- **Inverted label indexes**: VictoriaMetrics and Prometheus use inverted indexes for tag/label matching
- **Out-of-order handling**: WAL-based sorting (QuestDB), SkipList memtables (TDengine), dedup indexes (InfluxDB 3)
- **Continuous aggregates**: Pre-compute common rollups (TimescaleDB, InfluxDB, ClickHouse materialized views)
- **Retention policies**: Auto-delete data older than X (every major TSDB)
- **Downsampling**: Reduce resolution of old data (5-second → 1-minute → 1-hour)

---

## Part 3: ArcadeDB's LSM-Tree — Strengths & Gaps

### 3.1 Current Architecture (from codebase analysis)

ArcadeDB's LSM-Tree index (`com.arcadedb.index.lsm.*`):
- **Two-level structure**: Mutable (Level-0, append-only pages) → Compacted (Level-1, immutable merged pages)
- **Page size**: 256KB for indexes, 64KB for bucket data
- **Range queries**: `RangeIndex.range(ascending, beginKeys, beginInclusive, endKeys, endInclusive)` — fully supported
- **Compaction**: Multi-way merge with configurable RAM budget, deletion markers, root page with min-keys
- **Bucket system**: Multiple buckets per type, with pluggable `BucketSelectionStrategy` (RoundRobin, Partitioned, Thread-based)
- **Date types**: DATETIME, DATETIME_MICROS, DATETIME_NANOS, DATETIME_SECOND — all present
- **Aggregations**: COUNT, SUM, AVG, MIN, MAX with GROUP BY — present but row-at-a-time

### 3.2 Where ArcadeDB's LSM-Tree Is Competitive

- **Write throughput**: LSM-trees excel at append-only workloads (proven by InfluxDB v1/v2, TDengine, IoTDB, VictoriaMetrics, Datadog all using LSM)
- **Sequential I/O**: Flush and compaction produce sequential writes
- **Existing range query support**: The `RangeIndex` interface already handles ordered scans
- **Multi-model flexibility**: No dedicated TSDB offers graph + timeseries natively

### 3.3 Gaps vs. Dedicated TSDBs

| Gap | Impact | Dedicated TSDB Approach |
|---|---|---|
| **Row-oriented storage** | Reads ALL columns even if query needs one | Columnar files (1 file per column per partition) |
| **No timeseries compression** | 10-40x more disk/memory than needed | Gorilla, Delta-of-delta, Dictionary encoding |
| **No time-based partitioning** | Range queries scan all data | Automatic time-windowed partitions |
| **Row-at-a-time execution** | CPU underutilized | Vectorized batch execution (Arrow-style) |
| **No SIMD** | 4-8x slower aggregation | AVX2/SSE2 for SUM, AVG, MIN, MAX |
| **~~No continuous aggregates~~** | ~~Repeated expensive queries~~ | ~~Pre-computed rollup tables~~ ✅ **Implemented** (watermark-based incremental refresh) |
| **No retention/downsampling** | Manual data lifecycle | Automatic TTL + resolution reduction |
| **No out-of-order optimization** | Late data may cause performance issues | WAL sorting, SkipList memtables |

---

## Part 4: Graph + TimeSeries — The Killer Multi-Model Feature

### 4.1 The Opportunity

A SIGMOD 2025 survey confirms: **no existing production database natively unifies graph traversal with timeseries aggregation**. The HyGraph research project (EDBT 2025, University of Leipzig) proposes this theoretically but has no production implementation.

ArcadeDB can be first-to-market here.

### 4.2 High-Value Use Cases

| Market | Graph Model | TimeSeries Data | Combined Query Example |
|---|---|---|---|
| **Industrial IoT** | Device topology (sensors → machines → lines → plants) | Sensor telemetry (temp, vibration, pressure) | "Average temperature of all sensors downstream of HVAC unit #3 in the last hour" |
| **Observability** | Service dependency graph | Latency, error rate, CPU metrics | "When payment-gateway latency > P99, what's the blast radius on all downstream services?" |
| **FinTech / AML** | Account/entity transaction network | Transaction velocity, amounts over time | "Find accounts receiving from 5+ distinct sources within 10 minutes with no prior history" |
| **Cybersecurity** | Network topology (hosts, services, firewalls) | Security events, traffic volume | "Show hosts that communicated with compromised server + their traffic anomaly patterns" |
| **Digital Twins** | Physical structure (building → floor → room → device) | Live telemetry | "If Pump #3 fails, which downstream components are affected? Show their current operating margins" |
| **Energy / Utilities** | Grid topology | Load, generation, frequency | "Hierarchical energy consumption rollup: campus → building → floor → meter" |
| **Supply Chain** | Supplier → manufacturer → distributor → retailer | Throughput, lead times, inventory levels | "Find bottlenecks where throughput dropped 20% while supplier count stayed constant" |

### 4.3 Proposed Query Patterns

**Pattern 1: Graph Traversal + TimeSeries Aggregation**
```sql
SELECT sensor.name, avg(ts.value) AS avg_temp
FROM (
  TRAVERSE out('InstalledIn') FROM (SELECT FROM Building WHERE name = 'Building X')
  WHILE $depth <= 3
) AS sensor
TIMESERIES sensor.temperature AS ts
  FROM '2026-02-19' TO '2026-02-20'
WHERE sensor.@type = 'Sensor'
GROUP BY sensor.name
```

**Pattern 2: Blast Radius Analysis**
```sql
SELECT service.name, $depth AS hops,
       avg(ts.value) AS avg_latency, max(ts.value) AS peak_latency
FROM (
  TRAVERSE out('DependsOn') FROM (SELECT FROM Service WHERE name = 'payment-gateway')
  MAXDEPTH 5
) AS service
TIMESERIES service.latency_p99 AS ts
  FROM '2026-02-20T10:00:00Z' TO '2026-02-20T11:00:00Z'
  GRANULARITY '1m'
GROUP BY service.name, $depth
ORDER BY $depth, peak_latency DESC
```

**Pattern 3: Anomaly Detection with Graph Context**
```sql
SELECT sensor.name, last(ts.value) AS current,
       avg(neighbor_ts.value) AS neighbor_avg
FROM Sensor AS sensor
LET neighbors = (SELECT expand(both('ConnectedTo')) FROM $parent.sensor)
TIMESERIES sensor.temperature AS ts LAST '1h'
TIMESERIES neighbors.temperature AS neighbor_ts LAST '1h'
WHERE abs(current - neighbor_avg) > 3 * stdev(neighbor_ts.value)
```

**Pattern 4: Correlation Across Connected Entities**
```sql
SELECT e.in.name AS sensor_a, e.out.name AS sensor_b,
       correlate(ts_a, ts_b) AS correlation
FROM ConnectedTo AS e
TIMESERIES e.in.vibration AS ts_a LAST '1h'
TIMESERIES e.out.vibration AS ts_b LAST '1h'
WHERE correlation > 0.85
ORDER BY correlation DESC
```

---

## Part 5: Query & Ingestion Interface — SQL, Cypher, HTTP, Java API

### 5.1 SQL DDL — Schema Definition

New `CREATE TIMESERIES TYPE` statement extending `CreateTypeAbstractStatement` (same pattern as `CreateDocumentTypeStatement` and `CreateVertexTypeStatement`):

```sql
-- Full syntax
CREATE TIMESERIES TYPE SensorReading [IF NOT EXISTS]
  TIMESTAMP ts PRECISION NANOSECOND          -- mandatory: designated timestamp column
  TAGS (sensor_id STRING, location STRING)   -- indexed, low-cardinality
  FIELDS (                                   -- value columns, high-cardinality
    temperature DOUBLE,
    humidity DOUBLE,
    pressure DOUBLE
  )
  [SHARDS 8]                                 -- default = availableProcessors()
  [PARTITION BY (sensor_id)]                 -- tag-hash sharding (default: thread affinity)
  [RETENTION 90 DAYS]                        -- auto-delete old data
  [COMPACTION INTERVAL 30s]                  -- how often mutable → sealed
  [BLOCK SIZE 50000]                         -- samples per sealed block

-- Minimal syntax (defaults for everything optional)
CREATE TIMESERIES TYPE SensorReading
  TIMESTAMP ts
  TAGS (sensor_id STRING)
  FIELDS (temperature DOUBLE)

-- ALTER: add fields, change retention, adjust shards
ALTER TIMESERIES TYPE SensorReading
  ADD FIELD wind_speed DOUBLE

ALTER TIMESERIES TYPE SensorReading
  RETENTION 180 DAYS

-- DROP
DROP TIMESERIES TYPE SensorReading [IF EXISTS]
```

**Timestamp precision options**: `SECOND`, `MILLISECOND`, `MICROSECOND`, `NANOSECOND` (default). Maps to ArcadeDB's `DATETIME_SECOND`, `DATETIME`, `DATETIME_MICROS`, `DATETIME_NANOS` types.

**Implementation**: New `CreateTimeSeriesTypeStatement extends CreateTypeAbstractStatement`. Overrides `createType(Schema schema)` to call a new `schema.buildTimeSeriesType()` builder that creates the `TimeSeriesType`, N `TimeSeriesShard` instances, and configures the `BucketSelectionStrategy`.

### 5.2 SQL DML — Ingestion

#### Single-Row INSERT (Compatible with Existing Syntax)

```sql
-- Standard ArcadeDB INSERT syntax works
INSERT INTO SensorReading
  SET ts = '2026-02-20T10:00:00.000Z',
      sensor_id = 'sensor-A',
      location = 'building-1',
      temperature = 22.5,
      humidity = 65.0,
      pressure = 1013.25

-- Content syntax also works
INSERT INTO SensorReading
  CONTENT {
    "ts": "2026-02-20T10:00:00.000Z",
    "sensor_id": "sensor-A",
    "location": "building-1",
    "temperature": 22.5,
    "humidity": 65.0,
    "pressure": 1013.25
  }
```

This goes through the standard SQL parser → `InsertExecutionPlanner` → routes to `TimeSeriesEngine.appendSamples()` instead of `LocalBucket.createRecord()`. Works but is slower than batch APIs due to per-row SQL parsing overhead.

#### Batch INSERT (New Syntax for High-Throughput)

```sql
-- Batch insert: multiple rows in one statement
INSERT INTO SensorReading
  (ts, sensor_id, location, temperature, humidity, pressure)
  VALUES
    ('2026-02-20T10:00:00Z', 'sensor-A', 'building-1', 22.5, 65.0, 1013.25),
    ('2026-02-20T10:00:01Z', 'sensor-A', 'building-1', 22.6, 64.8, 1013.20),
    ('2026-02-20T10:00:02Z', 'sensor-A', 'building-1', 22.4, 65.2, 1013.30),
    ('2026-02-20T10:00:00Z', 'sensor-B', 'building-2', 19.1, 70.0, 1012.50)

-- Batch with subquery (import from another type)
INSERT INTO SensorReading
  SELECT ts, sensor_id, location, temperature, humidity, pressure
  FROM RawImportBuffer
  WHERE ts > '2026-02-20'
```

Batch inserts are parsed once, then all rows are appended in a single transaction. Shard routing happens per row (different rows may go to different shards based on `BucketSelectionStrategy`).

### 5.3 SQL Query — TimeSeries Functions

#### time_bucket() — The Core Aggregation Primitive

Equivalent to TimescaleDB's `time_bucket()` and QuestDB's `SAMPLE BY`. Implemented as a `SQLFunction` registered via `SQLFunctionFactoryTemplate`.

```sql
-- Basic time bucketing: 1-hour averages
SELECT time_bucket('1h', ts) AS hour,
       sensor_id,
       avg(temperature) AS avg_temp,
       max(temperature) AS max_temp,
       min(temperature) AS min_temp,
       count(*) AS sample_count
FROM SensorReading
WHERE ts BETWEEN '2026-02-19' AND '2026-02-20'
  AND sensor_id = 'sensor-A'
GROUP BY hour, sensor_id
ORDER BY hour

-- Supported intervals: 's' (seconds), 'm' (minutes), 'h' (hours),
--   'd' (days), 'w' (weeks), 'M' (months)
-- Also numeric: '5m', '15m', '30s', '4h', '1d', '1w', '1M'

-- Gap filling: fill missing time buckets
SELECT time_bucket('1h', ts) AS hour,
       sensor_id,
       coalesce(avg(temperature), prev(avg(temperature))) AS avg_temp
FROM SensorReading
WHERE ts BETWEEN '2026-02-19' AND '2026-02-20'
GROUP BY hour, sensor_id
ORDER BY hour
```

**How it works**: `time_bucket('1h', ts)` truncates the timestamp to the nearest hour boundary: `floor(ts / interval) * interval`. The `AggregateProjectionCalculationStep` uses the returned value as a GROUP BY key.

#### TimeSeries-Specific Aggregate Functions

New `SQLFunction` implementations, registered alongside existing functions:

```sql
-- first/last: value at earliest/latest timestamp in window
SELECT time_bucket('1h', ts) AS hour,
       first(temperature) AS open_temp,    -- first value in the hour
       last(temperature) AS close_temp,    -- last value in the hour
       max(temperature) AS high_temp,
       min(temperature) AS low_temp
FROM SensorReading
GROUP BY hour

-- rate: per-second rate of change (for monotonic counters)
SELECT time_bucket('5m', ts) AS window,
       sensor_id,
       rate(request_count) AS requests_per_sec
FROM ServiceMetrics
WHERE ts > now() - INTERVAL '1h'
GROUP BY window, sensor_id

-- delta: difference between last and first value in window
SELECT time_bucket('1h', ts) AS hour,
       delta(energy_kwh) AS energy_consumed
FROM MeterReading
GROUP BY hour

-- moving_avg: sliding window average
SELECT ts, temperature,
       moving_avg(temperature, 10) AS smoothed    -- 10-sample window
FROM SensorReading
WHERE sensor_id = 'sensor-A'
ORDER BY ts

-- percentile: approximate percentile (t-digest)
SELECT time_bucket('1h', ts) AS hour,
       percentile(latency_ms, 0.99) AS p99_latency,
       percentile(latency_ms, 0.50) AS median_latency
FROM ServiceMetrics
GROUP BY hour

-- interpolate: fill gaps with interpolated values
SELECT time_bucket('1m', ts) AS minute,
       interpolate(temperature, 'linear') AS temp_interpolated
FROM SensorReading
WHERE ts BETWEEN '2026-02-20T10:00:00Z' AND '2026-02-20T11:00:00Z'
GROUP BY minute

-- downsample: reduce resolution (convenience wrapper)
SELECT downsample(temperature, '1h', 'avg') AS hourly_avg_temp
FROM SensorReading
WHERE ts BETWEEN '2026-02-01' AND '2026-02-20'
```

**Complete list of new SQL functions** (Phase 1 = MVP, Phase 2 = later):

| Function | Phase | Description |
|---|---|---|
| `time_bucket(interval, timestamp)` | 1 | Truncate timestamp to interval boundary |
| `first(value)` | 1 | First value by timestamp in group |
| `last(value)` | 1 | Last value by timestamp in group |
| `rate(value)` | 2 | Per-second rate of change |
| `delta(value)` | 2 | Difference between last and first in group |
| `moving_avg(value, window)` | 2 | Sliding window average |
| `percentile(value, p)` | 2 | Approximate percentile (t-digest) |
| `interpolate(value, method)` | 2 | Fill gaps: 'linear', 'prev', 'next', 'none' |
| `downsample(value, interval, agg)` | 2 | Convenience: resample at lower frequency |
| `correlate(series_a, series_b)` | 2 | Pearson correlation between two series |

### 5.4 SQL Query — Graph + TimeSeries Integration

These patterns combine ArcadeDB's existing graph traversal with timeseries range queries (see Part 4 for use cases):

```sql
-- Pattern 1: Traverse graph, then aggregate timeseries for found vertices
SELECT sensor.name, avg(ts.temperature) AS avg_temp
FROM (
  TRAVERSE out('InstalledIn') FROM (SELECT FROM Building WHERE name = 'HQ')
  WHILE $depth <= 3
) AS sensor
WHERE sensor.@type = 'Sensor'
  AND ts.ts BETWEEN '2026-02-19' AND '2026-02-20'
TIMESERIES sensor -> SensorReading AS ts   -- link vertex to its timeseries type
GROUP BY sensor.name

-- Pattern 2: Blast radius with timeseries context
SELECT service.name, $depth AS hops,
       avg(ts.latency_ms) AS avg_latency
FROM (
  TRAVERSE out('DependsOn') FROM #12:0 MAXDEPTH 5
) AS service
TIMESERIES service -> ServiceMetrics AS ts
  LAST '1h'
  GRANULARITY '1m'
GROUP BY service.name, $depth
ORDER BY avg_latency DESC
```

**`TIMESERIES ... AS` clause**: New SQL clause that links a graph vertex to its timeseries type. The query planner:
1. First resolves the graph traversal → set of vertex RIDs
2. For each RID, looks up the linked timeseries data in `TimeSeriesEngine`
3. Applies time range filter and aggregation
4. Joins results back with vertex properties

This is parsed by extending the `SelectStatement` grammar in `SQLParser.g4`.

### 5.5 OpenCypher — TimeSeries Extensions

ArcadeDB has a **native OpenCypher engine** (`com.arcadedb.query.opencypher`) — a full implementation with its own ANTLR4 Cypher25 grammar, AST builder, execution planner, cost-based optimizer, and 50+ execution steps. It is NOT transpiled to Gremlin.

TimeSeries support integrates through **two mechanisms**:

#### 1. Namespaced Functions (registered in `CypherFunctionRegistry`)

The existing `CypherFunctionRegistry` supports namespaced functions (e.g., `text.split`, `math.sigmoid`, `date.format`). TimeSeries functions follow the same `ts.*` namespace pattern:

```cypher
// Query timeseries data for a specific vertex
MATCH (s:Sensor {name: 'sensor-A'})
RETURN s.name,
       ts.avg(s, 'SensorReading', 'temperature', '2026-02-19', '2026-02-20') AS avg_temp

// Traverse graph + aggregate timeseries
MATCH (b:Building {name: 'HQ'})<-[:InstalledIn*1..3]-(s:Sensor)
WITH s
RETURN s.name,
       ts.avg(s, 'SensorReading', 'temperature', '2026-02-19', '2026-02-20') AS avg_temp,
       ts.max(s, 'SensorReading', 'temperature', '2026-02-19', '2026-02-20') AS max_temp
ORDER BY avg_temp DESC

// Latest value per sensor
MATCH (s:Sensor)-[:InstalledIn]->(r:Room)
RETURN r.name, s.name,
       ts.last(s, 'SensorReading', 'temperature') AS current_temp

// Time-bucketed aggregation
MATCH (s:Sensor {name: 'sensor-A'})
WITH s, ts.query(s, 'SensorReading', 'temperature', '2026-02-19', '2026-02-20', '1h') AS buckets
UNWIND buckets AS bucket
RETURN bucket.time, bucket.avg, bucket.min, bucket.max

// Rate of change (counter metrics)
MATCH (svc:Service {name: 'api-gateway'})
RETURN ts.rate(svc, 'ServiceMetrics', 'request_count', '2026-02-20T10:00:00Z', '2026-02-20T11:00:00Z') AS rps
```

**Function signatures** (registered in `CypherFunctionRegistry` under `ts` namespace):

| Function | Arguments | Returns | Description |
|---|---|---|---|
| `ts.avg(vertex, type, field, from, to)` | Vertex, String, String, String, String | Double | Average value in time range |
| `ts.sum(vertex, type, field, from, to)` | Same | Double | Sum of values |
| `ts.min(vertex, type, field, from, to)` | Same | Double | Minimum value |
| `ts.max(vertex, type, field, from, to)` | Same | Double | Maximum value |
| `ts.count(vertex, type, field, from, to)` | Same | Long | Sample count |
| `ts.first(vertex, type, field)` | Vertex, String, String | Object | Earliest value |
| `ts.last(vertex, type, field)` | Vertex, String, String | Object | Latest value |
| `ts.rate(vertex, type, field, from, to)` | Same as avg | Double | Per-second rate of change |
| `ts.query(vertex, type, field, from, to, granularity)` | + String | List\<Map\> | Time-bucketed results |

Each function internally resolves the vertex → linked `TimeSeriesType` → `TimeSeriesEngine.aggregate()`, returning scalar or structured results.

#### 2. Procedures (registered in `CypherProcedureRegistry`)

For more complex operations that return tabular results (multiple rows), use procedures via `CALL`:

```cypher
// Range query returning raw samples
CALL ts.range('SensorReading', 'sensor-A', '2026-02-19', '2026-02-20', ['temperature', 'humidity'])
YIELD time, temperature, humidity
RETURN time, temperature, humidity
ORDER BY time

// Time-bucketed aggregation as procedure (returns rows)
CALL ts.aggregate('SensorReading', {
  from: '2026-02-19',
  to: '2026-02-20',
  field: 'temperature',
  granularity: '1h',
  filter: {sensor_id: 'sensor-A'}
})
YIELD bucket_time, avg_value, min_value, max_value, count
RETURN bucket_time, avg_value, count

// Combined: traverse graph, then fetch timeseries for each vertex
MATCH (b:Building {name: 'HQ'})<-[:InstalledIn*1..3]-(s:Sensor)
CALL ts.range('SensorReading', s.sensor_id, '2026-02-20T10:00:00Z', '2026-02-20T11:00:00Z', ['temperature'])
YIELD time, temperature
RETURN s.name, time, temperature
ORDER BY s.name, time
```

**Implementation**:
- Register `ts.*` functions in `CypherFunctionRegistry` (same as `text.*`, `math.*`, `date.*`)
- Register `ts.range`, `ts.aggregate` procedures in `CypherProcedureRegistry` (same as `algo.dijkstra`, `path.expand`)
- Functions are evaluated by `ExpressionEvaluator` via `CypherFunctionFactory`, which already supports namespaced function resolution
- Procedures are executed by `CallStep`, which already handles YIELD clauses
- No grammar changes needed — the Cypher25 grammar already supports namespaced functions and CALL procedures

### 5.6 HTTP Ingestion Endpoint — InfluxDB Line Protocol Compatible

#### Why InfluxDB Line Protocol?

ILP is the **de-facto standard** for timeseries ingestion. It is natively supported by:
- InfluxDB (v1, v2, v3) — the originator
- QuestDB — recommended ingestion path
- VictoriaMetrics — multiple endpoints
- GreptimeDB, openGemini, M3DB, Amazon Timestream for InfluxDB

Supporting ILP means **instant compatibility** with:
- **Telegraf** (300+ input plugins: system metrics, SNMP, MQTT, Kafka, etc.)
- **Grafana Agent** / Grafana Alloy
- **Vector** (Datadog's collection agent)
- Any IoT device or application that speaks ILP

#### Line Protocol Format

```
<measurement>[,<tag_key>=<tag_value>[,...]] <field_key>=<field_value>[,...] [<timestamp_ns>]
```

Examples:
```
SensorReading,sensor_id=sensor-A,location=building-1 temperature=22.5,humidity=65.0,pressure=1013.25 1708430400000000000
SensorReading,sensor_id=sensor-B,location=building-2 temperature=19.1,humidity=70.0 1708430400000000000
```

Rules:
- Measurement name = timeseries type name (auto-created if doesn't exist — configurable)
- Tags = comma-separated key=value after measurement name (no spaces around `=`)
- Fields = space-separated from tags, comma-separated key=value (floats default, `i` suffix for integers, quoted for strings)
- Timestamp = optional, nanosecond Unix epoch (precision configurable via query param)
- Multiple lines = multiple samples, newline-separated
- Batch = one HTTP POST with thousands of lines

#### HTTP Endpoint

```
POST /api/v1/ts/{database}/write?precision=<ns|us|ms|s>
Authorization: Bearer <token>     (or Basic auth)
Content-Type: text/plain; charset=utf-8
Content-Encoding: gzip            (optional, for compressed batches)

<line protocol data, newline-separated>
```

**Response codes:**
- `204 No Content` — success (all lines written)
- `400 Bad Request` — parse error (line protocol syntax invalid)
- `401 Unauthorized` — authentication failed
- `404 Not Found` — database not found
- `422 Unprocessable Entity` — valid syntax but semantic error (e.g., type mismatch)
- `500 Internal Server Error`

**Compatibility endpoint** (for existing Telegraf configurations):
```
POST /api/v2/write?org=default&bucket={database}
```
Maps directly to the same handler. Telegraf users just point their `output.influxdb_v2` config at ArcadeDB.

#### Implementation

New `PostTimeSeriesWriteHandler extends AbstractServerHttpHandler`:

```java
public class PostTimeSeriesWriteHandler extends AbstractServerHttpHandler {

  @Override
  protected ExecutionResponse execute(final HttpServerExchange exchange,
      final ServerSecurityUser user, final JSONObject payload) {

    final String databaseName = exchange.getQueryParameters().get("database");
    final String precision = exchange.getQueryParameters().getOrDefault("precision", "ns");
    final Database database = httpServer.getServer().getDatabase(databaseName);

    // 1. Read raw body (line protocol text, possibly gzip-compressed)
    final String body = readBody(exchange);

    // 2. Parse line protocol → batch of (type, tags, fields, timestamp)
    final List<LineProtocolSample> samples = LineProtocolParser.parse(body, precision);

    // 3. Group by type + shard, then append in parallel
    database.transaction(() -> {
      for (final LineProtocolSample sample : samples) {
        final TimeSeriesEngine engine = database.getSchema()
            .getTimeSeriesType(sample.measurement).getEngine();
        engine.appendSample(sample);
      }
    });

    return new ExecutionResponse(204, "");   // No Content = success
  }
}
```

**Registered in `HttpServer.setupRoutes()`:**
```java
routes.addPrefixPath("/api/v1", basicRoutes
    // ... existing routes ...
    .post("/ts/{database}/write", new PostTimeSeriesWriteHandler(this))
);
// Compatibility alias
routes.addPrefixPath("/api/v2", basicRoutes
    .post("/write", new PostTimeSeriesWriteHandler(this))   // InfluxDB v2 compat
);
```

#### Auto-Schema Creation (Configurable)

When ILP sends data for a type that doesn't exist:
- **Default (strict mode)**: Return 404, require explicit `CREATE TIMESERIES TYPE` first
- **Auto-create mode** (opt-in via server config `arcadedb.tsAutoCreateType=true`):
  - First line defines the schema: measurement → type, tags → TAG columns, fields → FIELD columns
  - Field types inferred: no suffix = DOUBLE, `i` = LONG, quoted = STRING, true/false = BOOLEAN
  - Subsequent lines with new fields → auto-alter to add columns (same as QuestDB behavior)

#### Performance: Why a Dedicated Endpoint Beats SQL

| Path | Operations per sample | Overhead |
|---|---|---|
| SQL INSERT | Parse SQL → plan → create Document → route → append | ~50-100μs/sample |
| HTTP Line Protocol | Parse text line → route → append (no SQL, no Document object) | ~1-5μs/sample |
| Java API (direct) | Route → append | ~0.5-1μs/sample |

The dedicated endpoint **skips SQL parsing, query planning, and Document object creation**. It parses the lightweight line protocol text directly into primitive arrays and calls `TimeSeriesEngine.appendSamples()`. For 1M samples/sec ingestion, this difference is critical.

### 5.7 Java API — Programmatic Access (Fastest Path)

The Java API bypasses all protocol overhead. Use it for embedded applications or custom ingestion pipelines:

```java
// Get the timeseries engine for a type
final TimeSeriesEngine engine = database.getSchema()
    .getTimeSeriesType("SensorReading").getEngine();

// Batch append — fastest path (primitive arrays, no object creation)
final long[] timestamps = { 1708430400000000000L, 1708430401000000000L, ... };
final String[] sensorIds = { "sensor-A", "sensor-A", ... };
final String[] locations = { "building-1", "building-1", ... };
final double[] temperatures = { 22.5, 22.6, ... };
final double[] humidities = { 65.0, 64.8, ... };

database.transaction(() -> {
  engine.appendSamples(timestamps,
      new Object[] { sensorIds, locations, temperatures, humidities });
});

// Async batch append — zero-contention, shard-per-core
database.async().timeseriesAppend("SensorReading",
    timestamps, new Object[] { sensorIds, locations, temperatures, humidities },
    successCallback, errorCallback);

// Query — range scan with column projection
try (TimeSeriesCursor cursor = engine.query(
    fromTimestamp, toTimestamp,
    new int[] { 0, 2 },          // columns: timestamp + temperature only
    TagFilter.eq("sensor_id", "sensor-A"))) {

  while (cursor.hasNext()) {
    final TimeSeriesRecord record = cursor.next();
    final long ts = record.getTimestamp();
    final double temp = record.getDouble(2);
  }
}

// Aggregation push-down — computed inside the engine, not row-by-row
final AggregationResult result = engine.aggregate(
    fromTimestamp, toTimestamp,
    2,                            // column index: temperature
    AggregationType.AVG,
    Duration.ofHours(1).toNanos(), // 1-hour buckets
    TagFilter.eq("sensor_id", "sensor-A"));

for (final TimeBucket bucket : result.getBuckets()) {
  System.out.println(bucket.getTimestamp() + " → " + bucket.getValue());
}
```

### 5.8 HTTP Query Endpoint

TimeSeries queries can use the existing ArcadeDB query endpoint (SQL goes through the standard parser):

```bash
# Via existing /api/v1/query endpoint (SQL)
curl -X POST "http://localhost:2480/api/v1/query/mydb" \
  -H "Content-Type: application/json" \
  -d '{
    "language": "sql",
    "command": "SELECT time_bucket('"'"'1h'"'"', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading WHERE ts BETWEEN '"'"'2026-02-19'"'"' AND '"'"'2026-02-20'"'"' GROUP BY hour"
  }'
```

Optionally, a dedicated timeseries query endpoint with a simpler JSON request format:

```
POST /api/v1/ts/{database}/query
Content-Type: application/json

{
  "type": "SensorReading",
  "from": "2026-02-19T00:00:00Z",
  "to": "2026-02-20T00:00:00Z",
  "columns": ["temperature", "humidity"],
  "filter": { "sensor_id": "sensor-A" },
  "aggregation": "avg",
  "granularity": "1h"
}
```

Response:
```json
{
  "result": [
    { "time": "2026-02-19T00:00:00Z", "temperature": 22.3, "humidity": 64.5 },
    { "time": "2026-02-19T01:00:00Z", "temperature": 21.8, "humidity": 65.1 },
    ...
  ]
}
```

This simplified endpoint is **Grafana-friendly** — it can power a Grafana JSON data source plugin with minimal configuration.

### 5.9 Protocol Compatibility Matrix

| Client / Tool | Protocol | ArcadeDB Endpoint | Notes |
|---|---|---|---|
| **Telegraf** | InfluxDB Line Protocol v2 | `POST /api/v2/write` | Point `output.influxdb_v2` at ArcadeDB |
| **Grafana Agent** | ILP or Prometheus remote write | `POST /api/v1/ts/{db}/write` | Via InfluxDB output |
| **curl / scripts** | ILP text | `POST /api/v1/ts/{db}/write` | Simplest integration |
| **PostgreSQL clients** | SQL (PG wire) | Port 5432 (postgresw module) | Full SQL, `time_bucket()` works |
| **Any SQL client** | SQL (HTTP) | `POST /api/v1/query/{db}` | Standard ArcadeDB SQL |
| **Java embedded** | Java API (direct) | `TimeSeriesEngine` class | Fastest: ~0.5-1μs/sample |
| **Grafana dashboards** | JSON query | `POST /api/v1/ts/{db}/query` | Simplified JSON request/response |
| **Cypher clients** | OpenCypher | `POST /api/v1/query/{db}` | `ts.*` functions for graph+TS |
| **IoT devices** | ILP over TCP (future) | Raw TCP socket | Like QuestDB's port 9009 |

### 5.10 Summary: What's New vs. What's Reused

| Component | New or Reused | Details |
|---|---|---|
| `CREATE TIMESERIES TYPE` parser | **New** | Extends `CreateTypeAbstractStatement`, adds TIMESTAMP/TAGS/FIELDS/SHARDS |
| `INSERT INTO` for timeseries | **Reused** | Existing `InsertStatement`, routes to `TimeSeriesEngine` instead of `LocalBucket` |
| `time_bucket()` function | **New** | `SQLFunctionTimeBucket extends SQLFunctionAbstract`, registered in `SQLFunctionFactoryTemplate` |
| `first()`, `last()` functions | **New** | `SQLFunctionFirst`, `SQLFunctionLast` — track min/max timestamp during aggregation |
| `GROUP BY` execution | **Reused** | Existing `AggregateProjectionCalculationStep` — `time_bucket()` returns a key, standard grouping |
| `TIMESERIES ... AS` clause | **New** | Extends `SelectStatement` grammar in `SQLParser.g4` for graph+TS joins |
| `ts.*` Cypher functions | **New** | Registered in native `CypherFunctionRegistry` (same as `text.*`, `math.*`), evaluated by `ExpressionEvaluator` |
| `ts.*` Cypher procedures | **New** | Registered in `CypherProcedureRegistry` (same as `algo.*`, `path.*`), executed by `CallStep` |
| HTTP ingestion endpoint | **New** | `PostTimeSeriesWriteHandler extends AbstractServerHttpHandler`, ILP parser |
| HTTP query endpoint | **New** | `PostTimeSeriesQueryHandler`, simplified JSON format |
| HTTP routing | **Reused** | Existing `HttpServer.setupRoutes()` — just add new routes |
| Authentication | **Reused** | Existing `AbstractServerHttpHandler` handles Basic/Bearer auth |

---

## Part 6: Storage Architecture — Two-Layer Design

### 6.1 Core Insight: Mutable Data Needs Pages, Immutable Data Does Not

ArcadeDB's WAL logs changes at the **page level**: `(fileId, pageNumber, deltaFrom, deltaTo, content)`. Replication sends pages. Transactions track modified pages via MVCC. These guarantees are essential for **mutable** data — data being written by concurrent transactions.

However, once timeseries data is **sealed** (compacted), it is never modified again. Sealed data has already been WAL-logged and replicated when it was mutable. Therefore:

- **Mutable data** → MUST be paginated (`PaginatedComponent`) for WAL, MVCC, transactions, replication
- **Sealed data** → does NOT need pages. It is immutable, so no WAL, no MVCC, no transactions. Each server can compact independently.

This leads to a **two-layer architecture** that separates the hot write path from the cold read-optimized storage:

```
MUTABLE LAYER (.tsbucket)                  SEALED LAYER (per-column files)
─────────────────────────                  ────────────────────────────────
PaginatedComponent (64KB pages)            Plain binary files
WAL-logged, MVCC, replicated               NOT in WAL, NOT replicated
Row-oriented (append-friendly)             Columnar (one file per column)
Holds last seconds/minutes of data         Holds 99%+ of all historical data
Concurrent transactions write here         Never modified after creation
Fixed 64KB page size                       Variable-size blocks, ZERO waste
                                           Each server compacts independently
```

### 6.2 Shard-Per-Core Parallelism — Zero-Contention Ingestion

#### The Problem with a Single Mutable File

If all threads write to a single `TimeSeriesBucket`, MVCC conflicts serialize writes: thread 1 commits, thread 2 retries, thread 3 waits. On an 8-core machine, 7 cores are idle most of the time during ingestion bursts.

#### ArcadeDB's Existing Solution: N Buckets Per Type

ArcadeDB already solves this for regular document/graph types:
- A type has **N buckets** (default = number of cores), each a separate `LocalBucket` file
- `ThreadBucketSelectionStrategy`: `Thread.currentThread().threadId() % N` → deterministic, lock-free
- Each bucket has its **own LSM index partition** (via `TypeIndex` → `List<IndexInternal>`)
- The async API (`DatabaseAsyncExecutorImpl`) routes tasks to thread slots via `getSlot(bucket.getFileId())`
- WAL files are also per-thread: `activeWALFilePool[threadId % poolSize]`
- Result: **zero contention** — each core writes to its own bucket, its own index, its own WAL file

#### TimeSeries Shard-Per-Core: Same Principle

A `TimeSeriesType` with N shards creates N independent write/compact/read units:

```
SHARD-PER-CORE ARCHITECTURE (8-core example):

  Thread 0 ──→ Shard 0: mutable_0.tsbucket + sealed_0.ts.* (own files, own compaction)
  Thread 1 ──→ Shard 1: mutable_1.tsbucket + sealed_1.ts.* (own files, own compaction)
  Thread 2 ──→ Shard 2: mutable_2.tsbucket + sealed_2.ts.* (own files, own compaction)
  ...
  Thread 7 ──→ Shard 7: mutable_7.tsbucket + sealed_7.ts.* (own files, own compaction)

  No locks. No MVCC conflicts. No shared state during writes.
  Each shard is a fully independent timeseries storage unit.
```

**What is a shard?** Each shard consists of:
- One `TimeSeriesBucket` (mutable, paginated, `PaginatedComponent`)
- One `TimeSeriesSealedStore` (sealed column files + index)
- Its own compaction thread/schedule
- Its own free page list, compaction watermark, checkpoint state

**Shard assignment:** Uses `BucketSelectionStrategy`, same as regular types:
- **`ThreadBucketSelectionStrategy`** (default for TimeSeries): `threadId % N` → maximum write parallelism, zero contention. Best for high-throughput ingestion from many sources.
- **`PartitionedBucketSelectionStrategy`**: hash(tag_values) % N → all data for a specific series (e.g., `sensor_id='A'`) lands in the same shard. Best for single-series query performance (no cross-shard merge needed for point queries).

**Async API integration:** The existing `DatabaseAsyncExecutorImpl` routes TimeSeries writes exactly like document writes:
```java
// Thread-affine routing (existing infrastructure)
TimeSeriesBucket shard = type.getShardByRecord(record, async);  // threadId % N
int slot = asyncExecutor.getSlot(shard.getFileId());
asyncExecutor.scheduleTask(slot, new AsyncTimeSeriesAppend(shard, samples, ...));
```

**WAL parallelism:** Each async thread already writes to its own WAL file (`activeWALFilePool[threadId % poolSize]`). Since each shard's mutable pages are only modified by one thread, WAL writes are lock-free.

#### Why This Achieves Datadog Monocle-Level Performance

| Aspect | Datadog Monocle | ArcadeDB TimeSeries |
|---|---|---|
| Architecture | Shard-per-core LSM (Rust) | Shard-per-core two-layer (Java) |
| Write contention | Zero (one LSM per core) | Zero (one mutable file per core) |
| Thread model | Lock-free, core-pinned | Thread-affine via `BucketSelectionStrategy` |
| Compaction | Per-shard | Per-shard |
| WAL | Per-core | Per-thread (`WALFilePool`) |
| Tag routing | Tag-hash sharding | Configurable: thread or tag-hash |

#### Read Path with Shards

Queries transparently merge across all shards:

```
Query: SELECT avg(temperature) FROM SensorReading
       WHERE timestamp BETWEEN T1 AND T2

For each shard (0..N-1) IN PARALLEL:
  1. Query shard's sealed store (binary search its index)
  2. Query shard's mutable bucket (scan active pages)
  3. Produce partial aggregation (sum, count)

Final merge:
  Combine partial aggregations from all shards → final result
  (SUM = sum of sums, COUNT = sum of counts, AVG = total_sum / total_count)
```

**Key optimization**: Shard queries run **in parallel** (one per core). A range query on an 8-shard type uses all 8 cores for both sealed and mutable reads. This is the same parallel-scan pattern ArcadeDB already uses for `database.scanType()` across buckets.

**Single-series queries with `PartitionedBucketSelectionStrategy`**: If the type uses tag-hash partitioning (e.g., partition by `sensor_id`), a query like `WHERE sensor_id = 'A'` can determine the exact shard: `hash('A') % N`. Only one shard is queried — zero cross-shard overhead.

#### Shard Count Configuration

```sql
-- Default: one shard per available core (maximum ingestion parallelism)
CREATE TIMESERIES TYPE SensorReading
  TIMESTAMP ts PRECISION NANOSECOND
  TAGS (sensor_id STRING, location STRING)
  FIELDS (temperature DOUBLE, humidity DOUBLE, pressure DOUBLE)

-- Explicit shard count
CREATE TIMESERIES TYPE SensorReading
  SHARDS 16
  ...

-- Tag-hash partitioning (data locality for single-series queries)
CREATE TIMESERIES TYPE SensorReading
  PARTITION BY (sensor_id)
  ...
```

Default shard count = `Runtime.getRuntime().availableProcessors()` (same convention as `ASYNC_WORKER_THREADS`).

### 6.3 File Layout Per TimeSeries Type

For a type `SensorReading` with 5 columns and 4 shards (4-core machine):

```
SHARD 0:
  MUTABLE (paginated — WAL, MVCC, replication)
    SensorReading_0.tsbucket
  SEALED (immutable — per-column files, no page overhead)
    SensorReading_0.ts.index              ← block directory (in memory)
    SensorReading_0.ts.col.0.timestamp    ← delta-of-delta compressed
    SensorReading_0.ts.col.1.sensor_id    ← dictionary + RLE compressed
    SensorReading_0.ts.col.2.temperature  ← Gorilla XOR compressed
    SensorReading_0.ts.col.3.humidity     ← Gorilla XOR compressed
    SensorReading_0.ts.col.4.pressure     ← Gorilla XOR compressed

SHARD 1:
    SensorReading_1.tsbucket
    SensorReading_1.ts.index
    SensorReading_1.ts.col.0.timestamp
    ... (same column files)

SHARD 2:
    SensorReading_2.tsbucket
    SensorReading_2.ts.index
    SensorReading_2.ts.col.0.timestamp
    ...

SHARD 3:
    SensorReading_3.tsbucket
    SensorReading_3.ts.index
    SensorReading_3.ts.col.0.timestamp
    ...
```

Each shard is **completely independent**: its own mutable file, its own sealed files, its own compaction watermark, its own free page list. No shared state between shards during writes or compaction.

### 6.4 Mutable File (.tsbucket) — The Transactional Write Buffer

`TimeSeriesBucket extends PaginatedComponent` — uses ArcadeDB's standard page infrastructure for full ACID compliance.

#### Page Types in the Mutable File

**Header Page (Page 0):**

```
[Standard page header: version(4B) + contentSize(4B)]
  magic_number              (4B)   "TSBC"
  format_version            (2B)
  column_count              (2B)   total columns (1 timestamp + N tags + M fields)
  column_definitions[]      (variable) - for each column:
    name_length             (2B)
    name                    (UTF-8 bytes)
    data_type               (1B)   LONG/DOUBLE/STRING/INTEGER/etc. (maps to Type enum)
    column_role             (1B)   TIMESTAMP=0, TAG=1, FIELD=2
    compression_hint        (1B)   DELTA_OF_DELTA=0, GORILLA_XOR=1, DICTIONARY=2, SIMPLE8B=3, NONE=4
  total_sample_count        (8B)   total samples in mutable file (not yet compacted)
  min_timestamp             (8B)   global min across active pages
  max_timestamp             (8B)   global max across active pages
  active_data_page_count    (4B)   number of data pages with uncompacted data
  compaction_watermark      (8B)   max timestamp of data confirmed in sealed files
                                   (used for crash recovery — see section 6.8)

  --- Free Page List (for page reuse after compaction) ---
  free_page_count           (4B)   number of reusable pages
  free_page_list[]          (4B each) page numbers available for reuse

  --- Pre-Compaction Checkpoint (crash safety for sealed files) ---
  compaction_in_progress    (1B)   0 = idle, 1 = compaction active
  sealed_col_offsets[]      (8B each, one per column) byte offset of each sealed column
                                   file BEFORE compaction started
  sealed_index_size         (8B)   byte size of .ts.index file BEFORE compaction started
```

These checkpoint fields enable crash recovery of sealed files (see section 6.8).

**Directory Pages (Page 1..D) — Mutable Data Page Index:**

The directory is a paginated list of active data pages inside the mutable file. It is **not sorted** — entries are appended when new data pages are created and removed when pages are compacted. Reads require a **linear scan**, which is efficient because the directory is tiny (typically ~100-200 entries covering the last seconds/minutes of data).

The directory is paginated (WAL-protected) because it is modified by transactions: compaction cleanup removes entries, new page creation adds entries. Both operations go through `TransactionContext`.

```
[Standard page header]
  entry_count            (4B)
  next_directory_page    (4B)   pointer to next directory page (0 = last)
[Entries] - unsorted, appended on page creation, removed on compaction:
  data_page_number       (4B)
  min_timestamp          (8B)
  max_timestamp          (8B)
  sample_count           (4B)
  series_count           (2B)
  is_sorted              (1B)   0 = timestamps in arrival order, 1 = sorted by timestamp
```

The `is_sorted` flag is set to 0 on creation and flipped to 1 if compaction discovers the page is already in order (optimization: skip sort step for in-order data).

**Active Data Pages (Page D+1..N) — Row-Oriented, MVCC-Safe:**

The active data pages use a **row-oriented layout** so that concurrent transactions can append samples via MVCC. This is the key difference from the sealed files.

```
[Standard page header: version(4B) + contentSize(4B)]
  sample_count           (4B)
  min_timestamp          (8B)
  max_timestamp          (8B)
  row_size               (2B)   fixed bytes per sample row (computed from schema)
[Sample rows — appended sequentially, fixed-size:]
  row 0: [timestamp(8B)][tag0_dictIndex(2B)][field0(8B)][field1(8B)]...
  row 1: [timestamp(8B)][tag0_dictIndex(2B)][field0(8B)][field1(8B)]...
  ...
[Tag Dictionary — at tail of page, grows backwards:]
  dict_count             (2B)
  entry 0: [length(2B)][string bytes]
  entry 1: [length(2B)][string bytes]
```

Fixed-size sample rows make appending trivial: write at `headerSize + sampleCount * rowSize`, increment count. The tag dictionary at the page tail maps string tags to small integer indices used in the sample rows.

#### Concurrent Transaction Handling (MVCC)

Multiple transactions can write to the same active page using standard ArcadeDB MVCC — the same mechanism `LocalBucket` uses:

```
tx1: begin
  → reads active page (version V, sample_count=200)
  → appends 100 samples at rows 200..299, sample_count becomes 300
  → commits → page version becomes V+1
  → WAL logs only the delta (the new bytes appended)

tx2: begin (concurrent with tx1)
  → reads active page (version V, sample_count=200)
  → appends 50 samples at rows 200..249, sample_count becomes 250
  → tries to commit → MVCC conflict! page is now V+1
  → automatic retry: reads page V+1 (sample_count=300, includes tx1's data)
  → appends 50 samples at rows 300..349, sample_count becomes 350
  → commits → page version V+2
  → WAL logs only the new delta (rows 300..349)
```

This works because:
- `TransactionContext` checks page versions at commit time (existing MVCC logic)
- `ConcurrentModificationException` triggers automatic retry (existing behavior)
- WAL logs only the changed byte range, not the full page (efficient)
- Replication propagates the page delta — identical to existing bucket replication

When the active page fills up (~2,400 samples at 26 bytes/row for a 3-column schema), a new empty active page is created and the full page awaits compaction.

#### Page Reuse — Free Page List

When compaction moves data from mutable pages to sealed files, those pages become empty. Rather than growing the mutable file indefinitely, compacted pages are returned to a **free page list** stored in the header page:

```
Lifecycle of a mutable page:
  1. ALLOCATE: Need a new data page
     → If free_page_list is non-empty: pop the last entry, reuse that page number
     → If free_page_list is empty: extend the file (append new page at the end)
  2. FILL: Transaction appends samples to the page via MVCC
  3. COMPACT: Background compaction reads all samples, writes to sealed files
  4. FREE: Compaction cleanup (in transaction):
     → Remove directory entry for the page
     → Push page number onto free_page_list in header
     → Increment free_page_count
     → Page is now available for step 1
```

**Steady-state behavior**: After initial ramp-up, the mutable file reaches a stable size. If ingestion rate is R samples/sec and compaction runs every T seconds, the mutable file holds ~R×T samples worth of pages. Compaction frees pages at the same rate new ones are allocated, so the free list stays near-empty and the file doesn't grow.

**Backpressure**: If compaction falls behind (ingestion spike), the file grows temporarily. Once compaction catches up, the excess pages join the free list. A configuration setting `max_mutable_pages` can optionally trigger throttling of writes if the mutable file exceeds a threshold, giving compaction time to drain.

#### Out-of-Order Data Handling

TimeSeries data frequently arrives out of order: sensors may have network delays, batch uploads may contain historical data, or distributed collectors may deliver data at different rates. The mutable file handles this at three levels:

**Level 1 — Within a single page (free, always works):**
Active data pages are row-oriented with no ordering requirement. Samples are appended in arrival order regardless of their timestamp value. When the page is later compacted, samples are sorted by timestamp at that point. Cost: zero at write time, negligible sort cost at compaction time (page fits in L1 cache).

**Level 2 — Across pages, before compaction (free, always works):**
Different pages in the mutable file may have overlapping timestamp ranges. For example:
- Page 5: timestamps [10:00:01 .. 10:00:05] — some early, some late arrivals
- Page 6: timestamps [10:00:03 .. 10:00:08] — overlapping range

Compaction reads ALL pages being compacted, collects all samples, sorts globally by timestamp, then writes sorted blocks to sealed files. The directory's `min_timestamp`/`max_timestamp` per page are used to select which pages to include in a compaction run.

**Level 3 — After compaction (late-arriving data older than compaction_watermark):**
This is the hard case: data arrives with a timestamp that falls within a range already compacted into sealed files.

**Strategy A (MVP — Overlapping Sealed Blocks):**
- Accept the late data into the mutable file normally (no rejection)
- When compacting, write the new sealed blocks even though they overlap existing sealed blocks
- The sealed index file records overlapping blocks: the `is_overlapping` flag is set
- At query time, if overlapping blocks exist in the requested range, merge-sort across all overlapping blocks (same as merging mutable + sealed)
- Periodic **major compaction** rewrites overlapping sealed blocks into a single sorted sequence (runs less frequently, e.g., daily)

```
Minor compaction (frequent, fast):
  Mutable pages → NEW sealed blocks (may overlap existing sealed blocks)

Major compaction (infrequent, more I/O):
  Overlapping sealed blocks → single sorted sequence (no more overlaps)
  Only touches the affected time range, not the entire sealed file
```

**Strategy B (Future — Configurable out-of-order tolerance window):**
- Configure a time window (e.g., 5 minutes) during which out-of-order data is expected
- Compaction only seals data older than `now - tolerance_window`
- Data within the tolerance window stays in the mutable file, even if the page is "full"
- This eliminates overlapping sealed blocks entirely for well-behaved data sources

### 6.5 Sealed Files — Per-Column Immutable Storage

The sealed layer is **not paginated**. It consists of plain binary files read via `java.nio.channels.FileChannel` positioned reads. This means:

- **Variable-size blocks**: No 64KB page boundary. A block of 50,000 compressed samples using 12,847 bytes occupies exactly 12,847 bytes. Zero waste.
- **No WAL overhead**: Sealed files are derived data — the mutable file was the WAL-protected source of truth.
- **No MVCC**: Sealed files are never modified by transactions. Compaction appends new blocks; retention rewrites the file.
- **No replication**: Each server compacts independently. The WAL-replicated mutable file ensures all servers have the same logical data.
- **Per-column I/O**: `SELECT avg(temperature)` reads only `.col.0.timestamp` and `.col.2.temperature`. Files for humidity, pressure, sensor_id are never opened.

#### Shared Index File (.ts.index) — NOT Paginated, Loaded In Memory

All column files share the same block boundaries — block N in every column file covers the same set of samples. A single shared index file provides the block directory.

**Key design decision**: The sealed index is a **plain file, NOT paginated**. It does not use `PaginatedComponent`, WAL, or MVCC. It is:
- **Loaded entirely into memory** at database open (trivially small — see size analysis below)
- **Sorted by `min_timestamp`** for binary search during range queries
- **Rewritten entirely** on each compaction (append new blocks, regenerate file)
- **Never modified by transactions** — only by the compaction background thread

This is safe because the sealed index is derived data: it can always be rebuilt from the sealed column files themselves. Crash safety is handled by the pre-compaction checkpoint in the mutable file header (see section 6.8).

```
FILE HEADER
  magic                (4B)   "TSIX"
  format_version       (2B)
  column_count         (2B)
  block_count          (4B)
  total_sample_count   (8B)
  min_timestamp        (8B)
  max_timestamp        (8B)

BLOCK DIRECTORY — one entry per block, sorted by min_timestamp:
  min_timestamp        (8B)
  max_timestamp        (8B)
  sample_count         (4B)
  is_overlapping       (1B)   0 = no overlap with other blocks, 1 = overlapping range
                              (set when late-arriving data creates blocks that overlap
                               existing sealed blocks — see Out-of-Order Handling)
  column_offsets[]     (8B each)  byte offset in each column file where this block starts
  column_sizes[]       (4B each)  compressed size in each column file for this block

FOOTER
  directory_offset     (8B)   byte position where the directory starts in this file
  magic                (4B)   "TSIX" (repeated for validation)
```

**Size**: For 5 columns, each directory entry is 21 + (5 x 8) + (5 x 4) = 81 bytes.
A dataset of 1 billion samples with 50,000 samples/block = 20,000 blocks → directory = **~1.6 MB**. Trivially fits in memory and is cached on first read.

**Why the directory is at the end** (like a Parquet footer): The file is append-only. New blocks are appended, then the directory is rewritten at the new end. A reader opens the file, reads the footer to find the directory offset, then reads the directory. This avoids reserving space at the beginning.

**Contrast with the mutable directory**: The mutable file's directory pages (section 6.4) ARE paginated because they are modified by transactions (compaction cleanup, new page creation). The sealed index is not — it is a standalone file managed exclusively by the compaction thread.

#### Per-Column Files (.ts.col.N.*)

Each column file is pure compressed data with a minimal header:

```
FILE HEADER
  magic                (4B)   "TSCL"
  column_index         (2B)   which column this file stores
  compression_type     (1B)   default codec for this column
  block_count          (4B)

BLOCK 0 (variable size — tightly packed, zero padding)
  base_value           (8B)   first raw value (for delta/XOR encoding)
  compressed_data      (N bytes)

BLOCK 1 (starts IMMEDIATELY after block 0)
  base_value           (8B)
  compressed_data      (M bytes)

... blocks continue with zero gaps ...
```

No per-block headers are needed inside the column file — the shared index file already knows each block's offset and size. The column file is essentially a concatenation of compressed byte arrays.

#### Compression Strategy Per Column Type

| Column Type | Codec | Typical Ratio | Notes |
|---|---|---|---|
| DATETIME/LONG (timestamp) | Delta-of-delta | 96% → 1 bit/sample | Regular intervals compress best |
| DOUBLE (field values) | Gorilla XOR | avg 1.37 bytes/sample | Slowly changing values compress best |
| INTEGER/LONG (counters) | Simple-8b RLE | 4-8x | Monotonic counters compress extremely well |
| STRING TAG (low cardinality) | Dictionary + Simple-8b RLE | 10-100x | Dictionary is per-block |
| STRING TAG (high cardinality) | Dictionary (block-local) | 2-5x | Each block builds its own dictionary |

#### I/O Strategy: FileChannel Positioned Reads

Sealed files are read via standard `java.nio.channels.FileChannel`:

```java
FileChannel channel = FileChannel.open(columnFilePath, StandardOpenOption.READ);
ByteBuffer buf = ByteBuffer.allocateDirect(blockSize);   // direct buffer, no extra copy
channel.read(buf, blockOffset);                           // positioned read at exact offset
```

Why `FileChannel` over `mmap`:
- **No TLB pressure**: mmap competes with JVM heap for translation lookaside buffer entries. Many large sealed files could degrade JVM performance.
- **No SIGBUS risk**: mmap throws SIGBUS (crashes JVM) on I/O errors. FileChannel throws a catchable `IOException`.
- **Controlled memory**: FileChannel reads into explicitly sized buffers. mmap lets the OS decide what stays in memory.
- **Sequential scan friendly**: Range queries read blocks sequentially. FileChannel with OS readahead is as fast as mmap for this pattern.
- **Java 21+ optimization**: `FileChannel.read(ByteBuffer.allocateDirect(...), position)` with direct buffers avoids the user-space copy.

The OS page cache still caches sealed file contents automatically — hot column files stay in memory without explicit management.

### 6.6 Write Path

#### Ingestion (Transactional, Shard-Per-Core)

```
1. Application calls appendSamples(timestamps[], tags[], values[]...)
   ↓
2. Shard selection (lock-free):
   → ThreadBucketSelectionStrategy: shardIdx = threadId % N (default)
   → PartitionedBucketSelectionStrategy: shardIdx = hash(tag_values) % N
   → Async API: task routed to slot = getSlot(shard.mutableBucket.getFileId())
   ↓
3. TransactionContext writes sample rows into the shard's active page
   → Standard MVCC: if concurrent tx committed first, retry on new page version
   → With ThreadBucketSelectionStrategy: ZERO conflicts (each thread owns its shard)
   → WAL logs only the appended byte range (delta)
   → WAL write is lock-free: activeWALFilePool[threadId % poolSize]
   → Page fills up → new active page created, old page awaits compaction
   ↓
4. Transaction commits → WAL + replication propagate the page changes
   ↓
5. Shard's mutable file now holds recent uncompacted data (seconds to minutes)
   Other shards are completely unaffected (no shared state).
```

**Throughput scaling**: With N shards and `ThreadBucketSelectionStrategy`, ingestion throughput scales linearly with cores. On an 8-core machine, 8 threads write to 8 independent shards with zero MVCC conflicts, zero WAL contention, and zero lock overhead. This matches Datadog Monocle's shard-per-core architecture.

#### Compaction (Background, Per-Shard, Crash-Safe)

Compaction moves data from a shard's mutable file to its sealed files. Each shard compacts independently — N shards means N concurrent compaction threads with zero contention. The algorithm is designed so that a **JVM crash at any point** leaves the system in a consistent state.

```
COMPACTION ALGORITHM (crash-safe):

PHASE 1 — PRE-COMPACTION CHECKPOINT (in transaction, WAL-protected):
  a. Record current state of sealed files in the mutable header page:
     → sealed_col_offsets[i] = current byte size of each column file
     → sealed_index_size    = current byte size of .ts.index
     → compaction_in_progress = 1
  b. Commit this transaction
     → WAL logs the header page change → replicated
     → This is the "rollback point" for crash recovery

  *** If JVM crashes here: checkpoint is committed, but no sealed writes yet.
      Recovery sees compaction_in_progress=1, truncates sealed files to
      checkpointed offsets (which are the current sizes — no-op). Safe. ***

PHASE 2 — READ & TRANSFORM (no locks, no transactions):
  a. Read all full data pages from mutable file directory
     (only pages marked as full / not the current active page)
  b. Collect all samples from those pages into memory
  c. Sort by timestamp (global sort across all pages)
  d. Split into columns
  e. Chunk into SEALED_BLOCK_SIZE rows (default 65,536) — avoids one giant block per shard
  f. Compress each column chunk independently using the configured codec

PHASE 3 — WRITE SEALED FILES (append-only, no WAL):
  a. For each chunk: write inline block metadata (magic 0x5453424C + minTs + maxTs +
     sampleCount + per-column compressed sizes), then append compressed column data
     → Block metadata enables directory reconstruction on cold open (loadDirectory())
  b. fsync ALL sealed files
     → After fsync, sealed data is durable on disk

  *** If JVM crashes here (mid-write): sealed files have partial data
      beyond the checkpointed offsets. Recovery truncates back to
      checkpointed offsets. Mutable pages still intact. Will re-compact. ***

PHASE 4 — COMMIT CLEANUP (in transaction, WAL-protected):
  a. In a NEW TRANSACTION on the mutable file:
     → Remove compacted pages from the directory
     → Push freed page numbers onto the free_page_list in header
     → Update free_page_count
     → Update compaction_watermark = max timestamp of compacted data
     → Update min_timestamp, max_timestamp, total_sample_count
     → Set compaction_in_progress = 0
     → Clear sealed_col_offsets[] and sealed_index_size
  b. Commit this transaction
     → WAL logs the cleanup → replicated

  *** If JVM crashes here (before commit): cleanup tx didn't commit.
      Recovery sees compaction_in_progress=1, truncates sealed files to
      checkpointed offsets. But the sealed data IS valid (it was fsync'd).
      However, the mutable pages weren't freed, so they'll be re-compacted.
      Result: duplicate data in sealed files after recovery? NO — because
      we truncated back to checkpoint offsets. The re-compaction produces
      the same sealed blocks. Safe and idempotent. ***

PHASE 5 — DONE
  Mutable file: only holds recent, uncompacted data (seconds to minutes)
  Sealed files: hold all historical data (days to years)
  Free pages: available for new ingestion
```

**Key invariant**: The `compaction_watermark` in the mutable header is ONLY advanced (step 4a) AFTER sealed files are fsync'd (step 3c). This guarantees that any data below the watermark is durably stored in sealed files. Data above the watermark is in the mutable file (WAL-protected). No data is ever lost.

### 6.7 Read Path (Range Query)

Queries use a **pull-based streaming iterator pipeline** that never materializes all rows in memory. The SQL execution engine calls `syncPull(context, nRecords)` which returns at most `nRecords` rows per call — aggregation steps pull batches in a loop until exhausted.

#### Iterator Chain

```
FetchFromTimeSeriesStep.syncPull(ctx, N)
  └→ TimeSeriesEngine.iterateQuery(fromTs, toTs, columnIndices, tagFilter)
       └→ PriorityQueue<PeekableIterator> — merge-sort across shards by timestamp
            ├→ TimeSeriesShard[0].iterateRange(fromTs, toTs, columnIndices, tagFilter)
            │    ├→ TimeSeriesSealedStore.iterateRange() — sealed blocks first
            │    └→ TimeSeriesBucket.iterateRange()     — mutable pages second
            ├→ TimeSeriesShard[1].iterateRange(...)
            └→ ...
```

Each `next()` call on the engine iterator advances only the shard with the smallest current timestamp (min-heap). Memory usage is O(shardCount × blockSize) — constant regardless of total dataset size.

#### Full Query Flow

```
Query: SELECT avg(temperature) FROM SensorReading
       WHERE timestamp BETWEEN '2026-02-19' AND '2026-02-20'
         AND sensor_id = 'A'

FOR EACH SHARD (0..N-1):

  STEP 1: SEALED FILES (99%+ of shard's data, columnar, fast)
    a. Binary search block directory → blocks overlapping time range         O(log B)
    b. Per block: decompress timestamps → binary search for exact range      O(log N)
    c. Lazy column decompression: only decode value columns if rows match
    d. Early termination: stop when minTimestamp > toTs
    e. Files NOT touched: .col.3.humidity, .col.4.pressure (zero I/O)

  STEP 2: MUTABLE FILE (last few seconds/minutes, small)
    a. Short-circuit if empty (getSampleCount() == 0)
    b. Scan pages lazily → filter by time range → yield matching rows

  STEP 3: CHAIN sealed iterator → mutable iterator (sealed first, mutable second)
    Apply tag filter inline during iteration

MERGE across shards:
  PriorityQueue<PeekableIterator> — min-heap by timestamp
  Each next() advances only the shard with smallest current timestamp
  For aggregations: AggregateProjectionCalculationStep pulls all rows via syncPull()
```

**Optimization — PartitionedBucketSelectionStrategy**: If the type partitions by `sensor_id` and the query filters on `sensor_id = 'A'`, the engine computes `hash('A') % N` to identify the single shard containing all data for sensor A. Only that one shard is queried — zero cross-shard overhead.

**Performance characteristics:**
- Streaming: O(shardCount × blockSize) memory — never materializes all rows
- Block selection: O(log B) binary search per shard (B = blocks in shard)
- Within-block search: O(log N) binary search on sorted timestamps
- Column I/O: reads ONLY the column files needed by the query
- Lazy decompression: value columns decoded only when timestamps match
- Tag filtering: dictionary-decoded bitmask, applied inline during iteration
- Early termination: stops scanning blocks once `minTimestamp > toTs`
- Empty bucket short-circuit: zero cost for mutable layer after compaction
- Cold queries: sealed block directory persisted inline (survives close/reopen)
- Profiling: `PROFILE SELECT ...` shows per-step cost and row counts via `FetchFromTimeSeriesStep`
- Cross-shard merge: min-heap merge-sort for raw scans, trivial for aggregations

### 6.8 Crash Recovery

Sealed files have no WAL. A JVM crash during compaction could leave them in an inconsistent state (partially written blocks). The **pre-compaction checkpoint** protocol in section 6.6 ensures crash safety. Here is the full recovery algorithm:

#### Recovery Algorithm (runs at database open)

```
On startup:

STEP 1 — Recover mutable file from WAL (standard ArcadeDB recovery)
  → All WAL-protected fields are now reliable:
    - compaction_watermark
    - compaction_in_progress flag
    - sealed_col_offsets[] (checkpoint of sealed file sizes before compaction)
    - sealed_index_size (checkpoint of index file size before compaction)
    - free_page_list
    - directory entries

STEP 2 — Check if compaction was interrupted
  IF compaction_in_progress == 1:
    → A compaction was running when the JVM crashed.
    → Sealed files may have partial/corrupt data beyond the checkpoint.

    a. For each sealed column file i:
       → Truncate to sealed_col_offsets[i] bytes
       → This removes any partially written blocks from the failed compaction

    b. Truncate .ts.index to sealed_index_size bytes
       → This removes any partially written index entries

    c. In a NEW TRANSACTION on the mutable file:
       → Set compaction_in_progress = 0
       → Clear sealed_col_offsets[] and sealed_index_size
       → Commit (WAL-logged)

    d. Log: "TimeSeries recovery: truncated sealed files to pre-compaction state.
            Mutable pages preserved, will be re-compacted."

  IF compaction_in_progress == 0:
    → No compaction was running, OR the compaction completed cleanly.
    → Sealed files are consistent. No truncation needed.

STEP 3 — Validate compaction_watermark consistency
  a. Read .ts.index → find the max timestamp across all sealed blocks
  b. Verify: sealed_max_timestamp <= compaction_watermark
     (If not, something is wrong — log error and truncate sealed files
      to match the watermark, then re-compact)

STEP 4 — Load sealed index into memory
  a. Read .ts.index into memory (sorted block directory)
  b. Ready for queries

STEP 5 — Resume normal operation
  → Mutable pages with data > compaction_watermark are valid, will be compacted
  → Mutable pages with data <= compaction_watermark may exist if cleanup
     didn't commit — safe to free (compaction will handle this)
  → Background compaction resumes on schedule
```

#### Crash Scenarios Matrix

| Crash Point | Mutable State | Sealed State | Recovery Action |
|---|---|---|---|
| Before Phase 1 commit | Unchanged | Unchanged | Nothing to do |
| After Phase 1, before Phase 3 | Has checkpoint | No new data written | Truncate to checkpoint (no-op) |
| During Phase 3 (mid-write) | Has checkpoint | Partially written | Truncate to checkpoint, discard partial blocks |
| After Phase 3 fsync, before Phase 4 | Has checkpoint | Fully written + fsync'd | Truncate to checkpoint. Data re-compacted (safe, idempotent) |
| After Phase 4 commit | Clean (pages freed) | Fully written | compaction_in_progress=0, nothing to do |

**Key invariant**: The mutable file is the **source of truth**. Sealed files are derived data and can always be rebuilt from mutable pages that haven't been cleaned up. The `compaction_watermark` is only advanced AFTER sealed files are fsync'd AND the cleanup transaction commits. This guarantees zero data loss in all crash scenarios.

### 6.9 Replication

The two-layer, sharded design has elegant replication properties:

```
Leader:   tx writes → shard K's mutable file → WAL → replicates to followers
          compaction (local, per-shard) → shard K's sealed files

Follower: receives WAL → applies to shard K's mutable file (identical mutable state)
          compaction (local, per-shard) → shard K's sealed files
```

- **WAL replication covers only the mutable files** (N small files, only recent data per shard)
- **Sealed files are NOT replicated** — each server compacts each shard independently
- Sealed files on leader and followers are **logically equivalent** (same data) but may differ in block boundaries. This is perfectly fine — same model as Cassandra's per-node compaction.
- **Zero replication overhead** for historical data (the vast majority of storage)
- **Leader failover**: the new leader's sealed files are already up to date (derived from the same WAL-replicated mutable data)
- **Shard count is the same** on leader and followers (it's part of the type schema)

### 6.10 Retention

**Strategy 1: Sealed file truncation (default)**
For each shard independently:
1. Read shard's `.ts.index` → find blocks where `max_timestamp < now - retention_period`
2. Rewrite shard's column files without those old blocks
3. Rewrite shard's `.ts.index` without old entries
4. Update shard's mutable file header's retention watermark (in transaction)

**Strategy 2: Time-partitioned sealed files (for instant retention)**
```sql
CREATE TIMESERIES TYPE SensorReading
  PARTITION BY INTERVAL 1 MONTH
  RETENTION 12 MONTHS
```
Creates a separate set of sealed files per time window:
```
SensorReading_202602.ts.index
SensorReading_202602.ts.col.0.timestamp
SensorReading_202602.ts.col.1.sensor_id
...
```
Retention = delete the entire set of files for expired months. Instant, zero I/O.

### 6.11 Major Compaction (Sealed File Defragmentation)

Minor compaction (described in 6.6) runs frequently and may produce overlapping sealed blocks when out-of-order data arrives after previous compaction. **Major compaction** consolidates overlapping blocks:

```
MAJOR COMPACTION (infrequent, e.g., daily or on-demand):

1. Scan .ts.index → identify time ranges with overlapping blocks
   (blocks where is_overlapping=1, or multiple blocks covering the same range)

2. For each overlapping region:
   a. Read all overlapping blocks from column files
   b. Decompress → merge-sort by timestamp → deduplicate
   c. Re-compress into new non-overlapping blocks
   d. Write replacement blocks to NEW temporary column files
   e. fsync temporary files

3. Rewrite sealed column files:
   a. Copy non-affected blocks from old files
   b. Insert replacement blocks in the correct position
   c. fsync new files

4. Atomically swap: rename new files over old files
   (POSIX rename is atomic on the same filesystem)

5. Rewrite .ts.index with all blocks now non-overlapping
```

Major compaction only touches the affected time ranges, not the entire dataset. For well-behaved data sources (no out-of-order after compaction), major compaction is rarely needed.

### 6.12 Series Filtering Optimization

**Default: In-block dictionary filtering**
- Each sealed block's tag column uses dictionary encoding
- To check if `sensor_id = 'A'` exists: scan the block dictionary (<100 entries typically)
- Build a bitmask from dictionary indices to select matching samples
- Fast enough for analytical queries (scan-oriented)

**Optional: LSM-Tree tag index**
- For high-cardinality point lookups, create an LSM-Tree index on `(tag_values, timestamp)`
- Maps `(sensor_id='A', timestamp=X)` → block number in sealed files
- Uses existing `LSMTreeIndex` infrastructure — no new index type needed
- Useful for: "get the latest value for sensor A" (point lookup, not range scan)

### 6.13 SIMD-Accelerated Aggregation (Project Panama)

TimeSeries aggregation (SUM, AVG, MIN, MAX, COUNT) over large decompressed arrays is the hottest path in range queries. SIMD (Single Instruction, Multiple Data) can process 4-8 doubles per CPU cycle instead of one.

ArcadeDB already uses SIMD for vector similarity via JVector's `VectorizationProvider`. The TimeSeries module follows the **same pattern**: an interface with two implementations (pure Java + SIMD), auto-detected at runtime.

#### Interface Design

```java
package com.arcadedb.engine.timeseries.simd;

/**
 * Vectorized operations for timeseries aggregation.
 * Two implementations: ScalarOps (pure Java) and SimdOps (Project Panama Vector API).
 * The provider auto-detects SIMD availability and returns the best implementation.
 */
public interface TimeSeriesVectorOps {

  // === Aggregation over double arrays (field values) ===
  double sum(double[] values, int offset, int length);
  double min(double[] values, int offset, int length);
  double max(double[] values, int offset, int length);
  // AVG = sum / count (no separate method needed)

  // === Aggregation over long arrays (timestamps, counters) ===
  long sumLong(long[] values, int offset, int length);
  long minLong(long[] values, int offset, int length);
  long maxLong(long[] values, int offset, int length);

  // === Filtered aggregation (apply bitmask from tag filtering) ===
  double sumFiltered(double[] values, long[] bitmask, int offset, int length);
  int    countFiltered(long[] bitmask, int offset, int length);   // popcount

  // === Comparison / filtering (produce bitmask) ===
  void greaterThan(double[] values, double threshold, long[] bitmaskOut, int offset, int length);
  void lessThan(double[] values, double threshold, long[] bitmaskOut, int offset, int length);
  void between(double[] values, double low, double high, long[] bitmaskOut, int offset, int length);

  // === Bitmask logic (combine tag filters) ===
  void bitmaskAnd(long[] a, long[] b, long[] out, int length);
  void bitmaskOr(long[] a, long[] b, long[] out, int length);
}
```

#### Pure Java Implementation (Always Available)

```java
package com.arcadedb.engine.timeseries.simd;

/**
 * Scalar (pure Java) implementation. Works on any JDK 21+.
 * No dependencies on incubator modules.
 */
public class ScalarTimeSeriesVectorOps implements TimeSeriesVectorOps {

  @Override
  public double sum(final double[] values, final int offset, final int length) {
    double result = 0.0;
    for (int i = offset; i < offset + length; i++)
      result += values[i];
    return result;
  }

  @Override
  public double min(final double[] values, final int offset, final int length) {
    double result = Double.MAX_VALUE;
    for (int i = offset; i < offset + length; i++)
      if (values[i] < result)
        result = values[i];
    return result;
  }

  // ... analogous for max, sumLong, minLong, maxLong, filtered variants, bitmask ops
}
```

#### SIMD Implementation (Auto-Detected via Project Panama)

```java
package com.arcadedb.engine.timeseries.simd;

import jdk.incubator.vector.*;

/**
 * SIMD-accelerated implementation using Java Vector API (Project Panama).
 * Processes 4 doubles (AVX2/256-bit) or 8 doubles (AVX-512) per cycle.
 * Only instantiated if jdk.incubator.vector module is available.
 */
public class SimdTimeSeriesVectorOps implements TimeSeriesVectorOps {

  private static final VectorSpecies<Double> SPECIES = DoubleVector.SPECIES_PREFERRED;
  // SPECIES_PREFERRED auto-selects: 256-bit (4 lanes) on AVX2, 512-bit (8 lanes) on AVX-512

  @Override
  public double sum(final double[] values, final int offset, final int length) {
    DoubleVector acc = DoubleVector.zero(SPECIES);
    final int bound = SPECIES.loopBound(length);
    int i = offset;
    for (; i < offset + bound; i += SPECIES.length())
      acc = acc.add(DoubleVector.fromArray(SPECIES, values, i));
    double result = acc.reduceLanes(VectorOperators.ADD);
    for (; i < offset + length; i++)   // tail
      result += values[i];
    return result;
  }

  @Override
  public double min(final double[] values, final int offset, final int length) {
    DoubleVector acc = DoubleVector.broadcast(SPECIES, Double.MAX_VALUE);
    final int bound = SPECIES.loopBound(length);
    int i = offset;
    for (; i < offset + bound; i += SPECIES.length())
      acc = acc.min(DoubleVector.fromArray(SPECIES, values, i));
    double result = acc.reduceLanes(VectorOperators.MIN);
    for (; i < offset + length; i++)
      if (values[i] < result)
        result = values[i];
    return result;
  }

  @Override
  public int countFiltered(final long[] bitmask, final int offset, final int length) {
    // SIMD popcount: count bits set in bitmask (number of matching samples)
    int count = 0;
    for (int i = offset; i < offset + length; i++)
      count += Long.bitCount(bitmask[i]);   // intrinsic → POPCNT instruction
    return count;
  }

  // ... analogous for max, sumFiltered, greaterThan, between, bitmask ops
}
```

#### Provider (Runtime Auto-Detection)

```java
package com.arcadedb.engine.timeseries.simd;

/**
 * Singleton provider that detects SIMD availability at startup.
 * Same pattern as JVector's VectorizationProvider.getInstance().
 */
public final class TimeSeriesVectorOpsProvider {

  private static final TimeSeriesVectorOps INSTANCE;

  static {
    TimeSeriesVectorOps ops;
    try {
      // Try to load SIMD implementation — will fail if jdk.incubator.vector is absent
      Class.forName("jdk.incubator.vector.DoubleVector");
      ops = new SimdTimeSeriesVectorOps();
      LogManager.instance().log(TimeSeriesVectorOpsProvider.class, Level.INFO,
          "TimeSeries SIMD acceleration enabled (Vector API, %d-bit lanes)",
          jdk.incubator.vector.DoubleVector.SPECIES_PREFERRED.vectorBitSize());
    } catch (final Throwable e) {
      ops = new ScalarTimeSeriesVectorOps();
      LogManager.instance().log(TimeSeriesVectorOpsProvider.class, Level.INFO,
          "TimeSeries SIMD acceleration not available, using scalar fallback");
    }
    INSTANCE = ops;
  }

  public static TimeSeriesVectorOps getInstance() {
    return INSTANCE;
  }
}
```

#### Where SIMD Is Used in the Query Path

```
Sealed block read → decompress column → double[] array (in heap)
                                              ↓
                          TimeSeriesVectorOpsProvider.getInstance()
                                              ↓
                          ┌─── SimdTimeSeriesVectorOps (if available)
                          │    → 4-8 doubles per cycle (AVX2/AVX-512)
                          │
                          └─── ScalarTimeSeriesVectorOps (fallback)
                               → 1 double per cycle (standard loop)
                                              ↓
                          partial aggregation result (per block, per shard)
```

**Operations that benefit most from SIMD:**

| Operation | SIMD Speedup | Notes |
|---|---|---|
| SUM / AVG over double[] | 4-8x | Process 4 (AVX2) or 8 (AVX-512) doubles per cycle |
| MIN / MAX over double[] | 4-8x | Lane-wise min/max with reduce |
| Bitmask AND/OR (tag filter combine) | 4-8x | 256/512-bit bitwise ops |
| COUNT (popcount on bitmask) | HW intrinsic | Maps to POPCNT instruction |
| Threshold filtering (WHERE temp > 30) | 4-8x | SIMD compare → bitmask |
| SUM with bitmask (filtered agg) | 3-6x | Masked lane operations |

**Operations where SIMD helps less:**
- Delta-of-delta decoding: sequential dependency (each value depends on previous). Can be partially vectorized with prefix-sum techniques but not in Phase 1.
- Gorilla XOR decoding: bit-level sequential. Pure Java is fine — decoding is not the bottleneck (I/O dominates).
- Dictionary lookup: indirect indexing, not SIMD-friendly. But dictionaries are tiny.

#### Runtime Requirements

- **JDK 21+**: `--add-modules jdk.incubator.vector` (already in ArcadeDB's server.sh and test argLine)
- **No additional dependency**: The Vector API is part of the JDK, not an external library
- **Automatic fallback**: If the module is not available (e.g., GraalVM native image), `ScalarTimeSeriesVectorOps` is used transparently
- **Future-proof**: When `jdk.incubator.vector` graduates to a stable module (expected in a future JDK LTS), simply update the import — the API is the same

### 6.14 Java API

```java
/**
 * Mutable transactional storage for timeseries data.
 * Extends PaginatedComponent for WAL, MVCC, and replication support.
 * Holds recent data in row-oriented pages. Compaction moves data to sealed files.
 */
public class TimeSeriesBucket extends PaginatedComponent {

  // === Schema ===
  List<ColumnDefinition> getColumns();
  int getTimestampColumnIndex();

  // === Write (transactional, MVCC-safe) ===
  void appendSamples(long[] timestamps, Object[]... columnValues);

  // === Read from mutable pages only ===
  List<Object[]> scanRange(long fromTs, long toTs, int[] columnIndices);  // materialized
  Iterator<Object[]> iterateRange(long fromTs, long toTs, int[] columnIndices);  // streaming (lazy, page-at-a-time)

  // === Metadata ===
  long getCompactionWatermark();
  long getSampleCount();           // uncompacted samples only
  int getActiveDataPageCount();

  // === Compaction ===
  void compact(TimeSeriesSealedStore sealedStore);   // move full pages → sealed files (chunked, 65K rows/block)
}

/**
 * Immutable columnar storage for timeseries data.
 * NOT a PaginatedComponent — uses plain FileChannel I/O.
 * One instance manages the index file + all per-column files for a type.
 */
public class TimeSeriesSealedStore {

  // === Read (the primary query path for historical data) ===
  List<Object[]> scanRange(long fromTs, long toTs, int[] columnIndices);  // materialized
  Iterator<Object[]> iterateRange(long fromTs, long toTs, int[] columnIndices);  // streaming
  // Streaming iterator uses: binary search on block directory → lazy column decompression
  // → binary search within blocks (lowerBound/upperBound) → early termination

  // === Metadata ===
  long getMinTimestamp();
  long getMaxTimestamp();
  long getSampleCount();
  int getBlockCount();

  // === Write (called by compaction only, NOT by user transactions) ===
  void appendBlock(int sampleCount, long minTs, long maxTs, byte[][] compressedColumns);
  // Writes inline block metadata (magic 0x5453424C + minTs + maxTs + sampleCount + colSizes)
  // before column data, enabling directory reconstruction on cold open

  // === Directory persistence ===
  void loadDirectory();  // reconstructs block directory by scanning inline metadata records

  // === Maintenance ===
  void truncateBefore(long timestamp);   // retention: remove old blocks
}

/**
 * A shard is a paired mutable bucket + sealed store.
 * One shard per core for zero-contention writes.
 * Compaction runs independently per shard.
 */
public class TimeSeriesShard {
  final TimeSeriesBucket   mutableBucket;   // PaginatedComponent — WAL, MVCC
  final TimeSeriesSealedStore sealedStore;  // plain files — immutable, per-column

  static final int SEALED_BLOCK_SIZE = 65_536;  // rows per sealed block (chunked compaction)

  void appendSamples(long[] timestamps, Object[]... columnValues);
  List<Object[]> scanRange(long fromTs, long toTs, int[] columns, TagFilter filter);  // materialized
  Iterator<Object[]> iterateRange(long fromTs, long toTs, int[] columns, TagFilter filter);  // streaming
  // Streaming: chains sealed iterator → mutable iterator, applies tag filter inline
  void compact();   // move full mutable pages → sealed files (chunked, shard-local)
}

/**
 * Coordinates reads across ALL shards (mutable + sealed layers).
 * This is what the SQL query engine interacts with.
 * Routes writes to the correct shard via BucketSelectionStrategy.
 */
public class TimeSeriesEngine {

  final TimeSeriesShard[] shards;           // one per core (default)
  final BucketSelectionStrategy strategy;   // Thread or Partitioned

  // Write: routes to correct shard (lock-free, zero contention)
  void appendSamples(Document record, long[] timestamps, Object[]... columnValues) {
    int shardIdx = strategy.getBucketIdByRecord(record, async);
    shards[shardIdx].appendSamples(timestamps, columnValues);
  }

  // Read (materialized): queries all shards, merges results into List
  List<Object[]> query(long fromTs, long toTs, int[] columns, TagFilter filter);

  // Read (streaming): lazy merge-sort across shard iterators via PriorityQueue<PeekableIterator>
  Iterator<Object[]> iterateQuery(long fromTs, long toTs, int[] columns, TagFilter filter) {
    // 1. Create per-shard iterators (each chains sealed → mutable)
    // 2. Min-heap merge-sort by timestamp: each next() advances only the
    //    shard with smallest current timestamp
    // 3. Memory: O(shardCount × blockSize) — constant regardless of dataset size
    // 4. Used by FetchFromTimeSeriesStep for SQL queries (prevents OOM on full scans)
  }
}
```

### 6.15 Integration with ArcadeDB Schema

```
TimeSeriesType extends DocumentType
  ├── owns N TimeSeriesShards (one per core, default = availableProcessors())
  │   └── each shard:
  │       ├── TimeSeriesBucket (PaginatedComponent — mutable, transactional)
  │       └── TimeSeriesSealedStore (plain files — immutable, per-column)
  ├── owns a TimeSeriesEngine (coordinates reads/writes across all shards)
  ├── uses BucketSelectionStrategy (ThreadBucket default, or PartitionedBucket)
  ├── optional LSM-Tree index on (tag_columns, timestamp) per shard
  │   for high-cardinality point lookups
  ├── TimeSeriesType knows:
  │   - which column is the designated timestamp
  │   - which columns are tags vs. fields
  │   - partition interval, retention policy, compression settings
  └── SQL DDL:
      CREATE TIMESERIES TYPE SensorReading
        TIMESTAMP ts PRECISION NANOSECOND
        PARTITION BY INTERVAL 1 DAY
        RETENTION 90 DAYS
        TAGS (sensor_id STRING, location STRING)
        FIELDS (temperature DOUBLE, humidity DOUBLE, pressure DOUBLE)
```

### 6.16 Compression Savings

Example schema: 1 timestamp + 1 string tag + 2 double fields.

**Mutable file** (row-oriented, uncompressed within pages):
- ~26 bytes/sample → ~2,400 samples per 64KB page
- Only holds recent data (seconds to minutes), so total size is small

**Sealed files** (columnar, compressed, per-column):
- ~3-4 bytes/sample across all columns combined
- Zero wasted space (variable-size blocks, no page padding)
- Per-column I/O: a query touching 2 of 5 columns reads only 40% of the data

| Layer | Bytes/Sample | Samples per 64KB equivalent | Notes |
|---|---|---|---|
| Mutable (row, uncompressed) | ~26 B | ~2,400 | Small dataset, fast MVCC append |
| Sealed (columnar, compressed) | ~3-4 B | ~16,000-21,000 | 99%+ of data, zero waste |
| Sealed (best case, slow values) | ~1.5 B | ~40,000+ | Regular intervals, stable values |

At 1M total samples with 5 columns:
- Mutable: holds last ~2,400 samples = 1 page = 64KB
- Sealed: ~3.5 MB across all column files (vs. ~25 MB uncompressed) — **7x compression**
- Query reading 2 of 5 columns: reads ~1.4 MB — **18x less I/O than uncompressed row storage**

---

## Part 7: Implementation Plan — Making ArcadeDB a Leading TSDB

### Phase 1: Foundation — Two-Layer Storage + Schema (Core)

**Goal**: Store and retrieve timeseries data efficiently with fast range queries using the two-layer mutable/sealed architecture.

#### 1a. Compression Codecs
- Implement timeseries-specific compression codecs as standalone classes (no storage dependency):
  - `DeltaOfDeltaCodec` — for timestamps (based on Facebook Gorilla paper)
  - `GorillaXORCodec` — for double values
  - `DictionaryCodec` — for low-cardinality string tags (dictionary + Simple-8b RLE indices)
  - `Simple8bCodec` — for integer packing with RLE
- Each codec: `byte[] encode(primitive_array, count)` and `primitive_array decode(byte[], count)`
- **Key package**: `com.arcadedb.engine.timeseries.codec`
- **Tests first**: Unit test each codec independently with known inputs/outputs, edge cases (all-same values, all-different, empty, single value, max precision, out-of-range)

#### 1b. TimeSeriesBucket (Mutable Layer)
- New `TimeSeriesBucket extends PaginatedComponent` with header page, directory pages, and row-oriented active data pages
- Concurrent transaction support via standard ArcadeDB MVCC (same as `LocalBucket`)
- Active page: row-oriented, fixed-size sample rows, tag dictionary at page tail
- Directory pages: sorted entries with min/max timestamp per data page for binary search
- `appendSamples()` appends to active page within a transaction
- `scanMutableRange()` reads uncompacted data pages
- **Key package**: `com.arcadedb.engine.timeseries`
- **Reuses**: `PaginatedComponent`, `PageManager`, `TransactionContext`, `WALFile`

#### 1c. TimeSeriesSealedStore (Sealed Layer)
- Per-column files (`.ts.col.N.*`) with variable-size compressed blocks, zero padding
- Shared index file (`.ts.index`) with block directory (min/max timestamp, column offsets/sizes)
- I/O via `java.nio.channels.FileChannel` positioned reads with direct ByteBuffers
- `scanRange()` reads index → binary search → reads only needed column files
- `appendBlock()` called by compaction to add new sealed blocks
- `truncateBefore()` for retention
- **Key package**: `com.arcadedb.engine.timeseries`

#### 1d. TimeSeriesShard (Paired Unit)
- Pairs a `TimeSeriesBucket` (mutable) with a `TimeSeriesSealedStore` (sealed)
- Each shard is an independent write/compact/read unit — no shared state
- Compaction runs per-shard: background thread reads shard's full mutable pages → sorts → compresses → appends to shard's sealed files → cleans shard's mutable directory (in transaction, crash-safe)
- Compaction watermark per shard for crash recovery
- Configurable compaction interval (default: 30 seconds or when N mutable pages are full)
- **Reuses**: Existing background task infrastructure

#### 1e. TimeSeriesEngine (Query Coordinator + Shard Router)
- Routes writes to the correct shard via `BucketSelectionStrategy` (lock-free)
- Coordinates reads across **all shards** in parallel (N shards = N parallel scans)
- Merges partial aggregations from shards → final result
- For `PartitionedBucketSelectionStrategy` + tag filter: routes to single shard (zero cross-shard overhead)
- **Reuses**: `BucketSelectionStrategy`, `DatabaseAsyncExecutorImpl` for parallel reads

#### 1f. Schema: TimeSeriesType
- New `TimeSeriesType` extending `DocumentType` with:
  - N `TimeSeriesShard` instances (default = `availableProcessors()`, configurable via `SHARDS`)
  - `BucketSelectionStrategy` (default `ThreadBucketSelectionStrategy`, or `PartitionedBucketSelectionStrategy` via `PARTITION BY`)
  - Mandatory designated timestamp column (DATETIME_NANOS default, configurable precision)
  - Tag columns (indexed, low-cardinality) vs. field columns (values, high-cardinality)
  - Configurable partition interval and retention policy
- SQL DDL support (CREATE/ALTER/DROP TIMESERIES TYPE)
- **Reuses**: `LocalDocumentType`, `LocalSchema`, `Type` enum, `BucketSelectionStrategy`

#### 1g. Basic Query Support
- Time-windowed aggregation: `GROUP BY time(interval)`
- Sealed: block pruning via index binary search + per-column I/O (only read needed columns)
- Mutable: scan active pages, filter by time range
- Tag filtering: dictionary-decoded bitmask in sealed blocks, direct comparison in mutable pages
- Streaming aggregation: one block/page at a time, constant memory
- **Reuses**: `AggregationContext`, SQL execution framework

#### 1h. Retention
- Sealed: `truncateBefore(timestamp)` rewrites column files and index without old blocks
- Mutable: remove compacted pages from directory (in transaction)
- Optional: time-partitioned sealed files (one set of column files per time window) for instant retention by file deletion

#### 1i. SIMD-Accelerated Aggregation
- `TimeSeriesVectorOps` interface with `sum`, `min`, `max`, `sumFiltered`, `countFiltered`, bitmask ops
- `ScalarTimeSeriesVectorOps`: pure Java loops (always works, no dependencies)
- `SimdTimeSeriesVectorOps`: Java Vector API (`jdk.incubator.vector`), processes 4-8 doubles per cycle
- `TimeSeriesVectorOpsProvider`: singleton auto-detection at startup (same pattern as JVector's `VectorizationProvider`)
- Used by sealed block reader and aggregation engine from day one — not a later optimization
- **Key package**: `com.arcadedb.engine.timeseries.simd`
- **Tests**: Benchmark both implementations, verify identical results, test edge cases (empty arrays, single element, non-aligned lengths)
- **No new dependency**: Vector API is part of the JDK; `--add-modules jdk.incubator.vector` already in server.sh

#### 1j. SQL DDL & DML
- `CreateTimeSeriesTypeStatement extends CreateTypeAbstractStatement` — parse TIMESTAMP/TAGS/FIELDS/SHARDS/RETENTION
- `time_bucket(interval, timestamp)` function: `SQLFunctionTimeBucket extends SQLFunctionAbstract`
- `first(value)` / `last(value)` aggregate functions: track min/max timestamp during `AggregateProjectionCalculationStep`
- Route `INSERT INTO` for timeseries types to `TimeSeriesEngine.appendSamples()` instead of `LocalBucket`
- **Reuses**: `CreateTypeAbstractStatement`, `SQLFunctionFactoryTemplate`, `InsertExecutionPlanner`, `AggregateProjectionCalculationStep`

#### 1k. HTTP Ingestion Endpoint (InfluxDB Line Protocol)
- `PostTimeSeriesWriteHandler extends AbstractServerHttpHandler`
- `LineProtocolParser`: parse ILP text → batch of (measurement, tags, fields, timestamp)
- Endpoints: `POST /api/v1/ts/{database}/write` + `POST /api/v2/write` (InfluxDB v2 compat)
- Auto-schema creation (opt-in): first line defines type schema, subsequent lines auto-alter
- Gzip decompression support for large batches
- **Reuses**: `AbstractServerHttpHandler`, `HttpServer.setupRoutes()`, existing auth
- **Tests**: Parse correctness (edge cases, escaping, type suffixes), batch throughput, error handling

### Phase 2: Query Engine — TimeSeries Functions & Aggregations

#### 2a. TimeSeries-Specific Functions — **COMPLETED**
- ✅ `ts.first(value, timestamp)` / `ts.last(value, timestamp)` — first/last value in time window
- ✅ `ts.rate(value, ts [, counterResetDetection])` — per-second rate of change with optional counter reset detection
- ✅ `ts.delta(value, ts)` — difference between first and last in window
- ✅ `ts.movingAvg(value, window)` — sliding window average
- ✅ `ts.percentile(value, percentile)` — approximate percentile (p50/p95/p99) with exact sort and rank interpolation
- ✅ `ts.interpolate(value, method [, timestamp])` — fill missing values (linear, prev, zero, none)
- ✅ `ts.correlate(series_a, series_b)` — Pearson correlation between two series
- ✅ `ts.timeBucket(interval, ts)` — time bucketing for GROUP BY aggregation
- **Reuses**: Existing `SQLFunction` registration framework

#### 2b. Continuous Aggregates (**IMPLEMENTED**)
- Watermark-based incremental aggregation — separate from MaterializedView to keep timeseries-specific logic clean:
  ```sql
  -- Create a continuous aggregate (initial full refresh runs automatically)
  CREATE CONTINUOUS AGGREGATE hourly_temps AS
    SELECT sensor_id, ts.timeBucket('1h', ts) AS hour,
           avg(temperature) AS avg_temp, max(temperature) AS max_temp
    FROM SensorReading
    GROUP BY sensor_id, hour

  -- Idempotent creation
  CREATE CONTINUOUS AGGREGATE IF NOT EXISTS hourly_temps AS ...

  -- Manual refresh
  REFRESH CONTINUOUS AGGREGATE hourly_temps

  -- Drop (removes backing type too)
  DROP CONTINUOUS AGGREGATE hourly_temps
  DROP CONTINUOUS AGGREGATE IF EXISTS hourly_temps

  -- Query metadata
  SELECT FROM schema:continuousAggregates
  ```
- **Automatic incremental refresh**: After each transaction that inserts into a TimeSeries type, a post-commit callback triggers incremental refresh of all continuous aggregates sourced from that type. Only data from the watermark forward is reprocessed — stale buckets are deleted and recomputed.
- **Watermark tracking**: Tracks the start of the last fully computed time bucket. On refresh, deletes rows where `bucketColumn >= watermark`, re-runs the query filtered by `WHERE ts >= watermark`, inserts results, advances watermark to `max(bucketColumn)`.
- **Query validation at creation**: Source must be a TimeSeries type, query must include `ts.timeBucket(interval, ts)` with an alias in projections and GROUP BY, only aggregate functions allowed in non-GROUP-BY projections.
- **Schema persistence**: Stored in `LocalSchema.toJSON()` under `"continuousAggregates"` section. Survives database close/reopen. Crash recovery marks BUILDING→STALE on restart.
- **Concurrency**: Atomic `tryBeginRefresh()` / `endRefresh()` guard prevents concurrent refresh of the same aggregate.
- **Java API**: `schema.buildContinuousAggregate().withName("...").withQuery("...").withIgnoreIfExists(true).create()`
- **Metrics**: refreshCount, refreshTotalTimeMs, refreshMinTimeMs, refreshMaxTimeMs, lastRefreshDurationMs, errorCount

#### 2c. Downsampling Policies
- Automatically reduce resolution of old data:
  ```sql
  ALTER TIMESERIES TYPE SensorReading
    ADD DOWNSAMPLING POLICY
    AFTER 7 DAYS GRANULARITY 1 MINUTE
    AFTER 30 DAYS GRANULARITY 1 HOUR
  ```

### Phase 3: Graph + TimeSeries Integration (The Differentiator)

#### 3a. TimeSeries-on-Vertex / TimeSeries-on-Edge
- Any vertex or edge can have associated timeseries data
- Schema declaration:
  ```sql
  CREATE VERTEX TYPE Sensor
    PROPERTIES (name STRING, location STRING)
    TIMESERIES temperature (DOUBLE, PARTITION 1 DAY, RETENTION 90 DAYS)
    TIMESERIES humidity (DOUBLE, PARTITION 1 DAY, RETENTION 30 DAYS)
  ```
- Under the hood: each timeseries property creates a linked `TimeSeriesType` with a foreign key back to the vertex RID
- The vertex stores a lightweight pointer (bucket + latest partition) for fast access

#### 3b. TIMESERIES Clause in SQL
- New SQL clause to access timeseries data from graph traversals:
  ```sql
  -- Access timeseries of vertices found by traversal
  SELECT v.name, avg(ts.value)
  FROM (TRAVERSE out('InstalledIn') FROM #12:0 MAXDEPTH 3) AS v
  TIMESERIES v.temperature AS ts FROM '2026-02-19' TO '2026-02-20'
  GROUP BY v.name
  ```
- Query planner optimizes: first resolve graph traversal to RID set, then batch-fetch timeseries data for all RIDs in parallel

#### 3c. Graph-Aware Aggregation Functions
- `ROLLUP ALONG path`: Aggregate timeseries following graph hierarchy
  ```sql
  SELECT node.name, node.@type,
         sum_along_children(node, 'ContainedIn', 'energy_kwh',
           FROM '2026-02-01' TO '2026-02-20', GRANULARITY '1h') AS total_energy
  FROM (SELECT FROM V WHERE @type IN ['Campus', 'Building', 'Floor'])
  ```

#### 3d. OpenCypher TimeSeries Functions & Procedures
- Register `ts.*` functions in native `CypherFunctionRegistry`: `ts.avg`, `ts.sum`, `ts.min`, `ts.max`, `ts.count`, `ts.first`, `ts.last`, `ts.rate`, `ts.query`
- Register `ts.range`, `ts.aggregate` procedures in `CypherProcedureRegistry` for tabular results via `CALL ... YIELD`
- Functions evaluated by existing `ExpressionEvaluator` via `CypherFunctionFactory` (already supports namespaced functions)
- Procedures executed by existing `CallStep` (already handles YIELD)
- No Cypher grammar changes needed — Cypher25 grammar already supports namespaced functions and CALL
- **Reuses**: `CypherFunctionRegistry`, `CypherProcedureRegistry`, `ExpressionEvaluator`, `CallStep`

#### 3e. Temporal Graph Snapshots (Future)
- Query the graph as it existed at a specific point in time
- Track edge creation/deletion timestamps
- `AT TIMESTAMP '2025-06-01'` clause for historical graph state

### Phase 4: Advanced Performance Optimizations

> **Note**: SIMD aggregation and shard-per-core parallelism are already in Phase 1 (core design).
> This phase focuses on additional optimizations beyond the foundation.

#### 4a. Advanced SIMD: Vectorized Decompression
- SIMD-accelerated delta-of-delta decoding using prefix-sum vectorization
- SIMD-accelerated Gorilla XOR decoding (batch bit-manipulation)
- Benchmark against scalar decompression to validate speedup

#### 4b. Write Path Optimization
- Batch ingestion API: `INSERT INTO SensorReading BATCH [...]` accepting arrays of values
- Configurable flush interval and buffer size
- Out-of-order tolerance window: buffer and sort before commit

#### 4c. Adaptive Block Sizing
- Dynamically size sealed blocks based on data characteristics
- Smaller blocks for high-cardinality data (faster filtering)
- Larger blocks for uniform data (better compression ratios)

### Phase 5: HTTP API & Studio Integration — **COMPLETED**

#### 5a. REST API for TimeSeries
- ✅ `POST /api/v1/ts/{database}/write?precision=ns|us|ms|s` — InfluxDB Line Protocol batch ingestion
- ✅ `POST /api/v1/ts/{database}/query` — JSON query with raw/aggregated response, time range, field projection, tag filtering
- ✅ `GET /api/v1/ts/{database}/latest?type=name&tag=key:value` — latest value per series with optional tag filter
- ✅ Grafana DataFrame-compatible endpoints (`GET .../grafana/health`, `GET .../grafana/metadata`, `POST .../grafana/query`) — works with Grafana Infinity datasource plugin
- Prometheus remote-write/remote-read compatibility endpoints (future — requires protobuf dependency)

#### 5b. Studio TimeSeries Explorer
- ✅ Full TimeSeries tab in Studio navigation with Query, Schema, and Ingestion sub-tabs
- ✅ Time-range picker (5min/1h/24h/7d/All/Custom) with configurable aggregation and bucket intervals
- ✅ ApexCharts line/area charts with datetime x-axis, zoom, dark mode support
- ✅ DataTable with pagination for raw and aggregated results
- ✅ Chart/table toggle switches with per-database localStorage persistence
- ✅ Schema introspection: columns, diagnostics, configuration, downsampling tiers, shard details
- ✅ Ingestion documentation: Line Protocol, SQL INSERT, Java API with examples and comparison table
- ✅ HTTP API Reference panel with 3 TimeSeries endpoints and interactive playground
- Combined graph + timeseries view (future — Phase 3 integration)

---

## Part 8: Prioritized Roadmap

### MVP (Phase 1 — "Two-Layer Storage + Fast Range Queries")
**Goal**: Users can store and query timeseries data with the sharded, two-layer mutable/sealed architecture.
- Compression codecs: DeltaOfDelta, GorillaXOR, Dictionary, Simple8b
- `TimeSeriesShard` = `TimeSeriesBucket` (mutable, paginated, MVCC) + `TimeSeriesSealedStore` (immutable, per-column files)
- **Shard-per-core**: N shards per type (default = availableProcessors()), zero-contention parallel writes
- `BucketSelectionStrategy` integration: `ThreadBucketSelectionStrategy` (default) or `PartitionedBucketSelectionStrategy`
- `TimeSeriesEngine` routing writes to shards + coordinating parallel reads across all shards
- Background compaction per shard (mutable → sealed), crash-safe via pre-compaction checkpoint
- Free page list for mutable file page reuse, out-of-order data handling (3 levels)
- `CREATE TIMESERIES TYPE` DDL with `TimeSeriesType` (configurable SHARDS, PARTITION BY)
- Range queries: parallel shard scans → index binary search → per-column I/O (sealed) + page scan (mutable)
- `GROUP BY time(interval)` aggregation with parallel partial aggregation per shard
- **SIMD-accelerated aggregation**: `TimeSeriesVectorOps` interface with auto-detected SIMD (Project Panama) or scalar fallback — 4-8x faster SUM/AVG/MIN/MAX from day one
- **SQL**: `CREATE TIMESERIES TYPE` DDL, `time_bucket()` function, `first()`/`last()` aggregates, standard INSERT
- **HTTP ingestion**: InfluxDB Line Protocol compatible endpoint (`POST /api/v1/ts/{db}/write` + `/api/v2/write`), Telegraf/Grafana Agent ready
- **Java API**: Direct `TimeSeriesEngine.appendSamples()` for maximum throughput (~0.5-1μs/sample)
- Retention policies (per-shard sealed file truncation + optional time-partitioned file sets)

### v2 (Phase 2 — "Rich Query Functions") — **COMPLETED**
**Goal**: Competitive query capabilities for analytics.
- ✅ TimeSeries-specific functions (rate, delta, moving_avg, interpolate, correlate, timeBucket, first, last, percentile)
- ✅ Counter reset detection in `ts.rate()` (optional 3rd param for Prometheus-style monotonic counters)
- ✅ Linear interpolation in `ts.interpolate()` (4th fill method)
- ✅ Approximate percentiles via `ts.percentile()` (p50/p95/p99)
- ✅ Continuous aggregates (watermark-based incremental refresh, automatic post-commit trigger, SQL DDL, schema metadata)
- ✅ Downsampling policies with automatic scheduler
- ✅ Window functions: `ts.lag()`, `ts.lead()`, `ts.rowNumber()`, `ts.rank()` — compare current/previous values, sequential numbering, ranking with ties

### v3 (Phase 3 — "Graph + TimeSeries, The Differentiator")
**Goal**: World's first native graph + timeseries integration.
- `TIMESERIES ... AS` clause in SQL (graph traversal + timeseries aggregation in one query)
- `ts.*` Cypher functions (`ts.avg`, `ts.max`, `ts.last`, etc.) for OpenCypher graph+TS queries
- TimeSeries-on-Vertex/Edge (vertex owns timeseries data)
- Graph-aware aggregation (`ROLLUP ALONG` graph hierarchy)
- Combined graph + timeseries Studio visualization

### v4 (Phase 4+5 — "Performance & Ecosystem") — **COMPLETED**
**Goal**: Advanced optimizations + full ecosystem integration.
- ✅ SIMD-accelerated aggregation: `TimeSeriesVectorOps` wired into `aggregateMultiBlocks()` slow path with segment-based vectorized `sum()/min()/max()`
- ✅ Parallel shard aggregation: `CompletableFuture`-based concurrent sealed store processing with flat-array merge
- ✅ Coalesced I/O: single pread per block, reusable decode buffers, flat array accumulation (no HashMap)
- ✅ BitReader sliding-window register: pre-loaded 64-bit window, lazy refill every ~7-8 bytes (decompVal 1305ms → 1224ms)
- ✅ Bucket-aligned compaction: `COMPACTION_INTERVAL` DDL splits blocks at bucket boundaries for 100% fast-path aggregation
- ✅ Dedicated timeseries JSON query endpoint (`POST /api/v1/ts/{db}/query`) with raw + aggregated responses
- ✅ Dedicated latest-value endpoint (`GET /api/v1/ts/{db}/latest`) with tag filtering
- ✅ Studio TimeSeries Explorer: Query (charts + tables), Schema (introspection), Ingestion (docs + examples)
- ✅ HTTP API Reference panel with TimeSeries section and interactive playground
- ✅ Multi-tag filtering in query engine and HTTP API
- ✅ Time range operator push-down (`>`, `>=`, `<`, `<=`, `=` — not just `BETWEEN`)
- ✅ Automatic retention/downsampling scheduler (`TimeSeriesMaintenanceScheduler` daemon thread)
- Advanced decompression: Gorilla XOR decode is inherently sequential (each value XORs with previous) — further gains require fused decode+aggregate or alternative encoding schemes

### v5 (Phase 6+7 — "Observability Ecosystem Integration")
**Goal**: Drop-in compatibility with the Prometheus/Grafana/OpenTelemetry ecosystem.

| Priority | Feature | Who has it | Effort |
|----------|---------|-----------|--------|
| P0 | PromQL / MetricsQL query language | Prometheus, VictoriaMetrics, Grafana Mimir | High |
| P0 | Grafana native datasource plugin | All 10 TSDBs | Medium |
| P0 | Prometheus `remote_write` / `remote_read` | 6/10 TSDBs | Medium |
| P0 | Alerting & recording rules | 7+/10 TSDBs | High |
| P1 | OpenTelemetry OTLP ingestion (gRPC + HTTP) | 5/10 TSDBs (growing) | Medium |
| P1 | Cardinality management & monitoring | VictoriaMetrics, Grafana Mimir, Prometheus | Medium |

### v6 (Phase 8 — "Advanced Analytics & Data Platform")
**Goal**: Feature parity with analytics-focused TSDBs.

| Priority | Feature | Who has it | Effort |
|----------|---------|-----------|--------|
| ~~P1~~ | ~~SQL window functions (LAG, LEAD, RANK, etc.)~~ | ~~TimescaleDB, QuestDB, ClickHouse, TDengine, Kdb+~~ | **DONE** |
| P1 | ASOF JOIN / temporal joins | QuestDB, ClickHouse, Kdb+ | High |
| P1 | Streaming aggregation at ingestion | TDengine, VictoriaMetrics, QuestDB | High |
| P2 | TimeSeries via PostgreSQL wire protocol | TimescaleDB, QuestDB | Medium |
| P2 | Native histogram support | Prometheus 3.0, VictoriaMetrics 3.0 | High |
| P2 | Tiered storage (S3/GCS/Azure Blob) | InfluxDB 3, Grafana Mimir, ClickHouse | High |
| P2 | Parquet/Arrow export/import | InfluxDB 3, QuestDB, ClickHouse, Kdb+ | Medium |
| P3 | MQTT protocol support | TDengine, Apache IoTDB | Medium |
| P3 | Exemplars (trace-to-metrics linking) | Prometheus, Grafana Mimir, VictoriaMetrics | Medium |
| P3 | Anomaly detection | VictoriaMetrics (enterprise) | High |
| P3 | TCP ingestion socket (raw ILP over TCP) | QuestDB | Low |

---

## Key Sources

- Facebook Gorilla paper (VLDB 2015) — Compression algorithms
- HyGraph (EDBT 2025, University of Leipzig) — Graph + TimeSeries unification theory
- "Combining Time-Series and Graph Data: A Survey" (arXiv:2601.00304, Jan 2025) — Confirms no production system unifies both
- InfluxDB 3.0 FDAP architecture — Modern Arrow/Parquet approach
- TimescaleDB compression docs — 7 compression algorithms reference
- QuestDB architecture — Columnar + SIMD reference implementation
- ClickHouse MergeTree — Sparse indexing + composable codecs
- Datadog Monocle — Shard-per-core LSM design
- Competitive gap analysis (Feb 2026) — Feature comparison against InfluxDB 3, TimescaleDB, Prometheus, QuestDB, TDengine, ClickHouse, Kdb+, Apache IoTDB, VictoriaMetrics, Grafana Mimir
