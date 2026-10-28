# Columnar Faceting - Query and API (plan 4 of 6) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `SELECT ... FACETS ... LIMIT n` returns rows and exact facet counts (terms, multi-valued terms, disjunctive filters, ranges, date histograms) in one round trip over the embedded API, HTTP JSON, ndjson, gRPC, `RemoteDatabase` and `RemoteGrpcDatabase`, computed from plan 3's columnar projection with patching, a server-wide memory budget, per-bucket security and a capped fallback scan.

**Architecture:** the grammar adds a `FACETS` clause before `SKIP`/`LIMIT`; the AST is compiled into immutable `FacetSpec` records. The planner chains one `FacetCollectStep` after the match-source plan is built (`buildExecutionPlan`, so after the fetch and WHERE steps), with every LIMIT pushdown upstream of it switched off: the step passes rows through (post-filtering them with the facet `FILTER`s), records every match into a `MatchSet` of per-bucket `PositionBitmap`s with a DrillSideways fail-mask, and, when the upstream is exhausted or `getFacets()` is asked, drains what is left and calls `FacetExecutor`. The executor counts per segment over `matches AND NOT patched` (the column reader already restricts to `live()`, roadmap R12; plan 4 never ANDs live itself), patches dirty/uncommitted positions by reading the records, ranks with a deterministic tie-break and returns `FacetResults`, which every transport serializes through one `toJSON()`.

**Tech Stack:** Java 21, ANTLR 4 (`engine/src/main/antlr4`), JUnit 5, AssertJ, Awaitility, Micrometer (server only), gRPC/protobuf (`grpc` module), swagger-models (`CoreApiSpec`). No new dependency.

**Spec:** `docs/superpowers/specs/2026-10-18-columnar-faceting-design.md` (sections 7, 8, 9, 10). Cross-plan contract: `docs/superpowers/plans/2026-10-19-columnar-faceting-0-roadmap.md` ("Plan 3 produces" is consumed, "Plan 4 produces" is implemented with those exact names).

## Global Constraints

- **No git commits.** The user commits after review. Where the plan template says "Commit", the step is "Stop: changes ready for review", listing the files touched.
- No em dash characters in code comments, docs or messages.
- Java style of the repo: `final` on variables and parameters where possible; single-statement `if` without braces; imports, never fully qualified names; no `System.out` left behind; no Claude author tags.
- JSON via `com.arcadedb.serializer.json.JSONObject` / `JSONArray`, using getters with defaults.
- Primitive arrays over collections of boxed values on every hot path.
- Tests: AssertJ style `assertThat(x).isTrue();`. Slow tests `@Tag("slow")`, benchmarks `@Tag("benchmark")`. Never assert raw wall-clock time; use `com.arcadedb.utility.StallAwareStopwatch`.
- Test servers only through fixture bases (`BaseGraphServerTest`, `StaticBaseServerTest`, `BaseRaftHATest`); URLs from bound ports; extra ports from `allocateFreePorts` / `allocateFixturePorts`.
- Module test runs use `-am` (`mvn -o -pl engine -am test -Dtest=... -Dsurefire.failIfNoSpecifiedTests=false`), and `verify` when gremlin or graphql are in the reactor. Skip lanes with `-DexcludedGroups`, never `-Dtest='!Foo'`.
- No `ForkJoinPool.commonPool()`; background work on a dedicated pool following `.claude/skills/engine-concurrency/SKILL.md`.
- No new dependency unless a plan says so; RoaringBitmap only after plan 1 measures a need (Apache 2.0, update `ATTRIBUTIONS.md` and `NOTICE`).
- After every Java change: compile the touched modules and fix all errors before the next step.
- Plan-specific: a new API ships proto + OpenAPI + `RemoteDatabase` + `RemoteGrpcDatabase` together (Tasks 14-16 land as one reviewable unit; none of them is "done" alone).
- Plan-specific: `FacetResults.toJSON()` is exactly the HTTP `facets` object, and every transport (HTTP JSON, ndjson, gRPC, both remote clients, the Raft leader-answer parser) is built from it or parsed back into it with `FacetResults.fromJSON`. No transport invents its own shape.
- Plan-specific: ITs run with failsafe: `mvn -o -pl <module> -am verify -DskipTests -Pintegration -Dit.test=<Class> -Dfailsafe.failIfNoSpecifiedTests=false` (never `-Dit.test` without `-pl`: at the reactor root it fails the parent module).
- Plan-specific: `FacetOracle.awaitCaughtUp(db)` (30 s) is called only outside any `suspendApply()` hold and after every `ColumnarTestHooks.killPoint` hook is released; a test that plants an unreadable record or a mark no page will satisfy calls `manager.awaitCaughtUp(timeoutMs)` with an explicit short timeout and asserts its boolean; production code never waits for catch-up.
- Plan-specific: tests on `ColumnarTestData` use its value domain (brands `brand-<n>`, tags `tag-<n>`, fileSize below 4 MiB, publishDate 2020-2024) and assert that every WHERE they compare matches at least one record. Budget refusals are produced by setting `COLUMNAR_QUERY_MEMORY_BUDGET` (restored in `finally`), never by constructing a `FacetMemoryBudget`.
- Plan-specific: placeholders only through `ColumnarTestData.placeholderRatio` on a fresh single-bucket document type; no test grows records into placeholders by hand. A test that hits LocalBucket spill corruption (#10617) reports seed and trace (NEEDS_CONTEXT) instead of fixing LocalBucket.

## Decisions taken by this plan

| # | Decision | Why |
|---|---|---|
| D1 | `HIERARCHY`, `DISTINCT` and `STATS` are **not parsed** by plan 4: plan 5 adds the `HIERARCHY` and `STATS` tokens, the `DISTINCT [PRECISION n]` alternative and `AFTER`. Plan 4 only creates the three records with plan 5's components (they are in the sealed `permits`) and leaves one extension point: `facetBody` is a labeled-alternative rule, and plan 5 appends alternatives to it (`# hierarchyFacet`, `# distinctFacet`, `# statsFacet`) plus `AFTER` to `facetTermsOption`; `FacetItem.Kind` already lists `HIERARCHY, DISTINCT, STATS` so the AST does not change shape. Until then `SELECT FROM A FACETS t: tags HIERARCHY` is an ordinary parse error, and `FacetExecutor` answers a spec it has no collector for with `CommandExecutionException("FACETS <alias>: <KIND> facets are not supported")` | No keyword is added twice and no grammar alternative of plan 5 can collide with one of plan 4 (roadmap "Contract additions") |
| D2 | `FacetSpec.filter()` returns `WhereClause`, not `Expression` (see "Contract changes") | `FILTER brand IN ['Nike']` is a condition; the grammar's `expression` only admits a condition in parentheses |
| D3 | FACETS only on the outermost `SELECT` (directly under `parse`/`parseScript`/`EXPLAIN`/`PROFILE`/a script block); in a subquery, `LET`, `INSERT ... SELECT` it is a parse error "FACETS is only allowed on the outermost SELECT" | A nested statement's facets cannot reach any `ResultSet`. It also makes gRPC `PAGED` (which wraps the query in `SELECT FROM (...)`) fail loudly |
| D4 | FACETS requires a `FROM`. Vector facets use `SELECT FROM (SELECT expand(vector.neighbors(...))) FACETS ...`; a row without a record identity raises `CommandExecutionException("FACETS needs rows that are records: ...")` | Facets over the returned k neighbors (spec 7.1) without a new vector code path |
| D5 | `FACETS` must come before `SKIP`/`LIMIT`; the grammar also accepts it after them only to raise `CommandSQLParsingException("FACETS must come before SKIP and LIMIT")` | The HTTP handler appends ` limit N` to the text (PostCommandHandler.java:299-304); a clear message beats ANTLR's |
| D6 | Facets cover the full `WHERE` match set: while planning the match source the planner runs with `info.limit = info.skip = null` and every `statement.getLimit()` read for a fetch sub-plan returns null; the real SKIP/LIMIT are restored before `handleProjectionsBlock` | One switch disables every pushdown: fetch limits (SelectExecutionPlanner.java:298, 1630, 4176, 4202, 4287, 4915, 4970, 5209), the full-text top-K (`indexedFunctionRowLimit`, :4390-4417, reads `info.limit`, so `FullTextFunctionSearch` takes its `completeSearch` branch, FullTextFunctionSearch.java:107), time-series scan limits (`timeSeriesDescendingScanLimit` :3418, `timeSeriesAscendingScanLimit` :3477, both read `info`). `handleHardwiredOptimizations` (:572) and `rewriteRangeMinMaxAsOrderedFetch` are skipped when facets are present |
| D7 | `FacetCollectStep` is pass-through: no row is buffered. It records each row's RID, evaluates the facet FILTERs into a fail-mask, forwards only rows with mask 0, and computes facets when the upstream is exhausted or when `getFacets()` is called (it then drains the rest without forwarding) | Downstream `ORDER BY` keeps its bounded heap, LIMIT keeps stopping early, and memory stays O(match bitmap) |
| D8 | Disjunctive bitmask (spec 7.5) is computed per row in `FacetCollectStep`. Evaluator per FILTER, chosen at planning: `INDEX` (single property, `=`/`IN`, an `LSM_TREE` index on it: RID set looked up once at step start, bit test per row), `PROJECTION` (single property with a `COLUMNAR` column, `=`/`IN`/`<`/`<=`/`>`/`>=`/`BETWEEN` over literals: column point read; pending or uncovered position falls back to the record), `RECORD` (anything else: `WhereClause.matchesFilters` on the row) | All three are exact; the cheap ones skip a record read. At most 31 facets may carry a FILTER (int mask); more is a parse error |
| D9 | Facet value conversion for patched/scanned records is `ColumnarValueExtractor.extract`; a property never indexed is faceted by fallback scan using the raw `Document.get` value, exactly what `GROUP BY` keys on | Spec 5.3 (equivalence) and spec 7.8 (no projection means scan) |
| D10 | Patch budget is all-or-nothing per query: if `|patch set|` summed over buckets exceeds `COLUMNAR_PATCH_BUDGET`, nothing is patched, counts come from the projection alone, `meta.exact=false`, `query.patch.overflows` +1. Matches the caller's own open transaction wrote (`TransactionContext.getWrittenRecordIdsIn` over the matched buckets) are **always** read from the records: they join the patch set; over budget, the buckets holding them go to the fallback scan (exact, `source: "scan"`) instead of the stale projection | Spec 7.3. The caller's own uncommitted writes are not "lag": silently counting the committed value would be a wrong answer, not an approximate one |
| D11 | Patch set per bucket = `(matches AND pending) OR (matches AND NOT (pending OR live of any segment))`; positions of the records the caller's open transaction wrote in the bucket (`TransactionContext.getWrittenRecordIdsIn`) are always patched | A matched position no segment covers and no dirty page explains is counted from the record rather than as missing: a projection bug degrades to a slower query, never to a wrong count |
| D12 | Counting mode per (facet, type): dense `int[cardinality]` when `cardinality <= 100_000` and `matchCount * 8 >= cardinality` (roadmap counter rule), else `IntIntHashMap` (`com.arcadedb.utility.IntIntHashMap`, reused) sized `min(matchCount, cardinality)`. LONG/DOUBLE terms use `LongLongHashMap` keyed by raw bits. Counting is sequential on the query thread in this plan | Lucene/ES heuristic; parallel counting is a later optimization the budget already accounts for |
| D13 | Budget: `FacetMemoryBudget` is a fair `Semaphore` of KiB permits sized `COLUMNAR_QUERY_MEMORY_BUDGET`, resized on `instance()` when the setting changed. Dense is tried with `maxWaitMs = 0`; refused, hash is tried with `COLUMNAR_QUERY_BUDGET_WAIT`; refused again, that facet is `source: "unavailable", reason: "memory budget"`. `MatchSet.Builder` reserves in 1 MiB steps with the same wait; refused, every facet of the query is unavailable (rows still flow). A wait that was not granted immediately increments `query.budget.waits` | Spec 5.7: wait or switch to hash, never unbounded |
| D14 | Fallback scan: one permit per query from a per-database `Semaphore(COLUMNAR_MAX_FALLBACK_SCANS)`, `tryAcquire()` without waiting; refused, every facet needing a scan is `unavailable` with reason `"fallback scans capped"`. A facet is `"scan"` if any bucket was scanned, `"unavailable"` if any bucket was, else `"index"` | Spec 7.8 |
| D15 | Ranking: count DESC then value ASC by default; `ORDER BY COUNT ASC` = count ASC then value ASC; `ORDER BY VALUE [ASC|DESC]` = value only. Value order: a single-bucket type uses `GlobalDictionary.compare(int, int)` of the snapshot's dictionary (rank first, value on a rank tie); merged ordinals of several buckets or subtypes (`CrossBucketOrdinalMap`, Task 7) and values from patching use `FacetValueComparator` (null last; values `DictionaryValueCodec.isSupported` accepts by `DictionaryValueCodec.compareValues`, made public by Task 5; any other class after them, by class name then `toString`) | Deterministic tie-break (spec 7.4) |
| D16 | Defaults: `TOP 10` (above `COLUMNAR_TOP_MAX` is a parse error naming the setting), `MIN_COUNT 1` for terms and histograms, ranges always returned (count 0 allowed). `INCLUDE` values are always returned, even at count 0, and are the only values returned. `MISSING` adds a bucket `{"value": null, "missing": true, "count": n}` ranked like the others; the `missing` member is always present regardless. `otherCount` = sum of the counts of the values not returned (Elasticsearch `sum_other_doc_count`: for a multi-valued field it counts value occurrences) | Spec 8.2 / 8.1 |
| D17 | Ranges: half-open `[lo, hi)`, `*` unbounded, may overlap (a value counts in each). Default key `"<lo>-<hi>"` with `*` for an unbounded end. `DEFAULT 'label'` adds a bucket `{"key": label, "default": true}` for present values in no range. Counting via elementary intervals: sorted distinct bounds, one `long[]` counter per interval, range count = sum of its intervals; a segment whose `[minLong, maxLong]` lies in one interval is counted by cardinality without decoding | Spec 7.7 ("segment tree" requirement met with O(log b) per value and O(1) per range) |
| D18 | Histogram `CALENDAR` intervals are case-sensitive `1m 1h 1d 1w 1M 1q 1y` (multiplier must be 1); `FIXED` takes `N(s|m|h|d|w)` through `SQLFunctionTimeBucket.parseInterval` (an uppercase `M`, `q`, `y` is refused before parsing, since `parseInterval` lowercases `M` to minutes) or a number for a numeric property. `FIXED` + `TIMEZONE`/`OFFSET` reuse `SQLFunctionTimeBucket.resolveOffset(Map.of("timezone", tz, "offset", off), interval, intervalMs)`, so FIXED buckets align with `ts.timeBucket` and refuse non-fixed-offset zones with its message. `BOUNDS [lo, hi]` are extended bounds (buckets exist, values outside still counted). More than 100,000 buckets is an error | Spec 8.1/7.7; one rule set with `ts.timeBucket` |
| D19 | Calendar boundaries are a `long[]` built once per query with `ZonedDateTime`: start = truncation of the min in the zone (day: `toLocalDate().atStartOfDay(zone)`, so a zone without midnight starts at the first valid instant; week: Monday; month/quarter/year: first day), shifted by `OFFSET`; next = `plusX(1)` (hours/minutes on the instant line, days and up on the local line). Values located by `Arrays.binarySearch` | Spec 7.7: DST inside a month is in the array, never per-value rounding |
| D20 | Snapshots are acquired at `execute()` time (after the drain); a commit racing the query may be counted on either side, as any read-committed scan does | Matches ArcadeDB's isolation for non-transactional reads; tests never commit while a facet query runs |
| D21 | Metrics: `ColumnarStats` (plan 3, one per database, `ColumnarIndexManager.getStats()`) gains database-level `LongAdder`s `queryPatched`, `queryBudgetWaits`, `queryPatchOverflows`, `queryFallbackScans` (Task 7); plan 3's `ColumnarMetrics.bindNewSeries` binds `arcadedb.columnar.query.patched`, `.query.budget.waits`, `.query.patch.overflows`, `.query.fallback.scans` with tag `database` next to `recovery.pages` (Task 13). A budget wait is charged to the database of the waiting query | One lifecycle for columnar meters (refresh, drop, close); the engine has Micrometer only in test scope (engine/pom.xml:259-264); roadmap requires the database tag on `/prometheus` |
| D22 | ndjson: one line `{"facets": {...}}` after the last `record` line and before the `stats` trailer, which stays the last line of a successful stream | Keeps the existing completeness rule (no `stats` = incomplete) |
| D23 | gRPC: `GrpcFacets facets` on `ExecuteQueryResponse` (field 4), `ExecuteCommandResponse` (field 7) and on the terminal `QueryResult` (field 5, `is_last_batch=true`, an empty terminal batch is sent when needed). `PAGED` retrieval with FACETS is `INVALID_ARGUMENT` | Unary and CURSOR/MATERIALIZE_ALL carry facets once; PAGED would recompute them per page |

## Contract changes needed (now recorded in the roadmap's "Plan 4 additions" and "Plan 3 must also provide")

1. `FacetSpec.filter()` returns `WhereClause` instead of `Expression` (D2). `FacetSpec` also gains `default Expression expression() { return null; }` (the faceted expression; overridden by the terms/range/histogram records; `property()` is null only for an expression facet). Plan 5's records keep exactly the components the roadmap lists.
2. `MatchSet` gains `PositionBitmap forFacet(int facetIndex, int bucketId)` and `long size()`. `FacetResult` gains `String reason()` (non-null only when `source` is `"unavailable"`).
3. `PositionBitmap` needs `PositionBitmap copy()` (plan 3 class).
4. `ColumnarValueExtractor` needs `long toColumnLong(String property, Object value)` (a literal range/histogram bound converted with the same rules as stored values) and `TimeUnit dateUnit(String property)` (null for non-date columns), for date bounds at the property's precision.
5. `ColumnarIndexManager` needs `Semaphore fallbackScans()` (per-database permits, sized `COLUMNAR_MAX_FALLBACK_SCANS`). Lag is produced in tests with plan 3's `suspendApply(): AutoCloseable` and caught-up state awaited with `FacetOracle.awaitCaughtUp(Database)` (both already in the roadmap's contract additions); it must be public, since `ha-raft` tests call it.
6. Assumed of `ColumnReader.forEachOrdinal`: positions are visited in ascending order (missing is computed by counting distinct positions).
7. New `GlobalConfiguration` key `COLUMNAR_QUERY_BUDGET_WAIT` (`arcadedb.columnar.queryBudgetWait`, ms, default 5000).
8. `FacetOracle` (plan 3 fixture) gains, in Task 11, `static void assertFacetsEqualGroupBy(Database db, String type, String property, String where)`; plan 4 assumes plan 3's `static Map<Object, Long> expectedCounts(Database db, String type, String property, String where)` (adapt the calls if plan 3 named it differently) and `ColumnarTestData(long seed)` with `createSchema(Database db)`, `List<RID> insertAssets(Database db, int count)`, `void mutate(Database db, int operations)` and the `buckets` knob (plan 3 Task 19); `FacetOracle.MISSING` is the key under which records without a value are counted.
9. Own-transaction checks reuse `TransactionContext.hasChangesInFiles(IntHashSet)` and `getWrittenRecordIdsIn(IntHashSet)` (both on main); plan 5 uses them too.
10. `CrossBucketOrdinalMap` (Task 7, roadmap "Dictionary scope change"): dictionaries are per bucket with generations; every reader decodes with `ColumnarSnapshot.dictionary(property)` and merges ordinals across buckets and subtypes through this map.

## Review Focus

1. **Literal parameterization swallowing facet literals.** `SQLLiteralParameterizer` (#8307) extracts numbers and strings into parameters before the statement cache; `TOP 10` or `RANGES ([0, 100))` would become parameters the grammar does not accept, or worse share a plan across different ranges. Expected: every literal under `facetsClause` is kept (Task 2, `SQLLiteralParameterizerFacetsTest`).
2. **A FROM alias or UNWIND alias eating the keyword.** `fromIdentifier` is `identifier (modifier)* (AS? identifier)?` and `FACETS` becomes an identifier: `SELECT FROM Asset FACETS brand TERMS` must parse as a facets clause, and `SELECT FROM Asset facets WHERE x = 1` must still parse as an alias (Task 2, `SQLFacetsParserTest.facetsIsNotTakenAsTargetAlias`, `aliasNamedFacetsStillParses`, `unwindThenFacets`).
3. **The HTTP auto-limit cutting facets short.** A FACETS query without LIMIT gets ` limit 21` appended; facets must still count every match, and `truncated`/`returned` must describe the rows only (Task 14, `FacetsHttpIT.autoLimitDoesNotShrinkFacets`).
4. **`FIXED '1M'` silently meaning one minute.** `SQLFunctionTimeBucket.parseInterval` lowercases its unit; a user asking for months through FIXED must get an error that points at `CALENDAR` (Task 3, `FacetSpecCompilerTest.fixedMonthIsRefused`).
5. **The caller's own uncommitted writes.** Inside an open transaction that updated a matched record, facets must count the transaction's value, never the committed projection value (Task 8, `FacetPatchingTest.ownTransactionWritesAreCounted`).

---

## File Structure

| Path | Action | Responsibility |
|---|---|---|
| `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLLexer.g4:257` (after `TAGS`) | Modify | 13 tokens: `FACETS TERMS TOP FILTER INCLUDE MIN_COUNT MISSING RANGES HISTOGRAM CALENDAR FIXED TIMEZONE BOUNDS` (`HIERARCHY`, `STATS` are plan 5's) |
| `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLParser.g4:203-217, 1376, 1655-1828` | Modify | `selectStatement` gains `facetsClause?`; new facet rules after `limit`; the 13 tokens in `identifier` |
| `engine/src/main/java/com/arcadedb/query/sql/parser/FacetsClause.java`, `FacetItem.java`, `FacetRangeItem.java` | Create | AST nodes (`toString`, `copy`, `equals`/`hashCode`, `toJSON`) |
| `engine/src/main/java/com/arcadedb/query/sql/parser/SelectStatement.java:50-57, 98-142, 144-167, 316-337` | Modify | `facets` field, getter, `toString`, `validate`, `copy`, `getIdentityElements` |
| `engine/src/main/java/com/arcadedb/query/sql/antlr/SQLASTBuilder.java:512-560` | Modify | build `FacetsClause`; outermost check; FACETS-after-LIMIT error |
| `engine/src/main/java/com/arcadedb/query/sql/antlr/SQLLiteralParameterizer.java:189-203` | Modify | keep literals under `FacetsClauseContext` |
| `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` | Modify | 5 keys (4 contract + `COLUMNAR_QUERY_BUDGET_WAIT`) |
| `engine/src/main/java/com/arcadedb/query/facet/*` | Create | `FacetSpec` + 6 records, `FacetOrder`, `RangeBound`, `HistogramInterval`, `DistinctValue`, `FacetStats`, `PatchedValue`, `CrossBucketOrdinalMap`, `FacetSpecCompiler`, `CalendarBoundaries`, `RangeBucketer`, `FacetCounter`/`DenseFacetCounter`/`HashFacetCounter`/`LongTermsCounter`, `FacetRanker`, `FacetValueComparator`, `FacetMemoryBudget`, `MatchSet`, `FacetFilterEvaluator`, `FacetExecutor`, `FacetPatcher`, `FacetFallbackScanner`, `FacetResults`, `FacetResult`, `FacetBucket`, `FacetCollector`, `TermsCollector`, `RangeCollector`, `HistogramCollector`, `FacetExecutionStats` |
| `engine/src/main/java/com/arcadedb/query/sql/executor/FacetCollectStep.java` | Create | match collection, filter mask, row post-filter, drain, EXPLAIN/PROFILE |
| `engine/src/main/java/com/arcadedb/query/sql/executor/SelectExecutionPlanner.java:179-216, 253-336, 572` | Modify | D6 and the step chaining |
| `engine/src/main/java/com/arcadedb/query/sql/executor/SelectExecutionPlan.java`, `InternalExecutionPlan.java` | Modify | `getFacets()` |
| `engine/src/main/java/com/arcadedb/query/sql/executor/ResultSet.java:64` | Modify | `default FacetResults getFacets() { return null; }` |
| `engine/src/main/java/com/arcadedb/query/sql/parser/LocalResultSet.java`, `executor/InternalResultSet.java`, `executor/IteratorResultSet.java` | Modify | carry/delegate facets |
| `engine/src/main/java/com/arcadedb/query/sql/parser/ProfileStatement.java:62-71, 85-93` | Modify | force facets before printing |
| `engine/src/main/java/com/arcadedb/query/sql/executor/FetchFromIndexedFunctionStep.java` | Modify | `IndexedMatchSource` fast path |
| `server/src/main/java/com/arcadedb/server/monitor/ColumnarMetrics.java` | Modify | bind query counters |
| `server/src/main/java/com/arcadedb/server/monitor/ProfilingResultSet.java:85` | Modify | delegate `getFacets()` |
| `server/src/main/java/com/arcadedb/server/http/handler/AbstractQueryHandler.java:332-389` | Modify | `reportFacets`, ndjson facets line |
| `server/src/main/java/com/arcadedb/server/http/handler/NdJsonResultStream.java:117-133` | Modify | `writeFacets(JSONObject)` + wire-format javadoc |
| `server/src/main/java/com/arcadedb/server/http/handler/PostCommandHandler.java:346-424, 482-501` | Modify | facets in envelope, `materializeResultSet` keeps them |
| `server/src/main/java/com/arcadedb/server/http/handler/GetQueryHandler.java:131-149` | Modify | facets in envelope |
| `server/src/main/java/com/arcadedb/server/http/handler/openapi/CoreApiSpec.java:198-207, 995, 1060` | Modify | schemas |
| `network/src/main/java/com/arcadedb/remote/RemoteDatabase.java:1696-1716` | Modify | parse `facets` |
| `network/src/main/java/com/arcadedb/remote/RemoteStreamingResultSet.java:139-170` | Modify | accept the `facets` line |
| `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java:5471-5490` | Modify | `parseResultSetFromJson` keeps facets |
| `grpc/src/main/proto/arcadedb-server.proto:150-155, 484-494, 529-533` | Modify | messages + fields |
| `grpc/src/main/java/com/arcadedb/server/grpc/FacetProtocol.java` | Create | `FacetResults` <-> `GrpcFacets` |
| `grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java:900-930, 1606-1790, 2412-2480, 2485-2550, 2555` | Modify | attach facets, refuse PAGED |
| `grpc-client/src/main/java/com/arcadedb/remote/grpc/RemoteGrpcDatabase.java` (`createGrpcResultSet`), `StreamingResultSet.java`, `BatchedStreamingResultSet.java` | Modify | read facets |

---

### Task 1: Settings, FacetSpec types and the result model with its JSON

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` (next to plan 3's `COLUMNAR_*` keys, GlobalConfiguration.java:361-403: append after `COLUMNAR_DICTIONARY_COMPACT_RATIO`)
- Create: `engine/src/main/java/com/arcadedb/query/facet/{FacetSpec,TermsFacetSpec,RangeFacetSpec,HistogramFacetSpec,HierarchyFacetSpec,DistinctFacetSpec,StatsFacetSpec,FacetOrder,RangeBound,HistogramInterval,FacetResults,FacetResult,FacetBucket,FacetCollector,PatchedValue,DistinctValue,FacetStats}.java`
- Test: `engine/src/test/java/com/arcadedb/query/facet/FacetResultsJsonTest.java`
- Test: `engine/src/test/java/com/arcadedb/query/facet/FacetResultsSamples.java` (public: `allKinds()` = the `roundTripIsIdentity` sample, `plan5Members()` = the `attributesAndPlan5MembersRoundTrip` sample)

**Interfaces:**
- Produces:
  - `GlobalConfiguration.COLUMNAR_PATCH_BUDGET` (`arcadedb.columnar.patchBudget`, `SCOPE.SERVER`, Integer, 10000), `COLUMNAR_QUERY_MEMORY_BUDGET` (`arcadedb.columnar.queryMemoryBudget`, `SCOPE.JVM`, Long, 268435456), `COLUMNAR_QUERY_BUDGET_WAIT` (`arcadedb.columnar.queryBudgetWait`, `SCOPE.SERVER`, Long ms, 5000), `COLUMNAR_TOP_MAX` (`arcadedb.columnar.topMax`, `SCOPE.SERVER`, Integer, 1000), `COLUMNAR_MAX_FALLBACK_SCANS` (`arcadedb.columnar.maxFallbackScans`, `SCOPE.DATABASE`, Integer, 2).
  - `sealed interface FacetSpec permits ... { String alias(); String property(); Expression expression(); WhereClause filter(); }`
  - `record TermsFacetSpec(String alias, String property, Expression expression, WhereClause filter, int top, int minCount, FacetOrder order, List<Object> include, boolean missingBucket) implements FacetSpec`
  - `record RangeFacetSpec(String alias, String property, Expression expression, WhereClause filter, List<RangeBound> ranges, String defaultLabel)`; `record RangeBound(Object from, Object to, String key)` (`null` = unbounded)
  - `record HistogramFacetSpec(String alias, String property, Expression expression, WhereClause filter, HistogramInterval interval, String timezone, String offset, int minCount, Object boundsFrom, Object boundsTo)`
  - `record HistogramInterval(boolean calendar, TemporalUnit calendarUnit, long fixedMillis, double fixedNumber, String text)`
  - Plan 5's records, created empty of behaviour: `record HierarchyFacetSpec(String alias, String property, WhereClause filter, int top, int minCount)`, `record DistinctFacetSpec(String alias, String property, WhereClause filter, long precisionThreshold)`, `record StatsFacetSpec(String alias, String property, WhereClause filter)` (they inherit the `expression()` default)
  - `TermsFacetSpec` has one more trailing component `Object after` (always null in plan 4; plan 5's `AFTER` cursor fills it), so its accessor `after()` exists
  - `interface FacetCollector { void beginBucket(int bucketId, ColumnarSnapshot snapshot, CrossBucketOrdinalMap map); void collect(ColumnarSegmentView segment, PositionBitmap matches); void collectPatched(long position, PatchedValue value); void merge(FacetCollector other); FacetResult finish(); }` (`ValueSink` is plan 3's; one collector per (facet, declaring type), the declaring type being `getPolymorphicColumnarIndex(property)`'s type (one per hierarchy); `merge` folds the collectors of one facet across declaring types (subquery targets) by value, `finish` ranks and releases its budget reservation). `matches` passed to `collect` is `forFacet AND NOT patched`, not ANDed with live: an ORDINAL/ORDINAL_LIST collector hands it to `ColumnReader.forEachOrdinal`, which visits only live rows (roadmap R12), and counts missing as `matches.andCardinality(segment.live()) - distinct positions visited`; a LONG/DOUBLE collector is the one path that ANDs `segment.live()` itself (R29). `final class PatchedValue implements ValueSink`, filled once per record and property by `ColumnarValueExtractor.extract`, is passed to `collectPatched` directly; collectors read it through its getters `boolean isMissing()`, `int ordinalCount()`, `int ordinalAt(int i)` (snapshot-generation ordinals, ascending; named `ordinalAt` because `ordinal(int)` is the ValueSink write method), `int unknownCount()`, `Object unknown(int i)`, `boolean hasLong()`, `long longValue()`, `boolean hasDouble()`, `double doubleValue()`; test factory `static PatchedValue ofValues(GlobalDictionary dictionary, Object... values)` (values the dictionary holds become ordinals, the others unknowns; no value = missing). The getters are the readable side the bare `ValueSink` lacks
  - `enum FacetOrder { COUNT_DESC, COUNT_ASC, VALUE_ASC, VALUE_DESC }`
  - `final class FacetBucket` with factories `term(Object value, long count)`, `missing(long count)`, `range(String key, Object from, Object to, long count)`, `rangeDefault(String key, long count)`, `histogram(String key, Object from, Object to, long count)`; getters `value() key() from() to() count() isMissing() isDefault()`; `JSONObject attributes()` (extra members merged into the bucket's JSON, empty in plan 4, plan 5 puts hierarchy paths there; `withAttributes(JSONObject)` sets them); `JSONObject toJSON()`; `static FacetBucket fromJSON(JSONObject)` (unknown members go back into `attributes()`).
  - `final class FacetResult { static FacetResult of(String kind, String source, List<FacetBucket> buckets, long otherCount, long missing); static FacetResult unavailable(String kind, String reason); static FacetResult ofDistinct(String source, DistinctValue distinct, long missing); static FacetResult ofStats(String source, FacetStats stats); FacetResult withAfter(Object after); String kind(); String source(); String reason(); List<FacetBucket> buckets(); long otherCount(); long missing(); DistinctValue distinct(); FacetStats stats(); Object after(); JSONObject toJSON(); static FacetResult fromJSON(JSONObject); }` (`distinct`, `stats`, `after` are null in plan 4, written to JSON only when non-null, and read back by `fromJSON`, so plan 5 changes no transport). `kind` is `"terms" | "range" | "histogram" | "hierarchy" | "distinct" | "stats"`, carried in JSON as `"type"` so a client can rebuild it. Plan 5's shapes, owned here so every transport carries them: `public record DistinctValue(long value, boolean exact)` written as members `"value"` and `"exact"` of the facet object; `public record FacetStats(long count, long missing, Object min, Object max, Number sum, Object avg)` written as members `count, missing, min, max, sum, avg` (`null` kept as JSON null; `fromJSON` returns integral numbers as `Long`, so a date min/max comes back as its epoch number); `after` as member `"after"` (JSON null when no value follows). A facet with `exact` false (approximate DISTINCT) makes `FacetResults.exact()` false
  - `final class FacetResults { FacetResults(boolean exact, long patched, long lagMs, LinkedHashMap<String, FacetResult> results); boolean exact(); long patched(); long lagMs(); FacetResult get(String alias); Set<String> aliases(); JSONObject toJSON(); static FacetResults fromJSON(JSONObject); }` (aliases keep declaration order).

- [ ] **Step 1: Write the failing test** `FacetResultsJsonTest`

```java
@Test void termsFacetAlwaysCarriesMissingAndOtherCount() {
  final FacetResults r = new FacetResults(true, 12, 340, map("brand",
      FacetResult.of("terms", "index", List.of(FacetBucket.term("Nike", 1240)), 0, 0)));
  final JSONObject brand = r.toJSON().getJSONObject("brand");
  assertThat(brand.getString("source")).isEqualTo("index");
  assertThat(brand.getString("type")).isEqualTo("terms");
  assertThat(brand.has("otherCount")).isTrue();
  assertThat(brand.getLong("otherCount")).isEqualTo(0L);
  assertThat(brand.has("missing")).isTrue();
  assertThat(brand.getJSONArray("buckets").getJSONObject(0).getString("value")).isEqualTo("Nike");
  assertThat(brand.getJSONArray("buckets").getJSONObject(0).getLong("count")).isEqualTo(1240L);
}
@Test void metaIsExactPatchedLag() {  // spec 8.3
  final JSONObject meta = sample().toJSON().getJSONObject("meta");
  assertThat(meta.getBoolean("exact")).isTrue(); assertThat(meta.getLong("patched")).isEqualTo(12L); assertThat(meta.getLong("lagMs")).isEqualTo(340L);
}
@Test void rangeBucketsCarryKeyFromTo() {      // unbounded end omits from/to
  final JSONObject b = FacetBucket.range("large", 10485760L, null, 7).toJSON();
  assertThat(b.getString("key")).isEqualTo("large"); assertThat(b.getLong("from")).isEqualTo(10485760L); assertThat(b.has("to")).isFalse();
  assertThat(FacetBucket.rangeDefault("other", 3).toJSON().getBoolean("default")).isTrue();
}
@Test void unavailableFacetHasNoBuckets() {
  final JSONObject u = FacetResult.unavailable("terms", "fallback scans capped").toJSON();
  assertThat(u.getString("source")).isEqualTo("unavailable"); assertThat(u.getString("reason")).isEqualTo("fallback scans capped");
  assertThat(u.has("buckets")).isFalse(); assertThat(u.has("otherCount")).isFalse(); assertThat(u.has("missing")).isFalse();
}
@Test void missingBucketIsNullValueFlagged() {
  final JSONObject m = FacetBucket.missing(4).toJSON();
  assertThat(m.isNull("value")).isTrue(); assertThat(m.getBoolean("missing")).isTrue();
}
@Test void attributesAndPlan5MembersRoundTrip() // bucket attributes {"parent":"a","depth":1}, ofDistinct(new DistinctValue(7, false)), ofStats(new FacetStats(10, 2, 1L, 99L, 500L, 50.0)), a terms result withAfter("Nike") and one withAfter(null): toJSON -> fromJSON -> toJSON identical; FacetResults with the inexact distinct has exact() false
@Test void patchedValueReadsBackWhatExtractWrote() // ofValues(dict, "Nike", "new"): ordinalCount 1, unknownCount 1; ofValues(dict): isMissing
@Test void roundTripIsIdentity() {           // every transport depends on it
  final FacetResults r = FacetResultsSamples.allKinds();
  assertThat(FacetResults.fromJSON(r.toJSON()).toJSON().toString()).isEqualTo(r.toJSON().toString());
  assertThat(FacetResults.fromJSON(r.toJSON()).aliases()).containsExactly("brand", "size", "pub", "gone");
}
```

- [ ] **Step 2: Run, expect compilation failure**

Run: `mvn -o -pl engine -am test -Dtest=FacetResultsJsonTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL, `FacetResults` not found.

- [ ] **Step 3: Implement the keys, the records and the result classes**

`FacetResults.toJSON()` writes `meta` first, then one member per alias in declaration order. JSON member order inside a facet: `type, source, [reason], [buckets], [otherCount (terms only)], [missing]`. `range`/`histogram` results carry `missing` but no `otherCount`. Numbers are written as `long` when integral, `double` otherwise; `fromJSON` turns integral JSON numbers back into `Long`. Each settings key description states its unit and that it applies to the `FACETS` clause. `COLUMNAR_PATCH_BUDGET`, `COLUMNAR_QUERY_BUDGET_WAIT`, `COLUMNAR_TOP_MAX` are read with `GlobalConfiguration.<KEY>.getValueAs...()` once per query; `COLUMNAR_MAX_FALLBACK_SCANS` with `database.getConfiguration()` at manager construction (R8); `COLUMNAR_QUERY_MEMORY_BUDGET` by `FacetMemoryBudget.instance()`.

- [ ] **Step 4: Run, expect PASS** (same command)

- [ ] **Step 5: Stop: changes ready for review** - `GlobalConfiguration.java`, `query/facet/*` (17 files), `FacetResultsJsonTest.java`, `FacetResultsSamples.java`.

---

### Task 2: FACETS grammar, keywords as identifiers, AST, literal parameterization

**Files:**
- Modify: `SQLLexer.g4` (after line 257), `SQLParser.g4` (`selectStatement` 203-217, new rules after `limit` at 1376, `identifier` 1655-1828)
- Create: `engine/src/main/java/com/arcadedb/query/sql/parser/FacetsClause.java`, `FacetItem.java`, `FacetRangeItem.java`
- Modify: `SelectStatement.java`, `SQLASTBuilder.java:512-560`, `SQLLiteralParameterizer.java:189-203`
- Test: `engine/src/test/java/com/arcadedb/query/sql/parser/SQLFacetsParserTest.java`, `FacetsKeywordsAsIdentifiersTest.java`, `engine/src/test/java/com/arcadedb/query/sql/antlr/SQLLiteralParameterizerFacetsTest.java`

**Interfaces:**
- Produces: `SelectStatement.getFacets(): FacetsClause` (null when absent); `FacetsClause.items: List<FacetItem>`; `FacetItem` public fields `Identifier alias; Expression expression; FacetItem.Kind kind; PInteger top; PInteger minCount; WhereClause filter; List<Expression> include; Boolean orderByCount; Boolean ascending; boolean missing; List<FacetRangeItem> ranges; String defaultLabel; Boolean calendar; Object interval; String timezone; String offset; Object boundsFrom, boundsTo;` with `enum Kind { TERMS, RANGES, HISTOGRAM, HIERARCHY, DISTINCT, STATS }`; `FacetRangeItem` fields `Object from, to; String label` (`null` = `*`; an `InputParameter` is kept as is).

Grammar (exact):

```antlr
selectStatement
    : SELECT projection? (FROM fromClause)? letClause? (WHERE whereClause)? groupBy? orderBy? unwind?
      facets=facetsClause?
      ( skip limit? | limit skip? )?
      lateFacets=facetsClause?          // D5: only to raise "FACETS must come before SKIP and LIMIT"
      timeout? PARALLEL?
    ;
facetsClause         : FACETS facetItem (COMMA facetItem)* ;
facetItem            : (identifier COLON)? expression facetBody ;
facetBody
    : TERMS facetTermsOption*                                                        # termsFacet
    | RANGES LPAREN facetRange (COMMA facetRange)* RPAREN (DEFAULT STRING_LITERAL)? facetFilter?   # rangesFacet
    | HISTOGRAM (CALENDAR | FIXED) facetInterval facetHistogramOption*              # histogramFacet
    // plan 5 appends: # hierarchyFacet, # distinctFacet, # statsFacet (and AFTER in facetTermsOption)
    ;
facetTermsOption     : TOP pInteger | MIN_COUNT pInteger | ORDER_BY (COUNT | VALUE) (ASC | DESC)?
                     | INCLUDE arrayLiteral | MISSING | facetFilter ;
facetHistogramOption : TIMEZONE STRING_LITERAL | OFFSET STRING_LITERAL | MIN_COUNT pInteger
                     | BOUNDS LBRACKET facetBound COMMA facetBound RBRACKET | facetFilter ;
facetFilter          : FILTER whereClause ;
facetRange           : LBRACKET facetBound COMMA facetBound RPAREN (AS STRING_LITERAL)? ;
facetBound           : STAR | MINUS? (INTEGER_LITERAL | FLOATING_POINT_LITERAL) | STRING_LITERAL | inputParameter ;
facetInterval        : STRING_LITERAL | INTEGER_LITERAL | FLOATING_POINT_LITERAL ;
```

The 13 lexer tokens use the existing case-insensitive fragments (`MIN_COUNT: M I N UNDERSCORE C O U N T;`), and all 13 are appended to `identifier` after `OVERWRITE` with a comment `// FACETS clause keywords (columnar faceting): still valid property names`.

- [ ] **Step 1: Write the failing tests**

`SQLFacetsParserTest` (parse with `new SQLAntlrParser(null).parse(sql)` as existing parser tests do, cast to `SelectStatement`):

```java
@Test void fullSpecExampleParses() {   // spec 8.2 verbatim minus the HIERARCHY/DISTINCT/STATS lines (plan 5)
  final SelectStatement s = parse("""
      SELECT @rid, title FROM Asset WHERE search_fields(['title', 'body'], 'running shoes')
      FACETS brand TERMS TOP 10 FILTER brand IN ['Nike', 'Adidas'] INCLUDE ['Nike', 'Adidas'],
        tags TERMS TOP 20 MIN_COUNT 2 ORDER BY COUNT DESC,
        size: fileSize RANGES ([0, 1048576) AS 'small', [1048576, 10485760) AS 'medium', [10485760, *) AS 'large') DEFAULT 'other',
        pub: publishDate HISTOGRAM CALENDAR '1M' TIMEZONE 'Europe/Rome' OFFSET '+6h' MIN_COUNT 0 BOUNDS ['2026-01-01', '2026-12-31']
      LIMIT 20""");
  assertThat(s.getFacets().items).hasSize(4);
  assertThat(s.getLimit().getValue(null)).isEqualTo(20);
  final FacetItem size = s.getFacets().items.get(2);
  assertThat(size.alias.getStringValue()).isEqualTo("size");
  assertThat(size.ranges.get(2).to).isNull();
  assertThat(size.defaultLabel).isEqualTo("other");
}
@Test void toStringRoundTrips()          { assertThat(parse(parse(SQL).toString()).toString()).isEqualTo(parse(SQL).toString()); }
@Test void copyIsEqual()                 { final SelectStatement s = parse(SQL); assertThat(s.copy()).isEqualTo(s); assertThat(s.copy().hashCode()).isEqualTo(s.hashCode()); }
@Test void facetsIsNotTakenAsTargetAlias() { assertThat(parse("SELECT FROM Asset FACETS brand TERMS").getFacets()).isNotNull(); }
@Test void aliasNamedFacetsStillParses() { assertThat(parse("SELECT FROM Asset facets WHERE facets.x = 1").getFacets()).isNull(); }
@Test void unwindThenFacets()            { assertThat(parse("SELECT FROM Asset UNWIND tags FACETS brand TERMS").getFacets()).isNotNull(); }
@Test void appendedLimitAfterFacetsParses() { assertThat(parse("SELECT FROM Asset FACETS brand TERMS TOP 5 limit 21").getLimit()).isNotNull(); }
@Test void facetsAfterLimitIsRefused()   { assertThatThrownBy(() -> parse("SELECT FROM Asset LIMIT 20 FACETS brand TERMS"))
                                              .isInstanceOf(CommandSQLParsingException.class).hasMessageContaining("FACETS must come before SKIP and LIMIT"); }
@Test void facetsInSubqueryIsRefused()   { assertThatThrownBy(() -> parse("SELECT FROM (SELECT FROM Asset FACETS brand TERMS)"))
                                              .hasMessageContaining("FACETS is only allowed on the outermost SELECT"); }
@Test void facetsInLetAndInsertAreRefused() // same message for LET $a = (SELECT ... FACETS ...) and INSERT INTO X SELECT ... FACETS ...
@Test void explainAndProfileAndScriptKeepFacets() // EXPLAIN SELECT ... FACETS, PROFILE ..., parseScript("BEGIN; SELECT ... FACETS ...; COMMIT;")
@Test void facetsWithoutFromIsRefused()  { assertThatThrownBy(() -> parse("SELECT 1 AS x FACETS x TERMS").validate()).hasMessageContaining("FACETS requires a FROM clause"); }
@Test void duplicateOptionIsRefused()    { assertThatThrownBy(() -> parse("SELECT FROM A FACETS b TERMS TOP 1 TOP 2")).hasMessageContaining("TOP given twice in facet 'b'"); }
@Test void plan5KindsAreNotParsedYet()  { assertThatThrownBy(() -> parse("SELECT FROM A FACETS t: tags HIERARCHY")).isInstanceOf(CommandSQLParsingException.class); }
@Test void distinctProjectionUnaffected(){ assertThat(parse("SELECT DISTINCT brand FROM A FACETS brand TERMS").getFacets()).isNotNull(); }
@Test void sameFieldTwiceNeedsAliases()  // "FACETS brand TERMS, brand TERMS TOP 3" -> "duplicate facet alias 'brand'"; with aliases b1:, b2: it parses
@Test void moreThan31FiltersIsRefused()  // 32 facets each with FILTER -> "at most 31 facets may carry a FILTER"
@Test void facetsInViewDefinitionsAreRefused() // CREATE MATERIALIZED VIEW v AS SELECT FROM A FACETS b TERMS, CREATE CONTINUOUS AGGREGATE ... FACETS ...: "FACETS is only allowed on the outermost SELECT"
@Test void subqueryAliasThenFacets()     // SELECT FROM (SELECT FROM A) FACETS b TERMS parses as facets; SELECT FROM (SELECT FROM A) facets WHERE facets.x = 1 as an alias
```

`FacetsKeywordsAsIdentifiersTest` - `@ParameterizedTest @ValueSource(strings = {"facets","terms","top","filter","include","min_count","missing","ranges","histogram","calendar","fixed","timezone","bounds"})`, one method per shape, each asserting the statement parses (and, for the first, that the projection item names the keyword):

```java
void projectionWhereOrderBy(String k)  // "SELECT %1$s FROM T WHERE %1$s = 1 ORDER BY %1$s"
void groupByAndAlias(String k)         // "SELECT %1$s AS %1$s, count(*) FROM T GROUP BY %1$s"
void createProperty(String k)          // "CREATE PROPERTY T.%s STRING"
void insertSet(String k)               // "INSERT INTO T SET %s = 'v'"
void pathSegment(String k)             // "SELECT FROM T WHERE a.%s = 1"
void mapKey(String k)                  // "SELECT {%s: 1} AS m FROM T"
void facetOnPropertyNamedK(String k)   // "SELECT FROM T FACETS %s TERMS"
void facetAliasNamedK(String k)        // "SELECT FROM T FACETS %s: brand TERMS"
```

plus one engine execution test in the same class (extends `TestHelper`): for every keyword create property `K` on type `KW`, insert `{K: 'v'}`, assert `SELECT K FROM KW WHERE K = 'v'` returns 1 row with property `K` = `'v'`.

`SQLLiteralParameterizerFacetsTest`:

```java
@Test void facetLiteralsAreKeptWhereLiteralsAreExtracted() {
  // the WHERE literal is extracted, every FACETS literal stays in the text
  final Lookup<Statement> p = new SQLLiteralParameterizer(new SQLAntlrParser(database), 16).lookup("SELECT FROM Asset WHERE size > 5 FACETS brand TERMS TOP 10 INCLUDE ['Nike'], s: size RANGES ([0, 100) AS 'a')", true);
  assertThat(p.cacheKey()).contains("TOP 10").contains("['Nike']").contains("[0, 100)").contains("'a'");
  // p.parameters() holds the WHERE literal 5 and nothing from FACETS
}
@Test void differentRangesDoNotShareAStatement() // two texts differing only in a range bound parameterize to different texts
```

(class extends `TestHelper`; entry point as in `Issue9714RepeatedLiteralTextLookupTest`.)

- [ ] **Step 2: Run, expect FAIL**

Run: `mvn -o -pl engine -am test -Dtest='SQLFacetsParserTest,FacetsKeywordsAsIdentifiersTest,SQLLiteralParameterizerFacetsTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL (compilation: `getFacets` missing).

- [ ] **Step 3: Grammar and tokens** as above; regenerate with the module build (`mvn -o -pl engine -am compile`).

- [ ] **Step 4: AST nodes and `SelectStatement`**

`SelectStatement`: `public FacetsClause facets;` + `getFacets()`; `toString` appends ` FACETS ...` after `unwind` and before `skip`; `validate()` adds `if (facets != null && target == null) throw new CommandSQLParsingException("FACETS requires a FROM clause")`; `copy()`, `getIdentityElements()` and `refersToParent()` include it. `FacetItem.toString` prints canonical keyword order: kind, `TOP`, `MIN_COUNT`, `ORDER BY`, `INCLUDE`, `MISSING`, ranges/`DEFAULT`, interval, `TIMEZONE`, `OFFSET`, `BOUNDS`, `FILTER` last (so `FILTER`'s condition never swallows a following option on re-parse).

- [ ] **Step 5: `SQLASTBuilder`**

In `buildSelectClausesInScope` after UNWIND: `if (ctx.lateFacets != null) throw new CommandSQLParsingException("FACETS must come before SKIP and LIMIT")`; `if (ctx.facets != null) { requireOutermost(ctx); stmt.facets = visitFacetsClause(ctx.facets); }`. `requireOutermost(ParserRuleContext ctx)` walks `getParent()` and accepts only `SelectStmtContext`, `ParseContext`, `ParseScriptContext`, `ExplainStatementContext`, `ProfileStatementContext`, `StatementContext` subclasses whose own parent is accepted, `ScriptStatementContext` subclasses, and the script block rules whose children are `scriptStatement` (IF/WHILE/FOREACH bodies); anything else throws "FACETS is only allowed on the outermost SELECT". Option duplicates, duplicate aliases (alias defaults to the property path text), the reserved alias `meta` (it is the meta member of the facets JSON: "facet alias 'meta' is reserved") and the 31-FILTER cap are checked here.

- [ ] **Step 6: `SQLLiteralParameterizer.keepsLiterals`** returns true for `SQLParser.FacetsClauseContext`; add it to the javadoc list ("in FACETS: the planner sizes and buckets on the values").

- [ ] **Step 7: Run the three tests, expect PASS** (Step 2 command).

- [ ] **Step 8: Run every SQL parser and parameterizer test** (the new tokens change tokenization everywhere)

Run: `mvn -o -pl engine -am test -Dtest='com/arcadedb/query/sql/*Test,com/arcadedb/query/sql/**/*Test,com/arcadedb/function/sql/vector/*Test' -Dsurefire.failIfNoSpecifiedTests=false -DexcludedGroups=benchmark,slow,vector`
Expected: BUILD SUCCESS, no new failure compared to `main`.

- [ ] **Step 9: Stop: changes ready for review** - `SQLLexer.g4`, `SQLParser.g4`, `FacetsClause.java`, `FacetItem.java`, `FacetRangeItem.java`, `SelectStatement.java`, `SQLASTBuilder.java`, `SQLLiteralParameterizer.java`, three test files.

---

### Task 3: FacetSpecCompiler (AST to specs, validation, intervals)

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/facet/FacetSpecCompiler.java`
- Test: `engine/src/test/java/com/arcadedb/query/facet/FacetSpecCompilerTest.java`

**Interfaces:**
- Consumes: Task 1 records, Task 2 AST.
- Produces: `static List<FacetSpec> FacetSpecCompiler.compile(FacetsClause clause, CommandContext ctx)` (resolves input parameters in bounds/include, throws `CommandSQLParsingException`); `static HistogramInterval FacetSpecCompiler.parseInterval(boolean calendar, Object text)`. A `FacetItem.Kind` it does not compile (plan 5's) throws `CommandSQLParsingException("FACETS <alias>: <KIND> facets are not supported")`; plan 5 replaces that branch.

- [ ] **Step 1: Write the failing test**

```java
@Test void defaults()            { final TermsFacetSpec t = (TermsFacetSpec) only("brand TERMS");
                                   assertThat(t.top()).isEqualTo(10); assertThat(t.minCount()).isEqualTo(1); assertThat(t.order()).isEqualTo(FacetOrder.COUNT_DESC);
                                   assertThat(t.property()).isEqualTo("brand"); assertThat(t.alias()).isEqualTo("brand"); }
@Test void topAboveMaxIsRefused(){ assertThatThrownBy(() -> only("brand TERMS TOP 1001")).hasMessageContaining("arcadedb.columnar.topMax").hasMessageContaining("1000"); }
@Test void orderByValueDefaultsAsc() { assertThat(((TermsFacetSpec) only("b TERMS ORDER BY VALUE")).order()).isEqualTo(FacetOrder.VALUE_ASC); }
@Test void calendarUnits()       { assertThat(FacetSpecCompiler.parseInterval(true, "1M").calendarUnit()).isEqualTo(ChronoUnit.MONTHS);
                                   assertThat(FacetSpecCompiler.parseInterval(true, "1m").calendarUnit()).isEqualTo(ChronoUnit.MINUTES);
                                   assertThat(FacetSpecCompiler.parseInterval(true, "1q").calendarUnit()).isEqualTo(IsoFields.QUARTER_YEARS); }
@Test void calendarMultiplierMustBeOne() { assertThatThrownBy(() -> FacetSpecCompiler.parseInterval(true, "2M")).hasMessageContaining("CALENDAR intervals take a multiplier of 1"); }
@Test void fixedMonthIsRefused() { assertThatThrownBy(() -> FacetSpecCompiler.parseInterval(false, "1M")).hasMessageContaining("use CALENDAR '1M'"); }
@Test void fixedUsesTimeBucketUnits() { assertThat(FacetSpecCompiler.parseInterval(false, "90m").fixedMillis()).isEqualTo(5_400_000L); }
@Test void fixedNumberForNumeric(){ assertThat(FacetSpecCompiler.parseInterval(false, 100).fixedNumber()).isEqualTo(100d); }
@Test void rangeDefaultKeys()    { assertThat(((RangeFacetSpec) only("s: size RANGES ([0, 10), [10, *))")).ranges()).extracting(RangeBound::key).containsExactly("0-10", "10-*"); }
@Test void emptyRangeIsRefused() { assertThatThrownBy(() -> only("s: size RANGES ([10, 10))")).hasMessageContaining("empty range [10, 10)"); }
@Test void unknownZoneIsRefused(){ assertThatThrownBy(() -> only("p: d HISTOGRAM CALENDAR '1d' TIMEZONE 'Mars/Base'")).hasMessageContaining("Mars/Base"); }
@Test void unknownKindIsRefused()       // a FacetItem built by hand with kind HIERARCHY -> "FACETS t: HIERARCHY facets are not supported"
@Test void parameterInIncludeAndBoundsResolves() // ":v" in INCLUDE and a range bound resolves from ctx input parameters
@Test void expressionFacetHasNullProperty()     // "x: brand.toLowerCase() TERMS" -> property() null, expression() non-null
```

- [ ] **Step 2: Run, expect FAIL** - `mvn -o -pl engine -am test -Dtest=FacetSpecCompilerTest -Dsurefire.failIfNoSpecifiedTests=false`

- [ ] **Step 3: Implement** per D16/D17/D18. `property()` is the dotted path text when `expression` is a plain identifier chain, else null. CALENDAR units: `m`->MINUTES, `h`->HOURS, `d`->DAYS, `w`->WEEKS, `M`->MONTHS, `q`->`IsoFields.QUARTER_YEARS`, `y`->YEARS. FIXED string: if the unit is `M`, `q` or `y` throw `"FIXED '1M' is not a fixed interval: months, quarters and years vary in length, use CALENDAR '1M'"`, else `SQLFunctionTimeBucket.parseInterval`. Zone validated with `ZoneId.of`.

- [ ] **Step 4: Run, expect PASS**

- [ ] **Step 5: Stop: changes ready for review** - `FacetSpecCompiler.java`, `FacetSpecCompilerTest.java`.

---

### Task 4: Calendar and fixed histogram boundaries (DST)

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/facet/CalendarBoundaries.java`
- Test: `engine/src/test/java/com/arcadedb/query/facet/CalendarBoundariesTest.java`

**Interfaces:**
- Produces: `final class CalendarBoundaries { static CalendarBoundaries calendar(String alias, TemporalUnit unit, ZoneId zone, Duration offset, long minMs, long maxMs, int maxBuckets); static CalendarBoundaries fixed(String alias, ZoneId keyZone, long intervalMs, long offsetMs, long minMs, long maxMs, int maxBuckets) /* keyZone: zone the keys are rendered in (the TIMEZONE, else UTC) */; int bucketOf(long epochMs) /* -1 outside */; int size(); long from(int i); long to(int i); String key(int i) /* ISO-8601 local date-time with offset in the zone, e.g. "2026-03-01T00:00+01:00" */ }`. Boundaries cover `[floor(min), ceil(max)]`; `maxBuckets` = 100_000, exceeded -> `CommandExecutionException("HISTOGRAM <alias> would produce more than 100000 buckets: use a coarser interval or BOUNDS")`.

- [ ] **Step 1: Write the failing tests** (all instants written as `Instant.parse(...)`; lengths in ms)

```java
@Test void romeMonthsAcrossDst2026() {   // EU DST: 2026-03-29 and 2026-10-25
  final CalendarBoundaries b = calendar(MONTHS, "Europe/Rome", "2026-01-15T00:00:00Z", "2026-12-15T00:00:00Z");
  assertThat(b.size()).isEqualTo(12);
  assertThat(len(b, "2026-03")).isEqualTo(31 * 86_400_000L - 3_600_000L);   // 2_674_800_000
  assertThat(len(b, "2026-10")).isEqualTo(31 * 86_400_000L + 3_600_000L);   // 2_682_000_000
  assertThat(b.from(0)).isEqualTo(Instant.parse("2025-12-31T23:00:00Z").toEpochMilli());
  assertThat(b.key(0)).isEqualTo("2026-01-01T00:00+01:00");
}
@Test void newYorkDaysAcrossDst2026() {  // US DST: 2026-03-08 (23h) and 2026-11-01 (25h)
  assertThat(dayLen("America/New_York", "2026-03-08")).isEqualTo(82_800_000L);
  assertThat(dayLen("America/New_York", "2026-11-01")).isEqualTo(90_000_000L);
}
@Test void lordHoweHalfHourShift() {     // DST ends 2026-04-05, clocks back 30 min
  assertThat(dayLen("Australia/Lord_Howe", "2026-04-05")).isEqualTo(88_200_000L);
}
@Test void saoPauloDayWithoutMidnight() { // 2018-11-04 00:00 did not exist; the day starts 01:00-02:00
  final CalendarBoundaries b = calendar(DAYS, "America/Sao_Paulo", "2018-11-04T12:00:00Z", "2018-11-04T12:00:00Z");
  assertThat(b.from(0)).isEqualTo(Instant.parse("2018-11-04T03:00:00Z").toEpochMilli());
  assertThat(b.to(0) - b.from(0)).isEqualTo(82_800_000L);
}
@Test void kolkataHalfHourOffsetNoDst() { assertThat(calendar(DAYS, "Asia/Kolkata", "2026-06-01T12:00:00Z", "2026-06-01T12:00:00Z").from(0))
                                          .isEqualTo(Instant.parse("2026-05-31T18:30:00Z").toEpochMilli()); }
@Test void hoursOnFallBackNightAreTwentyFive() { // instant-line hours: 25 hourly buckets on 2026-10-25 in Europe/Rome
  assertThat(calendar(HOURS, "Europe/Rome", "2026-10-24T22:00:00Z", "2026-10-25T22:59:59Z").size()).isEqualTo(25); }
@Test void weeksStartMonday()       { assertThat(calendar(WEEKS, "UTC", "2026-03-04T10:00:00Z", "2026-03-04T10:00:00Z").from(0))
                                       .isEqualTo(Instant.parse("2026-03-02T00:00:00Z").toEpochMilli()); }
@Test void quartersAndYears()       // Q1 2026 in UTC = [2026-01-01, 2026-04-01), year 2026 = [2026-01-01, 2027-01-01)
@Test void offsetShiftsStarts()     // DAYS, UTC, OFFSET +6h -> buckets start at 06:00Z
@Test void bucketOfIsHalfOpen()     // value == to(i) belongs to i+1; value < from(0) -> -1
@Test void fixedMatchesTimeBucket() // fixed(3_600_000, resolveOffset(tz "+08:00")) gives the same starts ts.timeBucket('1h', t, {timezone:'+08:00'}) returns for 1000 random t
@Test void tooManyBucketsRefused()  // MINUTES over ten years -> message contains "more than 100000 buckets"
```

- [ ] **Step 2: Run, expect FAIL** - `mvn -o -pl engine -am test -Dtest=CalendarBoundariesTest -Dsurefire.failIfNoSpecifiedTests=false`

- [ ] **Step 3: Implement** per D19; `bucketOf` is `Arrays.binarySearch` over the `long[]` of starts with the end in a trailing slot.

- [ ] **Step 4: Run, expect PASS**

- [ ] **Step 5: Stop: changes ready for review** - `CalendarBoundaries.java`, `CalendarBoundariesTest.java`.

---

### Task 5: Range bucketing, counters, ranking, memory budget

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/facet/{RangeBucketer,FacetCounter,DenseFacetCounter,HashFacetCounter,LongTermsCounter,FacetRanker,FacetValueComparator,FacetMemoryBudget}.java`
- Modify: `engine/src/main/java/com/arcadedb/index/columnar/DictionaryValueCodec.java` (`compareValues` public)
- Test: `engine/src/test/java/com/arcadedb/query/facet/{RangeBucketerTest,FacetCounterTest,FacetRankerTest,FacetMemoryBudgetTest}.java`

**Interfaces:**
- Produces:
  - `RangeBucketer(double[] from, double[] to)` (NaN = unbounded) for DOUBLE columns, and `RangeBucketer(long[] from, long[] to, boolean[] fromUnbounded, boolean[] toUnbounded)` with `int interval(long v)`, `void add(long v)`, `int singleIntervalOf(long min, long max)` for LONG/DATETIME columns (exact integer math, no rounding above 2^53); `int interval(double v)`; `void add(double v)`; `void addInterval(int interval, long n)`; `long rangeCount(int range)`; `long defaultCount()`; `int singleIntervalOf(double min, double max)` (-1 when `[min, max]` spans more than one).
  - `interface FacetCounter { void increment(int ordinal); void add(int ordinal, long n); long get(int ordinal); void forEachNonZero(IntLongConsumer c); long estimatedBytes(); }`, `DenseFacetCounter(int cardinality)`, `HashFacetCounter(int expected)` (wraps `IntIntHashMap`; counts above `Integer.MAX_VALUE` are impossible per query since positions are per bucket, but `add` saturates and the test pins it), `LongTermsCounter` (wraps `LongLongHashMap`).
  - `static FacetCounter FacetCounter.choose(long matchCount, int cardinality)`: dense iff `cardinality <= 100_000 && matchCount * 8 >= cardinality`.
  - `FacetRanker`: `static int[] topOrdinals(FacetCounter c, int top, int minCount, FacetOrder order, IntBinaryOperator valueOrder)` (primitive bounded heap, ties by `valueOrder`) and `static List<Map.Entry<Object, Long>> topValues(Map<Object, long[]> counts, int top, int minCount, FacetOrder order)` (uses `FacetValueComparator`).
  - `FacetMemoryBudget`: `static FacetMemoryBudget instance()`; package-private `FacetMemoryBudget(long bytes)` for tests; `Reservation reserve(long bytes, long maxWaitMs)` returns null when refused; `long available()`; `final class Reservation implements AutoCloseable { long bytes(); boolean waited(); void close(); }` (idempotent close). A request above capacity is refused at once without waiting.

- [ ] **Step 1: Write the failing tests**

```java
// RangeBucketerTest
@Test void halfOpenAndOverlapping() { final RangeBucketer r = new RangeBucketer(new double[]{0, 5, NaN}, new double[]{10, 15, 5});
  for (double v : new double[]{-1, 0, 5, 9.999, 10, 15}) r.add(v);
  assertThat(r.rangeCount(0)).isEqualTo(3); assertThat(r.rangeCount(1)).isEqualTo(3); assertThat(r.rangeCount(2)).isEqualTo(2); assertThat(r.defaultCount()).isEqualTo(1); }
@Test void singleIntervalForSkipping() { assertThat(new RangeBucketer(new double[]{0, 10}, new double[]{10, 20}).singleIntervalOf(11, 19)).isNotNegative(); }
// FacetCounterTest
@Test void denseWhenMatchesNearCardinality() { assertThat(FacetCounter.choose(125, 1000)).isInstanceOf(DenseFacetCounter.class);
                                              assertThat(FacetCounter.choose(124, 1000)).isInstanceOf(HashFacetCounter.class);
                                              assertThat(FacetCounter.choose(200_000, 100_001)).isInstanceOf(HashFacetCounter.class); }
@Test void denseAndHashAgree() // 100k random increments over 5000 ordinals: forEachNonZero equal
// FacetRankerTest
@Test void tieBreakByValueAscending() { counts {a:5, c:5, b:5, d:9}, top 3 -> d, a, b }
@Test void orderByCountAsc()         { -> ascending counts, ties by value }
@Test void minCountDropsBuckets()
@Test void mixedTypesAreDeterministic() // "x", Long 2, Integer 10, null (strings first, numbers as one class, null last): comparator total order, nulls last, stable across runs
@Test void unrankedTailTiesBreakByValue() // dictionary with 1100 values, rank view built, then 'aa', 'zz', 'mm' appended: equal counts rank aa, mm, zz
@Test void comparatorMatchesDictionaryOrder() // strings before numbers, Long 2 vs BigDecimal 2.5 exact, NaN last, null after all
// FacetMemoryBudgetTest
@Test void grantsAndReleases()      { final FacetMemoryBudget b = new FacetMemoryBudget(1 << 20); try (var r = b.reserve(512 * 1024, 0)) { assertThat(r).isNotNull(); assertThat(b.available()).isEqualTo(512 * 1024); } assertThat(b.available()).isEqualTo(1 << 20); }
@Test void refusesAboveCapacityWithoutWaiting() { assertThat(new FacetMemoryBudget(1024).reserve(4096, 60_000)).isNull(); } // returns at once (no wall-clock assert: it would hang the @Timeout(10) otherwise)
@Test void waitsThenGrantsInOrder() // thread A holds all, B waits with 5s, A closes, B granted with waited()=true
@Test void resizesOnSettingChange() // set COLUMNAR_QUERY_MEMORY_BUDGET, instance().available() follows; restore in finally
```

- [ ] **Step 2: Run, expect FAIL** - `mvn -o -pl engine -am test -Dtest='RangeBucketerTest,FacetCounterTest,FacetRankerTest,FacetMemoryBudgetTest' -Dsurefire.failIfNoSpecifiedTests=false`

- [ ] **Step 3: Implement** per D12, D13, D15, D17. The budget converts bytes to KiB permits rounding up; `instance()` compares the setting with the current capacity (one volatile long read) and resizes with `release`/`reducePermits`.

- [ ] **Step 4: Run, expect PASS**

- [ ] **Step 5: Stop: changes ready for review** - 8 main files, 4 tests.

---

### Task 6: MatchSet

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/facet/MatchSet.java`
- Test: `engine/src/test/java/com/arcadedb/query/facet/MatchSetTest.java`

**Interfaces:**
- Consumes: `PositionBitmap(int maxRecordsInPage)`, `set`, `or`, `copy()` (contract change 3), `cardinality`; `LocalBucket.getMaxRecordsInPage()` (LocalBucket.java:612).
- Produces: `MatchSet.builder(DatabaseInternal db, int filterCount, long budgetWaitMs): MatchSet.Builder`; `Builder.add(RID rid, int failMask): boolean` (false once over budget); `Builder.build(): MatchSet`; `MatchSet.forBucket(int bucketId)` (all WHERE matches), `forFacet(int facetIndex, int bucketId)` (pass-all, OR pass-all + failed-only-own-filter when that facet carries filter index `i`; `MatchSet.setFacetFilterIndex(int[] filterIndexByFacet)`, -1 for none), `bucketIds()` (ascending), `size()`, `isOverBudget()`, `close()` releases the reservation; `static MatchSet fromProjection(DatabaseInternal db, int[] bucketIds, long budgetWaitMs)`: the no-WHERE match set, per bucket the OR of every segment's `live()` plus `pendingPositions()` trimmed to occupied slots with `LocalBucket.forEachCommittedRecordSlot(pageNumber, LongConsumer)` (one slot-table read per pending page, no record decode; Task 8) (never a record scan; a pending position is confirmed or dropped by the patcher), null for a bucket with no projection, a throwing `acquire()` or an unusable snapshot (that bucket is collected from rows instead).

- [ ] **Step 1: Write the failing test**

```java
@Test void disjunctiveMasks() {  // spec 7.5: pass all -> every facet; fail exactly one -> that facet only; fail two -> none
  final MatchSet m = build(2, new Object[][]{{rid(3, 1), 0b00}, {rid(3, 2), 0b01}, {rid(3, 3), 0b10}, {rid(3, 4), 0b11}});
  m.setFacetFilterIndex(new int[]{0, 1, -1});
  assertThat(positions(m.forFacet(0, 3))).containsExactly(1L, 2L);
  assertThat(positions(m.forFacet(1, 3))).containsExactly(1L, 3L);
  assertThat(positions(m.forFacet(2, 3))).containsExactly(1L);
  assertThat(positions(m.forBucket(3))).containsExactly(1L, 2L, 3L, 4L);
}
@Test void stripeFollowsBucketMaxRecordsInPage() // a bucket with 256 slots per page and one with 2048: positions 300 and 3000 land right
@Test void overBudgetStopsRecording()            // `COLUMNAR_QUERY_MEMORY_BUDGET` = 1024 (restored in finally): add returns false, isOverBudget true, close releases
@Test void fromProjectionIsLivePlusPending()      // (engine fixture with a COLUMNAR index, suspended apply, 10 inserts) inside `suspendApply()`: insert 10, cardinality includes the 10 new positions (pending); after closing the hold, `FacetOracle.awaitCaughtUp`, then cardinality == committed count
```

- [ ] **Step 2: Run, expect FAIL** - `mvn -o -pl engine -am test -Dtest=MatchSetTest -Dsurefire.failIfNoSpecifiedTests=false`
- [ ] **Step 3: Implement**: per bucket one base bitmap, one pass-all bitmap and lazily one failed-only bitmap per filter index; reservation charged per new page stripe (`maxRecordsInPage / 8` bytes per bitmap) in 1 MiB steps (D13).
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Stop: changes ready for review** - `MatchSet.java`, `MatchSetTest.java`.

---

### Task 7: FacetExecutor - terms over the projection, security, cross-bucket ordinal map

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/facet/FacetExecutor.java`, `TermsCollector.java`, `FacetExecutionStats.java`, `CrossBucketOrdinalMap.java`
- Modify: `engine/src/main/java/com/arcadedb/index/columnar/ColumnarSnapshot.java`, `ColumnarProjection.java` (`publishes`)
- Modify: `engine/src/main/java/com/arcadedb/index/columnar/ColumnarStats.java` (the four query counters and their readers)
- Test: `engine/src/test/java/com/arcadedb/query/facet/FacetExecutorTermsTest.java`, `engine/src/test/java/com/arcadedb/query/facet/CrossBucketOrdinalMapTest.java`

**Interfaces:**
- Consumes: plan 3 `ColumnarIndexManager.projection(bucketId)`, `ColumnarProjection.acquire()`, `ColumnarSnapshot.dictionary(property)/segments()/isUsable()/publishes(property)/pendingPositions()/oldestPendingEpochStartMs()` (`publishes` is added by this task to the plan 3 interface and `ColumnarProjection.Snapshot`) (never `ColumnarProjection.dictionary`, which answers the current generation, not the snapshot's), `ColumnarSegmentView.live()/column(property)`, `ColumnReader.kind()/forEachOrdinal/isPresent/longValue/doubleValue`; `SecurityHelper.canAccessFile(db, bucketId, ACCESS.READ_RECORD)` (SecurityHelper.java:80); `Schema.getTypeByBucketId` (Schema.java:233); Tasks 5, 6.
- Produces: `FacetResults FacetExecutor.execute(DatabaseInternal db, MatchSet matches, List<FacetSpec> facets, CommandContext ctx)`; `TermsCollector implements FacetCollector` (Task 9 adds `RangeCollector`, `HistogramCollector`); `static FacetCollector FacetExecutor.collectorFor(FacetSpec spec, DocumentType type, ColumnarProjection projection, long matchCount)` (a spec with no collector -> `CommandExecutionException("FACETS <alias>: <KIND> facets are not supported")`, plan 5 adds its collectors here); `FacetExecutionStats` per facet (`segmentsScanned`, `segmentsSkipped`, `countingMode` DENSE/HASH, `budgetBytes`, `source`) attached to `FacetResults` through a package-private getter for EXPLAIN/PROFILE.
  - `CrossBucketOrdinalMap` (roadmap "Dictionary scope change", spec 5.4; Lucene `OrdinalMap` style):

```java
public abstract class CrossBucketOrdinalMap {
  // per query: the cached map of (type, property) extended to every snapshot's dictionary(property).size();
  // a bucket whose `snapshot.dictionary(property)` is not the instance the map holds is rebuilt (after a compaction)
  public static CrossBucketOrdinalMap of(DatabaseInternal db, String typeName, String property, ColumnarSnapshot[] snapshots);
  public abstract int merged(int bucketId, int bucketOrdinal);   // dense merged ordinal
  public abstract Object value(int mergedOrdinal);
  public abstract int size();                                     // merged ordinals
  public abstract int mergedOf(Object value);                     // -1 when no bucket holds it (patched values)
  public abstract long version();                                 // changes whenever a bucket array is rebuilt; consumers caching per-ordinal state (plan 5) key on it
}
```

Rules: one snapshot (single-bucket type, or one readable bucket) returns `IdentityOrdinalMap` (no array, no hash: `merged == bucketOrdinal`, `value` delegates to `snapshot.dictionary(property)`, `version` = a per-map counter, bumped whenever the dictionary instance it delegates to changes; never a hash of the instance): the single-bucket bypass. Otherwise `MergedOrdinalMap`: merged ordinals assigned by value in first-seen order and never reassigned while the map lives; per bucket `(GlobalDictionary dictionary, int[] bucketToMerged)` grown incrementally when that bucket's dictionary grew (append-only, so only the new tail is looked up); after a compaction (new dictionary instance) only that bucket's array is rebuilt, values keep their merged ordinal, `version` increments. Values no bucket references any more keep their merged ordinal until the cache entry is dropped (schema change of the type, database close), so size stays proportional to cardinality times buckets, never to records. Cached per database in a `ConcurrentHashMap<(declaringType, property), MergedOrdinalMap>` held in a static `WeakHashMap<ColumnarIndexManager, ...>` (synchronized); one `addSchemaEventListener` per manager drops the type's entries; extension and rebuild happen under the entry's lock and publish a new immutable bucket array (copy-on-write), so a concurrent query keeps the arrays it read. A query whose snapshot holds another dictionary instance than the cached one (compaction raced the acquire) builds that bucket's array privately for the query, uncached. Polymorphic queries list every readable bucket of the type and its subtypes, so a subtype is just more buckets of the same map. Heap is charged to `FacetMemoryBudget` only for the private arrays.

Algorithm (terms): acquire the snapshot of every readable bucket of `matches.bucketIds()` (`canAccessFile` first) into one array inside a `try` whose `finally` closes each acquired snapshot in its own `try`; then `map = CrossBucketOrdinalMap.of(db, declaringType, property, snapshots)`; then for each bucket: per facet compute `m = forFacet(i, b).copy(); m.andNot(patchSet_b)` (empty patch set until Task 8); `collector.beginBucket(b, snapshot, map)`, then per segment `collector.collect(seg, m)`, which does `column.forEachOrdinal(m, counter)` (visits only live rows) counting `map.merged(bucketId, ordinal)` and missing += `m.andCardinality(seg.live()) - distinct positions seen`. One `TermsCollector` per (facet, declaring type) over that type's merged ordinals (dense `int[map.size()]` or hash, D12), as Task 1 defines; when a facet spans several declaring types their collectors are merged by value (FacetValueComparator) at `finish`; `finish` resolves only the top through `map.value`. A bucket with no projection, an `acquire()` that throws `IllegalStateException` (closed or dropped meanwhile), an unusable snapshot, `!snapshot.publishes(property)`, or a view whose `column(property)` is null is deferred to the fallback (Task 10; until then: `IllegalStateException` in this task, covered by Task 10's tests). Terms over a LONG/DOUBLE column (`LongTermsCounter`) iterate positions the same way (Task 9 rule); `forEachOrdinal` is only for ORDINAL/ORDINAL_LIST.

- [ ] **Step 1: Write the failing test** (extends `TestHelper`; schema and its COLUMNAR indexes (brand, tags BY ITEM, fileSize, publishDate, author) from `new ColumnarTestData(42).createSchema(database)` (with `tagVocabulary = 500`), data from `insertAssets(database, 5_000)`; wait for lag 0 with `FacetOracle.awaitCaughtUp(database)`; build the `MatchSet` from `SELECT @rid FROM Asset WHERE <w>` RIDs)

```java
@Test void termsEqualGroupByAtLagZero()         // for 5 WHERE clauses: FacetOracle.expectedCounts(db, "Asset", "brand", w) == execute(...).get("brand") buckets as map, with top = cardinality
@Test void multiValuedCountsDocuments()        // a record with tags [a, a, b] contributes a=1, b=1 (dedup at write time)
@Test void missingAndOtherCount()              // 7 records without brand -> missing 7; top 2 of 5 brands -> otherCount = sum of the other 3
@Test void topNIsDeterministicOnTies()         // brands with equal counts come value-ascending, identical across 3 runs
@Test void includeReturnsZeroCountValues()     // INCLUDE ['brand-1', 'Nobody'] -> exactly those two, brand-1 count > 0, Nobody count 0, otherCount = rest
@Test void missingBucketRanks()                // MISSING -> bucket value null with missing=true placed by its count
@Test void polymorphicTypesMergeByValue()      // Asset and subtype `Image EXTENDS Asset` (created before inserting; Asset's COLUMNAR index projects Image's buckets, each bucket with its own dictionary): merged counts equal GROUP BY over Asset (polymorphic), FacetOracle.expectedCounts(db, "Asset", "brand", w)
@Test void multiBucketTypeEqualsGroupBy()      // ColumnarTestData.buckets = 3 (CREATE TYPE ... BUCKETS 3): brand and tags counts for 5 WHERE clauses equal FacetOracle.expectedCounts; same value in all 3 buckets counted once per document
@Test void denseAndHashModesGiveSameCounts()   // selective WHERE (hash) and broad WHERE (dense) both equal the oracle; stats report the mode
```

`CrossBucketOrdinalMapTest` (extends `TestHelper`, `ColumnarTestData` with `buckets = 3` and `tagVocabulary = 500`, `FacetOracle.awaitCaughtUp` before each map is taken):

```java
@Test void singleBucketTypeBypassesTheMap()     // buckets = 1: of(...) is IdentityOrdinalMap, merged(b, o) == o, value(o) == snapshot.dictionary("brand").valueOf(o)
@Test void sameValueInThreeBucketsIsOneMergedOrdinal() // "Nike" in every bucket: merged(b1, o1) == merged(b2, o2) == merged(b3, o3); size() == distinct brands over the type
@Test void mapIsExtendedAfterNewValues()        // take map m1; insert assets with 20 new brands into bucket 2; awaitCaughtUp; m2 = of(...): same cached instance, size() grew by 20, old merged ordinals unchanged, version unchanged
@Test void mapIsRebuiltForACompactedBucket()    // delete every record of 600 brands in bucket 1, force plan 3 dictionary compaction there (`ColumnarIndexManager.compactDictionaryNow(bucketId, "brand")`, plan 3 Task 24); of(...): version increased, every surviving value keeps its merged ordinal, facet counts equal FacetOracle.expectedCounts
@Test void polymorphicSubtypeSharesTheMap()     // Image extends Asset: SELECT FROM Asset FACETS brand TERMS equals FacetOracle.expectedCounts(db, "Asset", "brand", null); Image values merged with Asset's
@Test void olderSnapshotGenerationGetsAPrivateArray() // acquire s1, compact, acquire s2 for another query: of(..., {s1, ...}) answers s1's ordinals correctly and the cached entry stays on s2's dictionary
```

- [ ] **Step 2: Run, expect FAIL** - `mvn -o -pl engine -am test -Dtest='FacetExecutorTermsTest,CrossBucketOrdinalMapTest' -Dsurefire.failIfNoSpecifiedTests=false`
- [ ] **Step 3: Implement** as above; counters reserved through `FacetMemoryBudget` per D13 (reservation closed when the facet is ranked).
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Stop: changes ready for review** - `FacetExecutor.java`, `TermsCollector.java`, `FacetExecutionStats.java`, `CrossBucketOrdinalMap.java`, both tests.

---

### Task 8: Patching, own-transaction writes, budget and exactness

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/facet/FacetPatcher.java`
- Modify: `FacetExecutor.java`
- Test: `engine/src/test/java/com/arcadedb/query/facet/FacetPatchingTest.java`

**Interfaces:**
- Consumes: `ColumnarValueExtractor.extract(Document, String, GlobalDictionary, ValueSink)`, `TransactionContext.getWrittenRecordIdsIn(IntHashSet)` and `hasChangesInFiles(IntHashSet)` (R11), `ColumnarIndexManager.suspendApply()` and `FacetOracle.awaitCaughtUp(Database)` (plan 3), `db.lookupByRID(rid, true)` (LocalDatabase.java:1171).
- Produces: `FacetPatcher.patchSet(MatchSet, ColumnarSnapshot, int bucketId): PositionBitmap` (D11); `long FacetPatcher.countPatched(DatabaseInternal db, MatchSet m, ColumnarSnapshot snapshot, PositionBitmap patch, List<FacetSpec> facets, List<FacetCollector> collectors)` (bucket from `snapshot.bucketId()`; Task 8 signature) (one record read serves every facet through `collectPatched(position, sink)`; a pending position with no record, i.e. deleted, is not a match and reaches no collector); `FacetResults.exact()/patched()/lagMs()` filled: `lagMs = max(now - oldestPendingEpochStartMs)` over buckets with a non-zero start, else 0.

- [ ] **Step 1: Write the failing test** (lagging cases run inside `try (AutoCloseable hold = ((LocalDatabase) ((DatabaseInternal) database).getEmbedded()).getColumnarIndexManager().suspendApply()) { ... }`; lag-0 cases start with `FacetOracle.awaitCaughtUp(database)`)

```java
@Test void lagZeroPatchesNothing()            { assertThat(run().patched()).isZero(); assertThat(run().exact()).isTrue(); assertThat(run().lagMs()).isZero(); }
@Test void whileLaggingFacetsEqualGroupBy()   // pause; with `deleteRatio = 0.2`, `reuseRatio = 0.1`, `bigRecordRatio = 0.02` set before `insertAssets`, `ColumnarTestData.mutate(database, 500)` (inserts, updates, deletes, slot reuse, chunked records); oracle equal; patched > 0; lagMs > 0; exact
@Test void overBudgetIsInexactAndFlagged()    // COLUMNAR_PATCH_BUDGET=5, 50 updates while paused: exact false, patched 0, lagMs > 0, `manager.getStats().queryPatchOverflows()` +1
@Test void ownTransactionWritesAreCounted()   // begin; update 3 records' brand to 'Mine'; facets inside tx count Mine=3 (oracle run in the same tx); rollback
@Test void ownTransactionOverBudgetScans()    // same with budget 1: source "scan", exact true, counts still equal
@Test void newRecordsInOpenTransaction()      // inserted in the open tx (real positions, written by the transaction) are counted
@Test void deletedBetweenMatchAndPatch()      // a matched position whose record is gone is not counted (no exception)
@Test void deletedPendingPositionIsNotAMatch()  // no-WHERE match set (Task 11 path, built here with MatchSet.fromProjection): a record deleted while suspended is not counted
@Test void multiBucketWhileLagging()           // ColumnarTestData.buckets = 3: suspended apply, 300 mutate ops across buckets -> equal to FacetOracle.expectedCounts, patched > 0
```

- [ ] **Step 2: Run, expect FAIL** - `mvn -o -pl engine -am test -Dtest=FacetPatchingTest -Dsurefire.failIfNoSpecifiedTests=false`
- [ ] **Step 3: Implement** per D10/D11; the patcher fills one `PatchedValue` per record and property with `extractor(typeName).extract(record, property, snapshot.dictionary(property), value)`, so its ordinals are the snapshot dictionary's; an ordinal at or beyond the size the map was extended to extends that bucket's array first (append-only); collectors map them through `CrossBucketOrdinalMap.merged`, and values with no ordinal (`unknown`) go to a side `Map<Object, long[]>` keyed by value (`mergedOf(value)` first, so a value another bucket holds joins its merged ordinal) merged at ranking.
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Stop: changes ready for review** - `FacetPatcher.java`, `FacetExecutor.java`, `FacetPatchingTest.java`.

---

### Task 9: Range and histogram execution

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/facet/RangeCollector.java`, `HistogramCollector.java` (both `implements FacetCollector`)
- Modify: `FacetExecutor.java` (`collectorFor`)
- Test: `engine/src/test/java/com/arcadedb/query/facet/FacetExecutorRangeHistogramTest.java`

**Interfaces:**
- Consumes: Tasks 4, 5; `ColumnarSegmentView.minLong/maxLong`; `ColumnarValueExtractor.toColumnLong` and `dateUnit` (contract change 4).
- Produces: range results (declaration order, then `DEFAULT`), histogram results (`key` per Task 4, `from`/`to` in epoch ms for dates, numbers for numeric FIXED). Histogram boundaries are built from BOUNDS, else from segment `minLong/maxLong` of buckets with matches plus patched values; if that range exceeds 100,000 buckets a precise first pass computes min/max of the matched values before giving up (D18).

- [ ] **Step 1: Write the failing tests** (`data.timeOfDay = true` for every histogram test, so values straddle zone boundaries; `datePrecisionMicros` creates its own type with a `DATETIME_MICROS` COLUMNAR property)

```java
@Test void rangesEqualGroupByOfCaseExpression()   // oracle: SELECT (CASE WHEN fileSize < 1048576 THEN 'small' ...) AS k, count(*) ... GROUP BY k
@Test void overlappingRangesCountTwice()
@Test void defaultBucketAndMissing()              // values in no range -> DEFAULT; no value -> missing
@Test void segmentSkippedWhenInsideOneInterval()  // stats.segmentsSkipped > 0, counts unchanged
@Test void monthHistogramEqualsZonedGroupBy()     // publishDate CALENDAR '1M' TIMEZONE 'Europe/Rome' vs GROUP BY publishDate.format('yyyy-MM', 'Europe/Rome')
@Test void minCountZeroFillsGaps()                // BOUNDS + MIN_COUNT 0 -> every month present, empty ones count 0
@Test void fixedHistogramAlignsWithTimeBucket()   // FIXED '1h' TIMEZONE '+08:00' vs GROUP BY ts.timeBucket('1h', publishDate, {timezone:'+08:00'})
@Test void datePrecisionMicros()                  // property DATETIME_MICROS: bounds as strings convert through toColumnLong, keys equal the ms case
@Test void rangesWhileLaggingStayExact()          // paused apply + updates of fileSize: still equal to oracle
```

- [ ] **Step 2: Run, expect FAIL** - `mvn -o -pl engine -am test -Dtest=FacetExecutorRangeHistogramTest -Dsurefire.failIfNoSpecifiedTests=false`
- [ ] **Step 3: Implement**: per segment, skip a view with `column(property) == null` (its positions are not live there); `t = matches.copy(); t.and(segment.live())`; `t.forEach(p -> isPresent(p) ? longValue(p)/doubleValue(p) : missing++)`; DOUBLE `minLong/maxLong` decoded with `Double.longBitsToDouble`, `Long.MAX_VALUE`/`MIN_VALUE` meaning an empty column (skip)
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Stop: changes ready for review** - `RangeCollector.java`, `HistogramCollector.java`, `FacetExecutor.java`, test.

---

### Task 10: Fallback scan, per-database cap, unavailable facets

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/facet/FacetFallbackScanner.java`
- Modify: `FacetExecutor.java`
- Modify: `engine/src/main/java/com/arcadedb/index/columnar/ColumnarIndexManager.java` (`fallbackScans` sized `database.getConfiguration().getValueAsInteger(GlobalConfiguration.COLUMNAR_MAX_FALLBACK_SCANS)` at construction, replacing `FALLBACK_SCANS` at :165/:219)
- Test: `engine/src/test/java/com/arcadedb/query/facet/FacetFallbackTest.java`

**Interfaces:**
- Consumes: `ColumnarIndexManager.fallbackScans()` (contract change 5).
- Produces: `FacetFallbackScanner.scan(DatabaseInternal db, MatchSet m, int bucketId, List<FacetSpec> facets, ScanSink sink)`: reads each matched record once, for a property that is a column of the record's type: `extractor(typeName).extract(record, property, EMPTY_DICTIONARY, sink)` with one shared `GlobalDictionary.createInMemory()` never grown (every value arrives through `ValueSink.unknownValue`, list items once per distinct value); for any other property the raw `Document.get(property)` (or `expression.execute(record, ctx)` for expression facets); lists count each distinct element once per document.

- [ ] **Step 1: Write the failing tests** (`FacetOracle.expectedCounts` needs a COLUMNAR column: unindexed and expression facets are compared with a test-local `groupBy(sql)` helper)

```java
@Test void unindexedPropertyIsScanned()       // data.columnarIndexes = false, then CREATE INDEX ON Asset (brand) COLUMNAR only; FACETS author TERMS: source "scan", equals SELECT author AS v, count(*) FROM Asset GROUP BY author (records without author as missing), exact true
@Test void expressionFacetIsScanned()         // x: brand.toLowerCase() TERMS -> source "scan", equals GROUP BY brand.toLowerCase()
@Test void rebuildingProjectionIsScanned()    // `ColumnarTestHooks.killPoint = name -> { if ("rebuild.scanned".equals(name)) hold.await(); }`, then DROP and CREATE the brand index: while the create rebuild is held, source "scan" and counts equal the oracle; `finally` clears the hook, releases the latch, then `FacetOracle.awaitCaughtUp`
@Test void capReachedIsUnavailable()          // hold all fallbackScans() permits in the test: source "unavailable", reason "fallback scans capped", no buckets; `manager.getStats().queryFallbackScans()` not incremented for the refused one
@Test void mixedIndexAndScanIsScan()          // type `Base` (no index) and `Leaf EXTENDS Base` with `CREATE INDEX ON Leaf (brand) COLUMNAR`: `SELECT FROM Base FACETS brand TERMS` reads Leaf's buckets from the projection and scans Base's own bucket: source "scan", counts equal the plain GROUP BY
@Test void memoryBudgetRefusalIsUnavailable() // `COLUMNAR_QUERY_MEMORY_BUDGET` = 1024 (restored in finally): reason "memory budget"; rows of the query still complete (asserted in Task 11)
@Test void capFollowsTheSetting()             // database opened with maxFallbackScans 1: fallbackScans().availablePermits() == 1
@Test void unpublishedColumnIsScanned()       // usable projection with brand published; CREATE INDEX ON Asset (fileSize) COLUMNAR on existing data while the create rebuild is held: FACETS brand TERMS source "index", s: fileSize RANGES (...) source "scan", both equal GROUP BY
```

- [ ] **Step 2: Run, expect FAIL** - `mvn -o -pl engine -am test -Dtest=FacetFallbackTest -Dsurefire.failIfNoSpecifiedTests=false`
- [ ] **Step 3: Implement** per D14.
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Stop: changes ready for review** - `FacetFallbackScanner.java`, `FacetExecutor.java`, test.

---

### Task 11: SQL wiring - FacetCollectStep, planner, ResultSet.getFacets, full-text, vector, EXPLAIN/PROFILE

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/executor/FacetCollectStep.java`, `engine/src/main/java/com/arcadedb/query/sql/executor/IndexedMatchSource.java`
- Modify: `SelectExecutionPlanner.java` (`init` 179-216, `createExecutionPlan` 253-336, `handleHardwiredOptimizations` 572, the eight `statement.getLimit()` fetch reads listed in D6), `SelectExecutionPlan.java`, `InternalExecutionPlan.java`, `ResultSet.java:64`, `LocalResultSet.java`, `InternalResultSet.java`, `IteratorResultSet.java`, `ProfileStatement.java:62-71, 85-93`, `FetchFromIndexedFunctionStep.java`, `engine/src/main/java/com/arcadedb/query/sql/executor/QueryPlanningInfo.java` (field `List<FacetSpec> facets`, copied by `copy()` at :90)
- Modify (test fixture): `engine/src/test/java/com/arcadedb/index/columnar/FacetOracle.java` (add `assertFacetsEqualGroupBy`)
- Test: `engine/src/test/java/com/arcadedb/query/facet/SQLFacetsIntegrationTest.java`, `engine/src/test/java/com/arcadedb/query/facet/FacetsExplainTest.java`

**Interfaces:**
- Consumes: Tasks 3, 6-10.
- Produces:
  - `ResultSet.getFacets()` default null; `InternalExecutionPlan.getFacets()` default null; `SelectExecutionPlan.getFacets()` returns its `FacetCollectStep.facets()` (the step is found in `steps` after `copy()` too); `LocalResultSet.getFacets()` delegates to the plan; `InternalResultSet.setFacets(FacetResults)`/`getFacets()`; `IteratorResultSet` same pair.
  - `FacetCollectStep(List<FacetSpec> specs, int[] filterIndexByFacet, List<FacetFilterEvaluator> evaluators, CommandContext ctx)`; `FacetResults facets()` (drains and computes once, memoized; null when the plan was never executed); `canBeCached()` true, `copy(CommandContext)` deep-copies specs. `close()` closes the `MatchSet` (reservation, snapshots held by `fromProjection`) and every evaluator, whether or not `facets()` ran.
  - `interface IndexedMatchSource { boolean collectAllMatches(CommandContext ctx, MatchSet.Builder builder); }` implemented by `FetchFromIndexedFunctionStep` when its function is the whole WHERE (no residual `FilterStep` between it and the collect step) and no facet carries a FILTER: it adds the complete search's RID keys without loading records; the step then forwards rows without recording them.
  - `FacetOracle.assertFacetsEqualGroupBy(Database db, String type, String property, String where)`: runs `SELECT FROM <type> WHERE <where> FACETS f: <property> TERMS TOP <topMax>` and compares with `expectedCounts`; it first asserts `expectedCounts(...)` has at most `COLUMNAR_TOP_MAX` values (a clear failure instead of a truncated comparison) and maps the result's `missing` member to `FacetOracle.MISSING`.
- Planner changes (exact): in `init`, `info.facets = FacetSpecCompiler.compile(statement.getFacets(), context)`; if any `FacetItem` bound, interval or INCLUDE holds an `InputParameter`, `planDependsOnInputParameters = true`; if non-null, `deferredSkip = info.skip; deferredLimit = info.limit; info.skip = null; info.limit = null`. A private `Limit fetchLimit()` returns `info.facets != null ? null : statement.getLimit()` and replaces the eight `statement.getLimit()` reads. `handleHardwiredOptimizations` and `rewriteRangeMinMaxAsOrderedFetch` return early when `info.facets != null`. After `buildExecutionPlan(selectExecutionPlan, info)` (line 320) and before `handleProjectionsBlock` (line 322): `if (info.facets != null) { selectExecutionPlan.chain(new FacetCollectStep(...)); info.skip = deferredSkip; info.limit = deferredLimit; }`; in `foldEmptyByConstruction` (:373-390) the same three statements run before its `handleProjectionsBlock` call. The `init` switch runs after `rewriteDistinctAsGroupBy(context)` (so that decision still sees the LIMIT). `foldEmptyByConstruction` with facets still chains the step (empty facets, `exact` true). With no `WHERE` (after `init` drops an always-true one), no FILTER, a type or bucket target and no change of the caller's open transaction on those buckets (`TransactionContext.hasChangesInFiles`), the step takes its match set from `MatchSet.fromProjection` at open and forwards rows without recording them (match source `projection`); buckets without a usable projection are recorded from the rows. With FILTERs and no WHERE the same projection match set is used and each position gets its mask by RID (D8 evaluators). A target that is neither a type, a bucket list nor a subquery (`fromRids`, `index:`, `schema:`, function call) raises `CommandSQLParsingException("FACETS needs a type, a bucket or a subquery as target")`.
- `ProfileStatement`: after draining `rs`, call `rs.getFacets()` so a LIMIT downstream does not leave facets uncomputed.

- [ ] **Step 1: Write the failing tests**

`SQLFacetsIntegrationTest` (extends `TestHelper`, data from `ColumnarTestData(42)` with `tagVocabulary = 500`, 20,000 assets; full-text tests add `title STRING` filled from a seeded word list (`shoe` in about 5% of records) with a FULL_TEXT index; the vector test adds `embedding ARRAY_OF_FLOATS` (seeded, dimension 8) with an LSM vector index and is tagged `@Tag("vector")` if it waits on a rebuild):

```java
@Test void facetsEqualGroupByAtLagZero()      // FacetOracle.assertFacetsEqualGroupBy for brand and tags over 6 WHERE clauses (none, equality, range, OR, IN, LIKE)
@Test void facetsEqualGroupByWhileLagging()   // inside suspendApply(), with `deleteRatio = 0.2`, `reuseRatio = 0.1`, `bigRecordRatio = 0.02` set before `insertAssets`, mutate 300 ops, same assertions, meta exact true, patched > 0
@Test void limitDoesNotShrinkFacets()         // "... FACETS brand TERMS LIMIT 5": 5 rows; facet counts sum == count of WHERE matches with a brand
@Test void skipLimitAndOrderByKeepRows()      // rows of "WHERE w ORDER BY fileSize DESC SKIP 3 LIMIT 7 FACETS ..." == rows of the same query without FACETS
@Test void facetsComputedOnlyWhenAskedAfterPartialRead() // read 2 rows, getFacets() -> complete counts; close without getFacets() does not throw; after close, `FacetMemoryBudget.instance().available()` equals its capacity, and `DROP INDEX` + `CREATE INDEX ON Asset (brand) COLUMNAR` + `FacetOracle.awaitCaughtUp` succeeds (no leaked snapshot)
@Test void fullTextFacetsCoverEveryMatch()    // FULL_TEXT index on title; "WHERE search_fields(['title'], 'shoe') LIMIT 3 FACETS brand TERMS": facet sum == SELECT count(*) WHERE search_fields(...)
@Test void fullTextFastPathLoadsNoRecordForDrain() // stats/profile shows match source "index" and counts still equal (IndexedMatchSource)
@Test void vectorFacetsCoverReturnedNeighbors()   // SELECT FROM (SELECT expand(vector.neighbors('Asset[embedding]', ?, 25))) FACETS brand TERMS: sum of counts + missing == 25
@Test void rowsWithoutIdentityAreRefused()    // SELECT FROM (SELECT 1 AS x) FACETS x TERMS -> "FACETS needs rows that are records"
@Test void cachedPlanReusedGivesFreshFacets() // run the same text twice with data changed in between: second result reflects the change
@Test void cachedPlanWithOtherParametersGivesFreshBounds() // same text with :lo = 0 then :lo = 1048576 in RANGES ([:lo, *)): second count reflects the second bound
@Test void noWhereBuildsMatchSetFromProjection() // "SELECT FROM Asset FACETS brand TERMS LIMIT 2": PROFILE shows match source "projection" and no record read for the drain; counts equal the oracle with WHERE true, also while lagging (deleted pending positions not counted)
@Test void noWhereInOwnTransactionRecordsRows()  // open tx that inserted 3 assets: match source "rows", the 3 are counted
@Test void budgetRefusalKeepsRows()           // `COLUMNAR_QUERY_MEMORY_BUDGET` = 1024 (restored in finally): all rows returned, every facet unavailable "memory budget"
```

`FacetsExplainTest`:

```java
@Test void explainShowsFacetSteps()   // EXPLAIN text contains "+ COLLECT FACET MATCHES" and "brand TERMS TOP 10" and the filter evaluator ("INDEX"/"PROJECTION"/"RECORD")
@Test void profileShowsExecution()    // PROFILE text contains "segments=", "skipped=", "patched=", "counting=DENSE|HASH", "budget=", "source=index"; toResult() has a "facets" property
@Test void profileWithLimitStillComputesFacets()
```

- [ ] **Step 2: Run, expect FAIL** - `mvn -o -pl engine -am test -Dtest='SQLFacetsIntegrationTest,FacetsExplainTest' -Dsurefire.failIfNoSpecifiedTests=false`
- [ ] **Step 3: Implement** per D6/D7. `FacetCollectStep.syncPull` follows `FilterStep`'s shape (FilterStep.java, `copy(CommandContext)` at :151), checks `WorkGuard.forCommandDeadline(context)` per row, records `result.getIdentity()`, forwards only mask-0 rows (filters are a no-op mask until Task 12: evaluators list empty). `prettyPrint` prints `+ COLLECT FACET MATCHES` then one indented line per facet (`FacetItem.toString()` plus evaluator kind), and with profiling the `FacetExecutionStats` and `meta`. `ParallelRowPipeline.of` (ParallelRowPipeline.java:48-72) stops at the unknown step, so aggregations above it stay sequential: intended, add a one-line comment in `FacetCollectStep` saying so.
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Regression run of the planner suites**

Run: `mvn -o -pl engine -am test -Dtest='com/arcadedb/query/sql/executor/*Test,com/arcadedb/function/sql/text/*Test,com/arcadedb/query/sql/*Test' -Dsurefire.failIfNoSpecifiedTests=false -DexcludedGroups=benchmark,slow,vector`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Stop: changes ready for review** - files listed above plus the two tests.

---

### Task 12: Disjunctive FILTER (one pass, post-filter rows)

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/facet/FacetFilterEvaluator.java`
- Modify: `FacetCollectStep.java`, `SelectExecutionPlanner.java` (evaluator choice at planning)
- Test: `engine/src/test/java/com/arcadedb/query/facet/FacetFilterEvaluatorTest.java`, `engine/src/test/java/com/arcadedb/query/facet/DisjunctiveFacetsTest.java`

**Interfaces:**
- Produces: `abstract sealed class FacetFilterEvaluator permits Index, Projection, Record` with `static FacetFilterEvaluator choose(WhereClause filter, DocumentType targetType, DatabaseInternal db)`; `void open(CommandContext ctx)` (INDEX looks its RID set up here into a `RidHashSet`, reused from `com.arcadedb.utility`); `boolean passes(RID rid, Result row, CommandContext ctx)`; `String kind()`; `close()` (closes every snapshot the PROJECTION evaluator acquired).

- [ ] **Step 1: Write the failing tests**

```java
// FacetFilterEvaluatorTest
@Test void choosesIndexForLsmEquality()      // LSM index on brand: "brand IN ['a','b']" -> INDEX
@Test void choosesProjectionForColumnarRange()// no LSM, COLUMNAR on fileSize: "fileSize >= 10 AND fileSize < 20" -> PROJECTION
@Test void fallsBackToRecord()               // "brand.toLowerCase() = 'brand-1'" -> RECORD
@Test void projectionPendingPositionReadsRecord() // paused apply, update fileSize: PROJECTION evaluator answers from the record
// DisjunctiveFacetsTest  (oracle = separate GROUP BY queries)
@Test void eachFacetCountsAllFiltersButItsOwn() {
  final String w = "publishDate >= '2024-01-01'";
  final ResultSet rs = db.query("sql", "SELECT FROM Asset WHERE " + w + " FACETS "
      + "brand TERMS TOP 1000 FILTER brand IN ['brand-1', 'brand-2'], "
      + "size: fileSize RANGES ([0, 1048576) AS 'small', [1048576, *) AS 'big') FILTER fileSize < 1048576, "
      + "tags TERMS TOP 1000");
  final long rows = rs.stream().count();
  final FacetResults f = rs.getFacets();
  assertThat(asMap(f.get("brand"))).isEqualTo(groupBy("brand", w + " AND fileSize < 1048576"));
  assertThat(asMap(f.get("size"))).isEqualTo(rangeGroupBy(w + " AND brand IN ['brand-1', 'brand-2']"));
  assertThat(asMap(f.get("tags"))).isEqualTo(unwindGroupBy("tags", w + " AND brand IN ['brand-1', 'brand-2'] AND fileSize < 1048576"));
  assertThat(rows).isEqualTo(count(w + " AND brand IN ['brand-1', 'brand-2'] AND fileSize < 1048576"));
}
@Test void sameResultsForEveryEvaluatorKind()   // the test above with LSM index on brand, then without (PROJECTION), then with expression filters (RECORD): identical facets
@Test void whileLaggingStillEquivalent()       // paused apply + mutations (with `deleteRatio = 0.2`, `reuseRatio = 0.1`, `bigRecordRatio = 0.02` set before `insertAssets`)
@Test void filterWithoutOtherFacetsFiltersRows()
```

- [ ] **Step 2: Run, expect FAIL** - `mvn -o -pl engine -am test -Dtest='FacetFilterEvaluatorTest,DisjunctiveFacetsTest' -Dsurefire.failIfNoSpecifiedTests=false`
- [ ] **Step 3: Implement** per D8: mask bit `i` set when filter `i` fails; the step forwards a row only when the mask is 0; drained matches get their mask the same way (by RID; RECORD loads). A PROJECTION evaluator answers from the record for a bucket whose snapshot does not `publishes(property)`. INDEX is chosen only for an index of type `Schema.INDEX_TYPE.LSM_TREE` (COLUMNAR is returned by the plural property lookups since plan 3 ruling 121) on the target type or a supertype.
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Stop: changes ready for review** - `FacetFilterEvaluator.java`, `FacetCollectStep.java`, `SelectExecutionPlanner.java`, two tests.

---

### Task 13: Metrics

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/monitor/ColumnarMetrics.java` (bind the four counters in `bindNewSeries`); `FacetExecutor.java`, `FacetPatcher.java`, `FacetMemoryBudget.java` (waits reported to the caller), `FacetFallbackScanner.java` (increments)
- Test: `engine/src/test/java/com/arcadedb/query/facet/FacetQueryStatsTest.java` (asserts `ColumnarStats`), `metrics/src/test/java/com/arcadedb/metrics/prometheus/PrometheusColumnarQueryMetricsIT.java` (pattern of `PrometheusColumnarMetricsIT`)

**Interfaces:**
- Produces: the four counters on `ColumnarStats`; meters as D21.

- [ ] **Step 1: Write the failing tests**

```java
// FacetQueryStatsTest (asserts `manager.getStats()`)
@Test void countersMoveWithTheEvents()   // a patched query adds queryPatched(); an over-budget one adds queryPatchOverflows(); a capped scan does not add queryFallbackScans() but a run one does
// PrometheusColumnarQueryMetricsIT (metrics module, fixture as PrometheusColumnarMetricsIT)
@Test void prometheusExposesTaggedCounters() {
  // create COLUMNAR index, run one FACETS query on an unindexed property (fallback scan)
  final String body = httpGet(getServerHttpUrl(0, "/prometheus"));
  assertThat(body).contains("arcadedb_columnar_query_fallback_scans_total{database=\"" + getDatabaseName() + "\"");
  assertThat(Metrics.globalRegistry.find("arcadedb.columnar.query.patched").tag("database", getDatabaseName()).functionCounter()).isNotNull();
}
```

- [ ] **Step 2: Run, expect FAIL** - `mvn -o -pl engine -am test -Dtest=FacetQueryStatsTest -Dsurefire.failIfNoSpecifiedTests=false` and `mvn -o -pl metrics -am verify -DskipTests -Pintegration -Dit.test=PrometheusColumnarQueryMetricsIT -Dfailsafe.failIfNoSpecifiedTests=false`
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run both, expect PASS**; also run `PrometheusColumnarMetricsIT`, `EngineMetricsBinderTest`, `EngineMetricsExposedIT`.
- [ ] **Step 5: Stop: changes ready for review** - 0 created, 5 modified, 2 tests.

---

### Task 14: HTTP JSON, ndjson, handler limits, RemoteDatabase, Raft leader answer

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/AbstractQueryHandler.java` (add `protected static void reportFacets(JSONObject response, ResultSet qResult)`; in `streamResultSetAsNdJson` after the row loop and before `writeStats` at :387: `final FacetResults f = qResult != null ? qResult.getFacets() : null; if (f != null) stream.writeFacets(f.toJSON());` inside the same try so a failure becomes the in-band error line)
- Modify: `NdJsonResultStream.java` (`public void writeFacets(final JSONObject facets)` delegates to `writeEvent("facets", facets, false)` (:153) and writes `{"facets": ...}` with `forceFlush=false`; wire-format javadoc lists the line between records and `stats`)
- Modify: `PostCommandHandler.java` (call `reportFacets(response, qResult)` right after `serializeResultSetBounded` at :405, before `qResult.close()`; `materializeResultSet` (:482-501) copies `source.getFacets()` after the row loop into `materialized.setFacets(...)` - computed before `source.close()`)
- Modify: `GetQueryHandler.java:131-149` (same `reportFacets`)
- Modify: `server/src/main/java/com/arcadedb/server/monitor/ProfilingResultSet.java:85` (`getFacets()` delegates)
- Modify: `network/src/main/java/com/arcadedb/remote/RemoteDatabase.java:1696-1716` (`if (response.has("facets")) ((InternalResultSet) resultSet).setFacets(FacetResults.fromJSON(response.getJSONObject("facets")))`)
- Modify: `network/src/main/java/com/arcadedb/remote/RemoteStreamingResultSet.java:139-170` (a `facets` event is stored, `getFacets()` returns it once the stream has been consumed; reading `getFacets()` before the end drains the remaining rows into a small buffer is NOT done: it returns null until the trailer, documented in javadoc) (the event loop throws on an unrecognized event, :162: the `facets` branch stores it and reads the next line)
- Modify: `ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftReplicatedDatabase.java:5471-5490` (`parseResultSetFromJson` sets facets when present)
- Test: `server/src/test/java/com/arcadedb/server/http/FacetsHttpIT.java`, `server/src/test/java/com/arcadedb/server/http/handler/NdJsonResultStreamFacetsTest.java`, `server/src/test/java/com/arcadedb/remote/RemoteDatabaseFacetsIT.java`, `ha-raft/src/test/java/com/arcadedb/server/ha/raft/ParseResultSetFromJsonFacetsTest.java`

**Interfaces:**
- Consumes: `FacetResults.toJSON/fromJSON`, `InternalResultSet.setFacets`.
- Produces: HTTP envelope member `"facets"` (absent when the query has no FACETS); ndjson line `{"facets": {...}}`.

- [ ] **Step 1: Write the failing tests** (`FacetsHttpIT extends BaseGraphServerTest`, `getServerCount() = 1`; schema + 2,000 assets created through `getServerDatabase(0, getDatabaseName())` in `populateDatabase`; HTTP via `getServerHttpUrl(0, "/api/v1/command/" + getDatabaseName())`)

```java
@Test void bufferedResponseCarriesFacets() {
  final JSONObject r = post("SELECT FROM Asset WHERE fileSize > 0 FACETS brand TERMS TOP 5 LIMIT 3");
  assertThat(r.getJSONArray("result").length()).isEqualTo(3);
  final JSONObject f = r.getJSONObject("facets");
  assertThat(f.getJSONObject("meta").getBoolean("exact")).isTrue();
  assertThat(f.getJSONObject("brand").toString()).isEqualTo(embeddedFacets(sameSql).getJSONObject("brand").toString()); // == FacetResults.toJSON() embedded
}
@Test void autoLimitDoesNotShrinkFacets()   // no LIMIT, request limit 10: returned 10, truncated true, facet sum == 2000 - missing
@Test void requestLimitAndPlanLimit()       // "limit" request field 2 with query LIMIT 5: rows 2, facets complete
@Test void ndjsonFacetsLineBeforeStats()    // Accept: application/x-ndjson; lines: N records, then {"facets":...}, then {"stats":...} last
@Test void detailedProfileKeepsFacets()     // profileExecution "detailed": facets present and equal
@Test void getQueryEndpointCarriesFacets()  // GET /api/v1/query/<db>/sql/<urlencoded>
@Test void noFacetsMemberWithoutClause()
@Test void parseErrorsAre400()              // FACETS after LIMIT -> 400 with "FACETS must come before SKIP and LIMIT"
@Test void graphSerializerKeepsFacets()     // serializer "graph": facets member still present
```

`NdJsonResultStreamFacetsTest`: `writeRecord`, `writeFacets`, `writeStats` produce three lines in that order, each a one-key object.

`RemoteDatabaseFacetsIT extends BaseGraphServerTest`:

```java
@Test void remoteQueryReturnsFacets()        // new RemoteDatabase(host, getServerHttpPort(0), db, "root", DEFAULT_PASSWORD_FOR_TESTS).query(...).getFacets().toJSON() equals embedded toJSON()
@Test void remoteCommandReturnsFacets()
@Test void remoteStreamingReturnsFacetsAfterRows() // the streaming query API: getFacets() null before the end, equal after consuming all rows
```

`ParseResultSetFromJsonFacetsTest`: `RaftReplicatedDatabase.parseResultSetFromJson("{\"result\":[],\"facets\":" + sample + "}").getFacets().toJSON()` equals sample.

- [ ] **Step 2: Run, expect FAIL**

Run: `mvn -o -pl server -am test -Dtest=NdJsonResultStreamFacetsTest -Dsurefire.failIfNoSpecifiedTests=false` then `mvn -o -pl server -am verify -DskipTests -Pintegration -Dit.test='FacetsHttpIT,RemoteDatabaseFacetsIT' -Dfailsafe.failIfNoSpecifiedTests=false` then `mvn -o -pl ha-raft -am test -Dtest=ParseResultSetFromJsonFacetsTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL.

- [ ] **Step 3: Implement** as listed under Files.
- [ ] **Step 4: Run, expect PASS** (same commands).
- [ ] **Step 5: Regression run of the handler and driver suites**

Run: `mvn -o -pl server -am verify -DskipTests -Pintegration -Dit.test='*NdJson*IT,*PostCommand*IT,*QueryHandler*IT,RemoteDatabase*IT' -Dfailsafe.failIfNoSpecifiedTests=false` and `mvn -o -pl server -am test -Dtest='*NdJson*Test,PostCommandHandler*Test' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: BUILD SUCCESS.

- [ ] **Step 6: Stop: changes ready for review** - files listed above, four tests.

---

### Task 15: OpenAPI

**Files:**
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/openapi/CoreApiSpec.java` (`addSchemas` block :196-214, `createQueryResponseSchema` :995, `createNdJsonQueryEventSchema` :1060)
- Test: `server/src/test/java/com/arcadedb/server/http/handler/openapi/FacetsApiSpecTest.java`

**Interfaces:**
- Produces: component schemas `Facets` (object: `meta` -> `FacetsMeta`, `additionalProperties` -> `FacetResult`), `FacetsMeta` (required `exact`, `patched`, `lagMs`), `FacetResult` (required `type`, `source`; `type` enum `terms|range|histogram|hierarchy|distinct|stats`; `source` enum `index|scan|unavailable`; optional `reason`, `buckets` (array of `FacetBucket`), `otherCount`, `missing`, `after`, and plan 5's members `value`, `exact` (DISTINCT) and `count`, `min`, `max`, `sum`, `avg` (STATS)), `FacetBucket` (required `count`; optional `value`, `missing`, `key`, `from`, `to`, `default`, and plan 5's hierarchy attributes `parent`, `depth`, `orphan`); `QueryResponse.properties.facets` = `$ref Facets` (not required); `NdJsonQueryEvent` gains a `facets` variant.

- [ ] **Step 1: Write the failing test** (build the spec as `CoreApiSpecTest` does)

```java
@Test void queryResponseReferencesFacets()     { assertThat(schema("QueryResponse").getProperties().get("facets").get$ref()).endsWith("/Facets"); }
@Test void facetResultSourceEnumMatchesServer() { assertThat(schema("FacetResult").getProperties().get("source").getEnum()).containsExactly("index", "scan", "unavailable"); }
@Test void ndjsonEventHasFacetsLine()
@Test void serverResponseValidatesAgainstSchema() // the JSON of `FacetResultsSamples.allKinds()` and `plan5Members()`: every member is declared, every required member present
```

- [ ] **Step 2: Run, expect FAIL** - `mvn -o -pl server -am test -Dtest='FacetsApiSpecTest,CoreApiSpecTest,Issue7577NoBareObjectSurvivesInAnyContributorTest,Issue7578EveryComponentSchemaDeclaresRequiredTest,Issue7579EnumsMatchWhatTheServerAcceptsTest' -Dsurefire.failIfNoSpecifiedTests=false`
- [ ] **Step 3: Implement**; descriptions state that facets cover the full match set, not the LIMIT page, and that `exact=false` means the patch budget was exceeded.
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Stop: changes ready for review** - `CoreApiSpec.java`, `FacetsApiSpecTest.java`.

---

### Task 16: gRPC proto, service, RemoteGrpcDatabase

**Files:**
- Modify: `grpc/src/main/proto/arcadedb-server.proto`
- Create: `grpc/src/main/java/com/arcadedb/server/grpc/FacetProtocol.java`
- Modify: `grpcw/src/main/java/com/arcadedb/server/grpc/ArcadeDbGrpcService.java` (`executeCommand` response builder :900-930, `executeQuery` :1606 (builder :1782), `streamCursor` :2412-2480, `streamMaterialized` :2485, `streamPaged` :2555 via `hasOrderBy`'s parse at :2706)
- Modify: `grpc-client/src/main/java/com/arcadedb/remote/grpc/RemoteGrpcDatabase.java` (`createGrpcResultSet` for both responses), `StreamingResultSet.java`, `BatchedStreamingResultSet.java`
- Modify: `grpc-client/pom.xml` (test dependency `arcadedb-engine` `test-jar`)
- Test: `grpc-client/src/test/java/com/arcadedb/remote/grpc/FacetProtocolTest.java`, `grpc-client/src/test/java/com/arcadedb/remote/grpc/FacetsGrpcIT.java`

**Interfaces:**
- Produces (proto):

```proto
message GrpcFacetBucket { GrpcValue value = 1; string key = 2; GrpcValue from = 3; GrpcValue to = 4; int64 count = 5; bool missing = 6; bool is_default = 7; string attributes_json = 8; }
message GrpcFacetResult { string type = 1; string source = 2; string reason = 3; repeated GrpcFacetBucket buckets = 4; int64 other_count = 5; int64 missing = 6; bool has_counts = 7; string extensions_json = 8; }
message GrpcFacets { bool exact = 1; int64 patched = 2; int64 lag_ms = 3; repeated string aliases = 4; map<string, GrpcFacetResult> results = 5; }
// QueryResult: GrpcFacets facets = 5;  ExecuteCommandResponse: GrpcFacets facets = 7;  ExecuteQueryResponse: GrpcFacets facets = 4;
```

  (`aliases` keeps declaration order, which a proto map does not; `has_counts` false for `unavailable`, so `fromProto(toProto(x)).toJSON()` equals `x.toJSON()` including the absent members. `attributes_json` carries `FacetBucket.attributes()` and `extensions_json` the plan 5 members of a result (`after`, the DISTINCT `value`/`exact`, the STATS members) as JSON text, empty when absent, so plan 5 adds no proto field.)
- `FacetProtocol.toProto(FacetResults): GrpcFacets`, `FacetProtocol.fromProto(GrpcFacets): FacetResults` (values through a private converter in `FacetProtocol` for the scalar domain `FacetResults.fromJSON` yields: null = unset `GrpcValue`, String, Long as `int64_value`, Double, Boolean; `GrpcTypeConverter` (grpcw) and `ProtoUtils` (grpc-client) are not visible from the `grpc` module).

- [ ] **Step 1: Write the failing tests**

```java
// FacetProtocolTest
@Test void roundTripEqualsJson() { assertThat(FacetProtocol.fromProto(FacetProtocol.toProto(sample)).toJSON().toString()).isEqualTo(sample.toJSON().toString()); }
@Test void plan5MembersRoundTrip()   // `FacetResultsSamples.plan5Members()` (attributes, distinct, stats, after): same identity
// FacetsGrpcIT extends BaseGrpcClientServerTest (gRPC port from getServerGrpcPort())
@Test void unaryQueryCarriesFacets()        // RemoteGrpcDatabase.query(...).getFacets().toJSON() == embedded
@Test void commandCarriesFacets()
@Test void cursorStreamFacetsOnLastBatch()  // queryStream with batch 7 over 100 rows: facets present only in the is_last_batch message; client getFacets() after the last row equals embedded
@Test void cursorStreamWithZeroRowsStillSendsFacets() // WHERE matching nothing: one empty terminal batch with facets (all zero, exact)
@Test void materializeAllCarriesFacets()
@Test void pagedIsRefused()                 // RetrievalMode.PAGED with FACETS -> StatusRuntimeException INVALID_ARGUMENT, message "PAGED retrieval cannot carry FACETS: use CURSOR or MATERIALIZE_ALL"
@Test void maxRowsDoesNotShrinkFacets()     // ExecuteQueryRequest limit 3: 3 records, facets complete
```

- [ ] **Step 2: Run, expect FAIL** - `mvn -o -pl grpc-client -am test -Dtest=FacetProtocolTest -Dsurefire.failIfNoSpecifiedTests=false` and `mvn -o -pl grpc-client -am verify -DskipTests -Pintegration -Dit.test=FacetsGrpcIT -Dfailsafe.failIfNoSpecifiedTests=false`
- [ ] **Step 3: Implement**: in every server path read `getFacets()` before the `try`-with-resources closes the `ResultSet` (in `streamCursor` capture it after the loop inside the `try`, then attach to the terminal batch, emitting an empty terminal batch when `inBatch == 0 && pending == null && facets != null`). `streamPaged`: before the first page, parse once through `getStatementCache().getParameterized(sql)` and refuse a `SelectStatement` with `getFacets() != null`.
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Regression run** - `mvn -o -pl grpc-client -am verify -DskipTests -Pintegration -Dit.test='BatchedStreamingResultSetIT,Issue5047StreamingCorrectnessIT' -Dfailsafe.failIfNoSpecifiedTests=false` and `mvn -o -pl grpcw -am verify -DskipTests -Pintegration -Dit.test=GrpcServerIT -Dfailsafe.failIfNoSpecifiedTests=false` and `mvn -o -pl grpcw -am test -Dtest='ArcadeDbGrpcServiceExtendedTest,GrpcTypeConverterTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: BUILD SUCCESS.
- [ ] **Step 6: Stop: changes ready for review** - proto, `FacetProtocol.java`, `ArcadeDbGrpcService.java`, `RemoteGrpcDatabase.java`, `StreamingResultSet.java`, `BatchedStreamingResultSet.java`, two tests.

---

### Task 17: Security IT and memory-budget IT

**Files:**
- Test: `server/src/test/java/com/arcadedb/server/security/FacetsSubtypeAclIT.java`, `server/src/test/java/com/arcadedb/server/FacetsMemoryBudgetIT.java`

**Interfaces:**
- Consumes: the user/group helper pattern of `Issue9637ExternalBucketAclIT.createUser` (Issue9637ExternalBucketAclIT.java:213-229: `ServerSecurityTestAccess.databaseGroups(security, db).put(group, ...)`, `security.saveGroups()`, `security.createUser(...)`).

- [ ] **Step 1: Write the tests**

`FacetsSubtypeAclIT extends BaseGraphServerTest`: types `Asset` and subtype `Image extends Asset`, COLUMNAR on `Asset.brand` only (it projects Image's buckets), property `title STRING` with seeded words and FULL_TEXT on it declared on `Asset`; group `noImages` with `"*"` full access and `Image` access `[]`.

```java
@Test void subtypeBucketIsNotCounted() {
  // as the restricted user over RemoteDatabase: SELECT FROM Asset FACETS brand TERMS TOP 1000
  // equals root's SELECT brand, count(*) FROM Asset WHERE @type = 'Asset' GROUP BY brand; no Image-only brand appears (also the first test of the executor's per-bucket `canAccessFile` skip)
}
@Test void fullTextFastPathDoesNotLeakSubtype() // WHERE search_fields(['title'], 'x') (IndexedMatchSource builds the MatchSet from index RIDs): Image matches excluded
@Test void disjunctiveIndexFilterDoesNotLeak()  // FILTER brand IN [...] with an LSM index spanning both types: counts exclude Image
@Test void rootSeesEverything()                 // root facets == polymorphic GROUP BY
```

`FacetsMemoryBudgetIT extends BaseGraphServerTest`, `@Tag("slow")`: 200,000 assets with a high-cardinality `tag` (100,000 values, COLUMNAR); `COLUMNAR_QUERY_MEMORY_BUDGET` = 4 MiB, `COLUMNAR_QUERY_BUDGET_WAIT` = 2000.

```java
@Test void concurrentQueriesStayWithinBudget() {
  // 16 threads x 10 queries "SELECT FROM Asset FACETS tag TERMS TOP 10 LIMIT 1" through RemoteDatabase
  // a sampler thread records max(capacity - FacetMemoryBudget.instance().available()) every 5 ms
  assertThat(maxUsed).isLessThanOrEqualTo(4L << 20);
  // every response either matches the oracle or reports source "unavailable" reason "memory budget"; at least one matches the oracle
  // manager.getStats().queryBudgetWaits() > 0
}
```

- [ ] **Step 2: Run, expect FAIL before Tasks 7-14 exist, PASS after** - `mvn -o -pl server -am verify -DskipTests -Pintegration -Dit.test='FacetsSubtypeAclIT,FacetsMemoryBudgetIT' -Dfailsafe.failIfNoSpecifiedTests=false -Dfailsafe.excludedGroups=`
- [ ] **Step 3: Fix any leak found in the executor (never in the test)**
- [ ] **Step 4: Run, expect PASS**
- [ ] **Step 5: Stop: changes ready for review** - the two ITs (and any executor fix).

---

### Task 18: HA integration tests on followers

**Files:**
- Test: `ha-raft/src/test/java/com/arcadedb/server/ha/raft/FacetsFollowerQueryIT.java`

**Interfaces:**
- Consumes: `BaseRaftHATest` (`getServerCount`, `requireLeaderIndex()`, `firstFollowerOf(int)`, `getServerDatabase(int, String)`, `awaitCountOn(int, String, long)`); `ColumnarIndexManager.suspendApply()` reached through `((LocalDatabase) ((DatabaseInternal) db).getEmbedded()).getColumnarIndexManager()`; plan 3's `FacetOracle.expectedCounts(Database, String, String, String)` and `FacetOracle.awaitCaughtUp(Database)` on the node it queries (the engine test-jar is already a test dependency of `ha-raft`, `ha-raft/pom.xml:76`, and plan 3 Task 23 uses the same fixtures there).

- [ ] **Step 1: Write the test** (3 servers, `@Timeout(300)` per method, Awaitility `atMost(60, SECONDS)`)

```java
@Test void followersAndLeaderReturnEqualCountsAfterConvergence() {
  // on leader: CREATE TYPE Asset, CREATE INDEX ON Asset (brand) COLUMNAR, (tags BY ITEM) COLUMNAR; insert 3000 assets in 30 tx
  // for each server: await until awaitCountOn(i, "Asset", 3000) and facets meta.lagMs == 0
  // FACETS brand TERMS TOP 1000, tags TERMS TOP 1000 toJSON() minus meta is identical on all three servers and equals FacetOracle.expectedCounts on each
}
@Test void followerReportsLagAndStaysExactWhilePaused() {
  final int leader = requireLeaderIndex(); final int follower = firstFollowerOf(leader);
  try (AutoCloseable hold = manager(follower).suspendApply()) {
    // 200 updates of brand and 100 inserts on the leader; awaitCountOn(follower, "Asset", expected)
    final FacetResults f = facets(follower);
    assertThat(f.exact()).isTrue(); assertThat(f.patched()).isPositive(); assertThat(f.lagMs()).isPositive();
    assertThat(asMap(f.get("brand"))).isEqualTo(FacetOracle.expectedCounts(db(follower), "Asset", "brand", null));
  }
}
@Test void followerOverPatchBudgetReportsInexact() // COLUMNAR_PATCH_BUDGET = 5 (restored in finally): exact false, lagMs > 0 on the follower; after resume, await exact with patched 0
@Test void forwardedCommandOnFollowerKeepsFacets() // follower db.command("sql", "SELECT ... FACETS ...") (the path parseResultSetFromJson serves when forwarded): getFacets() equal to leader's
```

- [ ] **Step 2: Run, expect PASS** - `mvn -o -pl ha-raft -am verify -DskipTests -Pintegration -Dit.test=FacetsFollowerQueryIT -Dfailsafe.failIfNoSpecifiedTests=false`
- [ ] **Step 3: Run the neighbouring HA suites** - `-Dit.test='Issue10093CompositeLabelOverIndexedTypeIT,FollowerSessionTokenQueryIT,GrpcRaftReplicationIT'` (the last with `-pl grpcw`). Expected: BUILD SUCCESS.
- [ ] **Step 4: Stop: changes ready for review** - `FacetsFollowerQueryIT.java`.

---

### Task 19: Facets vs GROUP BY benchmark (report only)

**Files:**
- Test: `engine/src/test/java/com/arcadedb/query/facet/FacetsVsGroupByBenchmark.java` (`@Tag("benchmark")`, extends `TestHelper`)

**Interfaces:**
- Consumes: plan 3 `ColumnarTestData` (seed 42, `-DfacetBench.records`, default 1_000_000), `FacetOracle.awaitCaughtUp`; `StallAwareStopwatch`.

Spec 1 ("performance comparable to Elasticsearch aggregations, cost depending on matches") and spec 10 ("benchmarks tagged `@Tag("benchmark")`, measured with `StallAwareStopwatch`"); plan 1 measured a hand-built segment, this measures the shipped path.

- [ ] **Step 1: Write the benchmark.** For a selective (`fileSize < 10000`, LSM index) and a broad (`brand <> 'brand-1'`) WHERE, and for no WHERE: 2 warm-up and 5 measured rounds of `SELECT @rid FROM Asset WHERE <w> FACETS brand TERMS, tags TERMS, size: fileSize RANGES (...) LIMIT 20` (rows drained, `getFacets()` read) against the three equivalent `GROUP BY` queries; at lag 0 and while lagging (`suspendApply()` with 5_000 pending updates). Report median and min `effectiveMs`, `meta.patched`, counting mode per facet, and the ratio; also `ColumnarTestData.buckets = 3`. Correctness asserted on the last round only (facets equal `FacetOracle.expectedCounts`); timings logged through `LogManager`, never asserted.
- [ ] **Step 2: Run once** - `mvn -o -pl engine -am test -Dtest=FacetsVsGroupByBenchmark -DexcludedGroups=none -DfacetBench.records=200000 -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS; copy the report lines into the review notes. The normal lane never runs it (default `benchmark` exclusion).
- [ ] **Step 3: Stop: changes ready for review** - `FacetsVsGroupByBenchmark.java`.

---

## Gate (roadmap)

Plan 4 is done when Tasks 14, 16, 17, 18 ITs are green together with the unit and engine integration tests of Tasks 1-13:

```bash
mvn -o -pl engine -am test -Dtest='com/arcadedb/query/facet/*Test,SQLFacetsParserTest,FacetsKeywordsAsIdentifiersTest,SQLLiteralParameterizerFacetsTest' -Dsurefire.failIfNoSpecifiedTests=false
mvn -o -pl server,grpc-client,ha-raft,metrics -am verify -DskipTests -Pintegration -Dit.test='Facets*IT,PrometheusColumnarQueryMetricsIT,RemoteDatabaseFacetsIT' -Dfailsafe.failIfNoSpecifiedTests=false -Dfailsafe.excludedGroups=
```
