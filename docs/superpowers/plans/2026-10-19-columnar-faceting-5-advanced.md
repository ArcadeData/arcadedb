# Columnar Faceting - Advanced Facets Implementation Plan (plan 5 of 6)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** add hierarchical facets, distinct count, stats, facet-value prefix search, the `AFTER` terms cursor, and `GROUP BY` acceleration from the columnar projection, all exact by default and identical to the plain SQL answer.

**Architecture:** hierarchy is a node space plus a CSR `parents` array built in memory from the tag graph named in the index `METADATA`, rebuilt on a page-change generation and expanded per document at query time with a reusable visited bitset. Dictionaries are per bucket with generations (roadmap "Dictionary scope change"): every structure of this plan that is indexed by ordinal (the HIERARCHY collector's per-query ordinal-to-node memo, DISTINCT bitsets, GROUP BY group arrays) is indexed by the **merged** ordinals of plan 4's `CrossBucketOrdinalMap`, and every dictionary is read from `ColumnarSnapshot.dictionary(property)`. Distinct, stats and hierarchy are new collectors plugged into plan 4's `FacetExecutor`. `GROUP BY` acceleration is a planner step that, at execution time, either aggregates straight from the projection (with patching for pending positions) or runs the normal aggregation chain it carries as its fallback.

**Tech Stack:** Java 21, ArcadeDB engine (`com.arcadedb.index.columnar`, `com.arcadedb.query.facet`, `com.arcadedb.query.sql.executor`), ANTLR4 SQL grammar, JUnit 5 + AssertJ + Awaitility, `BaseGraphServerTest` for HTTP.

**Spec:** `docs/superpowers/specs/2026-10-18-columnar-faceting-design.md` (sections 5.1, 6.4, 7.3, 7.6, 7.7, 8.2). Cross-plan contract: `docs/superpowers/plans/2026-10-19-columnar-faceting-0-roadmap.md`. Read both before Task 1.

## Global Constraints

Copied from the roadmap; every task implicitly includes them.

- **No git commits.** The user commits after review. Every "Commit" step of the template is "Stop: changes ready for review", listing the files touched.
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

Plan-specific constraints:

- **No new dependency in this plan.** HyperLogLog is implemented in-house (Task 7): about 150 lines, no license review, no shading. DataSketches (Apache 2.0) was considered and rejected as a large jar for one estimator used only above a user-chosen threshold.
- Hierarchy state is **never persisted**: it is rebuilt from the graph on first use after open. No recovery story, no file.
- `GROUP BY` acceleration must never return a row the normal plan would not return. When in doubt the planner declines (ineligible shape) or the step falls back at run time.

## Decisions this plan makes (spec left them to the builder)

| Question | Decision | Why |
|---|---|---|
| Where `parents` lives | Own node space per hierarchy: node ids for every Tag vertex key, keyed by graph generation only; the **merged** ordinal (`CrossBucketOrdinalMap`, plan 4) to node mapping is a per-query memo in the collector (`int[]` sized `map.size()`, charged, filled on demand with `view.nodeOfValue(map.value(m))`, -1 entries counted as orphans by merged ordinal); no `extendTo`, no CAS publish | Ancestors that no document carries have no dictionary ordinal, and `GlobalDictionary.getOrAdd` is writer-only (roadmap, plan 3); map versions differ per single-bucket query and per restricted user, so a shared per-version mapping would thrash; the memo costs one hash lookup per distinct ordinal touched (tuning #10662 may add a version-keyed cache for the shared entry) |
| Tree or DAG | CSR (`int[] parentOffsets`, `int[] parents`): a tag may have several parents | The per-document visited bitset already dedups, so a DAG costs nothing extra; content tag trees are usually trees, other users' graphs are not |
| Parent direction | METADATA `parentDirection`, default `"IN"`: `(parent)-[ParentOf]->(child)`, so a child's parents are `getConnectedVertexRIDs(IN, parentEdge)` | Matches the edge name `ParentOf` read left to right |
| Cycles | Expansion visits each node at most once per document, so it terminates; every member of a cycle counts the document once. The builder counts nodes on a cycle (`cycleNodes()`), gives them `depth = -1` when no root reaches them, and logs one WARNING per build naming one key on the cycle | Rejecting a cycle at query time would make facets fail on a data error the user may not control; looping is never acceptable |
| Orphans | A document value with no Tag vertex is its own root bucket (`orphan = true`, no parents), resolved by the collector (the view has no node for it); bucket JSON carries `"orphan": true` | Counts are never dropped; a Tag created later re-maps it on the next rebuild |
| Duplicate keys | Tag vertices sharing a key collapse into one node, their parent edges unioned; counted in `duplicateKeys()` | Keys are the join with document values, so two vertices with one key are indistinguishable from the document side |
| Rebuild trigger | **Page hook, not schema event**: the Tag type's buckets, their edge buckets for `parentDirection`, and the edge type's buckets are registered with plan 3's `ColumnarIndexManager.watchFiles(fileIds, onChange)` (file set swapped later with the new `replaceWatchedFiles`, keeping the counter; a watched file that also has a projection notifies its watchers too); `markPages` on any of them bumps the watcher's `ColumnarIndexManager.watchGeneration(onChange)` synchronously and later runs `onChange` on `ColumnarApplyPool`. DDL on those types (drop, rename, bucket add/remove, key property change) arrives as plan 3's `ColumnarSchemaEvent` kind `HIERARCHY_SOURCE_CHANGED` (with `typeName`) through `ColumnarIndexManager.addSchemaEventListener(Consumer<ColumnarSchemaEvent>)` | Tag and ParentOf changes are data writes. They reach a node through commit, Raft apply and WAL replay as pages (spec 6.1), never as schema events; a follower would never hear of them through `onSchemaEvent`. A light (LIGHTWEIGHT) `ParentOf` edge writes only edge-list pages, which is why edge buckets are watched too |
| Rebuild timing | Generation-checked and single-flight: a query captures `watchGeneration(onChange)` at start and needs a view built from at least that generation; the coalesced `onChange` (on `ColumnarApplyPool`) schedules `rebuildIfStale()` on `ColumnarMaintenancePool` so queries normally find a fresh view | Exact for every tree change committed before the query started; rebuild is O(tags + parent edges) |
| Caller's own uncommitted tree change | The query builds a **private, uncached** view through its transaction when plan 4's `TransactionContext.hasChangesInFiles(watchedFileIds)` is true | Read-your-own-writes without leaking uncommitted state into the shared view |
| Distinct, dictionary fields | Always exact: `long[]` bitset over merged ordinals (`CrossBucketOrdinalMap`) + popcount, plus a small `HashSet` for patched values with no ordinal yet | Spec 7.7; merged ordinals so a value held by several buckets counts once |
| Distinct, numeric fields | Exact by default with two `LongHashSet`s (integral values, non-integral double bits) canonicalized like `DistinctNumericKey.canonicalize`; switches to HyperLogLog (p = 14, 16 KB) only when the user wrote `DISTINCT PRECISION n` and the set exceeds `n`, or, with PRECISION, the memory budget refuses the set; without PRECISION a refusal makes the facet `unavailable` (`memory budget`). Then the facet and `meta.exact` are false | Anti-goal "approximate counts by default"; spec 8.3 "exact is false only when ... an approximate algorithm was requested" |
| Distinct keyword | `DISTINCT PRECISION n` (existing tokens `DISTINCT`, `PRECISION`) | No new token |
| HLL flavour | 64-bit hash (splitmix64 finalizer over the canonical long), dense 6-bit registers in a `byte[16384]`, linear counting below `5 * m / 2`, no sparse encoding, no empirical bias tables | Small, deterministic, about 0.8% standard error at p = 14; documented as "HLL with HLL++'s 64-bit hash and small-range correction" |
| Stats types | `STATS` on properties declared BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DATE or DATETIME; dates return `count`, `missing`, `min`, `max` and `null` sum/avg; any other type (STRING, list, DECIMAL, undeclared) is refused by `validateAgainstSchema`; values are row values (min/max in the property's Java type, sum/avg as `sum()`/`avg()` produce them) | Sum of dates is meaningless; DECIMAL cannot be answered exactly from a projection that has no DECIMAL column |
| Sum overflow | `NumericSum` (made public, primitive adders added) accumulates: `long` until overflow, then `BigDecimal`, exactly as `sum()` does (`NumericSum.java:37`, issue #5906 tests) | Same value and same Java type as `SELECT sum(x)` |
| Stats skipping | A segment with zero `matches AND live` rows is skipped by cardinality; a segment whose `[minLong, maxLong]` cannot move the current min or max skips the per-row min/max comparisons (bounds only: segment stats include dead rows), only on LONG columns whose view holds a value; DOUBLE views never skip by bounds (bit patterns) | Spec 7.7 "per-segment min/max used for skipping", honest about what the bounds allow |
| Facet-value search order | Value order = the rank view of plan 3 (String natural order for strings) per bucket dictionary; the per-bucket prefix ranges are k-way merged by `String.compareTo` and deduplicated by value across buckets; prefix match is case-sensitive | Binary search over a sorted view needs an order consistent with `startsWith`; dictionaries are per bucket |
| `facet.searchValues` counts | Returns `{value, count}` rows, count = documents of the type carrying the value now, computed by an internal `FACETS ... TERMS INCLUDE [...]` query with no WHERE; candidates come from the prefix range in batches until `limit` values with count >= 1 are found or the range ends | Dictionary values whose documents are gone stay in the dictionary until compaction (spec 5.4); counting through plan 4 reuses its patching, security and fallback |
| `AFTER` | Strictly greater than the cursor value in value order; implies `ORDER BY VALUE ASC`; with `ORDER BY COUNT`, `ORDER BY VALUE DESC` or `INCLUDE` it is a validation error; the cursor is converted as INCLUDE literals are (plan 4 ruling 97); the facet result carries `after` = last returned value, or `null` when no value follows. Single declaring type over a dictionary: rank pruning through `rankAfter` (gap rank `2p`) with a value check on rank ties; every other path (merged map, foreign value map, merge across declaring types, LONG/DOUBLE terms) filters by `FacetValueComparator` against the cursor. MISSING is returned only on the page without AFTER; `otherCount` counts every matched value outside the page | Composite-aggregation semantics; a cursor that is not a dictionary value still works (lower bound by binary search) |
| `GROUP BY` acceleration placement | `SelectExecutionPlanner.createExecutionPlan`, after `buildExecutionPlan` (`SelectExecutionPlanner.java:445`) and `chainFacetCollect` (`:447`), before `handleProjectionsBlock` (`:449`): the normal aggregation steps are built into a fallback sub-plan and the new step replaces them; a statement with FACETS is declined | Same placement idea as the time-series push-down (`tryTimeSeriesAggregationPushDown`, `:3978`), but with a run-time fallback because exactness is decided per execution |
| `GROUP BY` patch budget | Unlimited (records read), but every patch bitmap, group array and per-group object is charged to `FacetMemoryBudget` (refusal: normal plan) | Patching reads at most the matched records on dirty pages, never more than the normal plan reads; an inexact GROUP BY is not allowed. Pending marks are page granular, so a lagging GROUP BY patches every matched record on a dirty page (record the patch counts for tuning #10662) |
| `GROUP BY` sum/avg on DOUBLE | Ineligible (count/min/max on DOUBLE are eligible) | Floating-point addition order differs from the scan order, so the last bits would differ from the normal plan |
| Kill switch | `GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION` (`arcadedb.columnar.groupByAcceleration`, `SCOPE.DATABASE`, Boolean, default `true`), read at execution time from `context.getDatabase().getConfiguration()` | Tests compare against the forced-normal plan; cached plans obey a runtime toggle |

## Contract this plan consumes (all recorded in the roadmap; names below are the owners')

Nothing here is redefined by this plan. If a call site below disagrees with the owner plan, the owner plan wins.

```java
// plan 3, com.arcadedb.index.columnar
ColumnarIndexManager.watchFiles(int[] fileIds, Runnable onChange);   // markPages on a watched file runs onChange (coalesced, ColumnarApplyPool), all three paths; plan 5 Task 5 makes mark() notify a projected file's watchers too (R9)
ColumnarIndexManager.unwatchFiles(Runnable onChange);
ColumnarIndexManager.replaceWatchedFiles(Runnable onChange, int[] fileIds); // added by plan 5 Task 5 (R9): swaps the file set, keeps the watcher and its counter
long ColumnarIndexManager.watchGeneration(Runnable onChange);        // bumped synchronously by markPages (plan 3 Task 12)
ColumnarIndexManager.addSchemaEventListener(Consumer<ColumnarSchemaEvent> listener); // kind HIERARCHY_SOURCE_CHANGED, typeName set
ColumnarIndexManager.suspendApply(): AutoCloseable                   // test hook
long ColumnarIndexManager.conversionFailures(String typeName, String property);
ColumnarSnapshot.dictionary(String property): GlobalDictionary       // the generation the snapshot's segments are encoded with
int[] GlobalDictionary.sortedOrdinals(); int rank(int); int compare(int, int);
ColumnarIndexMetadata.getHierarchy(): JSONObject                     // schema/ColumnarIndexMetadata.java; accepts optional "parentDirection"
FacetOracle.expectedCounts(Database, String type, String property, String where); FacetOracle.MISSING; FacetOracle.awaitCaughtUp(Database);
ColumnarTestData(long seed): createSchema(db), insertAssets(db, count), mutate(db, ops), knob buckets

// plan 4, com.arcadedb.query.facet
record HierarchyFacetSpec(String alias, String property, WhereClause filter, int top, int minCount) implements FacetSpec
record DistinctFacetSpec(String alias, String property, WhereClause filter, long precisionThreshold) implements FacetSpec // Long.MAX_VALUE = none
record StatsFacetSpec(String alias, String property, WhereClause filter) implements FacetSpec
TermsFacetSpec.after(): Object                                       // null when absent
interface FacetCollector {
  void beginBucket(int bucketId, ColumnarSnapshot snapshot, CrossBucketOrdinalMap map); // null snapshot and map for a scanned bucket
  void collect(ColumnarSegmentView segment, PositionBitmap matches); // matches = forFacet AND NOT patched, NOT ANDed with live: ORDINAL/ORDINAL_LIST collectors hand it to ColumnReader.forEachOrdinal (visits live rows only, roadmap R12); LONG/DOUBLE collectors AND segment.live() themselves
  void collectPatched(long position, PatchedValue value);            // read through PatchedValue getters
  void merge(FacetCollector other);
  FacetResult finish();
  default void release(); default FacetExecutionStats stats(); default int budgetWaits();
}
// collectors are built by the private FacetExecutor.collectorFor(spec, ColumnDefinition column, long matchCount, DatabaseInternal db, ColumnConversion extractor)
// once per (facet, declaring type), each owning a CollectorReservations (one queryBudgetWait window per collector, plan 4 D13);
// a null column means validation only (over-budget match set) or an empty match set: release() and finish() give an empty result
CrossBucketOrdinalMap.of(db, typeName, property, ColumnarSnapshot[] snapshots): merged(bucketId, ord), value(merged), size(), mergedOf(value), version()
                                                                     // null when the budget refuses a private array; AutoCloseable
FacetBucket.attributes(): JSONObject; withAttributes(JSONObject)
record DistinctValue(long value, boolean exact); record FacetStats(long count, long missing, Object min, Object max, Number sum, Object avg)
FacetResult.ofDistinct(...), ofStats(...), withAfter(...); distinct(): DistinctValue; stats(): FacetStats; after(): Object
FacetExecutor.collectorFor(...)                                      // plan 5 adds its three collectors here
FacetSpecCompiler.compile(...)                                       // plan 5 replaces the "not supported" branch
TransactionContext.hasChangesInFiles(IntHashSet) / getWrittenRecordIdsIn(IntHashSet) // reused by plan 4 (R11); plan 5 adds no variant
// a FACETS query with no WHERE takes its match set from live bitmaps plus pending positions, not from a record scan;
// the patcher treats a pending position with no record as not matching
```

Every task below that counts over ordinals has a multi-bucket test (`ColumnarTestData.buckets = 3`, i.e. `CREATE TYPE ... BUCKETS 3`) besides its single-bucket tests.

## Review Focus

1. A document tag whose Tag vertex is created **after** the document: counted as an orphan root first, then under its real ancestors after the next commit on the Tag type, with no document re-apply. Test: `HierarchyFacetIntegrationTest.orphanBecomesMappedWhenTagIsCreatedLater` (Task 6).
2. `GROUP BY` over records that lack the property and records with an explicit `null`: acceleration yields exactly the null group(s) the normal plan yields (expected one; the test compares with the forced-normal plan). Test: `ColumnarGroupByAccelerationTest.missingAndNullFormOneNullGroup` (Task 13).
3. `sum()` of an INTEGER property that leaves the `int` range, and of a LONG property that overflows `long`: Java type and value equal to `SELECT sum(x)` (`Long`, then `BigDecimal`). Tests: `StatsFacetCollectorTest.integerSumWidensLikeSqlSum`, `ColumnarGroupByAccelerationTest.sumOverflowMatchesNormalPlan` (Tasks 9 and 13).
4. `AFTER` with a cursor that is not a dictionary value (deleted value, typed by hand): returns the values strictly after it. Test: `TermsAfterCursorTest.cursorNotInDictionaryStartsAtLowerBound` (Task 10).
5. `facet.searchValues` edge inputs: empty prefix, prefix with regex characters (`"a.*"`), limit 0 or negative, unknown type, property without COLUMNAR index. Test: `SQLFunctionFacetSearchValuesTest.edgeInputs` (Task 11).
6. Dead rows (deleted or superseded) are never counted by the new collectors: `matches` is not ANDed with live. Tests: `HierarchyFacetCollectorTest`, `DistinctFacetCollectorTest`, `StatsFacetCollectorTest` `.deletedAndUpdatedRowsAreNotCounted` (Tasks 6, 8, 9).
7. A bucket read by the fallback scan (no projection, unpublished column, own written bucket) gives the same HIERARCHY / DISTINCT / STATS answer. Tests: `HierarchyFacetIntegrationTest`, `DistinctFacetIntegrationTest`, `StatsFacetIntegrationTest` `.scannedBucketEqualsOracle` (Tasks 6, 8, 9).
8. GROUP BY never answers from an unpublished or retyped column. Test: `ColumnarGroupByAccelerationTest.unpublishedColumnFallsBack` (Task 13).
9. The hierarchy generation never regresses (re-watch, bucket add, a Tag type with its own COLUMNAR index). Tests: `HierarchyManagerTest.newBucketOnTagTypeIsWatched` (view rebuilt after the ALTER, generation strictly greater), `HierarchyManagerTest.tagTypeWithItsOwnColumnarIndexStillBumps` (Task 5).
10. A user without READ on the Tag buckets sees no tag path through a HIERARCHY facet. Test: `HierarchyFacetIntegrationTest.userWithoutReadOnTagIsRefused` (Task 6).
11. Facet values of the new kinds are equal across HTTP, ndjson, gRPC and `RemoteDatabase` (dates, FLOAT, cursor). Tests: `FacetAdvancedHttpIT.statsDatesAndCursorUseTheRowEncoding`, `FacetProtocolTest`, `FacetsGrpcIT`, `RemoteDatabaseFacetsIT`, `NdJsonResultStreamFacetsTest` (Task 14), `StatsFacetIntegrationTest` FLOAT `ratio` case (Task 9).
12. A WHERE answered by an index is not taken for "no WHERE" by GROUP BY acceleration. Test: `ColumnarGroupByAccelerationTest.indexedWhereIsNotWhereless` (Task 13).

## Carried from plans 3-4

Open items of plans 3 and 4 and roadmap rules this plan inherits; each is owned by the task named.

| # | Item | Owner |
|---|---|---|
| C1 | OpenAPI `FacetBucket` `additionalProperties` once plan 5 adds attributes (plan 4 open item) | Task 14 (`CoreApiSpec`, `FacetsApiSpecTest`) |
| C2 | Python bindings (ArcadeData/arcadedb-python, #10666): plan 5 adds HIERARCHY attributes, DISTINCT, STATS (row-typed), the typed `after` cursor, proto fields 9-12 and `facet.searchValues` rows to its scope; nothing Python-side lands in this repository | Task 14 Stop note lists the members |
| C3 | Pending marks are page granular: a lagging GROUP BY patches every matched record on a dirty page (it falls back rather than answer inexact) | Task 13 (memory charges; patched counts recorded for tuning #10662) |
| C4 | Restricted users rebuild the ordinal merge per multi-bucket facet; the HIERARCHY per-query memo adds O(distinct tags) for every user | tuning #10662 (version-keyed memo for the shared entry) |
| C5 | DATETIME_MICROS/NANOS read at ms precision on the scan path | Task 9 `microAndNanoDatetimeMinMaxMatchSql` (equal to SQL, or STATS refused on those types in the scan path) |
| C6 | DECIMAL facet value equals GROUP BY by `compareTo` only | Task 1 refuses STATS on DECIMAL; Task 8 canonicalizes DISTINCT on DECIMAL (scan only) like `DistinctNumericKey` |
| C7 | Facet aliases `unwind`/`timeout` need back-ticks (docs note owed by plan 4) | the user docs for HIERARCHY/DISTINCT/STATS/AFTER carry the same note (Task 14 Stop note, for the docs pass) |
| C8 | A supertype with the index only on subtypes keeps the raw fallback for empty match sets | Tasks 6, 8, 9 `validationOnlyCollectorFinishesEmpty` |
| C9 | "N declaring types can wait N budget windows" | Tasks 6, 8, 9 keep one window per collector (same behavior as plan 4, documented) |
| C10 | Callers always pass a timeout to `awaitCaughtUp`; a column added to a usable projection is unpublished until its rebuild | Tasks 6, 8, 9 (`scannedBucketEqualsOracle`), 11 (`projectionNotReadyFailsWithRetry`), 13 (`unpublishedColumnFallsBack`); tests call `FacetOracle.awaitCaughtUp` only outside `suspendApply` holds |
| C11 | Parallel counting per segment, `CrossBucketOrdinalMap` caching, counter pooling are tuning items | Task 13 "parallel counting is a follow-up"; HIERARCHY/DISTINCT memos join the #10662 list |

---

## File Structure

| File | Responsibility |
|---|---|
| `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLLexer.g4`, `SQLParser.g4` | `HIERARCHY`, `STATS` tokens; facet kinds HIERARCHY / DISTINCT / STATS; options `PRECISION n`, `AFTER expr`; both tokens in `identifier` |
| `engine/src/main/java/com/arcadedb/query/sql/antlr/SQLASTBuilder.java` | Builds the three spec records and `TermsFacetSpec.after` |
| `engine/src/main/java/com/arcadedb/index/columnar/HierarchyDefinition.java` | METADATA `hierarchy` parse, validate, JSON |
| `engine/src/main/java/com/arcadedb/schema/ColumnarIndexMetadata.java` (plan 3) | `getHierarchyDefinition()` on top of plan 3's `getHierarchy()` |
| `engine/src/main/java/com/arcadedb/schema/TypeColumnarIndexBuilder.java` (plan 3) | Hierarchy METADATA validated at CREATE INDEX (schema at hand) |
| `engine/src/main/java/com/arcadedb/index/columnar/HierarchyView.java` | Immutable node space keyed by graph generation, CSR parents sorted by value, depth, orphan flags (no ordinal mapping) |
| `engine/src/main/java/com/arcadedb/index/columnar/HierarchyBuilder.java` | Builds a view from the graph |
| `engine/src/main/java/com/arcadedb/index/columnar/HierarchyManager.java` | Generation, watched files, single-flight rebuild on `ColumnarMaintenancePool`, private views, schema events |
| `engine/src/main/java/com/arcadedb/index/columnar/ColumnarIndexManager.java` (plan 3) | `replaceWatchedFiles`, watchers of projected files notified, lazy `hierarchy(type, property)` registry with one schema listener |
| `engine/src/main/java/com/arcadedb/index/columnar/ColumnarStats.java` (plan 3) | `groupByAccelerated` / `groupByFallbacks` counters |
| `engine/src/main/java/com/arcadedb/index/columnar/DictionaryPrefixSearch.java` | Prefix range over the rank view plus unranked tail |
| `engine/src/main/java/com/arcadedb/query/facet/AncestorExpander.java` | Per-document ancestor closure with reusable bitset |
| `engine/src/main/java/com/arcadedb/query/facet/HierarchyFacetCollector.java` | HIERARCHY counting |
| `engine/src/main/java/com/arcadedb/query/facet/DistinctFacetCollector.java` | DISTINCT counting |
| `engine/src/main/java/com/arcadedb/query/facet/StatsFacetCollector.java` | STATS |
| `engine/src/main/java/com/arcadedb/query/facet/DistinctValue.java`, `FacetStats.java` | Result records, created by plan 4 Task 1 (verified, not recreated) |
| `engine/src/main/java/com/arcadedb/query/facet/FacetFallbackScanner.java` (plan 4) | `sourceOf` routes HIERARCHY and DISTINCT on dictionary properties to raw values |
| `engine/src/main/java/com/arcadedb/query/facet/FacetResult.java` (plan 4) | Stats min/max and the AFTER cursor through the encoder and decoder |
| `engine/src/main/java/com/arcadedb/query/facet/FacetPatcher.java` (plan 4) | `recordAt` made public for GROUP BY patching |
| `grpc/src/main/proto/arcadedb-server.proto`, `grpc/.../FacetProtocol.java` (plan 4) | Typed `GrpcValue` `after`, `min`, `max` (fields 9-11), `has_after` (12) |
| `server/.../openapi/CoreApiSpec.java` (plan 4) | `FacetBucket` `additionalProperties: true`, plan 5 attributes and stats min/max described |
| `engine/src/main/java/com/arcadedb/utility/HyperLogLog.java` | Estimator |
| `engine/src/main/java/com/arcadedb/function/sql/math/NumericSum.java`, `SQLFunctionAverage.java` | Public primitive adders, shared average |
| `engine/src/main/java/com/arcadedb/function/sql/facet/SQLFunctionFacetSearchValues.java` | `facet.searchValues` |
| `engine/src/main/java/com/arcadedb/query/sql/executor/ColumnarGroupByPlanner.java` | Eligibility analysis |
| `engine/src/main/java/com/arcadedb/query/sql/executor/ColumnarGroupByStep.java` | Execution, fallback, EXPLAIN/PROFILE |
| `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` | `COLUMNAR_GROUP_BY_ACCELERATION` |
| `engine/src/test/java/com/arcadedb/index/columnar/FacetOracle.java`, `ColumnarTestData.java` | Reference implementations and tag-tree generator (extended) |
| `server/src/test/java/com/arcadedb/server/http/FacetAdvancedHttpIT.java` | HTTP end to end (next to plan 4's `FacetsHttpIT`) |

---

### Task 1: Grammar and spec records for HIERARCHY, DISTINCT, STATS, AFTER

**Files:**
- Modify: `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLLexer.g4` (tokens near `AFTER: A F T E R;` at `:184`)
- Modify: `engine/src/main/antlr4/com/arcadedb/query/sql/grammar/SQLParser.g4` (plan 4's facet rules `facetBody` `:1405`, `facetTermsOption` `:1411`; `identifier` rule at `:1726`)
- Modify: `engine/src/main/java/com/arcadedb/query/sql/antlr/SQLASTBuilder.java` (plan 4's facet item visitor)
- Modify/verify: `engine/src/main/java/com/arcadedb/query/facet/HierarchyFacetSpec.java`, `DistinctFacetSpec.java`, `StatsFacetSpec.java`, `TermsFacetSpec.java`
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/SelectExecutionPlanner.java` (`initFacets` `:242`, compile at `:256`: call `validateAgainstSchema` there; `facetsDependOnInputParameters` `:269` checks `item.after`)
- Test: `engine/src/test/java/com/arcadedb/query/facet/AdvancedFacetParserTest.java`
- Modify (test): `engine/src/test/java/com/arcadedb/query/sql/parser/FacetsKeywordsAsIdentifiersTest.java` (`newKeywordsStayIdentifiers`)

**Interfaces:**
- Consumes: plan 4's `FacetsClause` / `FacetItem` AST and the `FacetSpec` sealed interface.
- Produces: the four record shapes of plan 4 (see "Contract this plan consumes"), filled by `FacetSpecCompiler.compile` (plan 4 Task 3) in place of its "not supported" branch; a new `static void FacetSpecCompiler.validateAgainstSchema(List<FacetSpec> specs, DocumentType target)` called by `SelectExecutionPlanner.init` right after `compile`, failing with the exact messages below (`CommandSQLParsingException` at planning, when the schema is known). HIERARCHY is valid when `getPolymorphicColumnarIndex(property)` exists and its `ColumnarIndexMetadata.getHierarchy()` is not null (plan 3 JSON; Task 2's typed definition is not needed here). With a null `target` (a bucket or subquery FROM) only the type-free checks run; the executor applies the others per declaring type and answers `unavailable` with the same message.

- [ ] **Step 1: Write the failing parser test** `AdvancedFacetParserTest extends TestHelper`, schema: `Asset` document type with `tags LIST OF STRING`, `brand STRING`, `fileSize LONG`, each with a plain `COLUMNAR` index (no hierarchy METADATA yet: Task 2 adds it, and `parsesAdvancedKinds` only needs the parse, not the validation).

```java
@Test void parsesAdvancedKinds() {
  final List<FacetSpec> specs = facetsOf("SELECT FROM Asset FACETS t: tags HIERARCHY TOP 20 MIN_COUNT 2, a: brand DISTINCT, "
      + "n: fileSize DISTINCT PRECISION 3000, s: fileSize STATS, p: brand TERMS TOP 5 AFTER 'Nike'");
  assertThat(specs.get(0)).isEqualTo(new HierarchyFacetSpec("t", "tags", null, 20, 2));
  assertThat(((DistinctFacetSpec) specs.get(1)).precisionThreshold()).isEqualTo(Long.MAX_VALUE);
  assertThat(((DistinctFacetSpec) specs.get(2)).precisionThreshold()).isEqualTo(3000L);
  assertThat(specs.get(3)).isInstanceOf(StatsFacetSpec.class);
  assertThat(((TermsFacetSpec) specs.get(4)).after()).isEqualTo("Nike");
}
// in FacetsKeywordsAsIdentifiersTest (query.sql.parser), extending plan 4's test:
@Test void newKeywordsStayIdentifiers() {   // spec anti-goal: no schema breakage
  database.command("sql", "CREATE DOCUMENT TYPE K");
  database.command("sql", "CREATE PROPERTY K.stats STRING");
  database.command("sql", "CREATE PROPERTY K.hierarchy STRING");
  database.command("sql", "INSERT INTO K SET stats = 'a', hierarchy = 'b', after = 'c', precision = 'd'");
  assertThat(database.query("sql", "SELECT stats, hierarchy, after, precision FROM K").next().<String>getProperty("hierarchy")).isEqualTo("b");
}
// back in AdvancedFacetParserTest:
@Test void afterWithOrderByCountIsRefused()    // message: "AFTER pages in value order: remove ORDER BY COUNT"
@Test void statsOnStringIsRefused()            // message: "STATS requires a property of type BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DATE or DATETIME: brand"
@Test void statsOnDecimalIsRefused()           // "STATS requires a property of type BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE, DATE or DATETIME: price"
@Test void hierarchyWithoutMetadataIsRefused() // on brand: "HIERARCHY requires a COLUMNAR index with METADATA hierarchy on Asset.brand"
```

`facetsOf(sql)` is a test helper that parses the statement and returns plan 4's facet spec list (use plan 4's accessor on `SelectStatement`).

- [ ] **Step 2: Run to verify it fails**

Run: `mvn -o -pl engine -am test -Dtest=AdvancedFacetParserTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL (`HIERARCHY`/`STATS` unknown, or plan 4's placeholder "not supported" error).

- [ ] **Step 3: Implement**

Lexer: `HIERARCHY: H I E R A R C H Y;` and `STATS: S T A T S;` (`AFTER`, `DISTINCT`, `PRECISION` exist). Parser: append to plan 4's labeled `facetBody` rule `| HIERARCHY facetHierarchyOption* # hierarchyFacet`, `| DISTINCT (PRECISION INTEGER_LITERAL)? facetFilter? # distinctFacet`, `| STATS facetFilter? # statsFacet`, with `facetHierarchyOption : TOP pInteger | MIN_COUNT pInteger | facetFilter ;`; append `| AFTER expression` to `facetTermsOption`. `FacetItem.Kind` already lists `HIERARCHY, DISTINCT, STATS` (plan 4); add `FacetItem.after` and `FacetItem.precision` fields and print them in `toString` (plan 4's canonical order, `FILTER` last). Add `AFTER`, `HIERARCHY` and `STATS` to `identifier` (`PRECISION`, `DISTINCT` are already there; `AFTER` now also shares its prediction with `UPDATE ... RETURN AFTER` and `DOWNSAMPLING POLICY AFTER`, whose parser tests Step 4 runs). AST builder evaluates the `AFTER` expression once as a constant (literal or input parameter; anything else is a parse error "AFTER takes a literal or a parameter"). Validation runs in `FacetSpecCompiler.validateAgainstSchema`; `top` defaults to 10 and is capped by `COLUMNAR_TOP_MAX` like terms. AFTER is refused with ORDER BY VALUE DESC ("AFTER pages in ascending value order: remove ORDER BY VALUE DESC") and with INCLUDE ("AFTER and INCLUDE cannot be combined"); `SelectExecutionPlanner.facetsDependOnInputParameters` also checks `item.after`.

- [ ] **Step 4: Run to verify it passes** (same command). Expected: PASS. Then run plan 4's parser tests: `mvn -o -pl engine -am test -Dtest='SQLFacetsParserTest,FacetsKeywordsAsIdentifiersTest,FacetSpecCompilerTest,SQLLiteralParameterizerFacetsTest,SQLParser*Test,*UpdateStatement*Test,*TimeSeries*Downsampl*Test,*AlterTimeSeries*Test' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS.

- [ ] **Step 5: Stop: changes ready for review** - `SQLLexer.g4`, `SQLParser.g4`, `SQLASTBuilder.java`, `FacetItem.java`, `FacetSpecCompiler.java`, `SelectExecutionPlanner.java`, the four spec records, `AdvancedFacetParserTest.java`, `FacetsKeywordsAsIdentifiersTest.java`.

---

### Task 2: Hierarchy definition in COLUMNAR METADATA

**Files:**
- Create: `engine/src/main/java/com/arcadedb/index/columnar/HierarchyDefinition.java`
- Modify: plan 3's COLUMNAR index metadata class `engine/src/main/java/com/arcadedb/schema/ColumnarIndexMetadata.java`: add `getHierarchyDefinition()` built from plan 3's `getHierarchy()` JSON (which keeps its `JSONObject` type and already stores an optional `parentDirection`; `getHierarchy()` at `:51`)
- Modify: `engine/src/main/java/com/arcadedb/schema/TypeColumnarIndexBuilder.java` (`create()` `:102`: `HierarchyDefinition.fromJSON(columnarMetadata().getHierarchy()).validate(...)` before `super.create()`)
- Test: `engine/src/test/java/com/arcadedb/index/columnar/HierarchyDefinitionTest.java`

**Interfaces:**
- Produces:
```java
public record HierarchyDefinition(String vertexType, String key, String parentEdge, Vertex.DIRECTION parentDirection) {
  public static HierarchyDefinition fromJSON(JSONObject hierarchy); // null when the argument is null
  public JSONObject toJSON();                                      // parentDirection written as "IN" / "OUT"
  public void validate(Schema schema, String indexedType, String property); // throws IllegalArgumentException
}
ColumnarIndexMetadata.getHierarchyDefinition(): HierarchyDefinition // null when absent; getHierarchy(): JSONObject stays plan 3's
```

- [ ] **Step 1: Write the failing test** `HierarchyDefinitionTest extends TestHelper`. Schema: vertex type `Tag` with `path STRING`, edge type `ParentOf`, document type `Asset` with `tags LIST OF STRING`.

```java
@Test void createIndexStoresHierarchyAndSurvivesReopen() {
  database.command("sql", "CREATE INDEX ON Asset (tags BY ITEM) COLUMNAR METADATA "
      + "{ \"hierarchy\": { \"vertexType\": \"Tag\", \"key\": \"path\", \"parentEdge\": \"ParentOf\" } }");
  reopenDatabase();
  final HierarchyDefinition def = metadataOf("Asset", "tags").getHierarchyDefinition();
  assertThat(def).isEqualTo(new HierarchyDefinition("Tag", "path", "ParentOf", Vertex.DIRECTION.IN));
}
@Test void rejectsUnknownVertexType()   // message contains "hierarchy.vertexType 'Nope' is not a vertex type"
@Test void rejectsDocumentTypeAsVertexType()
@Test void rejectsUnknownKeyProperty()  // "hierarchy.key 'nope' is not a property of Tag"
@Test void rejectsNonEdgeParentEdge()   // "hierarchy.parentEdge 'Tag' is not an edge type"
@Test void rejectsNumericIndexedProperty() // hierarchy on fileSize LONG: "hierarchy requires a STRING or LIST OF STRING property"
@Test void rejectsBadDirection()        // "parentDirection": "BOTH" -> "hierarchy.parentDirection must be IN or OUT"
@Test void rejectsUnidirectionalEdgeWithInDirection() // CREATE EDGE TYPE ParentOf UNIDIRECTIONAL: "hierarchy.parentEdge 'ParentOf' is unidirectional: use parentDirection OUT with child-to-parent edges"
```

- [ ] **Step 2: Run to verify it fails**: `mvn -o -pl engine -am test -Dtest=HierarchyDefinitionTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL (class missing).

- [ ] **Step 3: Implement** `HierarchyDefinition` and the metadata hook. Validation runs in `TypeColumnarIndexBuilder.create()` (schema at hand, the CREATE INDEX path only; restore uses `fromJSON`, which stays tolerant). Besides the rules above it refuses a UNIDIRECTIONAL `parentEdge` with `parentDirection IN` (a unidirectional edge stores no IN list). `fromJSON` stays tolerant on restore: a missing source type at open leaves the definition in place and the `HierarchyManager` reports it unavailable (Task 5).

- [ ] **Step 4: Run to verify it passes** (same command). Expected: PASS.

- [ ] **Step 5: Stop: changes ready for review** - `HierarchyDefinition.java`, `ColumnarIndexMetadata.java`, `TypeColumnarIndexBuilder.java`, `HierarchyDefinitionTest.java`.

---

### Task 3: AncestorExpander (pure arrays)

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/facet/AncestorExpander.java`
- Test: `engine/src/test/java/com/arcadedb/query/facet/AncestorExpanderTest.java`

**Interfaces:**
- Produces:
```java
public final class AncestorExpander {             // one per collector task, not thread-safe
  public AncestorExpander(int nodeCapacity);
  // closure of startNodes (count entries, -1 entries ignored) over parents; each node once; returns how many
  public int expand(int[] parentOffsets, int[] parents, int nodeCount, int[] startNodes, int count);
  public int[] nodes();                           // the closure, valid until the next expand
}
```

- [ ] **Step 1: Write the failing test** (no database).

```java
// tree: 0 root; 1,2 children of 0; 3 child of 1; 4 child of 1
@Test void twoTagsInOneSubtreeCountAncestorsOnce() {
  final AncestorExpander e = new AncestorExpander(5);
  final int n = e.expand(OFFSETS, PARENTS, 5, new int[] { 3, 4 }, 2);
  assertThat(Arrays.copyOf(e.nodes(), n)).containsExactlyInAnyOrder(3, 4, 1, 0);
}
@Test void cycleTerminatesAndCountsEachMemberOnce()   // 0->1->2->0: expand {2} = {2,1,0}
@Test void dagSharedAncestorOnce()                    // 3 has parents 1 and 2, both under 0: {3,1,2,0}
@Test void stateIsResetBetweenDocuments()             // expand {3} then {2}: second is {2,0}, not containing 3 or 1
@Test void growsWhenViewGrew()                        // capacity 2, nodeCount 5: no exception, correct closure
@Test void minusOneStartIsIgnored()
```

- [ ] **Step 2: Run to verify it fails**: `mvn -o -pl engine -am test -Dtest=AncestorExpanderTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.

- [ ] **Step 3: Implement.** Visited set is a `long[]` bitset sized `(nodeCapacity + 63) >>> 6`, grown when `nodeCount` exceeds it. Reset clears only the bits of the previous `nodes()` content (no `Arrays.fill`), so cost per document is proportional to its closure. `nodes()` doubles as the DFS stack: push starts, then scan the array from index 0 appending unvisited parents.

- [ ] **Step 4: Run to verify it passes** (same command). Expected: PASS.

- [ ] **Step 5: Stop: changes ready for review** - `AncestorExpander.java`, `AncestorExpanderTest.java`.

---

### Task 4: HierarchyView and HierarchyBuilder

**Files:**
- Create: `engine/src/main/java/com/arcadedb/index/columnar/HierarchyView.java`
- Create: `engine/src/main/java/com/arcadedb/index/columnar/HierarchyBuilder.java`
- Modify: `engine/src/test/java/com/arcadedb/index/columnar/ColumnarTestData.java` (tag tree generator creating `Tag(path STRING)` with a NOTUNIQUE index, knobs `tagHierarchy` and `tagValues`)
- Test: `engine/src/test/java/com/arcadedb/index/columnar/HierarchyBuilderTest.java`

**Interfaces:**
- Consumes: `HierarchyDefinition` (Task 2); `Database.iterateType(String, boolean)` (`BasicDatabase.java:228`); `Vertex.getConnectedVertexRIDs(DIRECTION, String...)` (`Vertex.java:93`); `RidLongHashMap` (`utility/RidLongHashMap.java`) for RID to node.
- Produces:
```java
public final class HierarchyView {                          // node space only, keyed by graph generation
  public int nodeCount(); public int[] parentOffsets(); public int[] parents();   // CSR, parentOffsets.length == nodeCount + 1
  public Object value(int node); public int depth(int node); public boolean orphan(int node);
  public int nodeOfValue(Object value);           // -1 when unknown
  public long generation(); public int cycleNodes(); public int duplicateKeys();
}
public final class HierarchyBuilder {
  static HierarchyView build(Database db, HierarchyDefinition def, long generation);
}
// ColumnarTestData
static List<String> createTagTree(Database db, long seed, int depth, int fanout); // Tag{path} + NOTUNIQUE LSM on Tag.path + ParentOf edges, returns all paths
// knobs: tagHierarchy (METADATA JSON put on the tags index by createSchema), tagValues (the list insertAssets/mutate draw tags from)
```

- [ ] **Step 1: Write the failing test** `HierarchyBuilderTest extends TestHelper`. Tags `a`, `a/b`, `a/b/c`, `a/d`, `x`; edges `a->a/b`, `a/b->a/b/c`, `a->a/d`; assets carrying `a/b/c`, `a/d`, `zzz` (no Tag).

```java
@Test void buildsParentsDepthAndOrdinalMapping() {
  final HierarchyView v = HierarchyBuilder.build(database, def, 1);
  final int c = v.nodeOfValue("a/b/c");
  assertThat(v.value(parentsOf(v, c)[0])).isEqualTo("a/b");
  assertThat(v.depth(c)).isEqualTo(2);
  assertThat(v.depth(v.nodeOfValue("x"))).isEqualTo(0);
  assertThat(v.nodeOfValue("a/d")).isGreaterThanOrEqualTo(0);
}
@Test void documentValueWithoutTagIsOrphanRoot()     // nodeOfValue("zzz") == -1 (orphans are a collector concern)
@Test void cycleIsCountedNotLooped()                 // add edge a/b/c -> a: cycleNodes() == 3, build returns
@Test void unrootedCycleHasDepthMinusOne()           // p->q->p only: depth(p) == -1
@Test void duplicateKeysCollapse()                   // second Tag{path:'a/d'} with parent x: one node, parents {a, x}, duplicateKeys() == 1
@Test void lightweightParentEdgesAreFollowed()       // ParentOf created LIGHTWEIGHT (#10168): same parents
@Test void parentDirectionOut()                      // edges child->parent with "parentDirection":"OUT"
@Test void parentsAreSortedByValue()                 // a/d with parents x and a (x inserted first): parentsOf(a/d) == [a, x] on every rebuild
```

`parentsOf(view, node)` is a test helper slicing `parents()` between `parentOffsets()[node]` and `[node + 1]`.

- [ ] **Step 2: Run to verify it fails**: `mvn -o -pl engine -am test -Dtest=HierarchyBuilderTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.

- [ ] **Step 3: Implement.** Two passes over `iterateType(vertexType, true)`: pass 1 assigns a node per distinct key (key read with `vertex.get(key)`, null keys skipped and counted), records RID to node in a `RidLongHashMap`, and fills `Object[] values`; pass 2 reads each vertex's parent RIDs through `getConnectedVertexRIDs(parentDirection, parentEdge)` (no vertex load), maps them through the RID map (parents outside `vertexType` ignored), and writes CSR with deduplicated parent lists sorted by parent value (`FacetValueComparator`, so `parent`, the first, is deterministic across rebuilds: `getConnectedVertexRIDs` iterates newest first). Then depth by BFS from nodes with no parents, cycle nodes by iterative three-colour DFS. The `HashMap<Object, Integer> keyToNode` stays in the view for `nodeOfValue` (heap is proportional to tag cardinality, spec 5.7). One WARNING per build when `cycleNodes() > 0`, via `LogManager.instance().log(this, Level.WARNING, ...)`, naming the index and one key on a cycle.

- [ ] **Step 4: Run to verify it passes** (same command). Expected: PASS.

- [ ] **Step 5: Stop: changes ready for review** - `HierarchyView.java`, `HierarchyBuilder.java`, `ColumnarTestData.java`, `HierarchyBuilderTest.java`.

---

### Task 5: HierarchyManager, page-change generation, own-transaction views

**Files:**
- Create: `engine/src/main/java/com/arcadedb/index/columnar/HierarchyManager.java`
- Modify: `engine/src/main/java/com/arcadedb/index/columnar/ColumnarIndexManager.java` (plan 3): `replaceWatchedFiles(Runnable, int[])`; `mark()` notifies the watchers of a file whether or not it has a projection; a lazy `hierarchy(typeName, property)` registry with one schema listener
- Test: `engine/src/test/java/com/arcadedb/index/columnar/HierarchyManagerTest.java`

**Interfaces:**
- Consumes: Task 4; plan 3 `ColumnarIndexManager.watchFiles(int[], Runnable)`, `unwatchFiles(Runnable)`, `watchGeneration(Runnable)`, `addSchemaEventListener(Consumer<ColumnarSchemaEvent>)` (kind `HIERARCHY_SOURCE_CHANGED`, `typeName`); plan 4 `TransactionContext.hasChangesInFiles(IntHashSet)` (`:2253`); `ColumnarMaintenancePool`; `GraphEngine.getEdgesBucketName(int bucketId, Vertex.DIRECTION)` (`GraphEngine.java:3228`) to find edge-list buckets; `DocumentType.getBucketIds(true)`.
- Produces:
```java
public final class HierarchyManager {
  HierarchyManager(DatabaseInternal db, ColumnarIndexManager columnar, String typeName, String property, HierarchyDefinition def);
  int[] watchedFileIds();
  long generation();                     // columnar.watchGeneration(onChange): bumped synchronously by markPages, the only commit-path work
  public HierarchyView view();           // for the calling thread's query; never null when isAvailable()
  void rebuildIfStale();                 // scheduled single-flight on ColumnarMaintenancePool by the coalesced onChange
  void onSchemaEvent(ColumnarSchemaEvent event);        // registered with addSchemaEventListener; acts on HIERARCHY_SOURCE_CHANGED naming one of its source types
  public boolean isAvailable(); public String unavailableReason();
  long rebuildCount();                   // test observability
  void close();                          // unwatchFiles
}
ColumnarIndexManager.hierarchy(String typeName, String property): HierarchyManager   // lazy, from the TypeIndex METADATA; null when the index has no hierarchy
ColumnarIndexManager.replaceWatchedFiles(Runnable onChange, int[] fileIds);           // keeps the watcher and its counter
```

- [ ] **Step 1: Write the failing tests.**

`HierarchyManagerTest extends TestHelper` (fixture of Task 4):
```java
@Test void committedEdgeChangeIsVisibleToTheNextView() {
  final HierarchyView before = manager.view();
  database.transaction(() -> reparent("a/d", "x"));          // delete ParentOf a->a/d, add x->a/d
  final HierarchyView after = manager.view();                // no wait: watchGeneration was bumped before commit returned
  assertThat(after.generation()).isGreaterThan(before.generation());
  assertThat(parentValues(after, "a/d")).containsExactly("x");
}
@Test void lightweightEdgeChangeBumpsGeneration()            // ParentOf LIGHTWEIGHT: only edge buckets change
@Test void tagKeyRenameRebuilds()                            // UPDATE Tag SET path = 'a/e' WHERE path = 'a/d'
@Test void unrelatedWritesDoNotRebuild()                     // 100 Asset inserts: rebuildCount() unchanged after view()
@Test void ownUncommittedChangeIsPrivate() {
  database.begin();
  reparent("a/d", "x");
  assertThat(parentValues(manager.view(), "a/d")).containsExactly("x");  // read-your-own-writes via hasChangesInFiles
  final AtomicReference<List<String>> other = new AtomicReference<>();
  runInOtherThread(() -> other.set(parentValues(manager.view(), "a/d")));
  assertThat(other.get()).containsExactly("a");                                   // shared view untouched
  database.rollback();
}
@Test void concurrentQueriesBuildOnce()                      // 8 threads call view() after one change: rebuildCount() increases by 1
@Test void newBucketOnTagTypeIsWatched()                     // CREATE BUCKET Tag_extra, ALTER TYPE Tag BUCKET +Tag_extra (HIERARCHY_SOURCE_CHANGED "Tag"), insert a Tag there: generation bumps; the view built before the ALTER is rebuilt after it (generation strictly greater)
@Test void tagTypeWithItsOwnColumnarIndexStillBumps()        // CREATE INDEX ON Tag (path) COLUMNAR, then reparent: generation bumps
@Test void droppedTagTypeMakesHierarchyUnavailable()         // DROP TYPE ParentOf UNSAFE: isAvailable() false, reason names the type
@Test void watchedFilesAreMarkedOnWalReplay()                // change tags, kill the database and reopen (the ColumnarCrashRecoveryTest pattern): first view reflects the change
@Test void backgroundRebuildAfterCommit()                    // after a change, without any query, await (hang detector 10 s) rebuildCount() + 1: onChange scheduled rebuildIfStale on ColumnarMaintenancePool
```

- [ ] **Step 2: Run to verify they fail**: `mvn -o -pl engine -am test -Dtest=HierarchyManagerTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.

- [ ] **Step 3: Implement `HierarchyManager`.** State: `final Runnable onChange = this::scheduleRebuild` (submits `rebuildIfStale` to `ColumnarMaintenancePool`, at most one queued), `volatile HierarchyView current`, a `ReentrantLock buildLock`, the watched ids as one `IntHashSet` per file-set version. `view()`: if the thread has an active transaction and `hasChangesInFiles(watchedIdSet)` (one `IntHashSet` per file-set version) is true, return `HierarchyBuilder.build(...)` through that transaction, uncached; else capture `g = generation()`, return `current` when `current.generation() >= g`, else take `buildLock`, re-check, build with `generation()` read **before** the scan (a change during the scan bumps it again, the next query rebuilds: same ordering protocol as dirty pages, spec 6.2), publish, unlock. `rebuildIfStale()` builds when `current.generation() < generation()`; on the pool thread it runs with a database context opened for that thread (as the apply pass does) and no user. Watched files: every bucket id of `vertexType` polymorphic, the edge-list bucket of each of those for `parentDirection` (looked up by `getEdgesBucketName`, skipped when absent), every bucket of `parentEdge` polymorphic; on a `HIERARCHY_SOURCE_CHANGED` event naming `vertexType` or `parentEdge`, and at every rebuild (which catches stripe pool edge buckets created at a super-node's first promotion), the file set is recomputed and swapped with `replaceWatchedFiles(onChange, newIds)` (the generation never goes back) and the view is marked stale. Drop or rename of `vertexType`, `parentEdge` or `key` sets unavailable with reason `"hierarchy source <name> was renamed or dropped"` until the index is recreated (controller ruling on preflight R10: the event names only the old type, and rewriting index METADATA from a listener would be a schema write on every node; spec 6.4's Rename row is about the indexed type). Step 1 gains `@Test void renamedTagTypeMakesHierarchyUnavailable() // ALTER TYPE Tag NAME Label`.

- [ ] **Step 4: Wire `ColumnarIndexManager`**: add `replaceWatchedFiles`; make `mark()` notify the watchers of a projected file too; create a manager on the first `hierarchy(typeName, property)` call (one per (declaring type, property), from the TypeIndex METADATA, since a bucket index added by ALTER TYPE carries default metadata), `close()` it on the DROP_COLUMN/DROP_PROJECTION event of its index and when the database closes; the registry registers ONE schema listener for all managers (`addSchemaEventListener` has no remove). Compile: `mvn -o -pl engine -am compile -q`.

- [ ] **Step 5: Run to verify they pass** (Step 2 command). Expected: PASS. Then plan 3's manager and recovery tests and plan 4's `Issue10450IndexBuildWithUnrelatedPendingWritesTest`: `mvn -o -pl engine -am test -Dtest='Columnar*Test,Issue10450IndexBuildWithUnrelatedPendingWritesTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS.

- [ ] **Step 6: Stop: changes ready for review** - `HierarchyManager.java`, `ColumnarIndexManager.java`, `HierarchyManagerTest.java`.

---

### Task 6: HIERARCHY facet collector and integration with the oracle

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/facet/HierarchyFacetCollector.java`
- Modify: plan 4's `FacetExecutor.collectorFor` (private switch `:467` over `FacetSpec`; add the case before the `default` branch (the switch has one, so the compiler does not list missing kinds)). `FacetBucket.attributes()` / `withAttributes` and their JSON already exist (plan 4 Task 1)
- Modify: `engine/src/main/java/com/arcadedb/query/facet/FacetFallbackScanner.java` (`sourceOf` routes HIERARCHY and DISTINCT on dictionary properties to raw values)
- Modify: `engine/src/test/java/com/arcadedb/index/columnar/FacetOracle.java`
- Modify: `engine/src/test/java/com/arcadedb/query/facet/FacetsExplainTest.java` (one PROFILE case per new kind, Tasks 6, 8, 9)
- Test: `engine/src/test/java/com/arcadedb/query/facet/HierarchyFacetCollectorTest.java`, `engine/src/test/java/com/arcadedb/query/facet/HierarchyFacetIntegrationTest.java`

**Interfaces:**
- Consumes: Tasks 3-5; plan 4 `FacetCollector` (incl. `beginBucket`, `release`, `stats`, `budgetWaits`), `CollectorReservations`, `FacetExecutionStats`, `PatchedValue`, `CrossBucketOrdinalMap`, `FacetFallbackScanner`; plan 3 `ColumnReader.forEachOrdinal`; `SecurityHelper.currentUser(db).requestAccessOnFile(int, READ_RECORD)`.
- Produces:
```java
final class HierarchyFacetCollector implements FacetCollector {
  HierarchyFacetCollector(HierarchyFacetSpec spec, ColumnDefinition column, long matchCount, DatabaseInternal db, String declaringType, long budgetWaitMs);
  // one per (facet, declaring type); the view is taken at the first beginBucket with a non-null map: db's manager.hierarchy(declaringType, property).view();
  // a null column (validation only, empty match set) is accepted and finishes an empty result
}
public static HierarchyView HierarchyView.of(int[] parentOffsets, int[] parents, Object[] values, int[] depth, boolean[] orphan); // added in this task, for tests
// FacetOracle
static Map<Object, Long> hierarchicalCounts(Database db, String type, String property, String where,
    String vertexType, String key, String parentEdge);   // reference: SQL rows + Vertex.getVertices(IN, parentEdge) walk, HashSet per document
```
Result JSON per bucket: `{"value": "a/b", "count": 7, "parent": "a", "depth": 1}`, plus `"orphan": true` for orphans; `parent` is the value of the first parent in CSR order, `null` for a root; `otherCount` = sum of the counts of the nodes not returned (a document counts in several nodes, so it can exceed the matches); `missing` = matches with no value.

- [ ] **Step 1: Write the failing unit test** `HierarchyFacetCollectorTest` against a hand-built `HierarchyView` (public static factory `HierarchyView.of(int[] parentOffsets, int[] parents, Object[] values, int[] depth, boolean[] orphan)`, added in this task) and a fake `ColumnarSegmentView` built from plan 3's in-memory overlay test helper.

```java
@Test void documentWithTwoTagsInOneSubtreeCountsOnce()  // doc [a/b/c, a/b/d]: a=1, a/b=1, a/b/c=1, a/b/d=1
@Test void topAndMinCountApplyAfterExpansion()          // top 2 of {a:5, a/b:3, x:3}: [a:5, a/b:3] (tie by value asc)
@Test void patchedValueWithoutOrdinalUsesNodeOfValue()  // collectPatched(pos, PatchedValue.ofValues(dict, "a/d")) with a/d absent from the dictionary: counts a/d and a
@Test void patchedUnknownValueIsOrphanBucket()          // collectPatched(pos, PatchedValue.ofValues(dict, "nope")): bucket nope:1 with orphan attribute
@Test void twoBucketsShareMergedOrdinals()              // one collector, beginBucket(0, ...) then beginBucket(1, ...) over a hand-built 2-bucket CrossBucketOrdinalMap, same tag in both: one node, counted once per document
@Test void mergeOfTwoTasksSumsCounts()
@Test void countsArrayIsChargedToTheBudget()            // FacetMemoryBudget.instance().available() drops by >= 4L * nodeCount while counting and is restored after finish
@Test void validationOnlyCollectorFinishesEmpty()       // collectorFor(spec, null, null, 0): release() and finish() give an empty result, no exception
@Test void deletedAndUpdatedRowsAreNotCounted()         // segment with dead rows (overlay view with a delete and an update): counts equal the oracle
@Test void mergeAcrossDeclaringTypesByValue()           // two declaring types, two maps and two views: node values and counts summed by value
// moved from Task 4 (the ordinal-to-node mapping is the collector's per-query memo):
@Test void ordinalsAddedAfterBuildMapByValue()          // asset with 'a/b' inserted after the view was built: counted under a/b and a
@Test void newValueIsOrphanBucket()                     // asset with 'new': orphan bucket, view untouched
@Test void multiBucketMergedOrdinalsShareANode()        // buckets = 3, 'a/d' in buckets 0 and 2: one node, one bucket
@Test void compactedBucketStillMapsByValue()            // compactDictionaryNow(...) returned true: every value still maps to its node
```

- [ ] **Step 2: Write the failing integration test** `HierarchyFacetIntegrationTest extends TestHelper`: `ColumnarTestData` with `tagHierarchy = {vertexType Tag, key path, parentEdge ParentOf}` and `tagValues` = the paths of `createTagTree` plus 5 orphan values; `ColumnarTestData.createTagTree(database, seed, 4, 4)` plus 2000 assets with 1-4 tags each from the tree (seeded), some with an orphan tag, `FacetOracle.awaitCaughtUp(database)`.

```java
@ParameterizedTest @ValueSource(longs = { 1L, 7L, 42L })
void hierarchicalCountsEqualReference(final long seed) {
  final FacetResult r = facet("SELECT FROM Asset WHERE fileSize > 1000 FACETS t: tags HIERARCHY TOP 1000", "t");
  assertThat(toMap(r.buckets())).isEqualTo(FacetOracle.hierarchicalCounts(database, "Asset", "tags", "fileSize > 1000", "Tag", "path", "ParentOf"));
}
@Test void twoTagsInOneSubtreeCountedOnce()
@Test void treeChangeIsReflectedWithoutReapply() {
  final long applied = appliedPagesMetric();          // arcadedb.columnar.apply.pages (plan 3)
  database.transaction(() -> reparent("t/0/1", "t/2"));
  assertThat(toMap(facet(...).buckets())).isEqualTo(FacetOracle.hierarchicalCounts(...));
  assertThat(appliedPagesMetric()).isEqualTo(applied); // no document page re-applied (Tag type has no COLUMNAR index)
}
@Test void orphanBecomesMappedWhenTagIsCreatedLater() // Review Focus 1
@Test void cycleInTagGraphDoesNotLoop()               // add edge leaf -> root; facet returns, every cycle member counted once per doc
@Test void patchedDocumentsWhileApplyIsSuspended()    // try (suspendApply()) { insert 50 assets; facet equals oracle; meta.patched >= 50 } (pending marks are page granular)
@Test void ownTransactionTreeChangeIsSeen()           // begin(); reparent; facet equals oracle computed inside the same tx; rollback()
@Test void unavailableHierarchySource()               // DROP TYPE ParentOf UNSAFE: facet source "unavailable"
@Test void multiBucketTypeEqualsReference()            // ColumnarTestData.buckets = 3: hierarchicalCounts equal for seed 42
@Test void polymorphicSubtypeEqualsReference()         // Image extends Asset with its own tags dictionary: SELECT FROM Asset ... HIERARCHY equals the polymorphic reference
@Test void scannedBucketEqualsOracle()                 // the type's projection made unusable (plan 3 rebuild held by ColumnarTestHooks.killPoint "rebuild.scanned"): facet source "scan", values equal the oracle
@Test void userWithoutReadOnTagIsRefused()             // user without READ on the Tag buckets: facet source "unavailable", reason "hierarchy source not readable"
```

- [ ] **Step 3: Run to verify they fail**: `mvn -o -pl engine -am test -Dtest='HierarchyFacet*Test' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.

- [ ] **Step 4: Implement the collector.** Per collector: `int[] counts` sized `view.nodeCount()`, one `AncestorExpander`, one `int[] starts` scratch, and the per-query ordinal-to-node memo (`int[]` sized `map.size()`, charged, filled on demand with `view.nodeOfValue(map.value(m))`, -1 entries counted as orphans by merged ordinal). The collector takes the view at its first `beginBucket` with a non-null map (the executor already built that map per declaring type, `FacetExecutor.addGroup`). Counters are reserved through the collector's own `CollectorReservations` (dense `int[]` without waiting, else a hash map waiting one `queryBudgetWait` window, as `TermsCollector`). Before counting, the collector checks `SecurityHelper.currentUser(db).requestAccessOnFile(fileId, READ_RECORD)` for each of `manager.watchedFileIds()`; any refusal answers `unavailable` (`hierarchy source not readable`). `collect`: `column.forEachOrdinal(matches, consumer)` for ORDINAL and ORDINAL_LIST; the consumer buffers the ordinals of the current position and expands them when the position changes (and once after the call); missing = `matches.andCardinality(segment.live())` minus the positions visited. A scanned bucket (`beginBucket(b, null, null)`) delivers every value through `collectPatched`, dictionary values as `PatchedValue.unknown`. `collectPatched` reads the `PatchedValue`: its ordinals go through `merged` then the memo, its unknowns through `nodeOfValue`, and values with no node go to a small `HashMap<Object, long[]>` of orphan counts merged into buckets at `finish`. `finish` ranks by count desc then value asc (plan 4's tie-break), applies `minCount` and `top`, fills `attributes`. A collector of another declaring type is merged by node value (a node's `parent`/`depth` taken from the first collector that has it). `stats()` returns the collector's `FacetExecutionStats` (merged collectors added, plus the expanded node visits), as `TermsCollector`.

- [ ] **Step 5: Implement the oracle** `FacetOracle.hierarchicalCounts`: `SELECT @rid, <property> FROM <type> WHERE <where>`, for each row build a `HashSet<Object>` of its values plus ancestors found by `database.lookupByKey(vertexType, key, value)` and repeated `getVertices(IN, parentEdge)` with its own visited set; values with no Tag count only themselves. It shares no code with the production path.

- [ ] **Step 6: Run to verify they pass** (Step 3 command). Expected: PASS. Then `mvn -o -pl engine -am test -Dtest='Facet*Test' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS.

- [ ] **Step 7: Stop: changes ready for review** - `HierarchyFacetCollector.java`, `HierarchyView.java`, `FacetExecutor.java`, `FacetFallbackScanner.java`, `FacetOracle.java`, `FacetsExplainTest.java`, both tests.

---

### Task 7: HyperLogLog

**Files:**
- Create: `engine/src/main/java/com/arcadedb/utility/HyperLogLog.java`
- Test: `engine/src/test/java/com/arcadedb/utility/HyperLogLogTest.java`

**Interfaces:**
- Produces:
```java
public final class HyperLogLog {
  public static final int PRECISION = 14;           // m = 16384 registers, 16 KB
  public HyperLogLog();
  public void addLong(long value);                  // hash = splitmix64 finalizer
  public void merge(HyperLogLog other);             // register-wise max
  public long estimate();
  public static long hash(long value);
}
```

- [ ] **Step 1: Write the failing test.**

```java
@Test void smallCardinalitiesThroughLinearCounting() { exact for 0 and 1; isCloseTo(10, within(1L)); isCloseTo(1000, within(10L)) } // linear counting's standard error is near 5 at n = 1000
@Test void relativeErrorBelowThreePercent() { for (n : {10_000, 100_000, 1_000_000}) assertThat(Math.abs(hll(n).estimate() - n) / (double) n).isLessThan(0.03); }
@Test void duplicatesDoNotCount()                   // 1M adds of 1000 distinct values: estimate == hll(1000).estimate()
@Test void mergeEqualsUnion()                       // hll(0..500k) merge hll(250k..750k) estimate within 3% of 750k
@Test void deterministic()                          // same input twice: same estimate
```

`hll(n)` adds `0..n-1`. Values are fixed, so the assertions are deterministic.

- [ ] **Step 2: Run to verify it fails**: `mvn -o -pl engine -am test -Dtest=HyperLogLogTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.

- [ ] **Step 3: Implement.** Index = top 14 bits of the hash, rank = `Long.numberOfLeadingZeros((hash << 14) | (1L << 13)) + 1`. Raw estimate `alpha_m * m^2 / sum(2^-register)` with `alpha_m = 0.7213 / (1 + 1.079 / m)`; when raw `<= 5 * m / 2` and empty registers exist, linear counting `m * ln(m / empty)`. No large-range correction (64-bit hash).

- [ ] **Step 4: Run to verify it passes** (same command). Expected: PASS.

- [ ] **Step 5: Stop: changes ready for review** - `HyperLogLog.java`, `HyperLogLogTest.java`.

---

### Task 8: DISTINCT facet collector

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/facet/DistinctFacetCollector.java`
- Modify: plan 4's `FacetExecutor.collectorFor` (case before the `default` branch). Consumed unchanged from plan 4 Task 1: `DistinctValue`, `FacetResult.ofDistinct/distinct()`, and `FacetResults.exact()` false when any distinct is inexact (pinned there by `attributesAndPlan5MembersRoundTrip`)
- Modify: `engine/src/main/java/com/arcadedb/query/facet/FacetFallbackScanner.java` (`sourceOf` routes DISTINCT on dictionary properties to raw values; numeric/date properties read by declared type as today)
- Modify: `FacetOracle.java` (`distinctCount`)
- Test: `engine/src/test/java/com/arcadedb/query/facet/DistinctFacetCollectorTest.java`, `DistinctFacetIntegrationTest.java`

**Interfaces:**
- Consumes: Task 7; plan 4 `DistinctValue`, `CrossBucketOrdinalMap`, `PatchedValue`, `CollectorReservations`, `FacetExecutionStats`; `LongHashSet` (`utility/LongHashSet.java`); `DistinctNumericKey.canonicalize` (`function/DistinctNumericKey.java:105`).
- Produces: `DistinctFacetCollector(DistinctFacetSpec spec, ColumnDefinition column, long matchCount, DatabaseInternal db, String declaringType, long budgetWaitMs)`, same shape as Task 6: one per (facet, declaring type), bucket, snapshot and map through `beginBucket`, a null column accepted (validation only, empty result).
- Produces: JSON `{"type": "distinct", "source": "index", "value": 1234, "exact": true}` (plan 4's writer); `FacetOracle.distinctCount(Database, String type, String property, String where)` = `SELECT count(DISTINCT p)` for single-valued, `SELECT count(*) FROM (SELECT p FROM T WHERE w UNWIND p) GROUP BY p` row count for lists.

- [ ] **Step 1: Write the failing unit test.**

```java
@Test void ordinalBitsetCountsEachValueOnce()            // ordinals {1,1,5,63,64}: 4, exact
@Test void sameValueInTwoBucketsCountsOnce()            // 2-bucket CrossBucketOrdinalMap, "Nike" in both with different bucket ordinals: 1
@Test void listColumnCountsDistinctItems()               // docs [a,b],[b,c]: 3
@Test void missingAndNullAreNotValues()                  // like count(DISTINCT x), which skips null
@Test void patchedValueWithoutOrdinalIsCounted()         // collectPatched(pos, PatchedValue.ofValues(dict, "new")): +1, and an "existing" value not double counted (mergedOf)
@Test void numericExactByDefault()                       // 200k distinct longs, no PRECISION: value 200000, exact true
@Test void integralDoubleAndLongAreOneValue()            // 1L and 1.0d and -0.0d/0L: canonical like DistinctNumericKey
@Test void precisionThresholdSwitchesToHll()             // PRECISION 1000 over 100k values: exact false, within 3%
@Test void budgetRefusalWithoutPrecisionIsUnavailable() // reservation refused, no PRECISION: source "unavailable", reason "memory budget"
@Test void budgetRefusalWithPrecisionSwitchesToHll()     // exact false
@Test void mergeAcrossTasksExactAndHll()                 // exact+exact, exact+hll, hll+hll
@Test void validationOnlyCollectorFinishesEmpty()        // collectorFor(spec, null, null, 0): release() and finish() give an empty result, no exception
@Test void deletedAndUpdatedRowsAreNotCounted()          // segment with dead rows (overlay view with a delete and an update): counts equal the oracle
@Test void mergeAcrossDeclaringTypesByValue()            // two declaring types with different maps, "Nike" in both: 1
@Test void missingIsWritten()                            // ofDistinct(source, value, missing): missing equals count(*) - count(p)
```

- [ ] **Step 2: Write the failing integration test** `DistinctFacetIntegrationTest extends TestHelper`, `ColumnarTestData` 5000 assets, seeds {1, 7, 42}:

```java
@ParameterizedTest @ValueSource(longs = { 1L, 7L, 42L })
void distinctEqualsCountDistinct(final long seed) {
  for (final String p : List.of("brand", "author", "tags", "fileSize"))
    assertThat(facet("SELECT FROM Asset WHERE fileSize > 500 FACETS d: " + p + " DISTINCT", "d").distinct())
        .isEqualTo(new DistinctValue(FacetOracle.distinctCount(database, "Asset", p, "fileSize > 500"), true));
}
@Test void lagging()                                    // under suspendApply(): 300 inserts and 100 deletes, still equal
@Test void multiBucketTypeEqualsCountDistinct()          // ColumnarTestData.buckets = 3: brand, tags, fileSize distinct equal FacetOracle.distinctCount
@Test void approximateIsFlaggedInMeta()                 // fileSize DISTINCT PRECISION 10: facets.meta.exact false
@Test void scannedBucketEqualsOracle()                  // the type's projection made unusable (plan 3 rebuild held by ColumnarTestHooks.killPoint "rebuild.scanned"): facet source "scan", values equal the oracle
```

- [ ] **Step 3: Run to verify they fail**: `mvn -o -pl engine -am test -Dtest='DistinctFacet*Test' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.

- [ ] **Step 4: Implement.** Dictionary columns: `long[] bits` over merged ordinals sized from `CrossBucketOrdinalMap.size()` at task start (bucket ordinals translated with `merged(bucketId, ord)`), grown when a patched value maps to a newer ordinal; values with no ordinal into a `HashSet<Object>`; result `Long.bitCount` sum plus set size; dictionary positions come through `column.forEachOrdinal(matches, consumer)` (live rows only); merge ORs bitsets when both collectors share the map (same `version()`), else folds the other's values (through its map's `value(m)`) into a charged value set. A scanned bucket (`beginBucket(b, null, null)`) delivers values through `collectPatched` only. Numeric columns visit `matches.forEachAnd(segment.live(), ...)`. A DECIMAL property (scan path only) canonicalizes like `DistinctNumericKey`. Numeric columns: two `LongHashSet`s (integral canonical values, `Double.doubleToLongBits` of non-integral canonical doubles); reservation of `16 bytes * size` taken in steps of 64k entries; when `size > precisionThreshold` or a step is refused, pour both sets into a `HyperLogLog` (integral values hashed as is, double bits XORed with a fixed salt so they cannot collide with the integral domain) and continue there. Without PRECISION a refused step answers `unavailable` (`memory budget`), never the HLL. The HLL's 16 KB are reserved before it is allocated. `stats()` returns the collector's `FacetExecutionStats` (merged collectors added), as `TermsCollector`.

- [ ] **Step 5: Run to verify they pass** (Step 3 command). Expected: PASS.

- [ ] **Step 6: Stop: changes ready for review** - `DistinctFacetCollector.java`, `FacetExecutor.java`, `FacetFallbackScanner.java`, `FacetOracle.java`, `FacetsExplainTest.java`, both tests.

---

### Task 9: STATS facet collector and public NumericSum

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/function/sql/math/NumericSum.java` (`:37`, package-private `final class`)
- Modify: `engine/src/main/java/com/arcadedb/function/sql/math/SQLFunctionAverage.java` (`computeAverage` at `:141`)
- Create: `engine/src/main/java/com/arcadedb/query/facet/StatsFacetCollector.java`
- Modify: plan 4's `FacetExecutor.collectorFor` (case before the `default` branch; the stats result carries `withValueType(propertyType)`); `FacetOracle.java` (`stats`). Consumed from plan 4 Task 1: `FacetStats`, `FacetResult.ofStats/stats()`
- Modify: `engine/src/main/java/com/arcadedb/query/facet/FacetFallbackScanner.java` (STATS read by declared type on scanned buckets, as today; routing reviewed with Tasks 6 and 8)
- Modify: `engine/src/main/java/com/arcadedb/query/facet/FacetResult.java` (stats min/max and the AFTER cursor through the encoder and decoder), `grpc/src/main/proto/arcadedb-server.proto`, `grpc/src/main/java/com/arcadedb/server/grpc/FacetProtocol.java` (typed `GrpcValue` `after`, `min`, `max` as additive fields 9-11 on `GrpcFacetResult`, `has_after` 12)
- Test: `engine/src/test/java/com/arcadedb/query/facet/StatsFacetCollectorTest.java`, `StatsFacetIntegrationTest.java`; existing `engine/src/test/java/com/arcadedb/function/sql/math/Issue9496NumericSumTest.java`, `Issue5906SumAverageIntegerOverflowTest.java`

**Interfaces:**
- Produces:
```java
public final class NumericSum {                          // was package-private
  public NumericSum();
  public void add(Number value);                         // unchanged behaviour
  public void addInt(int value); public void addLong(long value); public void addDouble(double value); // no boxing, same transitions as add()
  public Number get(); public double doubleValue(); public boolean isDecimal();
  public void merge(NumericSum other);                   // other's total folded in as add(other.get()) when not empty
}
public static Object SQLFunctionAverage.average(NumericSum sum, long count); // what getResult() returns for these inputs: null for count 0, the double fast path unless isDecimal(), else computeAverage (made static, long total) with decimalInput false
StatsFacetCollector(StatsFacetSpec spec, ColumnDefinition column, long matchCount, DatabaseInternal db, long budgetWaitMs); // shape of Task 6 minus the declaring type
// plan 4: public record FacetStats(long count, long missing, Object min, Object max, Number sum, Object avg)
// FacetOracle
static FacetStats stats(Database db, String type, String property, String where); // count(p), min(p), max(p), sum(p), avg(p), count(*) - count(p)
```
JSON: `{"source": "index", "count": 10, "missing": 2, "min": 1, "max": 99, "sum": 500, "avg": 50.0}`; `min`/`max` in the Java type a row of the property holds (`Integer` for INTEGER, `Float` for FLOAT, the date object GROUP BY returns for DATE/DATETIME, converted from the column's epoch with its unit as `TermsCollector` converts date terms), encoded by every transport as a row's value; `sum`/`avg` as `sum()`/`avg()` produce them, `null` for dates and when `count == 0`.

- [ ] **Step 1: Write the failing unit tests.**

```java
// StatsFacetCollectorTest
@Test void basicStatsSkipMissingAndNull()
@Test void integerSumWidensLikeSqlSum()          // INTEGER values summing past Integer.MAX_VALUE: sum is a Long equal to SQL sum()
@Test void longOverflowBecomesBigDecimal()        // Long.MAX_VALUE + 1: BigDecimal 9223372036854775808
@Test void emptyMatchGivesCountZeroAndNulls()
@Test void dateColumnHasNoSumOrAvg()
@Test void segmentWithNoMatchesIsNotDecoded()     // counting reader wrapper: zero longValue() calls for that segment
@Test void mergeOfTasks()
@Test void validationOnlyCollectorFinishesEmpty() // collectorFor(spec, null, null, 0): release() and finish() give an empty result, no exception
@Test void deletedAndUpdatedRowsAreNotCounted()   // segment with dead rows (overlay view with a delete and an update): counts equal the oracle
@Test void negativeDoublesDoNotSkipByBounds()     // DOUBLE view with negative values: min/max equal SQL min()/max()
// NumericSum: extend Issue9496NumericSumTest
@Test void primitiveAddersMatchBoxedAdd()         // random mix of int/long/double sequences: get() equals the boxed add() path, including class
```

- [ ] **Step 2: Write the failing integration test** `StatsFacetIntegrationTest extends TestHelper` (seeds {1, 7, 42}): `facet("... FACETS s: fileSize STATS", "s").stats()` equals `FacetOracle.stats(...)` for `fileSize` (LONG) exactly, for a DOUBLE property `score` with `min`/`max`/`count`/`missing` exact and `sum`/`avg` `isCloseTo(expected, withinPercentage(1e-9))` (addition order differs), and for `publishDate` (min/max as dates); a FLOAT property `ratio` (sum, avg, min, max equal `SELECT sum(ratio), avg(ratio), min(ratio), max(ratio)` including class); plus a lagging case under `suspendApply()`. `ColumnarTestData` with `doubleColumn = true` and `dateColumn = true`; the test adds `ratio FLOAT` with a COLUMNAR index. Also `@Test void scannedBucketEqualsOracle()` (the type's projection made unusable, plan 3 rebuild held by `ColumnarTestHooks.killPoint "rebuild.scanned"`: facet source "scan", values equal the oracle) and `@Test void microAndNanoDatetimeMinMaxMatchSql()` (DATETIME_MICROS/NANOS min/max equal SQL `min()`/`max()` on the index and the scan path, or STATS on those types is refused on the scan path with a message naming the type).

- [ ] **Step 3: Run to verify they fail**: `mvn -o -pl engine -am test -Dtest='StatsFacet*Test,Issue9496NumericSumTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.

- [ ] **Step 4: Implement** the `NumericSum` changes (the existing `add` delegates to the primitive adders for `Integer`/`Long`/`Double`) and `SQLFunctionAverage.average`, then the collector: `long count, missing`, `long minL/maxL` or `double minD/maxD`, one `NumericSum`; per segment `matches.andCardinality(segment.live()) == 0` skips; rows are visited with `matches.forEachAnd(segment.live(), ...)`; on a LONG column whose view holds a value (`minLong != Long.MAX_VALUE`) and whose `[minLong, maxLong]` lies inside `[minL, maxL]` the loop only feeds the sum and the count; DOUBLE views never skip by bounds; INTEGER/SHORT/BYTE-declared properties feed `addInt`, LONG `addLong`, DOUBLE `addDouble`, FLOAT `add(Float.valueOf((float) v))` (the boxed path `sum()` takes). A scanned bucket (`beginBucket(b, null, null)`) delivers values through `collectPatched` only. `stats()` returns the collector's `FacetExecutionStats` (merged collectors added), as `TermsCollector`. `FacetResult.toJSON` encodes min/max through the encoder and `fromJSON` decodes them; `FacetProtocol` writes them as typed `GrpcValue`.

- [ ] **Step 5: Run to verify they pass** (Step 3 command). Expected: PASS. Then the aggregate regression set: `mvn -o -pl engine -am test -Dtest='Issue5906SumAverageIntegerOverflowTest,SQLFunctionSumAverageMultiArgTest,Issue9496*Test,GroupByExecutionTest,FacetResultsJsonTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS. The proto change is pinned in Task 14 (`FacetProtocolTest`, `FacetsGrpcIT`).

- [ ] **Step 6: Stop: changes ready for review** - `NumericSum.java`, `SQLFunctionAverage.java`, `StatsFacetCollector.java`, `FacetExecutor.java`, `FacetFallbackScanner.java`, `FacetResult.java`, `arcadedb-server.proto`, `FacetProtocol.java`, `FacetOracle.java`, `FacetsExplainTest.java`, the tests.

---

### Task 10: Dictionary prefix search and the AFTER cursor

**Files:**
- Create: `engine/src/main/java/com/arcadedb/index/columnar/DictionaryPrefixSearch.java`
- Modify: plan 4's `TermsCollector` (selection only; `FacetResult.withAfter/after()` exist in plan 4 Task 1)
- Test: `engine/src/test/java/com/arcadedb/index/columnar/DictionaryPrefixSearchTest.java`, `engine/src/test/java/com/arcadedb/query/facet/TermsAfterCursorTest.java`

**Interfaces:**
- Consumes: `GlobalDictionary.sortedOrdinals()`, `rank(int)`, `valueOf(int)`, `size()` of `ColumnarSnapshot.dictionary(property)`; plan 4 `CrossBucketOrdinalMap`, `FacetValueComparator`.
- Produces:
```java
public final class DictionaryPrefixSearch {
  // ordinals whose value starts with prefix, in value order, at most max; ordinals newer than the rank view merged in order
  // resumes after a value, not a rank: unranked ordinals share a gap rank
  public static int search(GlobalDictionary dictionary, String prefix, String afterExclusiveOrNull, int max, int[] out);
  // gap rank 2p of the first view entry strictly greater than value (value need not be in the dictionary); 0 without a rank view
  public static int rankAfter(GlobalDictionary dictionary, Object value);
}
```
`FacetResult.after()` = last returned value, `null` when no value follows; JSON key `"after"`.

- [ ] **Step 1: Write the failing tests.**

```java
// DictionaryPrefixSearchTest (dictionary filled through plan 3's writer API, rank view rebuilt, then 3 more values added unranked)
@Test void prefixRangeInValueOrder()               // "run": [run, runner, running] not "rum"
@Test void unrankedTailIsMergedInOrder()           // "runa" added after rank build appears between run and runner
@Test void emptyPrefixReturnsFirstMax()
@Test void regexCharactersAreLiteral()             // "a.*" matches only values starting with "a.*"
@Test void rankAfterValueNotInDictionary()         // rankAfter("rum2") == rank("run") - 1 (the gap before run)
// TermsAfterCursorTest extends TestHelper, 26 brands "a".."z" with counts
@Test void pagesThroughAllValuesInValueOrder()     // TOP 10 AFTER loop: concatenated pages == SELECT brand ... GROUP BY brand ORDER BY brand
@Test void cursorNotInDictionaryStartsAtLowerBound() // AFTER 'cz' -> first bucket "d" (Review Focus 4)
@Test void lastPageHasNullAfter()
@Test void minCountStillApplies()
@Test void afterOnNumericTermsComparesNumerically() // fileSize buckets AFTER 1000: only > 1000, ascending
@Test void multiBucketPagesInValueOrder()          // ColumnarTestData.buckets = 3, brands spread over buckets: AFTER loop pages == GROUP BY brand ORDER BY brand, each brand once
@Test void unrankedTailPagesInValueOrder()         // 1100 brands, rank view built, 30 more appended: the AFTER loop equals GROUP BY brand ORDER BY brand
@Test void dateCursorEchoedAsString()              // DATETIME terms: the returned cursor echoed back as a formatted string pages on without skip or repeat
@Test void afterWithIncludeOrDescIsRefused()       // "AFTER and INCLUDE cannot be combined", "AFTER pages in ascending value order: remove ORDER BY VALUE DESC"
@Test void missingOnlyOnFirstPage()                // MISSING bucket returned only on the page without AFTER
```

- [ ] **Step 2: Run to verify they fail**: `mvn -o -pl engine -am test -Dtest='DictionaryPrefixSearchTest,TermsAfterCursorTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.

- [ ] **Step 3: Implement.** Prefix: lower bound by binary search over `sortedOrdinals()` comparing `valueOf(ord)` to `prefix` with `String.compareTo`, walk while `startsWith`; the unranked tail (`size() > sortedOrdinals().length`) is scanned linearly, filtered and sorted (bounded by growth since the last rank rebuild), and merged. AFTER in the terms collector: counting unchanged; single-type dictionary selection keeps ordinals with `rank(ord) > rankAfter(after)`, or equal rank and value greater than the cursor (`FacetValueComparator`), and orders by `GlobalDictionary.compare`; `rankAfter` is the gap rank `2p` of the first view entry strictly greater than the cursor (0 when the dictionary has no rank view); patched values without ordinal are compared by value; takes `top`; on every other path (merged map, foreign value map, merge across declaring types, LONG/DOUBLE terms compared numerically, date terms on their epoch after converting the cursor) the same predicate filters before ranking, ordered by the same comparator (bounded heap of `top`), so no rank is needed across buckets. Numeric terms: keep keys `> after` compared as numbers (double keys are bit patterns: decode before comparing) and sort ascending. The cursor is converted as INCLUDE literals are (plan 4 ruling 97) before it is compared.

- [ ] **Step 4: Run to verify they pass** (Step 2 command). Expected: PASS. Then `mvn -o -pl engine -am test -Dtest='Terms*Test' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS.

- [ ] **Step 5: Stop: changes ready for review** - `DictionaryPrefixSearch.java`, `TermsCollector.java`, both tests.

---

### Task 11: facet.searchValues SQL function

**Files:**
- Create: `engine/src/main/java/com/arcadedb/function/sql/facet/SQLFunctionFacetSearchValues.java`
- Modify: `engine/src/main/java/com/arcadedb/function/sql/DefaultSQLFunctionFactory.java` (new `// Facets (facet.* namespace)` block after the `ts.*` block at `:295-305`)
- Test: `engine/src/test/java/com/arcadedb/function/sql/facet/SQLFunctionFacetSearchValuesTest.java`

**Interfaces:**
- Consumes: Task 10; `ColumnarIndexManager.projection(bucketId).acquire().dictionary(property)` for every readable polymorphic bucket (snapshots closed after the search); plan 4 `FACETS ... TERMS INCLUDE [...]` through `database.query("sql", ...)` with parameters.
- Produces: `public class SQLFunctionFacetSearchValues extends SQLFunctionAbstract { public static final String NAME = "facet.searchValues"; }`, 4 arguments `(type, property, prefix, limit)`, returns `List<Map<String, Object>>` of `{"value": v, "count": n}` in value order.

- [ ] **Step 1: Write the failing test** `SQLFunctionFacetSearchValuesTest extends TestHelper`, assets with tags `run`, `runner`, `running`, `rum`, and a tag `runaway` whose only document was deleted.

```java
@Test void returnsLiveValuesWithCountsInValueOrder() {
  final List<Result> rows = toList(database.query("sql", "SELECT expand(facet.searchValues('Asset', 'tags', 'run', 20))"));
  assertThat(rows).extracting(r -> r.getProperty("value")).containsExactly("run", "runner", "running"); // runaway gone
  assertThat(rows.get(0).<Long>getProperty("count")).isEqualTo(FacetOracle.expectedCounts(database, "Asset", "tags", null).get("run")); // plan 3 oracle, no WHERE
}
@Test void limitStopsAtLimitLiveValues()      // limit 2: [run, runner]
@Test void edgeInputs()                       // Review Focus 5: '' prefix -> first values; 'a.*' -> []; limit 0 -> []; limit -1 -> IllegalArgumentException "limit must be >= 0";
                                              // 'Nope' type -> CommandExecutionException naming it; 'brand' without COLUMNAR -> "no COLUMNAR index on Asset.brand"
@Test void limitIsCappedByTopMax()            // limit 5000 > COLUMNAR_TOP_MAX: at most COLUMNAR_TOP_MAX rows
@Test void respectsBucketSecurity()           // user without READ on a subtype bucket: counts exclude it, and its dictionary is not searched
@Test void multiBucketValuesAreMergedOnce()    // ColumnarTestData.buckets = 3, 'run' and 'runner' in several buckets: each value once, in value order, count = FacetOracle.expectedCounts
@Test void zeroCountCandidatesAreDropped()     // INCLUDE returns count 0 for 'runaway': not in the rows
@Test void projectionNotReadyFailsWithRetry()  // create rebuild held (killPoint "rebuild.scanned"): CommandExecutionException "facet.searchValues: the projection of bucket <name> is not ready, retry"
```

- [ ] **Step 2: Run to verify it fails**: `mvn -o -pl engine -am test -Dtest=SQLFunctionFacetSearchValuesTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.

- [ ] **Step 3: Implement.** Resolve the type's polymorphic buckets with a COLUMNAR index on the property and READ access (dictionaries are per bucket, plan 3). Loop: per bucket `DictionaryPrefixSearch.search(snapshot.dictionary(property), ..., batch = max(limit * 2, 16))`, k-way merge of the per-bucket results by `String.compareTo` with duplicates dropped, each bucket resuming after the last value it returned, run `SELECT FROM <type> FACETS v: <property> TERMS INCLUDE [:v0, :v1, ...] LIMIT 1` (INCLUDE returns every listed value, count 0 included: values with count 0 are dropped here) (rows unused) with one named parameter per candidate value (type and property names validated against the schema, never concatenated from unchecked input), keep values with counts in value order, continue after the last value until `limit` rows or the range is exhausted, at most 8 batches (then return what was found). Register with `register(SQLFunctionFacetSearchValues.NAME, new SQLFunctionFacetSearchValues());`. A readable bucket whose `acquire()` throws or whose snapshot does not `publishes(property)` fails the call: `"facet.searchValues: the projection of bucket <name> is not ready, retry"`.

- [ ] **Step 4: Run to verify it passes** (same command). Expected: PASS. Then `mvn -o -pl engine -am test -Dtest='DefaultSQLFunctionFactory*Test,SQLFunctionTimeBucket*Test' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS.

- [ ] **Step 5: Stop: changes ready for review** - `SQLFunctionFacetSearchValues.java`, `DefaultSQLFunctionFactory.java`, the test.

---

### Task 12: GROUP BY acceleration, eligibility and EXPLAIN

**Files:**
- Create: `engine/src/main/java/com/arcadedb/query/sql/executor/ColumnarGroupByPlanner.java`
- Create: `engine/src/main/java/com/arcadedb/query/sql/executor/ColumnarGroupByStep.java` (skeleton: planning, `prettyPrint`, `copy`, fallback-only `syncPull`)
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/SelectExecutionPlanner.java` (between `:445` and `:449`, after `chainFacetCollect` at `:447`; `whereless` captured in `init`)
- Modify: `engine/src/main/java/com/arcadedb/index/columnar/ColumnarStats.java` (`groupByAccelerated()` / `groupByFallbacks()`, Micrometer `arcadedb.columnar.groupby.accelerated`, `arcadedb.columnar.groupby.fallbacks`)
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` (`COLUMNAR_GROUP_BY_ACCELERATION` next to plan 3/4 columnar keys)
- Test: `engine/src/test/java/com/arcadedb/query/sql/executor/ColumnarGroupByEligibilityTest.java`

**Interfaces:**
- Consumes: `QueryPlanningInfo` fields (`groupBy`, `whereClause`, `perRecordLetClause`, `globalLetPresent`, `unwind`, `expand`, `distinct`, `orderByAggregateAdded`, `projectionAfterOrderBy`, `limit`, `orderBy`, `target`); `statement.getProjection()` (the unsplit projection, as `tryTimeSeriesAggregationPushDown` uses at `:4003`); `DocumentType.getPolymorphicColumnarIndex(String)`; `FunctionCall.isDistinct()`; `info.distinctRewrittenAsGroupBy`.
- Produces:
```java
final class ColumnarGroupByPlanner {
  static ColumnarGroupByPlan analyze(Projection statementProjection, QueryPlanningInfo info, CommandContext context); // null = ineligible
}
record ColumnarGroupByPlan(String typeName, int[] bucketIds, String groupProperty, ColumnKind groupKind, Type groupType,
    String[] outputAliases, AggregateSpec[] aggregates /* null entry = the group key column */, boolean whereless) {}
record AggregateSpec(Kind kind, String property, ColumnKind columnKind, Type propertyType) { enum Kind { COUNT_STAR, COUNT, MIN, MAX, SUM, AVG, COUNT_DISTINCT } }
public class ColumnarGroupByStep extends AbstractExecutionStep {
  ColumnarGroupByStep(ColumnarGroupByPlan plan, SelectExecutionPlan fallback, CommandContext context);
  String lastPath();                  // "columnar" | "normal: <reason>" | null before execution
}
```

Eligible shape, all required:

| Clause | Rule |
|---|---|
| Target | one schema type identifier (not `$var`, index, subquery, RIDs, bucket), not a `LocalTimeSeriesType` |
| GROUP BY | exactly one bare property `p`; `target.getPolymorphicColumnarIndex(p)` exists and its buckets cover every polymorphic bucket of the target; `p` declared with type STRING (ORDINAL) or INTEGER/LONG/SHORT/BYTE (LONG kind); not a list; `conversionFailures(type, p) == 0` |
| Projection | each item is `p` (optionally aliased) or an aggregate with no modifier and no arithmetic: `count(*)`, `count(q)`, `min(q)`, `max(q)` with `q` COLUMNAR and declared INTEGER/LONG/SHORT/BYTE/DOUBLE/FLOAT; `sum(q)`/`avg(q)` with `q` INTEGER/LONG; `count(DISTINCT q)` with `q` STRING |
| Other | no DISTINCT, expand, UNWIND, per-record or global LET; `LIMIT`/`SKIP` only with `ORDER BY` (the aggregation limit push-down at `SelectExecutionPlanner.java:1118` picks scan-order groups); ORDER BY only on projected aliases (`projectionAfterOrderBy == null`, `!orderByAggregateAdded`); no FACETS clause; not a DISTINCT rewritten as GROUP BY (`info.distinctRewrittenAsGroupBy`) |

- [ ] **Step 1: Write the failing test** `ColumnarGroupByEligibilityTest extends TestHelper`, `Asset` with COLUMNAR on `brand`, `author`, `tags`, `fileSize`, `score DOUBLE`, plus `rating INTEGER` without index. `ColumnarTestData` with `doubleColumn = true`; the test adds `rating INTEGER` (no index), `count INTEGER` with a COLUMNAR index and `ratio FLOAT` with a COLUMNAR index.

```java
@ParameterizedTest @ValueSource(strings = {
  "SELECT brand, count(*) FROM Asset GROUP BY brand",
  "SELECT brand AS b, count(*) AS c, sum(fileSize), avg(fileSize), min(score), max(score), count(fileSize) FROM Asset GROUP BY brand",
  "SELECT author, count(DISTINCT brand) FROM Asset GROUP BY author",
  "SELECT fileSize, count(*) FROM Asset WHERE brand = 'b1' GROUP BY fileSize",
  "SELECT brand, count(*) AS c FROM Asset GROUP BY brand ORDER BY c DESC LIMIT 5" })
void eligibleShapesShowColumnarStep(final String sql) {
  assertThat(explain(sql)).contains("AGGREGATE FROM COLUMNAR PROJECTION").contains("fallback: CALCULATE AGGREGATE PROJECTIONS");
}
@ParameterizedTest @ValueSource(strings = {
  "SELECT tags, count(*) FROM Asset GROUP BY tags",                       // list
  "SELECT brand.toLowerCase(), count(*) FROM Asset GROUP BY brand.toLowerCase()",
  "SELECT brand, count(*) + 1 FROM Asset GROUP BY brand",
  "SELECT brand, sum(score) FROM Asset GROUP BY brand",                   // DOUBLE sum
  "SELECT rating, count(*) FROM Asset GROUP BY rating",                   // no COLUMNAR
  "SELECT brand, author, count(*) FROM Asset GROUP BY brand, author",
  "SELECT brand, count(*) FROM Asset GROUP BY brand LIMIT 3",             // LIMIT without ORDER BY
  "SELECT brand, count(*) FROM Asset LET $x = 1 GROUP BY brand",
  "SELECT brand, count(*) FROM (SELECT FROM Asset) GROUP BY brand",
  "SELECT DISTINCT brand, count(*) FROM Asset GROUP BY brand",
  "SELECT brand, count(*) FROM Asset GROUP BY brand FACETS b: brand TERMS",
  "SELECT DISTINCT brand FROM Asset" })                                   // with a type above the rewrite threshold
void ineligibleShapesKeepTheNormalPlan(final String sql) { assertThat(explain(sql)).doesNotContain("COLUMNAR PROJECTION"); }
@Test void subtypeBucketWithoutIndexIsIneligible()
@Test void fallbackOnlySkeletonReturnsNormalRows()   // until Task 13: rows equal the forced-normal plan, lastPath() starts with "normal"
```

`explain(sql)` = `database.query("sql", "EXPLAIN " + sql).getExecutionPlan().get().prettyPrint(0, 2)`.

- [ ] **Step 2: Run to verify it fails**: `mvn -o -pl engine -am test -Dtest=ColumnarGroupByEligibilityTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL.

- [ ] **Step 3: Implement the planner hook** as a `SelectExecutionPlanner` instance method `tryColumnarGroupBy(SelectExecutionPlan plan, QueryPlanningInfo info, CommandContext context)`, called right before `handleProjectionsBlock(selectExecutionPlan, info, context)` at `:449` (after `chainFacetCollect`); `whereless` is captured in `init` from `info.whereClause` after the always-true drop, before `optimizeQuery` (as plan 4's `facetsWithoutWhere`, `:260`), because an index search may consume `info.whereClause` later (only on the non-empty branch; `foldEmptyByConstruction` is untouched). When `analyze` returns a plan: build `final SelectExecutionPlan fallback = new SelectExecutionPlan(context, 0); handleProjections(fallback, info, context);` (this chains the pre-aggregate projection, `AggregateProjectionCalculationStep` and the post-aggregate projection exactly as today, and sets `info.projectionsCalculated`, so `handleProjectionsBlock` does not chain them again), then `plan.chain(new ColumnarGroupByStep(columnarPlan, fallback, context))`. ORDER BY, SKIP and LIMIT are still chained by `handleProjectionsBlock` after it.

- [ ] **Step 4: Implement the skeleton step.** `syncPull` (fallback only in this task): on first call `((AbstractExecutionStep) fallback.getSteps().getFirst()).setPrevious(prev)` and delegate to the last fallback step. `prettyPrint`:

```
+ AGGREGATE FROM COLUMNAR PROJECTION (Asset.brand)
    aggregates: count(*), sum(fileSize)
    match set: all records | from upstream
    fallback: CALCULATE AGGREGATE PROJECTIONS
```
After execution (PROFILE) a line `path: columnar (segments N, patched M)` or `path: normal (reason: <reason>)`, and when the path was normal the fallback chain printed indented below. `copy()` copies the plan record and `fallback.copy(context)`. `canBeCached()` true: the decision is taken per execution. Add `COLUMNAR_GROUP_BY_ACCELERATION("arcadedb.columnar.groupByAcceleration", SCOPE.DATABASE, "Answer eligible GROUP BY queries from the COLUMNAR projection", Boolean.class, true)`. The step reads the switch with `context.getDatabase().getConfiguration().getValueAsBoolean(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION)` at each execution, and counts its path in `ColumnarStats.groupByAccelerated()` / `groupByFallbacks()`.

- [ ] **Step 5: Run to verify it passes** (Step 2 command). Expected: PASS. Then planner regressions: `mvn -o -pl engine -am test -Dtest='GroupByExecutionTest,GroupByStepTest,Issue6924GroupByMultiKeyAliasTest,Issue8973OrderByAggregateTest,Issue8976HardwiredAggregateProjectionTest,Issue8523ParallelScanAggregationTest,AggregateProjectionCalculationStepTest,SelectStatementExecutionTest' -Dsurefire.failIfNoSpecifiedTests=false`. Expected: PASS.

- [ ] **Step 6: Stop: changes ready for review** - `ColumnarGroupByPlanner.java`, `ColumnarGroupByStep.java`, `SelectExecutionPlanner.java`, `GlobalConfiguration.java`, `ColumnarStats.java`, the test.

---

### Task 13: GROUP BY acceleration, execution and equivalence

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/query/sql/executor/ColumnarGroupByStep.java`
- Modify: `engine/src/main/java/com/arcadedb/query/facet/FacetPatcher.java` (`recordAt` made public)
- Test: `engine/src/test/java/com/arcadedb/query/sql/executor/ColumnarGroupByAccelerationTest.java`

**Interfaces:**
- Consumes: Task 12; `ColumnarIndexManager.projection(bucketId).acquire()` snapshot (`segments()`, `pendingPositions()`, `isUsable()`, `publishes(property)`, `publishedColumn(property)`, `dictionary(property)`; `acquire()` throws `IllegalStateException` when closed or dropped), `ColumnReader`, plan 4 `CrossBucketOrdinalMap` (`merged`, `value`, `mergedOf`, `size`; null when the budget refuses, `AutoCloseable`), plan 4 `MatchSet.fromProjection(db, bucketIds, waitMs)`, plan 4 `FacetPatcher.recordAt` (made public by this task), `FacetMemoryBudget.reserve`, plan 4 `TransactionContext.hasChangesInFiles`, `NumericSum` and `SQLFunctionAverage.average` (Task 9), `DatabaseInternal.checkPermissionsOnFile(int, ACCESS.READ_RECORD)` (`LocalDatabase.java:1078`), `IntIntHashMap`, `LongLongHashMap`, `IntHashSet` (`utility/`).
- Produces: rows with the statement projection's aliases, same Java types as the normal plan.

Execution decision, in order, each failing check giving `normal: <reason>`:
1. `COLUMNAR_GROUP_BY_ACCELERATION` false: `"disabled by arcadedb.columnar.groupByAcceleration"`.
2. `checkPermissionsOnFile(bucketId, READ_RECORD)` for every bucket: throws exactly as the normal scan would.
3. The active transaction `hasChangesInFiles` for any bucket: `"the transaction modified bucket <name>"` (spec 7.3).
4. Any bucket with no projection, an `acquire()` that throws, `!isUsable()`, or a group/aggregate property its snapshot does not `publishes` with the schema's encoding (`publishedColumn(p).sameEncoding(schema column)`): `"projection of bucket <name> is not usable"`. A null `CrossBucketOrdinalMap.of`: `"query memory budget not granted"`.
5. Budget reservation for the group arrays refused within `maxWaitMs = 0`: `"query memory budget not granted"`.

Accelerated path:
- **Match set**: `whereless` -> `MatchSet.fromProjection(db, bucketIds, 0)` (live plus pending trimmed to occupied slots, charged; refused budget: normal plan); positions on pending pages are read through `FacetPatcher.recordAt` (made public), raw values (what the normal plan groups on). With WHERE → drain upstream rows, positions from `getIdentity()` into one `PositionBitmap` per bucket (plan 3 class); rows on pending positions take their values from the row's own record, not a re-read.
- **Groups**: ORDINAL key → dense arrays indexed by merged ordinal sized `CrossBucketOrdinalMap.size()` (bucket ordinals through `merged(bucketId, ord)`; a single-bucket type gets the identity map), plus a `HashMap<Object, Integer>` for patched values with no ordinal (`mergedOf` first); LONG key → `LongLongHashMap` key to group index; the null group(s) the normal plan's `GroupByKey` forms (expected one for missing and null together; `missingAndNullFormOneNullGroup` decides).
- **Aggregates**: `long[] count`; COUNT(q) counts present non-null; MIN/MAX in `long[]`/`double[]` with a presence count; SUM/AVG lazily one `NumericSum` per group fed by `addInt`/`addLong`; COUNT_DISTINCT one `IntHashSet` of merged ordinals per group. Values boxed only when emitting: key and min/max through `Type.convert(db, value, groupType.getDefaultJavaType())`, count as `Long`, avg through `SQLFunctionAverage.average`.
- Groups with zero rows are not emitted.

- [ ] **Step 1: Write the failing test** `ColumnarGroupByAccelerationTest extends TestHelper` with `ColumnarTestData` (seeds {1, 7, 42}, 5000 assets, updates, deletes, slot reuse), `FacetOracle.awaitCaughtUp(database)` unless stated. `ColumnarTestData` with `doubleColumn = true`; the test adds `rating INTEGER` (no index), `count INTEGER` with a COLUMNAR index and `ratio FLOAT` with a COLUMNAR index.

```java
static final List<String> QUERIES = List.of( /* the five eligible shapes of Task 12 */ );

@ParameterizedTest @MethodSource("seedsAndQueries")
void identicalToForcedNormalPlan(final long seed, final String sql) {
  final List<Map<String, Object>> accelerated = rows(sql);
  assertThat(lastPath(sql)).isEqualTo("columnar");
  database.getConfiguration().setValue(GlobalConfiguration.COLUMNAR_GROUP_BY_ACCELERATION, false);
  final List<Map<String, Object>> normal = rows(sql);
  assertThat(accelerated).containsExactlyInAnyOrderElementsOf(normal);   // Map.equals: Integer vs Long would fail
}
@Test void orderedQueriesKeepOrder()               // ORDER BY c DESC, brand ASC LIMIT 5: containsExactlyElementsOf(normal)
@Test void identicalWhileLagging()                 // try (suspendApply()) { 500 inserts, 200 updates, 100 deletes; all QUERIES identical; PROFILE "patched" > 0 }
@Test void readYourOwnWritesFallsBack() {
  database.begin();
  database.newDocument("Asset").set("brand", "Z").save();
  assertThat(rows(Q1)).contains(Map.of("brand", "Z", "count(*)", 1L));
  assertThat(lastPath(Q1)).isEqualTo("normal: the transaction modified bucket " + bucketName);
  database.rollback();
  assertThat(lastPath(Q1)).isEqualTo("columnar");    // next execution outside the tx
}
@Test void unusableProjectionFallsBack()           // plan 3 rebuild hook (truncate or forced rebuild): normal path, identical rows
@Test void missingAndNullFormOneNullGroup()        // Review Focus 2: identical to the forced-normal plan (expected one null group; if the normal plan forms two, acceleration must too)
@Test void sumOverflowMatchesNormalPlan()          // Review Focus 3: INTEGER past int range (Long), LONG past long range (BigDecimal)
@Test void groupKeyTypeFollowsSchema()             // INTEGER key returns Integer, like the normal plan
@Test void permissionDeniedLikeNormalPlan()        // user without READ on Asset bucket: same exception class and message in both modes
@Test void cachedPlanObeysRuntimeToggle()          // same SQL twice without PROFILE, toggle between: getExecutionPlanCache().contains(sql) after the first, groupByAccelerated +1 then groupByFallbacks +1
@Test void multiBucketTypeIdenticalToNormalPlan()  // ColumnarTestData.buckets = 3: every QUERIES entry identical to the forced-normal plan, lastPath columnar; same brand in several buckets is one group
@Test void polymorphicTargetIdenticalToNormalPlan() // Image extends Asset, COLUMNAR on Asset (projects the Image buckets too): SELECT brand, count(*) FROM Asset GROUP BY brand identical, lastPath columnar
@Test void indexedWhereIsNotWhereless()             // LSM on author, WHERE author = 'x': identical rows
@Test void unpublishedColumnFallsBack()             // CREATE INDEX on a new property while the create rebuild is held (killPoint "rebuild.scanned"): lastPath "normal: ...", identical rows
@Test void floatMinMaxKeepFloat()                   // min(ratio), max(ratio) grouped by brand: Float, equal to the normal plan
```

`rows(sql)` maps each result with `toMap()`; `lastPath(sql)` runs `PROFILE <sql>` and finds the `ColumnarGroupByStep` in the plan.

- [ ] **Step 2: Run to verify it fails**: `mvn -o -pl engine -am test -Dtest=ColumnarGroupByAccelerationTest -Dsurefire.failIfNoSpecifiedTests=false`. Expected: FAIL (`lastPath` is `normal` everywhere).

- [ ] **Step 3: Implement** the decision list and the accelerated path above. Close every snapshot and the budget reservation in `finally` and in `close()`. Every bitmap, group array and per-group `NumericSum`/`IntHashSet` is charged to `FacetMemoryBudget` before allocation; a refusal falls back. The loops check `context` deadline and termination every 1024 rows (as `AggregateProjectionCalculationStep` honors TIMEOUT). Close the ordinal map with the snapshots. Default output aliases come from the projection items (`getProjectionAliasAsString`). Sequential per bucket and segment; parallel counting is a follow-up, not in this plan (tuning #10662). Record the patched counts of `identicalWhileLagging` in the Stop note for #10662 (page-granular pending marks).

- [ ] **Step 4: Run to verify it passes** (Step 2 command). Expected: PASS. Then Task 12's regression command plus `ColumnarGroupByEligibilityTest`. Expected: PASS.

- [ ] **Step 5: Stop: changes ready for review** - `ColumnarGroupByStep.java`, `FacetPatcher.java`, `ColumnarGroupByAccelerationTest.java`.

---

### Task 14: Transports: HTTP IT, OpenAPI, gRPC, RemoteDatabase and ndjson for the new facet kinds

Spec 8.3: "gRPC proto, OpenAPI, `RemoteDatabase` and `RemoteGrpcDatabase` are updated together", and every transport encodes a facet value as it encodes a row's value.

**Files:**
- Create: `server/src/test/java/com/arcadedb/server/http/FacetAdvancedHttpIT.java` (next to plan 4's `FacetsHttpIT`)
- Modify: `server/src/main/java/com/arcadedb/server/http/handler/openapi/CoreApiSpec.java`: `FacetBucket` gets `additionalProperties: true` (plan 4 open item) with `parent`/`depth`/`orphan` described as plan 5 defines them (orphan = a value with no vertex in the hierarchy, depth -1 for a node on an unrooted cycle); `FacetResult` stats min/max described as "a number or a date"
- Modify: `server/src/test/java/com/arcadedb/server/http/handler/openapi/FacetsApiSpecTest.java`, `grpc-client/src/test/java/com/arcadedb/remote/grpc/FacetProtocolTest.java`, `grpc-client/src/test/java/com/arcadedb/remote/grpc/FacetsGrpcIT.java`, `server/src/test/java/com/arcadedb/remote/RemoteDatabaseFacetsIT.java`, `server/src/test/java/com/arcadedb/server/http/handler/NdJsonResultStreamFacetsTest.java` (pin the new members: HIERARCHY attributes, DISTINCT `value`/`exact`, STATS row-typed `min`/`max`/`sum`/`avg`, typed `after`/`has_after`)

**Interfaces:**
- Consumes: `BaseGraphServerTest.executeCommand(int serverIndex, String language, String payloadCommand)` (`BaseGraphServerTest.java:794`), `command(int, String)` (`:758`), `getDatabaseName()`; plan 4's HTTP `facets` JSON (spec 8.3); Task 9's `FacetResult` encoder and `FacetProtocol` typed fields.

- [ ] **Step 1: Write the IT** `FacetAdvancedHttpIT extends BaseGraphServerTest`. Setup through `command(0, ...)`: vertex type `Tag(path STRING)`, edge `ParentOf`, document `Asset(brand STRING, tags LIST OF STRING, fileSize LONG)`, COLUMNAR indexes (tags with hierarchy METADATA), 300 assets and a 3-level tag tree; wait until a FACETS query reports `facets.meta.lagMs == 0` (Awaitility, `atMost(30, SECONDS)` as a hang detector).

```java
@Test void searchValuesOverHttp() {
  final JSONArray result = executeCommand(0, "sql", "SELECT expand(facet.searchValues('Asset', 'tags', 't/1', 5))").getJSONArray("result");
  assertThat(result.length()).isBetween(1, 5);
  assertThat(result.getJSONObject(0).getString("value")).startsWith("t/1");
  assertThat(result.getJSONObject(0).getLong("count")).isGreaterThan(0);
}
@Test void afterCursorPagesOverHttp()       // loop "FACETS b: brand TERMS TOP 3 AFTER <cursor>": union of pages equals GROUP BY brand rows; final "after" is JSON null
@Test void hierarchyDistinctStatsJsonShape() // buckets carry "parent" and "depth"; distinct {"value","exact":true}; stats keys count,missing,min,max,sum,avg
@Test void groupByAccelerationOverHttp()    // SELECT brand, count(*) ... GROUP BY brand equals the same with arcadedb.columnar.groupByAcceleration=false set via ALTER DATABASE
@Test void statsDatesAndCursorUseTheRowEncoding() // publishDate STATS min/max and a DATETIME TERMS AFTER cursor are written in the database's datetime format
```

- [ ] **Step 2: Run it**

Run: `mvn -o -pl server -am verify -DskipITs=false -Dit.test='FacetAdvancedHttpIT,RemoteDatabaseFacetsIT' -Dfailsafe.failIfNoSpecifiedTests=false -Dtest='FacetsApiSpecTest,NdJsonResultStreamFacetsTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: `Tests run: 5, Failures: 0, Errors: 0` for `FacetAdvancedHttpIT` in the failsafe `Results:` summary, the other classes green. If the server module shows a wall of 403s first check `lsof -nP -iTCP:2480-2489 -sTCP:LISTEN`.

Then gRPC: `mvn -o -pl grpc-client -am verify -DskipITs=false -Dit.test=FacetsGrpcIT -Dtest=FacetProtocolTest -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false`. Expected: PASS.

- [ ] **Step 3: Fix** whatever a transport drops. Known targets: stats min/max written raw by `FacetBucket.integral` (Task 9 routes them through the encoder), the gRPC `extensions_json` carrying `after`/min/max untyped (Task 9's typed fields), and the OpenAPI schema above. Rerun Step 2 until green.

- [ ] **Step 4: Stop: changes ready for review** - `FacetAdvancedHttpIT.java`, `CoreApiSpec.java`, the transport tests above, and any writer fix. The Stop note lists, for the Python bindings sub-issue #10666 (ArcadeData/arcadedb-python; nothing Python-side lands here): HIERARCHY bucket attributes `parent`/`depth`/`orphan`, DISTINCT `value`/`exact`, STATS `count`/`missing`/`min`/`max`/`sum`/`avg` (row-typed), the `after` cursor (typed, `has_after`), the new proto fields 9-12, and `facet.searchValues` rows. It also records, for the user docs of HIERARCHY/DISTINCT/STATS/AFTER, plan 4's note that facet aliases spelled `unwind` or `timeout` need back-ticks.

---

### Task 15: Connected regression run

**Files:** none new.

- [ ] **Step 1: Engine, everything this plan touched or could affect**

Run: `mvn -o -pl engine -am test -Dtest='*Facet*Test,Columnar*Test,Hierarchy*Test,AncestorExpanderTest,HyperLogLogTest,DictionaryPrefixSearchTest,TermsAfterCursorTest,Issue10450IndexBuildWithUnrelatedPendingWritesTest,CrossBucketOrdinalMapTest,SQLFacetsParserTest,FacetsKeywordsAsIdentifiersTest,FacetSpecCompilerTest,FacetResultsJsonTest,GroupBy*Test,Issue*GroupBy*Test,Issue9496*Test,Issue5906*Test,Issue8523ParallelScanAggregationTest,Issue8973OrderByAggregateTest,Issue8976HardwiredAggregateProjectionTest,AggregateProjection*Test,SelectStatementExecutionTest,SQLFunction*Test,*TimeSeries*Aggregat*Test,*Parser*Test' -Dsurefire.failIfNoSpecifiedTests=false -DexcludedGroups=benchmark`
Expected: `BUILD SUCCESS`, failures 0, from Maven's `Results:` summary (not the report files).

- [ ] **Step 2: Graph tests touched by the edge-bucket watch**: `mvn -o -pl engine -am test -Dtest='com.arcadedb.graph.*Test' -Dsurefire.failIfNoSpecifiedTests=false -DexcludedGroups=benchmark,slow`. Expected: `BUILD SUCCESS`.

- [ ] **Step 3: Server IT again** (Task 14 Step 2 commands, server and grpc-client), plus `mvn -o -pl ha-raft -am verify -DskipITs=false -Dit.test=FacetsFollowerQueryIT -Dtest=ParseResultSetFromJsonFacetsTest -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false` (Task 9 changed the `FacetResult` JSON that the HA leader-forward path uses). Expected: PASS.

- [ ] **Step 4: Remove any debug `System.out`**: `grep -rn "System.out" engine/src/main/java/com/arcadedb/index/columnar engine/src/main/java/com/arcadedb/query/facet engine/src/main/java/com/arcadedb/utility/HyperLogLog.java engine/src/main/java/com/arcadedb/query/sql/executor/ColumnarGroupBy*.java engine/src/main/java/com/arcadedb/function/sql/facet` returns nothing.

- [ ] **Step 5: Stop: changes ready for review** - full list of files from Tasks 1-14.
