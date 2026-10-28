# Columnar Faceting - Roadmap (plan 0 of 6)

> **For agentic workers:** this file is not executed task by task. It fixes the order, the gates and the cross-plan contract that plans 1-6 implement. Every plan reads it first.

**Goal:** deliver facets (terms, multi-valued, disjunctive, range, date histogram, hierarchical, distinct, stats) computed from an async, node-local, on-disk columnar projection, exact by default, in one round trip.

**Spec:** `docs/superpowers/specs/2026-10-18-columnar-faceting-design.md` (section numbers below refer to it).

## Plans and order

| # | File | Delivers | Depends on | Gate to start the next |
|---|---|---|---|---|
| 1 | `2026-10-19-columnar-faceting-1-spike.md` | Throwaway benchmark: one string field, terms top-N from a projection vs `GROUP BY` | none | **Go/no-go**: building the match set with a native match source (index range or columnar filter, no SQL result rows) plus counting from the projection is faster end to end than `GROUP BY` with the same `WHERE`, for both a selective indexed and a broad unindexed predicate (count time alone, the K = 5 facet page, an off-heap block variant, cold and concurrent runs are reported, not decisive; plan 1 Tasks 5-10, added 2026-10-21 after the SQL-match-source run was NO-GO). Records segment size and page re-read cost in the plan 3 file before plan 3 starts. Spike classes use a `Spike*` prefix, are throwaway, and no later plan imports them |
| 2 | `2026-10-19-columnar-faceting-2-extraction.md` | Shared codecs, block header/CRC format, server-wide block cache, new bit-packed codecs | none (can run in parallel with 1) | Full time-series suite green |
| 3 | `2026-10-19-columnar-faceting-3-core.md` | `COLUMNAR` index type, segment store, dictionary, manifest, dirty page tracking, apply thread, recovery, schema events, HA integration tests | 1 (go), 2 | Equivalence oracle green at lag 0; `ha-raft` ITs green |
| 4 | `2026-10-19-columnar-faceting-4-query-api.md` | `FACETS` grammar, executor, patching, disjunctive facets, range/histogram, budget, security, HTTP/ndjson/gRPC/remote clients, `EXPLAIN` | 3 | Server ITs and `ha-raft` follower-query ITs green |
| 5 | `2026-10-19-columnar-faceting-5-advanced.md` | Hierarchical facets, distinct, stats, facet-value search, `AFTER` cursor, `GROUP BY` acceleration | 4 | Unit + integration green |
| 6 | `2026-10-19-columnar-faceting-6-resilience-load.md` | Facet invariant in `HaChaosIT` and `e2e-ha` scenarios; facet load tests | 4 (5 optional for hierarchy workload) | Chaos runs and load runs report no facet violation |

## Test layers per plan (team requirement)

| Layer | Where | Plans |
|---|---|---|
| Unit | `engine/src/test/java/com/arcadedb/...` | 1, 2, 3, 4, 5 |
| Integration (engine + server) | `engine/src/test/...` (embedded `TestHelper`), `server/src/test/...` (`BaseGraphServerTest`) | 3, 4, 5 |
| HA integration | `ha-raft/src/test/java/com/arcadedb/server/ha/raft/` (`BaseRaftHATest`) | 3, 4 |
| HA resilience | `e2e-ha/src/test/java/com/arcadedb/containers/ha/` and `.../chaos/` (`HaChaosIT`) | 6 |
| Load | `load-tests/src/test/java/com/arcadedb/test/load/` | 6 |

## Global constraints (apply to every plan)

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

## Cross-plan contract

Names below are fixed. A plan may add members; it must not rename these or change their signatures without updating this file.

### Packages

| Package (module `engine` unless noted) | Owner plan | Content |
|---|---|---|
| `com.arcadedb.engine.codec` | 2 | Codecs moved out of `engine.timeseries.codec` plus new ones |
| `com.arcadedb.engine.block` | 2 | `BlockHeader`, `BlockCache` |
| `com.arcadedb.index.columnar` | 3 | Index type, projection, segments, dictionary, tracker, applier, recovery |
| `com.arcadedb.query.facet` | 4 | Facet specs, executor, counters, budget, patcher, results |
| `com.arcadedb.query.sql.parser` | 4 | `FacetsClause`, `FacetItem` AST nodes |

### Plan 2 produces

```java
// com.arcadedb.engine.codec - static utilities over primitive arrays, no state
public final class BitPackedCodec {
  static byte[] encode(long[] values, int offset, int length);          // frame of reference + GCD + bit width in header
  static void decode(byte[] encoded, long[] out);
  static long get(byte[] encoded, int index);                           // O(1) point read
}
public final class LinearCodec { /* same three methods */ }
public final class BlockwiseLinearCodec { /* same three methods, 512-value blocks */ }
public final class NumericCodecChooser { static byte[] encodeBest(long[] values, int offset, int length); static long get(byte[] encoded, int index); static void decode(byte[] encoded, long[] out); }
// DeltaOfDeltaCodec, GorillaXORCodec, Simple8bCodec, DictionaryCodec move here unchanged; timeseries imports updated.

// com.arcadedb.engine.block
public final class BlockHeader {   // self-describing header: magic, version, column count, column offsets/sizes, CRC32
  static BlockHeader read(ByteBuffer buffer);
  void write(ByteBuffer buffer);
  int columnOffset(int column); int columnSize(int column);
  static int crc32(ByteBuffer data, int offset, int length);
}
public final class BlockCache {    // one per server (static instance), byte budget from GlobalConfiguration.BLOCK_CACHE_MAX_BYTES
  static BlockCache instance();
  <T> T get(long fileId, long blockId, int column, Supplier<T> loader, ToLongFunction<T> sizer);
  long newFileId();                 // process-local id for a file's cache entries; never persisted
  void invalidateFile(long fileId);
  long usedBytes();
}
```

### Plan 3 produces

```java
// com.arcadedb.index.columnar
public final class PositionBitmap {                    // sparse per-bucket bitmap of positions, one long[maxRecordsInPage / 64] stripe per bucket page (32 words for 2048 slots, 4 for 256-slot external buckets)
  PositionBitmap(int maxRecordsInPage);
  void set(long position); boolean get(long position); void clear(long position);
  void and(PositionBitmap other); void andNot(PositionBitmap other); void or(PositionBitmap other);
  long cardinality(); long andCardinality(PositionBitmap other);
  void forEach(LongConsumer position);
  int[] pages();                                       // page numbers with at least one bit
}
public final class GlobalDictionary {                  // one per (bucket, property), per generation; single writer
  int getOrAdd(Object value);                          // writer only
  int ordinalOf(Object value);                         // -1 when absent
  Object valueOf(int ordinal);
  int size();                                          // published size, volatile read
  int rank(int ordinal);                               // value-order rank; computed on the fly for ordinals newer than the rank view
}
public interface ColumnReader {
  ColumnKind kind();                                   // enum ColumnKind { ORDINAL, ORDINAL_LIST, LONG, DOUBLE }
  boolean isPresent(long position);                    // false = missing (no value)
  int ordinal(long position);                          // ORDINAL
  int ordinals(long position, int[] out);              // ORDINAL_LIST, returns count, sorted and deduplicated
  long longValue(long position);                       // LONG (epoch for dates)
  double doubleValue(long position);                   // DOUBLE
  void forEachOrdinal(PositionBitmap matches, OrdinalConsumer consumer);  // consumer.accept(long position, int ordinal); dense or point reads chosen inside
}
public interface ColumnarSegmentView {
  PositionBitmap live();                               // presence AND live
  ColumnReader column(String property);
  long minLong(String property); long maxLong(String property);  // for skipping
}
public interface ColumnarSnapshot extends AutoCloseable { // ref-counted, segments not deleted while held
  int bucketId();
  List<ColumnarSegmentView> segments();                // overlay included as the last view
  PositionBitmap pendingPositions();                   // positions on dirty, not yet applied pages (input to patching)
  long oldestPendingEpochStartMs();                    // 0 when caught up
  boolean isUsable();                                  // false while missing or rebuilding
  void close();
}
public final class ColumnarProjection {                // one per bucket
  ColumnarSnapshot acquire();
  GlobalDictionary dictionary(String property);
}
public final class ColumnarIndexManager {              // one per LocalDatabase, LocalDatabase.getColumnarIndexManager()
  ColumnarProjection projection(int bucketId);         // null when the bucket has no COLUMNAR index
  ColumnarValueExtractor extractor(String typeName);
  void markPages(int fileId, int[] pageNumbers, int count);   // called by commit, Raft apply, WAL replay
  void onSchemaEvent(ColumnarSchemaEvent event);
}
public final class ColumnarValueExtractor {            // the conversion rules shared with patching (spec 5.3)
  void extract(Document record, String property, ValueSink sink);   // sink receives ordinal(s), long or double, or missing
}
// Schema.INDEX_TYPE.COLUMNAR, registered through ColumnarIndexFactoryHandler in LocalSchema
// ColumnarIndex implements IndexInternal, one per bucket, opts out of TransactionIndexContext and planner selection
```

`GlobalConfiguration` keys added by plan 3 (defaults are starting values, tuned with plan 1 numbers):

| Key | Default |
|---|---|
| `COLUMNAR_FLUSH_INTERVAL` (`arcadedb.columnar.flushInterval`, ms) | 1000 |
| `COLUMNAR_FLUSH_MAX_ROWS` (`arcadedb.columnar.flushMaxRows`) | 65536 |
| `COLUMNAR_MERGE_DEAD_RATIO` (`arcadedb.columnar.mergeDeadRatio`) | 0.3 |
| `COLUMNAR_MAX_CARDINALITY` (`arcadedb.columnar.maxCardinality`) | 1000000 |
| `COLUMNAR_APPLY_THREADS` (`arcadedb.columnar.applyThreads`) | 2 |

### Plan 4 produces

```java
// com.arcadedb.query.facet
public sealed interface FacetSpec permits TermsFacetSpec, RangeFacetSpec, HistogramFacetSpec, HierarchyFacetSpec, DistinctFacetSpec, StatsFacetSpec {
  String alias(); String property(); WhereClause filter();   // filter may be null
  default Expression expression() { return null; }
}
public final class MatchSet { PositionBitmap forBucket(int bucketId); int[] bucketIds(); }
public final class FacetExecutor { FacetResults execute(DatabaseInternal db, MatchSet matches, List<FacetSpec> facets, CommandContext ctx); }
public final class FacetMemoryBudget { static FacetMemoryBudget instance(); Reservation reserve(long bytes, long maxWaitMs); }  // server-wide
public final class FacetResults {
  boolean exact(); long patched(); long lagMs();
  FacetResult get(String alias); Set<String> aliases();
  JSONObject toJSON();
}
public final class FacetResult { String source(); /* "index" | "scan" | "unavailable" */ List<FacetBucket> buckets(); long otherCount(); long missing(); }
// ResultSet gains: default FacetResults getFacets() { return null; }
```

`GlobalConfiguration` keys added by plan 4:

| Key | Default |
|---|---|
| `COLUMNAR_PATCH_BUDGET` (`arcadedb.columnar.patchBudget`, records per query) | 10000 |
| `COLUMNAR_QUERY_MEMORY_BUDGET` (`arcadedb.columnar.queryMemoryBudget`, bytes) | 268435456 |
| `COLUMNAR_TOP_MAX` (`arcadedb.columnar.topMax`) | 1000 |
| `COLUMNAR_MAX_FALLBACK_SCANS` (`arcadedb.columnar.maxFallbackScans`, per database) | 2 |

Counters (plan 3 spike input): dense `int[]` up to 100,000 ordinals (spike-measured), an open-addressing map when matches < cardinality / 8 or ordinals exceed that threshold; count arrays pooled per thread and drawn from `FacetMemoryBudget`.

Response JSON shape: spec 8.3, `facets.meta = {exact, patched, lagMs}`. `FacetResults.toJSON()` is exactly the HTTP `facets` object; terms facets always include `missing` and `otherCount` (0 allowed); range buckets carry `key`; a facet with `source: "unavailable"` has no `buckets`.

Plan 3 also excludes the projection directory from Raft snapshots and backups' replicated content: a snapshot install always triggers a `snapshot_install` rebuild.

### Metrics

Micrometer names under `arcadedb.columnar.`, registered on `Metrics.globalRegistry` with a `database` tag so `GET /prometheus` exposes them (e.g. `arcadedb_columnar_rebuilds_total`). Plan 3 adds counter `rebuilds` tagged `database`, `type`, `reason` (`create | snapshot_install | missing | unclean_no_wal | schema | check_fix`); normal open and crash recovery never increment it (plan 6 asserts this), and optional `recovery.pages`. Other names (plan 3: `apply.lag.ms`, `apply.pages`, `apply.placeholders.resolved`, `overlay.rows`, `flush.count`, `merge.count`, `segments`, `dictionary.size`, `conversion.failures`; plan 4: `query.patched`, `query.budget.waits`, `query.patch.overflows`, `query.fallback.scans`).

## Shared test fixtures

- `engine/src/test/java/com/arcadedb/index/columnar/FacetOracle.java` (plan 3): given a database, a type, a property and a WHERE clause, returns the expected counts by running the equivalent `GROUP BY`; compares against projection counts (plan 3: via `ColumnarSnapshot`; plan 4 onwards: via `FACETS`). Reused by plans 4, 5, 6.
- `engine/src/test/java/com/arcadedb/index/columnar/ColumnarTestData.java` (plan 3): seeded random `Asset` generator (brand, tags list, fileSize, publishDate, author) with knobs for record size (to force placeholders and chunks), deletes, slot reuse. Reused by plans 4 and 5. Plan 6 has its own `AssetGenerator` in the `load-tests` test-jar, which `e2e-ha` already depends on (one copy for both modules).

## Contract additions requested by later plans

All additive. Owners implement them in their plan.

**Plan 3, for plan 5:**
- `ColumnarIndexManager.watchFiles(int[] fileIds, Runnable onChange)` / `unwatchFiles(Runnable onChange)`: `markPages` also dispatches for watched files with no projection, on all three paths (commit, Raft apply, WAL replay). Used for hierarchy source buckets (Tag vertices, their edge-list buckets, ParentOf).
- `onSchemaEvent` also receives events for hierarchy source types.
- `ColumnarIndexManager.suspendApply(): AutoCloseable` test hook (holds the apply thread so tests can observe lag).
- `ColumnarIndexManager.conversionFailures(String type, String property): long`.
- `GlobalDictionary.sortedOrdinals(): int[]`, strings ranked by `String.compareTo`.
- Metadata class name `ColumnarIndexMetadata` (holds the optional hierarchy definition).
- `FacetOracle.awaitCaughtUp(Database db)`.

**Plan 4, for plan 5:**
- Records `HierarchyFacetSpec(alias, property, filter, top, minCount)`, `DistinctFacetSpec(alias, property, filter, long precisionThreshold)`, `StatsFacetSpec(alias, property, filter)`; `TermsFacetSpec.after()`.
- `FacetCollector` interface: `collect(ColumnarSegmentView segment, PositionBitmap matches)`, `collectPatched(long position, PatchedValue value)`, `merge(FacetCollector other)`, `finish(): FacetResult`. Plan 4's terms, range and histogram collectors implement it.
- `FacetBucket.attributes(): JSONObject`; `FacetResult.distinct()`, `stats()`, `after()`.
- A FACETS query with no `WHERE` builds its match set from live bitmaps plus pending positions, never a record scan.
- The patcher treats a pending position with no record (deleted) as not matching.

**Plan 3 additions (reported by plan 3):**
- `ColumnarSchemaEvent(kind, bucketFileId, indexFileId, reason, typeName)` with kind `HIERARCHY_SOURCE_CHANGED`; `ColumnarIndexManager.addSchemaEventListener(Consumer<ColumnarSchemaEvent>)` is how plan 5 receives schema events.
- `rebuilds` reasons gain `corrupt` (distinct from `missing`).
- `PositionBitmap.snapshot()`, `setPage`, `clearPage`, `isEmpty`, `maxRecordsInPage`; `GlobalDictionary.compare(int, int)`; `ValueSink` (incl. `unknownValue(Object)`); `ColumnarSegmentView.live(fromPage, toPageExclusive)`; manager `markPagesWithoutWAL`, `requestReconcile`, `persistRecoverySet`, `awaitCaughtUp(long)`, `flushNow`, `getStats`.
- `GlobalConfiguration`: `COLUMNAR_APPLY_INTERVAL` (100 ms), `COLUMNAR_MAINTENANCE_THREADS` (2), `COLUMNAR_REBUILD_CONCURRENCY` (1), `COLUMNAR_SEGMENT_BLOCK_ROWS` (16384), `COLUMNAR_MERGE_MAX_SEGMENTS` (10).
- Gauge `arcadedb.columnar.cardinality.exceeded`.
- COLUMNAR is kept out of planners through `TypeIndex.isReadyForQueries()` returning false; projection data lives in `<db>/columnar/`, outside snapshot zips, backups and checksum scans.
- `ColumnarValueExtractor.extract(Document record, String property, GlobalDictionary dictionary, ValueSink sink)`: read-only extraction against an explicit dictionary generation, so plan 4 patching decodes with `ColumnarSnapshot.dictionary(property)`; `extractor(String typeName)` resolves each record's dictionary from its bucket (plan 3 pre-flight R5).

**Plan 4 additions (reported by plan 4):**
- `FacetSpec.filter()` returns `WhereClause` (applies to plan 5's records too); `default Expression expression()`.
- `MatchSet.forFacet(int facetIndex, int bucketId)`, `size()`, `static fromProjection(...)`; `FacetResult.reason()`.
- Key `COLUMNAR_QUERY_BUDGET_WAIT` (`arcadedb.columnar.queryBudgetWait`, ms, 5000).
- `FacetOracle.assertFacetsEqualGroupBy(...)` (plan 4 adds it).
- Engine: plan 4 reuses `TransactionContext.hasChangesInFiles(IntHashSet)` and `getWrittenRecordIdsIn(IntHashSet)`; **plan 5 uses these, not a new method.**
- `FacetCollector.beginBucket(int, ColumnarSnapshot, CrossBucketOrdinalMap)` (additive).
- `ColumnarSnapshot.publishes(String property)` (plan 4 adds it to the plan 3 interface).
- Engine (plan 4 Task 8): `LocalBucket.forEachCommittedRecordSlot(int pageNumber, LongConsumer)` lists the record slots of a committed page (in place, placeholder pointer, chunk head) without decoding records; `MatchSet.fromProjection` uses it so pending positions count real records.
- `long FacetPatcher.countPatched(DatabaseInternal, MatchSet, ColumnarSnapshot, PositionBitmap, List<FacetSpec>, List<FacetCollector>, ColumnDefinition[], WorkGuard)` (`MatchSet.sharesFacetMatches` was not kept).
- A bucket the caller's open transaction changed (`hasChangesInFiles`) never takes its matches from `MatchSet.fromProjection`: its matches are collected from rows (no-WHERE included), else the patcher refuses it.
- Counter arrays are not pooled per thread in plan 4 (a pooled array would sit outside its budget reservation); revisit with plan 6 load numbers.
- Plan 5 adds tokens `HIERARCHY`, `STATS`, `AFTER` and `DISTINCT [PRECISION n]` as alternatives of plan 4's labeled `facetBody` rule.

**Plan 3 must also provide (requested by plan 4):**
- `PositionBitmap.copy()`.
- `ColumnarValueExtractor.toColumnLong(String property, Object value): long` and `dateUnit(String property): TimeUnit`.
- `ColumnarIndexManager.fallbackScans(): Semaphore`; `suspendApply()` is public.
- `ColumnReader.forEachOrdinal` visits positions in ascending order (documented and tested).
- `ColumnReader.forEachOrdinal(matches, consumer)` visits exactly `matches AND live()` of its view (dead rows never visited, presence and dead set consulted only for `matches.pages()`); plan 4 never ANDs live itself (plan 3 pre-flight R12).
- `FacetOracle.expectedCounts(Database db, String type, String property, String where)`, `ColumnarTestData(long seed)` with `insertAssets(...)` and `mutate(...)`.

**Dictionary scope change (decided after plan 3):**
- Dictionaries are per **bucket** and property, with generations (`dict-<indexFileId>.<gen>.dic`); spec 5.4 updated.
- `ColumnarSnapshot.dictionary(String property): GlobalDictionary` is the dictionary to decode a snapshot with; plan 4 and 5 read dictionaries from the snapshot, not from `ColumnarProjection.dictionary`.
- New in plan 4: `CrossBucketOrdinalMap` (`com.arcadedb.query.facet`), cached per type: `int merged(int bucketId, int bucketOrdinal)`, `Object value(int mergedOrdinal)`, `int size()`; extended incrementally as dictionaries grow, rebuilt for a bucket after its dictionary compaction. Single-bucket types bypass it.
- Plan 5's hierarchy `parents[]`, DISTINCT bitsets, prefix search and GROUP BY acceleration work over merged ordinals.
- New plan 3 metrics `dictionary.compactions`, `dictionary.ordinals.reclaimed`; `rebuilds` reason `corrupt`; key `COLUMNAR_DICTIONARY_COMPACT_RATIO` (0.5).

**Reconciliation additions:**
- Plan 3: `ColumnarIndexManager.watchGeneration(Runnable watcher): long` (counter bumped synchronously in `markPages`, so a query can tell a watched tree changed before it started); `compactDictionaryNow(int bucketId, String property)` test hook; spec 9 meters `apply.records.reread`, `apply.unreadable`, `rebuild.pages.reused`, `flush.duration`, `merge.duration`, `segments.dead.ratio`, `files.open`, and `cache.hits` / `cache.misses` / `cache.used.bytes` tagged `database="*"` (the block cache is JVM-wide, the one exception to the per-database tag rule); `ColumnarTestData` `buckets` knob; hierarchy metadata accepts `parentDirection`.
- Plan 4: `CrossBucketOrdinalMap.mergedOf(Object value)`, `version()`; `PatchedValue implements ValueSink` with read getters, the type passed to `FacetCollector.collectPatched`; records `DistinctValue` and `FacetStats`, with `FacetResult.distinct(): DistinctValue` and `stats(): FacetStats`; gRPC fields `attributes_json`, `extensions_json`.

## Plan 3 Task 2 additions (2026-10-21, from its review)

- `GlobalDictionary.rank(int)` is sparse, not dense: an ordinal at position p of the rank view has rank `2p + 1`; ordinals newer than the view rank `2 * insertionPoint` and tie with each other inside one gap. Before the first view (fewer than 1024 values) every rank is 0. Consumers that need a total value order (plan 4 `FacetRanker` ORDER BY VALUE, plan 5 value search and `rankAfter`) must break rank ties with `GlobalDictionary.compare(int, int)` (or a value check), never assume `rank == position in sortedOrdinals()`. Plan 5 `rankAfter(value)` returns the gap rank `2p` of the first view entry strictly greater than `value`, and AFTER filtering keeps ordinals whose rank is greater, or equal with `compare` greater than the cursor value.
- Numeric value order: `DictionaryValueCodec.compareValues` treats LONG, DECIMAL and DOUBLE as one numeric class (exact Long vs BigDecimal comparison, -Infinity first, +Infinity next to last, NaN last). Plan 4 `FacetValueComparator` must use the same rule so single-bucket (rank) and merged/patched ordering agree.
- `GlobalDictionary.fsync()` returns the durable byte length and `durableEntries()` returns the entry count consistent with it (captured together); Task 5's `DictionaryEntry(entries, bytes)` takes both from the same fsync.
- `GlobalDictionary.openReadOnly(Path, long validBytes, int entries)`: READ only, no tail truncation, appends refused; Task 16 READ_ONLY opens use it. A missing file surfaces as `NoSuchFileException` (rebuild reason `missing`, not `corrupt`).

## Plan 3 Task 5 additions (2026-10-21, from its review)

- `ColumnarManifest.FILE_NAME`, `ColumnarManifest.files()` (every file a manifest names; Tasks 6/8 use it for the "current manifest files" and V-B7 state file sets, never recompute it), `SegmentEntry.deadFile()`, `ColumnarStore.createBucketDir(...)` (durable directory creation; Task 8 must use it, never a bare `Files.createDirectories`), `ColumnarStore.TMP_SUFFIX`.
- Dead-set versions start at 1: `deadVersion == 0` means "no dead file". Tasks 10/11 (and V-B2's initial `supersededInFrozen` dead set) write version >= 1.
- `ColumnarStore.deleteUnreferenced` must run under `publishLock` (or with the in-flight flush/merge file names in its held set), or it deletes files a publish is about to name.
- The publish kill points `publish.segmentsSynced` / `publish.dictionariesSynced` are reached by the publishers (Task 10 flush/merge, Task 24 compaction); Task 21 arms them.
- `ColumnarStore.fsyncDirectory` follows the engine-wide `FileUtils.forceDirectory` policy: a file store that refuses or fails the directory force is tolerated and logged once.

## Plan 3 Task 6 additions (2026-10-21, from its review)

- `ColumnarOverlay(int maxRecordsInPage, ColumnDefinition[] columns, LongSupplier clockMs)`; `oldestRowMs()` is set when the overlay goes from no rows to some rows and is 0 whenever it holds none.
- `ContentOwnerMap` nodes are 3 longs (`[owner][page<<32 | nextOfOwner][prevOnPage<<32 | nextOnPage]`), about 62 B/owner; single writer, readers other than the apply thread hold the projection's `ownersLock` read lock.
- Extra members: `ColumnarOverlay.pages()`, `OverlayPageBlock.readRow/position/row/present/scalar/ordinals/min/max`, `PositionBitmap.wrapShared`, `RowValues.check` (shared by `RowBuffer.put` and `SegmentWriter`), `LongLongHashMap.capacity()`.

## Plan 3 Task 16-17 additions (2026-10-22, from their reviews)

- A COLUMNAR column counts as published only when the projection's published entry for its index file id is `sameData` with the schema's current `ColumnDefinition`. `ColumnarIndexManager.isUsable(ColumnarIndex)` requires it, so a column added to a usable projection, or a retyped column, is not usable until its `create` / `schema` rebuild publishes. Plans 4 and 6 must fall back to the record path in that window, and must always pass a timeout to `awaitCaughtUp`.
- After any rebuild publishes, a registered column still unpublished schedules `create` (coalesced): the plan's "ADD_COLUMN is a no-op while a rebuild is queued or running" rule is superseded.
- At open, a manifest column whose definition differs from the schema's schedules a `schema` rebuild, so a retype survives a restart.
- Column-level property RENAME is not reachable by DDL today: `LocalDocumentType.renameProperty` refuses any indexed property, COLUMNAR included. The reconcile handles RENAME (tested through a package-private seam) so lifting that refusal later needs no projection change. Type and bucket renames are reachable and tested end to end.
- `CHECK DATABASE FIX` sends `check_fix` only for buckets the fix repaired; COLUMNAR shell indexes are excluded from the checker's index rebuild (R19).
- `ColumnarIndex.build()` delegates to `ColumnarIndexManager.buildRegistered` (under `registryLock`, the outermost columnar lock).

## Tuning phase (after plans 4-6; user decision 2026-10-23; GitHub #10662)

Spec 2's "ingest cost unchanged" is not met yet for inserts: with five COLUMNAR indexes, inserts are +6-10% (plan 3 ingest benchmark, A/B/B' medians), updates +4.5%. The commit path is clean (variant B' with apply held: +2-4%, bytes per commit and GC unchanged); the rest is the background apply competing for CPU. The user asked to tune performance once the whole facet subsystem (plans 4-6) is in place, so measurements cover ingest, queries and HA together.

Baseline to beat: `ColumnarIngestOverheadBenchmark` (plan 3), `ColumnarCountBenchmark` (plan 3), the plan 4 facets-vs-GROUP-BY benchmark, the plan 6 load tests.

Candidate levers (to be measured, not assumed):
- Apply pass: fewer re-reads under sustained ingest (pacing beyond 500 ms with a configurable lag bound; skip pages whose only change is outside projected properties); cheaper record decode (already one header walk per record); batch dictionary lookups.
- Apply pool: thread count and priority vs the committer; per-database fairness.
- Commit path: skip the second mark when no drain happened in between; skip the sort when keys are already ordered (worth < 0.5 ms per 400 commits, last).
- Flush/merge: segment size, merge policy, fsync batching (the WAL force per publish is within noise today).
- Query side: counter pooling (deferred by plan 4, a pooled array must sit inside its budget reservation), parallel counting per segment, CrossBucketOrdinalMap caching, rank-view refresh cadence.
- GC: allocation per apply pass (ImmutableDocument per re-read record), off-heap buffers reuse.

Exit criterion: inserts and updates within noise of no-COLUMNAR on the ingest benchmark at default settings, with facet query latency and apply lag no worse than the plan 4/6 numbers; any remaining cost documented with its knob.
- Plan caching: a FACETS query with `AFTER :c` (a parameter) is never cached, so every page of a paging loop is planned again (plan 5 Task 1); measure, and cache the plan with the cursor bound per execution if it matters.
- Hierarchy rebuild: `HierarchyBuilder` scans every Tag record twice (keys, then parent links); collecting the links in the first pass halves the load and closes the window where a tag created between the passes waits for the next rebuild (plan 5 Task 4).
- Hierarchy shared build: BEFORE/AFTER READ triggers on the tag type run during the build with no user (a BEFORE READ returning false hides a tag from every user); consider building with read events bypassed (plan 5 Task 5).

## Pre-merge steps (before the feature branch is merged to main)

- **Python bindings live in their own repository** (`ArcadeData/arcadedb-python`, user note 2026-10-23; tracked as sub-issue #10666). No plan in this branch touches Python, so every Python-side piece of this feature is done there, not here, and lands before the merge to main: the `FACETS` clause usable from the Python client, the `facets` member of query responses (HTTP JSON at top level, ndjson in the `stats` trailer as `stats.facets`, the shape `FacetResults.toJSON()` defines, values written as rows write them, with a `@props` `value:<type id>` hint under `typeHints`) parsed into client objects, and tests against a server built from this branch. The Python change follows the same API surface rule as Java: whatever plan 4 exposes over HTTP and gRPC is reachable from Python with the same shape.
- Measured by plan 4 Task 19 (200k records): with 5000 pending mutations a broad facet query's patch set is ~120k records (60% of its matches), because pending marks are page-granular; the absolute `arcadedb.columnar.patchBudget` (10000) alone answers `exact=false`. Since the plan 4 final fix the budget is relative: an answer stays exact while it patches at most `max(patchBudget, arcadedb.columnar.patchBudgetRatio x matches)` positions, the matches being those of the buckets patched; the ratio defaults to 1 (validated to [0, 1], 0 = absolute budget only), and the memory budget still bounds the patch sets. Re-measured at 200k / one bucket / 5000 mutations with the defaults: broad 120,152 patched, exact, 105 ms vs 329 ms GROUP BY (0.32x); no WHERE 120,723 patched, exact, 97 ms vs 320 ms (0.30x); selective 296 patched, exact. With ratio 0 the broad and no-WHERE cases answer `exact=false` at 27-30 ms. Remaining levers for tuning #10662: record-granular pending marks, patching per page read, a budget expressed in pages read.

## Plan 5 additions

- `HierarchyBuilder.build(DatabaseInternal, HierarchyDefinition, long generation, String indexName)`: the index name labels the cycle WARNING. `HierarchyView.nullKeys()` (records of the vertex type whose key is null) is part of the view contract (plan 5 Task 4).
