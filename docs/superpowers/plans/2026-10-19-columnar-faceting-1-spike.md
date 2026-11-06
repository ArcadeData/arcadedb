# Columnar Faceting - Spike Implementation Plan (plan 1 of 6)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** a throwaway, test-only benchmark that decides go/no-go for the columnar projection: terms top-N counted from a hand-built columnar segment must beat the equivalent SQL `GROUP BY` on the same data and the same match set.

**Architecture:** all code lives under `engine/src/test/java/com/arcadedb/index/columnar/spike/`, none in `src/main`. A seeded generator fills one `Asset` type with one bucket. A builder scans that bucket once and produces one in-memory segment for the `brand` property: a presence bitmap over bucket pages (position = `pageId * maxRecordsInPage + slot`, one `long[maxRecordsInPage / 64]` stripe per page), a per-page row prefix for rank, bit-packed ordinals for occupied slots only, and a global insert-ordered dictionary. A counter ANDs a match bitmap (built from the `@rid`s a SQL `WHERE` returns) with presence and counts into a dense `int[]`. A benchmark compares that against `SELECT brand, count(*) ... WHERE <same> GROUP BY brand` and writes a results table to `target/`.

**Tech Stack:** Java 21, JUnit 5, AssertJ, ArcadeDB embedded engine (`TestHelper`), `com.arcadedb.utility.StallAwareStopwatch` (engine test sources), `com.arcadedb.log.LogManager`.

**Spec:** `docs/superpowers/specs/2026-10-18-columnar-faceting-design.md` (sections 3.3.1, 5.2, 5.3, 5.4, 5.7, 6.3, 7.1, 7.4, 11 "Spike go/no-go", 12 step 1). Roadmap and cross-plan contract: `docs/superpowers/plans/2026-10-19-columnar-faceting-0-roadmap.md`.

## Global Constraints

Copied from the roadmap, plus spike-specific lines at the end:

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
- Spike only: **no file under `src/main` changes**, and no file outside `engine/src/test/java/com/arcadedb/index/columnar/spike/` changes except the plan 3 file in Task 4. The spike's class names carry the `Spike` prefix so they never collide with the plan 3 contract names (`GlobalDictionary`, `PositionBitmap`, ...).
- Spike only: the root pom excludes the `benchmark` group by default (`<excludedGroups>benchmark</excludedGroups>`), so the benchmark run passes `-DexcludedGroups=none`.
- Spike only: every timing is `StallAwareStopwatch.effectiveMs()` (or nanos derived the same way); timings are reported, never asserted. Only the count equality is asserted.

## Review Focus

1. **Holes inside a page** (deleted records leave free slots; a reused slot is an overwrite, spec 3.3.2): rank math over the presence bitmap must still map position to the right row. Pinned by `segmentMatchesRecordsAfterDeletes` in Task 1.
2. **Missing values** (records without `brand`): GROUP BY reports them as a `null` group; the spike must report them as `missing`, not as ordinal 0 or as a dictionary entry. Pinned by `countsEqualGroupByWithMissingValues` in Task 2.
3. **Ties in the top-N** (two brands with the same count): ordering must be count descending then value ascending (spec 7.4), and the GROUP BY side must be sorted the same way before comparing, or the check flakes. Pinned by `topNBreaksTiesByValueAscending` in Task 2.
4. **A match on a position the segment does not cover** (a record created after the segment was built): the counter must skip it and report it, never throw `ArrayIndexOutOfBoundsException`. Pinned by `matchBeyondSegmentIsReportedNotCounted` in Task 2.
5. **Planner choice for the selective predicate**: if `fileSize < ?` does not use the index, the "selective" row measures a full scan and the comparison is meaningless. Pinned by the benchmark logging the `EXPLAIN` plan of both predicates into the report (Task 3, Step 3).

---

## File Structure

All under `engine/src/test/java/com/arcadedb/index/columnar/spike/`:

| File | Responsibility |
|---|---|
| `SpikeDataset.java` | Seeded content-repository-like generator: schema (`Asset`, 1 bucket, NOTUNIQUE LSM index on `fileSize`), Zipf-skewed `brand` over ~200 values, `status`, `fileSize`, padding to ~200-byte records, optional missing `brand` and deletes |
| `SpikeDictionary.java` | Global dictionary, value to dense `int` ordinal in insert order |
| `SpikeSegment.java` | Immutable segment: presence stripes, per-page row prefix, bit-packed ordinals; `build(LocalBucket, String, SpikeDictionary)`; `ordinalAt(long)`; `serialize()` |
| `SpikeTermsCounter.java` | Match bitmap from positions, count over `matches AND presence`, top-N with tie-break |
| `SpikeReport.java` | Collects result rows, writes `target/columnar-spike/results.md`, logs each line through `LogManager` |
| `ColumnarSpikeCorrectnessTest.java` | Normal-lane test, small N: segment and counts equal the records and `GROUP BY` |
| `ColumnarSpikeBenchmark.java` | `@Tag("benchmark")`, N from `-DcolumnarSpike.records` (default 1 000 000): timings, sizes, page re-read cost, memory, go/no-go |

Both test classes extend `com.arcadedb.TestHelper` (`engine/src/test/java/com/arcadedb/TestHelper.java`), which gives a fresh `database` under `target/databases/<ClassName>` and drops it after each test.

---

### Task 1: Dataset, dictionary and segment built by a bucket scan

**Files:**
- Create: `engine/src/test/java/com/arcadedb/index/columnar/spike/SpikeDataset.java`
- Create: `engine/src/test/java/com/arcadedb/index/columnar/spike/SpikeDictionary.java`
- Create: `engine/src/test/java/com/arcadedb/index/columnar/spike/SpikeSegment.java`
- Test: `engine/src/test/java/com/arcadedb/index/columnar/spike/ColumnarSpikeCorrectnessTest.java`

**Interfaces:**
- Consumes (existing engine API): `LocalBucket.getMaxRecordsInPage()`, `LocalBucket.getTotalPages()`, `LocalBucket.getPageSize()`, `LocalBucket.iterator()` (returns `Iterator<Record>`), `RID.getPosition()`, `Document.getString(String)`, `Schema.buildDocumentType().withName(..).withTotalBuckets(1).create()`, `Schema.getBucketById(int)`, `DocumentType.getBucketIds(false)`.
- Produces:
  - `final class SpikeDataset` with constants `TYPE = "Asset"`, `BRAND = "brand"`, `STATUS = "status"`, `FILE_SIZE = "fileSize"`, `BRAND_CARDINALITY = 200`, `FILE_SIZE_MAX = 1_000_000`; `static void createSchema(Database db)`; `static void populate(Database db, int records, long seed, double missingBrandRatio)` (commits every 10 000 records); `static long[] deleteEvery(Database db, int nth)` (deletes every nth record in RID order, returns the deleted positions, ascending); `static LocalBucket bucket(Database db)` (asserts the type has exactly one bucket and returns it cast to `LocalBucket`); `static String brandName(int i)` returning `"brand-%03d"`.
  - `final class SpikeDictionary`: `int getOrAdd(String value)`, `int ordinalOf(String value)` (-1 when absent), `String valueOf(int ordinal)`, `int size()`.
  - `final class SpikeSegment`: `static final int MISSING = -1`; `static SpikeSegment build(LocalBucket bucket, String property, SpikeDictionary dictionary)`; `int maxRecordsInPage()`; `int pageCount()`; `long rows()`; `boolean isPresent(long position)`; `int ordinalAt(long position)` (returns `MISSING` for a present row without value, throws `IllegalArgumentException` for an absent position); `int bitWidth()`; `long[] presence()`; `int[] pageRowStart()`; `byte[] serialize()`; `long heapBytes()`.

- [ ] **Step 1: Write the failing tests** in `ColumnarSpikeCorrectnessTest extends TestHelper`

```java
@Test
void segmentMatchesRecordsOfTheBucket() {
  SpikeDataset.createSchema(database);
  SpikeDataset.populate(database, 20_000, 42L, 0.0);
  final SpikeDictionary dictionary = new SpikeDictionary();
  final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.BRAND, dictionary);

  assertThat(segment.rows()).isEqualTo(20_000L);
  assertThat(dictionary.size()).isBetween(150, SpikeDataset.BRAND_CARDINALITY);
  assertThat(segment.bitWidth()).isEqualTo(8);                 // ceil(log2(size + 1)) for 150..255 values (+1 reserves MISSING)
  database.iterateType(SpikeDataset.TYPE, false).forEachRemaining(r -> {
    final long position = r.getIdentity().getPosition();
    assertThat(segment.isPresent(position)).isTrue();
    assertThat(dictionary.valueOf(segment.ordinalAt(position))).isEqualTo(r.asDocument().getString(SpikeDataset.BRAND));
  });
}

@Test
void segmentMatchesRecordsAfterDeletes() {
  SpikeDataset.createSchema(database);
  SpikeDataset.populate(database, 20_000, 7L, 0.05);
  final long[] deleted = SpikeDataset.deleteEvery(database, 3);
  final SpikeDictionary dictionary = new SpikeDictionary();
  final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.BRAND, dictionary);

  assertThat(segment.rows()).isEqualTo(20_000L - deleted.length);
  for (final long position : deleted)
    assertThat(segment.isPresent(position)).isFalse();
  // plus the same per-record loop as above: every live record maps to its own value
}
```

The second test also asserts, for a record without `brand`, `segment.ordinalAt(position) == SpikeSegment.MISSING`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -o -pl engine -am test -Dtest=ColumnarSpikeCorrectnessTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAIL, `SpikeDataset`, `SpikeDictionary`, `SpikeSegment` not found.

- [ ] **Step 3: Implement `SpikeDataset`**

Records carry `brand` (Zipf over `BRAND_CARDINALITY` values, exponent 1.0, drawn from a cumulative-weight `double[]` by binary search on `Random(seed).nextDouble()`; brand index 0 is the most frequent; with probability `missingBrandRatio` the property is not set), `status` (`"published"` 70%, `"draft"` 20%, `"archived"` 10%), `fileSize` (uniform `long` in `[0, FILE_SIZE_MAX)`), `title` (random ASCII of 150 chars, so a record is about 200 bytes and a 64 KB page holds about 280 records, spec 3.3.1). `createSchema` declares `fileSize` as `Type.LONG` with a NOTUNIQUE `LSM_TREE` index and leaves `brand`, `status` schemaless-typed `Type.STRING`.

- [ ] **Step 4: Implement `SpikeDictionary`** with a `HashMap<String, Integer>` plus a growing `String[]` (spike only; plan 3 owns the real chunked design).

- [ ] **Step 5: Implement `SpikeSegment.build`** with one pass of `bucket.iterator()`:

```
wordsPerPage = maxRecordsInPage / 64
presence     = long[totalPages * wordsPerPage]
for each record r (ascending position):
  page = position / maxRecordsInPage ; slot = position % maxRecordsInPage
  presence[page * wordsPerPage + slot / 64] |= 1L << (slot % 64)
  append (ordinal + 1) to a growable int[] values    // 0 = MISSING, so stored width = ceil(log2(dictionary.size() + 1))
pageRowStart[p] = number of present rows in pages < p   (int[totalPages + 1])
pack values at bitWidth into long[] packed (values may straddle two words)
row(position) = pageRowStart[page] + popcount of presence words of that page before slot/64 + bitCount(word & ((1L << (slot % 64)) - 1))
```

`serialize()` writes, little-endian through a `ByteBuffer`: `maxRecordsInPage`, `pageCount`, `rows`, `bitWidth`, the presence words, `pageRowStart`, the packed words, then the dictionary as length-prefixed UTF-8 strings (the dictionary is passed in or held by reference). `heapBytes()` sums the array lengths times element size.

- [ ] **Step 6: Run tests to verify they pass**

Run: `mvn -o -pl engine -am test -Dtest=ColumnarSpikeCorrectnessTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS, 2 tests.

- [ ] **Step 7: Stop: changes ready for review** - `SpikeDataset.java`, `SpikeDictionary.java`, `SpikeSegment.java`, `ColumnarSpikeCorrectnessTest.java`.

---

### Task 2: Terms top-N counter equal to GROUP BY

**Files:**
- Create: `engine/src/test/java/com/arcadedb/index/columnar/spike/SpikeTermsCounter.java`
- Modify: `engine/src/test/java/com/arcadedb/index/columnar/spike/ColumnarSpikeCorrectnessTest.java`

**Interfaces:**
- Consumes: `SpikeSegment` (`presence()`, `pageRowStart()`, `maxRecordsInPage()`, `pageCount()`, packed-ordinal read through a new package-private `int storedValueAtRow(long row)` returning the stored `ordinal + 1`), `SpikeDictionary`.
- Produces:
  - `final class SpikeTermsCounter`:
    - `static long[] positions(Database db, String whereClause, Object... params)` runs `SELECT @rid FROM Asset WHERE <whereClause>` and returns the RID positions, sorted ascending.
    - `static long[] matchBitmap(SpikeSegment segment, long[] sortedPositions)` same stripe layout as presence; positions on pages `>= segment.pageCount()` are not set.
    - `static Counts count(SpikeSegment segment, long[] matchBitmap, long[] sortedPositions, int cardinality)`.
    - `static List<Bucket> topN(Counts counts, SpikeDictionary dictionary, int n)` ordered count descending, then value ascending.
    - `record Counts(int[] byOrdinal, long missing, long uncovered)` where `uncovered` is matches the segment does not cover.
    - `record Bucket(String value, long count)`.
  - `static List<SpikeTermsCounter.Bucket> groupBy(Database db, String whereClause, int n, long[] missingOut, Object... params)` runs `SELECT brand, count(*) AS cnt FROM Asset WHERE <whereClause> GROUP BY brand`, routing the `null` group to `missingOut[0]`, and sorting the rest by the same comparator before taking `n` (do not rely on SQL `ORDER BY` for ties).

- [ ] **Step 1: Write the failing tests** in `ColumnarSpikeCorrectnessTest`

```java
@ParameterizedTest
@ValueSource(strings = { "fileSize < 10000", "status = 'published'", "status = 'archived' AND fileSize < 500000" })
void countsEqualGroupByWithMissingValues(final String where) {
  SpikeDataset.createSchema(database);
  SpikeDataset.populate(database, 20_000, 11L, 0.02);
  SpikeDataset.deleteEvery(database, 5);
  final SpikeDictionary dictionary = new SpikeDictionary();
  final SpikeSegment segment = SpikeSegment.build(SpikeDataset.bucket(database), SpikeDataset.BRAND, dictionary);

  final long[] positions = SpikeTermsCounter.positions(database, where);
  final SpikeTermsCounter.Counts counts = SpikeTermsCounter.count(segment, SpikeTermsCounter.matchBitmap(segment, positions), positions, dictionary.size());
  final long[] missing = new long[1];

  assertThat(SpikeTermsCounter.topN(counts, dictionary, SpikeDataset.BRAND_CARDINALITY))
      .containsExactlyElementsOf(SpikeTermsCounter.groupBy(database, where, SpikeDataset.BRAND_CARDINALITY, missing));
  assertThat(counts.missing()).isEqualTo(missing[0]).isGreaterThan(0L);
  assertThat(counts.uncovered()).isZero();
}

@Test
void topNBreaksTiesByValueAscending() {
  // 3 brands inserted as "b", "a", "c" with counts 5, 5, 2 (insert order deliberately not value order)
  // expected topN(2): [Bucket("a", 5), Bucket("b", 5)]
}

@Test
void matchBeyondSegmentIsReportedNotCounted() {
  // populate 2_000, build segment, then populate 50_000 more so new pages exist,
  // positions = all @rid; count must not throw, counts.uncovered() == number of positions on pages >= segment.pageCount()
  // and sum(byOrdinal) + missing == 2_000 records' worth that the segment covers
}
```

Use `@ParameterizedTest` / `@ValueSource` from `org.junit.jupiter.params` (already used across engine tests).

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn -o -pl engine -am test -Dtest=ColumnarSpikeCorrectnessTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAIL, `SpikeTermsCounter` not found.

- [ ] **Step 3: Implement `SpikeTermsCounter`**

`count` walks pages present in the match bitmap; per page it keeps a running rank (`pageRowStart[page]` plus popcounts of earlier presence words of the page) and, for each word, iterates the bits of `match & presence` with `Long.numberOfTrailingZeros` / `w &= w - 1`, computing the row as running rank plus `Long.bitCount(presence & ((1L << bit) - 1))`. Stored value 0 increments `missing`, otherwise `byOrdinal[stored - 1]++`. A matched position whose presence bit is clear (deleted after build) is ignored; `uncovered` counts positions on pages beyond `pageCount()` from `sortedPositions` with one binary search. `topN` uses a small `int[]` index sort by the comparator; the dictionary lookup happens only for the returned `n` (spec 7.4, rank first, resolve strings last).

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn -o -pl engine -am test -Dtest=ColumnarSpikeCorrectnessTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS, 7 tests (2 from Task 1, 3 parameterized, 2 new).

- [ ] **Step 5: Stop: changes ready for review** - `SpikeTermsCounter.java`, `SpikeSegment.java`, `ColumnarSpikeCorrectnessTest.java`.

---

### Task 3: Benchmark, measurements and go/no-go report

**Files:**
- Create: `engine/src/test/java/com/arcadedb/index/columnar/spike/SpikeReport.java`
- Create: `engine/src/test/java/com/arcadedb/index/columnar/spike/ColumnarSpikeBenchmark.java`

**Interfaces:**
- Consumes: everything from Tasks 1 and 2; `StallAwareStopwatch.start()`, `.effectiveMs()`, `.stallMs()`; `LocalBucket.iterator(int fromPage, int toPage)`; `LogManager.instance().log(Object, Level, String, Object...)`.
- Produces:
  - `final class SpikeReport`: `void row(String metric, String value, String note)`; `void table(String title, String[] header, List<String[]> rows)`; `Path write()` writing `target/columnar-spike/results.md` (Markdown) and returning it; every line also logged at `Level.INFO`.
  - `@Tag("benchmark") class ColumnarSpikeBenchmark extends TestHelper` with one `@Test void run()`.

- [ ] **Step 1: Write the benchmark** `ColumnarSpikeBenchmark.run()`

Constants: `RECORDS = Integer.getInteger("columnarSpike.records", 1_000_000)`, `WARMUP = 2`, `RUNS = 5`, `TOP = 10`, seed `42L`, missing ratio `0.01`. Override `isCheckingDatabaseIntegrity()` to return `false` (an integrity check of 1M records adds minutes and measures nothing here).

Sequence, each phase timed with its own `StallAwareStopwatch`, reported as `effectiveMs` with `stallMs` in the note:

1. `createSchema` + `populate(RECORDS)`: ingest time (context only).
2. `SpikeSegment.build`: build time and records per second.
3. Sizes: bucket bytes = `getTotalPages() * getPageSize()`; `serialize()` length, also written to `target/columnar-spike/segment.bin` and its `Files.size` reported; bytes per record; presence share of the segment bytes (spec 3.3.1 sparsity); `heapBytes()`; dictionary size.
4. For each predicate `selective = "fileSize < 10000"` (about 1%, index) and `broad = "status = 'published'"` (about 70%, no index): record the `EXPLAIN SELECT @rid FROM Asset WHERE ...` plan text in the report; then `WARMUP` unmeasured rounds and `RUNS` measured rounds of, alternating order per round:
   - **projection**: `matchMs` = `positions(...)`; `countMs` = `matchBitmap` + `count` + `topN(TOP)`;
   - **groupBy**: `groupByMs` = `groupBy(database, where, TOP, missing)`.
   Report median and min of `matchMs`, `countMs`, `matchMs + countMs`, `groupByMs`, and match count.
5. Correctness, a normal assertion on the last measured round of each predicate: `topN(counts, dictionary, BRAND_CARDINALITY)` equals `groupBy(..., BRAND_CARDINALITY, missing)` and `counts.missing() == missing[0]`.
6. Page re-read cost (spec 6.3): 200 pages evenly spread over `[0, getTotalPages())`; for each, `bucket.iterator(page, page + 1)` reading every record's `brand`; report average records per page, total ms, microseconds per page, microseconds per record.
7. Memory of count arrays (spec 5.7): `int[cardinality]` bytes = `dictionary.size() * 4`; match bitmap bytes = `pageCount * wordsPerPage * 8`; and the spec's extrapolation `4 bytes * 1_000_000 ordinals * 16 tasks * 50 queries` next to the measured per-query figure.
8. Go/no-go line: `GO` when, for **both** predicates, median `matchMs + countMs` < median `groupByMs`; otherwise `NO-GO` naming the predicate. Also report `countMs / groupByMs` (the projection's own share, which is what plan 4's executor adds on top of a match source it already pays for). The verdict is written to the report, not asserted.

- [ ] **Step 2: Implement `SpikeReport`** with a `StringBuilder` per table, `Files.createDirectories(Path.of("target", "columnar-spike"))`, and `LogManager.instance().log(this, Level.INFO, "%s", line)` per line. No `System.out`.

- [ ] **Step 3: Smoke-run the benchmark on a small N**

Run: `mvn -o -pl engine -am test -Dtest=ColumnarSpikeBenchmark -DexcludedGroups=none -DcolumnarSpike.records=50000 -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS; `engine/target/columnar-spike/results.md` exists with the sizes, both predicates (with their `EXPLAIN` text, the selective one naming the `fileSize` index), page re-read and memory tables and one `GO`/`NO-GO` line. If the selective plan does not use the index, change the predicate (not the dataset) until it does, and note it in the report.

- [ ] **Step 4: Confirm the normal lane does not pick up the benchmark**

Run: `mvn -o -pl engine -am test -Dtest='ColumnarSpike*' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS with only `ColumnarSpikeCorrectnessTest` executed (7 tests), `ColumnarSpikeBenchmark` excluded by the default `benchmark` group.

- [ ] **Step 5: Check no debug output is left**

Run: `grep -rn "System.out\|System.err\|printStackTrace" engine/src/test/java/com/arcadedb/index/columnar/spike/`
Expected: no output.

- [ ] **Step 6: Stop: changes ready for review** - `SpikeReport.java`, `ColumnarSpikeBenchmark.java`.

---

### Task 4: Full run and spike results recorded in plan 3

**Files:**
- Modify: `docs/superpowers/plans/2026-10-19-columnar-faceting-3-core.md` (fill its existing `## Spike results` section, before "Global Constraints": replace the placeholder paragraph, keep the defaults table and update its "Starting value" column only where the numbers argue for it)

**Interfaces:**
- Consumes: `engine/target/columnar-spike/results.md` from Task 3.
- Produces: a `## Spike results` section that plan 3 reads before it starts (roadmap gate for plan 1).

- [ ] **Step 1: Run the benchmark at full size**, on an otherwise idle machine, twice

Run: `mvn -o -pl engine -am test -Dtest=ColumnarSpikeBenchmark -DexcludedGroups=none -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS (counts equal GROUP BY); `engine/target/columnar-spike/results.md` written. Keep both runs' files (copy the first to the scratchpad before the second overwrites it) and report the run with the higher `groupByMs / (matchMs + countMs)` ratio only if both agree on GO/NO-GO; if they disagree, report both and the verdict is NO-GO until explained.

- [ ] **Step 2: Fill the `## Spike results` section of the plan 3 file** containing, in tables:
  - environment: date, commit (`git rev-parse --short HEAD`), JVM version, CPU, heap, `RECORDS`, `maxRecordsInPage`, page size, records per page;
  - sizes: bucket bytes, segment bytes and bytes per record, presence share, dictionary size, segment heap bytes;
  - per predicate: matches, median and min `matchMs`, `countMs`, `matchMs + countMs`, `groupByMs`, ratio;
  - page re-read: records per page, microseconds per page and per record (input for `COLUMNAR_FLUSH_INTERVAL` / apply-thread sizing, spec 6.3);
  - memory: count array bytes, match bitmap bytes per query, the 5.7 extrapolation;
  - verdict: `GO` or `NO-GO` with the criterion "projection (match + count) faster than GROUP BY with the same WHERE, for both predicates";
  - observations for plan 3: whether presence sparsity (2048 slots for about 280 records) dominates the segment size, and whether that argues for per-page compact presence or RoaringBitmap (roadmap: RoaringBitmap only after plan 1 measures a need).

- [ ] **Step 3: If the verdict is NO-GO, stop** and report to the user with the numbers; plans 3 onwards do not start (roadmap gate).

- [ ] **Step 4: Stop: changes ready for review** - `docs/superpowers/plans/2026-10-19-columnar-faceting-3-core.md`, plus the spike test files from Tasks 1-3 (throwaway: the user decides whether they are committed or deleted after plan 3 reads the numbers).

---

## Extension: predictive spike (Tasks 5-10)

Added 2026-10-21 after the first full run returned NO-GO (Task 4). That run showed the verdict was decided by the match step, not by counting: `SELECT @rid` materialized one result row per match (125 ms of 128 ms on the broad predicate) while counting cost 8-9% of one GROUP BY. The user chose to make the spike predictive of the real system before deciding. Task 4's numbers stay in plan 3 as "run 1 (SQL match source)" until Task 10 replaces the section.

**Gate, unchanged in spirit (user's bar "faster than GROUP BY"):** GO when, for both predicates, the projection with a native match source (match + count of one facet) has a lower median than one `GROUP BY` with the same `WHERE`. Reported next to it, not decisive: the K = 5 facet page (match once + 5 counts) against 5 `GROUP BY`s; the off-heap block variant; cold and concurrent figures. Those tell plan 3/4 how much of the advantage survives.

Extension constraints (in addition to Global Constraints):
- Still test-only, `Spike` prefix, same directory, nothing under `src/main`, no import of plan 2 code (plan 2 has not run yet; the block variant is a spike-local approximation).
- Concurrency in Task 9 uses a dedicated `ExecutorService` created and shut down by the benchmark, never `ForkJoinPool.commonPool()` or parallel streams.
- Every new count path is asserted equal to its SQL oracle on the measured data, as in Task 3 Step 5.

### Task 5: Native match sources

**Files:**
- Create: `engine/src/test/java/com/arcadedb/index/columnar/spike/SpikeMatchSource.java`
- Modify: `ColumnarSpikeCorrectnessTest.java`

**Interfaces:**
- Consumes: `TypeIndex.range(boolean ascending, Object[] beginKeys, boolean beginInclusive, Object[] endKeys, boolean endInclusive)` returning `IndexCursor` (`engine/src/main/java/com/arcadedb/index/TypeIndex.java:120`), `IndexCursor.next()` returning an `Identifiable`; `SpikeSegment`, `SpikeDictionary`.
- Produces:
  - `static long[] indexRange(Database db, SpikeSegment layout, String property, long fromInclusive, long toExclusive)` builds the match bitmap (same stripe layout as `SpikeTermsCounter.matchBitmap`) directly from the index cursor, no SQL, no `Result` objects; returns the bitmap. Positions on pages `>= layout.pageCount()` are counted into a returned uncovered figure via a `long[] uncoveredOut` parameter.
  - `static long[] columnEquals(SpikeSegment column, SpikeDictionary dictionary, String value)` builds the match bitmap by scanning the stored values of a columnar segment for `ordinalOf(value) + 1` (a columnar filter: how the real system serves an unindexed equality on a projected property). Unknown value gives an empty bitmap.
  - `static long cardinality(long[] bitmap)`.

- [ ] **Step 1: Failing tests** `indexRangeMatchesSqlPositions` (selective `fileSize < 10000` on 20 000 records with deletes: `indexRange` bitmap equals `matchBitmap(segment, positions(db, "fileSize < 10000"))` word for word) and `columnEqualsMatchesSqlPositions` (segment over `status`, `columnEquals(.., "published")` equals the bitmap of `positions(db, "status = 'published'")`; an unknown value yields cardinality 0).
- [ ] **Step 2: Run, expect compilation failure.**
- [ ] **Step 3: Implement.** `indexRange` obtains the index with `db.getSchema().getType(TYPE).getPolymorphicIndexByProperties(FILE_SIZE)` (or the equivalent lookup the engine offers; verify the name) and casts to `RangeIndex`. The bitmap is allocated once per call.
- [ ] **Step 4: Run** `ColumnarSpikeCorrectnessTest`, expect PASS (10 tests).
- [ ] **Step 5: Stop: changes ready for review.**

### Task 6: Multi-facet dataset, high cardinality and multi-valued segment

**Files:**
- Modify: `SpikeDataset.java` (new properties), `SpikeSegment.java` or Create `SpikeMultiSegment.java` (multi-valued), `SpikeTermsCounter.java` (multi-valued count and GROUP BY oracle), `ColumnarSpikeCorrectnessTest.java`

**Interfaces:**
- Produces:
  - `SpikeDataset` adds `MIME_TYPE = "mimeType"` (12 values, Zipf 1.0), `AUTHOR = "author"` (`AUTHOR_CARDINALITY = 100_000`, Zipf 0.8, `"author-%06d"`), `TAGS = "tags"` (`List<String>`, 1 to 5 distinct values per record, uniform count, values Zipf 1.0 over `TAG_CARDINALITY = 1_000`, `"tag-%04d"`). Same seed drives all properties; existing properties keep their values for a given seed (draw the new ones from a second `Random(seed + 1)`), so Task 1-5 tests stay valid. Record size grows; keep `title` padding so a page still holds roughly 250-300 records and report the actual figure.
  - `final class SpikeMultiSegment`: presence like `SpikeSegment`, plus per-row `int[] rowOffsets` (rows + 1) and bit-packed value ordinals (`ordinal`, no MISSING slot; an empty or absent list is MISSING, counted per row); `static SpikeMultiSegment build(LocalBucket, String, SpikeDictionary)`; `long serializedBytes()`.
  - `SpikeTermsCounter.countMulti(SpikeMultiSegment, long[] match, int cardinality)` returning `Counts` where `byOrdinal` counts records per value (a record with a tag listed once counts once) and `missing` counts matched rows with no tags.
  - `static List<Bucket> groupByUnwind(Database db, String whereClause, String property, int n)` oracle: `SELECT <p>, count(*) AS cnt FROM (SELECT <p> FROM Asset WHERE <where> UNWIND <p>) GROUP BY <p>` (verify the exact UNWIND form the SQL engine accepts; if the subquery form is not supported, use the form that is and say so in the report), same comparator as `groupBy`.
- [ ] **Step 1: Failing tests** `highCardinalityCountsEqualGroupBy` (author, 20 000 records, `fileSize < 500000`, full top-N equals `groupBy` oracle generalized to a property parameter) and `multiValuedCountsEqualUnwindGroupBy` (tags, same WHERE, full top-N equals `groupByUnwind`, `missing` equals matched records without tags).
- [ ] **Step 2: Run, expect failure.** **Step 3: Implement** (generalize `groupBy` to take the property name; keep the old signature delegating with `BRAND`). **Step 4: Run**, PASS (12 tests). **Step 5: Stop.**

### Task 7: Off-heap block-encoded segment

**Files:**
- Create: `SpikeBlockSegment.java`; Modify: `ColumnarSpikeCorrectnessTest.java`

**Interfaces:**
- Produces: `final class SpikeBlockSegment` built from a `SpikeSegment` (same presence and rank, so the same match bitmaps apply): stored values split into blocks of `BLOCK_ROWS = 16384` rows, each block frame-of-reference encoded (`min` stored value + bit width of `max - min`, bit-packed) into one direct `ByteBuffer` (`ByteBuffer.allocateDirect`, little-endian), with a `long[] blockOffsets` index; `int decodeBlock(int block, int[] scratch)` returns the row count decoded into a caller-owned `int[BLOCK_ROWS]` scratch; `long offHeapBytes()`; `long heapBytes()`.
  - `SpikeTermsCounter.countBlocks(SpikeBlockSegment, long[] match, int cardinality)` returning `Counts`: walks matched rows in row order, decodes each touched block once into the scratch (a block with no matched row is not decoded), counts from the scratch.
- This approximates plan 2/3 (codec + off-heap block + decode per block) without importing them: it measures the decode cost the heap spike hid.
- [ ] **Step 1: Failing test** `blockSegmentCountsEqualHeapSegment`: for the three Task 2 predicates, `countBlocks` equals `count` exactly (byOrdinal, missing); and a block containing only MISSING rows decodes to zeros.
- [ ] **Steps 2-5** as usual; PASS (13 tests).

### Task 8: Benchmark v2: native match, K-facet page, block variant

**Files:**
- Modify: `ColumnarSpikeBenchmark.java`, `SpikeReport.java` if needed

Measured per predicate (`selective` via `indexRange(fileSize, 0, 10000)`, `broad` via `columnEquals(status segment, "published")`), with the same rounds, balance, nanosecond timing and plan checks as Task 3:
- **K = 1 (the gate):** `nativeMatchMs` + `countMs` (brand, heap segment) vs `groupByMs` (brand). Keep the old SQL-match row too, labelled "SQL match source (run 1 method)".
- **K = 5 (reported):** facets brand, status, mimeType, author (top 10), tags (multi-valued, top 10): `nativeMatchMs` once + sum of 5 counts + 5 top-N vs the 5 `GROUP BY` queries run back to back (tags via the UNWIND oracle query). Report each facet's count time and each GROUP BY time.
- **Block variant (reported):** K = 1 and K = 5 with `countBlocks` for brand (and author), heap vs off-heap, plus off-heap bytes.
- Sizes: per segment (brand, status, mimeType, author, tags) bytes, bytes per record, presence share; high-cardinality count array bytes (`AUTHOR_CARDINALITY * 4`) per query and the spec 5.7 extrapolation with the measured dictionary size.
- Correctness asserted on the last measured round for every facet and both count paths.
- Verdict line: the gate as stated above, plus a second line "K = 5 page: projection X ms vs GROUP BY Y ms" and a third "block variant overhead vs heap: +Z%".
- [ ] **Step 1: Implement.** **Step 2: Smoke** `-DcolumnarSpike.records=50000`, PASS, results.md complete. **Step 3: Normal lane** runs only the correctness class (13 tests). **Step 4: grep** for System.out, none. **Step 5: Stop.**

### Task 9: Cold cache and concurrency

**Files:**
- Modify: `ColumnarSpikeBenchmark.java`

- **Cold:** after the warm measurements, `database.close()` and reopen the same database directory (ArcadeDB page cache emptied; the OS file cache is not, say so in the report), then measure exactly one round of K = 1 and K = 5 for both predicates, projection first on one reopen and GROUP BY first on a second reopen. Report as "cold (engine cache only)". The segments stay in heap / off-heap across the reopen (they model the projection's own cache, which the design keeps warm separately); state that assumption in the report.
- **Concurrent:** a fixed pool of `THREADS = Integer.getInteger("columnarSpike.threads", 8)` threads (dedicated `Executors.newFixedThreadPool`, shut down in `finally`), each running the K = 5 page `ITERATIONS = 20` times for one predicate; measure wall time for all, report queries per second for projection vs GROUP BY, and p50/p95 per query latency (nanosecond clock per query; stall-discounted over the whole window). Correctness asserted on one result per thread.
- [ ] **Step 1: Implement.** **Step 2: Smoke at 50 000.** **Step 3: Stop.**

### Task 10: Full runs and spike results v2 in plan 3

Same as Task 4 (two full runs at 1 000 000, both copied to the scratchpad as `columnar-spike-v2-run1.md` / `-run2.md`, load recorded, runs must agree on the gate, within-noise flag under a 10% gap), then rewrite plan 3's `## Spike results`: the gate verdict first; then K = 1, K = 5, block variant, cold, concurrent tables; sizes per segment; high-cardinality memory; page re-read (keep Task 4's figure if unchanged); the run 1 (SQL match source) table kept under "Superseded method" with one line on why it was not predictive; and the observations for plan 3 (presence encoding, block size, decode share of count time, multi-valued layout, whether count arrays for 100 000 values argue for a sparse counter). Update the defaults table only where numbers argue for it. If the gate is NO-GO, stop and report to the user.

## Self-review

- **Spec coverage:** one string field, one bucket, one segment with presence bitmap and global dictionary (Task 1); terms top-N from a SQL `WHERE` vs `GROUP BY` (Task 2, Task 3 step 4); repository-sized dataset, ~200 brands, skew (Task 1 step 3, Task 3 constants); go/no-go faster than `GROUP BY` (Task 3 step 8, Task 4 step 3); segment size (Task 3 step 3); page re-read cost for 6.3 (Task 3 step 6); count array memory for 5.7 (Task 3 step 7); results into plan 3 (Task 4). Out of scope by design: multi-valued, numeric codecs, live bitmaps, overlay, dirty pages.
- **Type consistency:** `SpikeSegment.MISSING`, `storedValueAtRow` (stored `ordinal + 1`), `SpikeTermsCounter.Counts(byOrdinal, missing, uncovered)`, `SpikeTermsCounter.Bucket(value, count)` are used with the same names in Tasks 2 to 4.
- **Proportion:** four tasks, seven small test files, no production code.
