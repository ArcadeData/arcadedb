# Columnar Faceting - Extraction (plan 2 of 6) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** move the time-series codecs into a shared `com.arcadedb.engine.codec` package without changing a byte they write, add random-access numeric codecs (bit-packed, linear, blockwise linear, a chooser), a self-describing block header with CRC32, and a server-wide byte-budget block cache, so plan 3 can build its segment store on them.

**Architecture:** codecs are stateless static utilities over primitive arrays. `BlockHeader` and `BlockCache` live in a new `com.arcadedb.engine.block` package. The time-series engine changes only its imports: its file format, its `TimeSeriesCodec` enum and its decoded-column cache stay exactly as they are (decisions D1-D3 below). Every task that touches time-series code ends with the full time-series suite.

**Tech Stack:** Java 21, JUnit 5, AssertJ, Maven. No new dependency.

**Spec:** `docs/superpowers/specs/2026-10-18-columnar-faceting-design.md` (sections 3.1, 5.2, 5.3, 5.6, 5.7). Cross-plan contract: `docs/superpowers/plans/2026-10-19-columnar-faceting-0-roadmap.md` ("Plan 2 produces").

## Global Constraints

- **No git commits.** The user commits after review. Where the plan template says "Commit", the step is "Stop: changes ready for review", listing the files touched. (`git mv` is allowed: it stages a rename, it does not commit.)
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
- Plan-specific: the bytes `DeltaOfDeltaCodec`, `GorillaXORCodec`, `Simple8bCodec` and `DictionaryCodec` produce, and the names of the `TimeSeriesCodec` constants, must not change (they are the TS on-disk and schema-JSON format).
- Plan-specific: implement the Tantivy-style codecs from their description only; do not copy Tantivy source.

## Decisions taken by this plan

| # | Decision | Why |
|---|---|---|
| D1 | `TimeSeriesCodec` stays in `com.arcadedb.engine.timeseries.codec` | It is TS-specific (it names TS column codecs and `NONE`). It is persisted **by name**, not by ordinal or code: `LocalTimeSeriesType.java:423` writes `getCompressionHint().name()` into the schema JSON and `:482`, `RemoteTimeSeriesType.java:85`, `JsonlImporterFormat.java:640` read it back with `valueOf`. Sealed blocks store no codec id at all, the codec comes from the schema column. Leaving the enum where it is keeps every one of those, plus `network`, `integration` and `server`, untouched. Only engine files import the four moved classes, so no other module changes |
| D2 | TS keeps its own block format; `BlockHeader` is a new class modelled on it | The TSB4 block record (`TimeSeriesSealedStore.java:82-88`, `blockMeta` at `:3423`) interleaves min/max timestamps, block id, per-column numeric statistics and tag metadata between the column sizes and the data, stores absolute file offsets in `BlockEntry`, and puts its CRC32 in a trailer over meta plus data. No generic header can express that without changing bytes, and a byte change means a TSB5 magic and a file-version bump (`CURRENT_VERSION = 3`), which the team decision ("do not refactor `TimeSeriesSealedStore`") rules out. `BlockHeader` keeps the same ideas (magic, version, per-column sizes, CRC32, big-endian) and fixes two TS weaknesses: column offsets are relative to the block, and a separate header CRC lets a reader refuse a corrupted column size before allocating for it |
| D3 | `TimeSeriesDecodedColumnCache` does **not** move onto `BlockCache` in this plan | `arcadedb.timeSeriesDecodedBlockCacheRAM` is a documented `SCOPE.DATABASE`, per-shard, megabyte setting (`GlobalConfiguration.java:269-289`, `decodedBlockCacheBytes` at `:3850`); replacing it with a JVM-wide budget changes what operators configured and needs a deprecation decision. Its size accounting is type-aware (`IntegralColumn`, boxed shapes, distinct strings) and per-store hit counters are pinned by `Issue8179DecodedBlockCacheTest` and `TimeSeriesDecodedColumnCacheTest`. `BlockCache` is shaped so the move is mechanical later (`column` can carry `columnIndex * 3 + shape`, `sizer` takes the TS `sizeOf`, `invalidateFile` replaces `clear()`): follow-up issue, not a roadmap change |
| D4 | `BLOCK_CACHE_MAX_BYTES` = `arcadedb.blockCache.maxBytes`, `SCOPE.JVM`, `Long`, bytes; unset default `max(16 MB, maxHeap / 32)`, 16 MB when the heap is unbounded | The cache is a static singleton, one per JVM, exactly like `QUERY_PARALLELISM_POOL_THREADS` (`SCOPE.JVM`, line 303), so `SCOPE.SERVER` or `DATABASE` would promise an overlay that cannot exist. Heap/32 (256 MB on an 8 GB heap) sits well under the page cache (`MAX_PAGE_RAM`, a quarter of the heap) and next to plan 4's 256 MB query budget; decoded blocks are cheap to rebuild from the OS page cache, so a modest share is right. The 16 MB floor holds 32 full 65,536-long columns, and with 16 stripes still admits one 512 KB column per stripe. Uses `callbackIfNoSet` (like `TIMESERIES_DECODED_BLOCK_CACHE_RAM`), not `callback`, so `Issue7163DatabaseScopeCallbackTest.onlyTheAuditedSettingsCarryACallbackTheOverlayHookReaches` stays green |
| D5 | `BlockCache` = 16 lock-striped access-ordered `LinkedHashMap`s, each with an exact byte budget of `maxBytes / stripes`; loader and sizer run outside the stripe monitor | Same reasoning as `TimeSeriesDecodedColumnCache`'s javadoc (the budget is the safety property, so the charge, map and eviction move together under one monitor), striped because this cache is shared by every query in the JVM instead of one shard. Per-stripe budgets keep `usedBytes() <= maxBytes` true at every instant without cross-stripe locking. An entry larger than one stripe's share is returned uncached |

## Review Focus

1. **A sealed file written before the move must read identically after it.** Nothing in the TS suite compares bytes against a build from before the move. `CodecByteCompatibilityTest` (Task 1) pins length and CRC32 of each moved codec's output for fixed inputs, recorded on the unmoved code.
2. **Values at the edges of `long`.** `Long.MIN_VALUE` and `Long.MAX_VALUE` in one array give a range of 2^64-1 and deltas at or above 2^63: frame of reference, GCD, width and the linear prediction must use wrapping, unsigned arithmetic and still return every value exactly (tests in Tasks 2, 3, 4).
3. **Encoding a slice of a larger array.** `encode(values, offset, length)` with `offset > 0` must encode only the slice and never read past it (subrange tests in Tasks 2, 3, 4).
4. **Two queries missing on the same block at the same time.** Both loaders may run; the cache must hold one entry, charge it once, and hand both callers usable values (Task 7, `concurrentFirstLoadsOfOneKeyChargeOnce`).
5. **A corrupted header byte in a segment file.** A flipped bit in a column size must be refused by the header CRC before anything is allocated from that size, with a `BlockCorruptionException`, not a `BufferUnderflowException` or an `OutOfMemoryError` (Task 6).

---

## File Structure

| Path (module `engine`) | Action | Responsibility |
|---|---|---|
| `src/main/java/com/arcadedb/engine/codec/DeltaOfDeltaCodec.java` | Move from `engine/timeseries/codec/` | Unchanged content, new package |
| `src/main/java/com/arcadedb/engine/codec/GorillaXORCodec.java` | Move | idem |
| `src/main/java/com/arcadedb/engine/codec/Simple8bCodec.java` | Move | idem |
| `src/main/java/com/arcadedb/engine/codec/DictionaryCodec.java` | Move | idem |
| `src/main/java/com/arcadedb/engine/timeseries/codec/TimeSeriesCodec.java` | Modify javadoc only | Stays (D1) |
| `src/main/java/com/arcadedb/engine/timeseries/TimeSeriesSealedStore.java:25-28` | Modify imports | |
| `src/main/java/com/arcadedb/engine/timeseries/TimeSeriesShard.java:26-27` | Modify imports | |
| `src/main/java/com/arcadedb/engine/timeseries/IntegralColumn.java:21` | Modify import | |
| `src/main/java/com/arcadedb/engine/timeseries/TimeSeriesEngine.java:159` | Modify javadoc link | |
| `src/main/java/com/arcadedb/engine/codec/BitPackedCodec.java` | Create | Frame of reference + GCD + bit width, O(1) `get` |
| `src/main/java/com/arcadedb/engine/codec/LinearCodec.java` | Create | Line through first and last value, residuals bit-packed |
| `src/main/java/com/arcadedb/engine/codec/BlockwiseLinearCodec.java` | Create | One linear fit per 512 values, offset table |
| `src/main/java/com/arcadedb/engine/codec/NumericCodecChooser.java` | Create | Picks the smallest of the three, dispatches reads on the codec id byte |
| `src/main/java/com/arcadedb/engine/block/BlockHeader.java` | Create | Self-describing header, column offsets/sizes, header and payload CRC32 |
| `src/main/java/com/arcadedb/engine/block/BlockCorruptionException.java` | Create | Unchecked, extends `ArcadeDBException` |
| `src/main/java/com/arcadedb/engine/block/BlockCache.java` | Create | JVM-wide byte-budget cache |
| `src/main/java/com/arcadedb/GlobalConfiguration.java` (after `:289`) | Modify | `BLOCK_CACHE_MAX_BYTES` |
| `src/test/java/com/arcadedb/engine/codec/{DeltaOfDeltaCodecTest,GorillaXORCodecTest,Simple8bCodecTest,DictionaryCodecTest,SlidingBitReaderTest}.java` | Move from `engine/timeseries/codec/` | They use package-private `BitWriter`/`BitReader`, so they follow the codecs |
| `src/test/java/com/arcadedb/engine/codec/CodecByteCompatibilityTest.java` | Create | Golden length + CRC32 of the moved codecs |
| `src/test/java/com/arcadedb/engine/codec/{BitPackedCodecTest,LinearCodecTest,BlockwiseLinearCodecTest,NumericCodecChooserTest}.java` | Create | |
| `src/test/java/com/arcadedb/engine/block/{BlockHeaderTest,BlockCacheTest}.java` | Create | |
| `src/test/java/com/arcadedb/engine/block/BlockCacheConfigurationTest.java` | Create | Singleton reads `BLOCK_CACHE_MAX_BYTES` |
| 17 test files under `src/test/java/com/arcadedb/engine/timeseries/` importing the moved classes | Modify imports | Listed in Task 1 Step 4 |

## Verification commands (referenced by the tasks)

**TS suite** (every time-series test of the engine, plus the schema, SQL and config tests that create time-series types; the time-series test directory holds no `*IT` class, so `-Dtest` drags none in; `@Tag("slow")` tests run, only `benchmark` is excluded by the parent pom default):

```bash
mvn -o -pl engine -am test -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest='com/arcadedb/engine/timeseries/*,com/arcadedb/engine/timeseries/**/*,com/arcadedb/engine/codec/*,com/arcadedb/engine/codec/**/*,com/arcadedb/engine/block/*,com/arcadedb/engine/block/**/*,com/arcadedb/schema/*TimeSeries*,com/arcadedb/schema/Issue7702*,com/arcadedb/schema/Issue7703*,com/arcadedb/schema/Issue7299*,com/arcadedb/schema/Issue7589*,com/arcadedb/schema/Issue9747*,com/arcadedb/query/sql/executor/*TimeSeries*,com/arcadedb/function/sql/time/*,com/arcadedb/*Configuration*'
```

Passing: `BUILD SUCCESS` and, in surefire's `Results:` block, `Failures: 0, Errors: 0`, with a test count no lower than the baseline recorded in Task 1 Step 0 (plus the tests this plan adds).

**Downstream compile** (modules that use time-series classes; they must compile against the moved packages untouched):

```bash
mvn -o -pl network,integration,server,ha-raft,grpcw -am test-compile -q
```

Passing: exit code 0, no output.

---

### Task 1: Move the four codecs to `com.arcadedb.engine.codec`

**Files:**
- Move: the four main classes and five tests listed in File Structure
- Create: `engine/src/test/java/com/arcadedb/engine/codec/CodecByteCompatibilityTest.java` (written first in the old test package, moved in Step 4)
- Modify: `TimeSeriesSealedStore.java:25-28`, `TimeSeriesShard.java:26-27`, `IntegralColumn.java:21`, `TimeSeriesEngine.java:159`, `TimeSeriesCodec.java` javadoc, the 17 test files of Step 4

**Interfaces:**
- Consumes: nothing
- Produces: `com.arcadedb.engine.codec.DeltaOfDeltaCodec`, `GorillaXORCodec`, `Simple8bCodec`, `DictionaryCodec` with exactly today's public and package-private members (`DeltaOfDeltaCodec.MAX_BLOCK_SIZE`, `DictionaryCodec.MAX_DICTIONARY_SIZE`, `DeltaOfDeltaCodec.BitWriter`/`BitReader`, `Simple8bCodec.MAX_ZIGZAG_VALUE`, ...). `com.arcadedb.engine.timeseries.codec.TimeSeriesCodec` unchanged.

- [ ] **Step 0: Record the baseline**

Run the **TS suite** on the untouched tree. Write down the `Tests run` total and any test already red; a test red here is reported to the user, not fixed in this plan.

- [ ] **Step 1: Write the golden-bytes test against the unmoved codecs**

Create it in `engine/src/test/java/com/arcadedb/engine/timeseries/codec/CodecByteCompatibilityTest.java` (package `com.arcadedb.engine.timeseries.codec`). Fixed, seed-free inputs; each test asserts the encoded length and the `java.util.zip.CRC32` of the encoded bytes:

```java
@Test void deltaOfDeltaBytesAreUnchanged()     // long[1000], t[i] = 1_700_000_000_000L + i * 10_000L + (i % 7) * 3
@Test void gorillaXorBytesAreUnchanged()       // double[1000], v[i] = 20.0 + (i % 13) * 0.25, plus v[500] = Double.NaN
@Test void simple8bBytesAreUnchanged()         // long[1000], v[i] = (i % 2 == 0 ? i : -i) * 37L; also encode(values, nulls) with nulls bit 3 and 999 set
@Test void dictionaryBytesAreUnchanged()       // String[1000], v[i] = "host-" + (i % 17)
// each: assertThat(encoded.length).isEqualTo(EXPECTED_LEN); assertThat(crc32(encoded)).isEqualTo(EXPECTED_CRC);
```

Constants start at `0`.

- [ ] **Step 2: Record the constants from the current code**

Run: `mvn -o -pl engine -am test -Dtest=CodecByteCompatibilityTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL, each assertion reporting the actual length and CRC. Paste the actual values into the constants (CRC as hex literal), rerun, expect PASS. These constants now describe the format the TS files on disk carry.

- [ ] **Step 3: Move the files with history**

From the worktree root, no `cd` (a stray `cd` misroutes every later Maven run):

```bash
S=engine/src/main/java/com/arcadedb/engine; T=engine/src/test/java/com/arcadedb/engine
mkdir -p $S/codec $T/codec
for c in DeltaOfDeltaCodec GorillaXORCodec Simple8bCodec DictionaryCodec; do git mv $S/timeseries/codec/$c.java $S/codec/$c.java; done
for t in DeltaOfDeltaCodecTest GorillaXORCodecTest Simple8bCodecTest DictionaryCodecTest SlidingBitReaderTest; do
  git mv $T/timeseries/codec/$t.java $T/codec/$t.java; done
mv $T/timeseries/codec/CodecByteCompatibilityTest.java $T/codec/
```

Then change the `package` line of these ten files to `package com.arcadedb.engine.codec;`. No other edit inside them. `engine/src/test/java/com/arcadedb/engine/timeseries/codec/` is now empty: remove the directory.

- [ ] **Step 4: Rewrite the imports**

```bash
grep -rlE 'engine\.timeseries\.codec\.(DeltaOfDeltaCodec|GorillaXORCodec|Simple8bCodec|DictionaryCodec)' engine/src \
  | xargs sed -i '' -E 's/engine\.timeseries\.codec\.(DeltaOfDeltaCodec|GorillaXORCodec|Simple8bCodec|DictionaryCodec)/engine.codec.\1/g'
```

This covers the three main files, the javadoc link at `TimeSeriesEngine.java:159`, and the test files: `DictionaryCodecOverflowTest`, `Issue6360SealedStoreIntegrityTest`, `Issue7089NaNTransparentSumAvgTest`, `Issue7973SealedBlockIdentityTest`, `Issue8043SealedWalkAcrossInstallTest`, `Issue8166WalkAcrossDownsampleTest`, `Issue8793SharedTagValuesOnLoadTest`, `Issue8813SealedStoreDirectoryOrderTest`, `Issue9621SealedStoreChannelReopenTest`, `Issue9640SealedStoreShortTransferTest`, `Issue9721TimeSeriesIntegralNullTest`, `Issue9770SealedRewriteLeftoverTempTest`, `Issue9982DescendingOverlappingBlocksTest`, `TimeSeriesBlockStatsTest`, `TimeSeriesCrashRecoveryTest`, `TimeSeriesFormatVersionTest`, `TimeSeriesSealedStoreTest`. Re-sort each touched import block so `com.arcadedb.engine.codec.*` sits in alphabetical order. In `TimeSeriesCodec`'s class javadoc add one sentence: the implementations live in `com.arcadedb.engine.codec`, and the constant **names** are persisted in the schema JSON, so a constant must never be renamed.

- [ ] **Step 5: Compile and check nothing still points at the old package**

Run: `mvn -o -pl engine -am test-compile -q` then `grep -rnE 'timeseries\.codec\.(DeltaOfDelta|GorillaXOR|Simple8b|Dictionary)Codec' --include='*.java' .`
Expected: compile exit 0; grep prints nothing.

- [ ] **Step 6: Run the golden test, the moved tests and the TS suite**

Run the **TS suite**.
Expected: PASS, `CodecByteCompatibilityTest` included, count = baseline + 4.

- [ ] **Step 7: Downstream compile**

Run the **Downstream compile**. Expected: exit 0.

- [ ] **Step 8: Stop: changes ready for review**

Files: the 9 moved files, `CodecByteCompatibilityTest.java`, `TimeSeriesSealedStore.java`, `TimeSeriesShard.java`, `IntegralColumn.java`, `TimeSeriesEngine.java`, `TimeSeriesCodec.java`, the 17 test files of Step 4.

---

### Task 2: `BitPackedCodec`

**Files:**
- Create: `engine/src/main/java/com/arcadedb/engine/codec/BitPackedCodec.java`
- Test: `engine/src/test/java/com/arcadedb/engine/codec/BitPackedCodecTest.java`

**Interfaces:**
- Consumes: nothing
- Produces:

```java
public final class BitPackedCodec {
  public static final byte CODEC_ID     = 1;
  public static final int  HEADER_BYTES = 24;
  public static byte[] encode(long[] values, int offset, int length);   // IndexOutOfBoundsException on a bad slice
  public static void   decode(byte[] encoded, long[] out);              // IllegalArgumentException: foreign id, truncated, out shorter than count
  public static long   get(byte[] encoded, int index);                  // IndexOutOfBoundsException outside [0, count)
  public static int    count(byte[] encoded);
  // package-private, used by LinearCodec / BlockwiseLinearCodec / NumericCodecChooser
  static int  encodedSize(long[] values, int offset, int length);       // exactly encode(...).length, one pass, no allocation
  static void encodeInto(long[] values, int offset, int length, byte[] out, int base);
  static long get(byte[] encoded, int base, int index);
  static void decode(byte[] encoded, int base, long[] out, int outOffset);
  static int  count(byte[] encoded, int base);
  static long unsignedGcd(long a, long b);                              // gcd of two unsigned 64-bit values, gcd(0, x) = x
}
```

Layout (all multi-byte fields little-endian, so the packed words are read with one `VarHandle` load):

| Offset | Size | Field |
|---|---|---|
| 0 | 1 | codec id = 1 |
| 1 | 1 | bit width `w`, 0..64 |
| 2 | 2 | reserved, 0 |
| 4 | 4 | count |
| 8 | 8 | `min` (frame of reference) |
| 16 | 8 | `gcd` (1 when all deltas are 0) |
| 24 | `ceil(count * w / 64) * 8` | packed `q[i] = (v[i] - min) / gcd`, unsigned, value `i` at bit `i * w` |

- [ ] **Step 1: Write the failing tests**

```java
@Test void emptyEncodesHeaderOnly()            // encode(new long[0],0,0).length == 24; count == 0; decode into long[0] does not throw
@Test void singleValueHasWidthZero()           // [42] -> length 24, encoded[1] == 0, get(e,0) == 42
@Test void allEqualHasWidthZero()              // 1000 x -7 -> length 24, every get == -7
@Test void negativeValuesRoundTrip()           // -500..499 -> encoded[1] == 10, decode equals input
@Test void gcdReducesWidth()                   // v[i] = 5 + i * 1000, n = 1000 -> encoded[1] == 10, round trip
@Test void fullRangeUsesWidth64()              // {MIN, MAX, 0, -1, 12345} -> encoded[1] == 64, decode and every get exact
@Test void gcdHandlesUnsignedDeltas()          // {Long.MIN_VALUE, 0L} -> delta 2^63, encoded[1] == 1, round trip
@Test void everyWidthRoundTrips()              // for w in 1..64: Random(w), n = 1000, values masked to w bits, values[0] = mask(w), values[1] = 0, values[2] = 1 (gcd 1)
                                               //   -> encoded[1] == w; decode equals; get(i) equals for every i
@Test void subrangeEncodesOnlyTheSlice()       // values 0..99, encode(values, 10, 20) -> count 20, get(0) == 10, get(19) == 29
@Test void encodedSizeMatchesEncode()          // 50 random arrays (Random(7)), lengths 0..3000
@Test void decodeRefusesForeignOrTruncatedInput() // e[0] = 2 -> IAE; Arrays.copyOf(e, e.length - 1) -> IAE; out too short -> IAE
@Test void getOutsideTheRangeThrows()          // get(e, -1), get(e, count) -> IndexOutOfBoundsException
```

- [ ] **Step 2: Run to verify they fail**

Run: `mvn -o -pl engine -am test -Dtest=BitPackedCodecTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAIL, `BitPackedCodec` not found.

- [ ] **Step 3: Implement `BitPackedCodec`**

One pass for `min`, `max` (signed); second pass for `gcd` of `v - min` with `unsignedGcd` (Stein's binary algorithm on unsigned values: `Long.numberOfTrailingZeros`, `Long.compareUnsigned`); `w = 64 - Long.numberOfLeadingZeros(Long.divideUnsigned(max - min, gcd))`. All arithmetic on deltas wraps; decoding is `min + gcd * q`, also wrapping, which restores the exact value. Point read, with `LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN)`:

```java
final long bit = (long) index * w;  final int word = (int) (bit >>> 6);  final int shift = (int) (bit & 63);
long q = (long) LONG_LE.get(encoded, base + HEADER_BYTES + word * 8) >>> shift;
if (shift + w > 64) q |= (long) LONG_LE.get(encoded, base + HEADER_BYTES + (word + 1) * 8) << (64 - shift);
if (w < 64) q &= (1L << w) - 1;
```

`w == 0` returns `min` without touching data. `decode` unpacks sequentially word by word (no per-index `get`). Validation lives in `decode` and `count`; `get` checks only the index, to stay O(1).

- [ ] **Step 4: Run to verify they pass**

Run: same command as Step 2. Expected: PASS, 12 tests.

- [ ] **Step 5: Stop: changes ready for review**

Files: `BitPackedCodec.java`, `BitPackedCodecTest.java`.

---

### Task 3: `LinearCodec`

**Files:**
- Create: `engine/src/main/java/com/arcadedb/engine/codec/LinearCodec.java`
- Test: `engine/src/test/java/com/arcadedb/engine/codec/LinearCodecTest.java`

**Interfaces:**
- Consumes: Task 2 package-private `BitPackedCodec.encodedSize`, `encodeInto`, `get(byte[], int, int)`, `decode(byte[], int, long[], int)`, `count(byte[], int)`
- Produces:

```java
public final class LinearCodec {
  public static final byte CODEC_ID     = 2;
  public static final int  HEADER_BYTES = 24;
  public static byte[] encode(long[] values, int offset, int length);
  public static void   decode(byte[] encoded, long[] out);
  public static long   get(byte[] encoded, int index);
  public static int    count(byte[] encoded);
  static int  encodedSize(long[] values, int offset, int length);
  static void encodeInto(long[] values, int offset, int length, byte[] out, int base);
  static long get(byte[] encoded, int base, int index);
  static void decode(byte[] encoded, int base, long[] out, int outOffset);
  static int  count(byte[] encoded, int base);
}
```

Layout (little-endian): `[0]` id = 2, `[1..7]` reserved 0, `[8..15]` intercept = first value, `[16..23]` slope as `Double.doubleToRawLongBits`, `[24..]` a complete `BitPackedCodec` encoding of the residuals. Prediction, identical at encode and decode (Java floating point is strict, so it is bit-reproducible):

```java
static long predict(final long intercept, final double slope, final int i) { return intercept + (long) (slope * i); }
// slope = length > 1 ? ((double) last - (double) first) / (length - 1) : 0.0 ; residual r[i] = v[i] - predict(i), wrapping
```

`encodedSize` computes residual min/max/gcd in one pass without allocating; `encodeInto` may allocate one `long[length]` residual array (encode runs on flush, not on the query path). `decode` decodes residuals straight into `out` and adds `predict(i)` in place.

- [ ] **Step 1: Write the failing tests**

```java
@Test void emptyAndSingleValue()                    // [] -> count 0; [9] -> get 0 == 9
@Test void arithmeticSequenceHasZeroWidthResiduals() // v[i] = 5 + 3L * i, n = 10_000 -> encoded.length == 48, every get exact
@Test void noisyLineRoundTrips()                    // v[i] = i * 1000L + Random(3).nextInt(101) - 50, n = 5000
@Test void negativeSlopeRoundTrips()                // v[i] = 1_000_000L - 17L * i
@Test void extremesRoundTrip()                      // {MIN, MAX, MIN, MAX, 0}, and MAX descending to MIN in 1000 even steps
@Test void subrangeEncodesOnlyTheSlice()            // encode(values, 7, 100) -> count 100, get(0) == values[7]
@Test void encodedSizeMatchesEncode()               // 50 random arrays
@Test void decodeRefusesForeignInput()              // encoded[0] = 1 -> IllegalArgumentException
```

- [ ] **Step 2: Run to verify they fail**

Run: `mvn -o -pl engine -am test -Dtest=LinearCodecTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAIL, `LinearCodec` not found.

- [ ] **Step 3: Implement `LinearCodec`** as specified in the Interfaces block.

- [ ] **Step 4: Run to verify they pass**

Run: `mvn -o -pl engine -am test -Dtest='LinearCodecTest,BitPackedCodecTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS.

- [ ] **Step 5: Stop: changes ready for review**

Files: `LinearCodec.java`, `LinearCodecTest.java`.

---

### Task 4: `BlockwiseLinearCodec`

**Files:**
- Create: `engine/src/main/java/com/arcadedb/engine/codec/BlockwiseLinearCodec.java`
- Test: `engine/src/test/java/com/arcadedb/engine/codec/BlockwiseLinearCodecTest.java`

**Interfaces:**
- Consumes: Task 3 package-private `LinearCodec.encodedSize`, `encodeInto`, `get(byte[], int, int)`, `decode(byte[], int, long[], int)`
- Produces:

```java
public final class BlockwiseLinearCodec {
  public static final byte CODEC_ID   = 3;
  public static final int  BLOCK_SIZE = 512;
  public static byte[] encode(long[] values, int offset, int length);
  public static void   decode(byte[] encoded, long[] out);
  public static long   get(byte[] encoded, int index);     // block = index >>> 9, LinearCodec.get(encoded, offsets[block], index & 511)
  public static int    count(byte[] encoded);
  static int encodedSize(long[] values, int offset, int length);
}
```

Layout (little-endian): `[0]` id = 3, `[1..3]` reserved, `[4..7]` count, `[8 .. 8 + 4 * blocks)` int offset of each block's `LinearCodec` encoding from the start of the array, zero-padded to a multiple of 8, then the blocks in order (each a multiple of 8 bytes, so every block's packed words stay 8-aligned). `blocks = ceil(count / 512)`; count 0 is the 8-byte header alone.

- [ ] **Step 1: Write the failing tests**

```java
@Test void emptyIsHeaderOnly()                      // length 8, count 0
@Test void lessThanOneBlockRoundTrips()             // 100 random values
@Test void blockBoundariesRoundTrip()               // n in {511, 512, 513, 1024, 1025}: decode equals; get(511), get(512), get(n - 1) exact
@Test void piecewiseLinearCompressesToHeaders()     // 4 blocks of 512, block b: v = b * 1_000_000_000_000L + i * (b + 1) * 7L
                                                    //   -> encoded.length == 24 + 4 * 48 == 216
@Test void extremesRoundTrip()                      // 1500 values alternating MIN / MAX
@Test void subrangeEncodesOnlyTheSlice()            // encode(values, 300, 900): count 900, get(0) == values[300], get(899) == values[1199]
@Test void encodedSizeMatchesEncode()               // 50 random arrays, lengths 0..3000
@Test void decodeRefusesForeignInput()              // encoded[0] = 2 -> IllegalArgumentException
```

- [ ] **Step 2: Run to verify they fail**

Run: `mvn -o -pl engine -am test -Dtest=BlockwiseLinearCodecTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAIL.

- [ ] **Step 3: Implement `BlockwiseLinearCodec`** as specified; each block is `LinearCodec.encodeInto(values, offset + b * 512, min(512, remaining), out, blockOffset)`.

- [ ] **Step 4: Run to verify they pass**

Run: `mvn -o -pl engine -am test -Dtest='BlockwiseLinearCodecTest,LinearCodecTest,BitPackedCodecTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS.

- [ ] **Step 5: Stop: changes ready for review**

Files: `BlockwiseLinearCodec.java`, `BlockwiseLinearCodecTest.java`.

---

### Task 5: `NumericCodecChooser`

**Files:**
- Create: `engine/src/main/java/com/arcadedb/engine/codec/NumericCodecChooser.java`
- Test: `engine/src/test/java/com/arcadedb/engine/codec/NumericCodecChooserTest.java`

**Interfaces:**
- Consumes: `encodedSize`, `encode`, `get`, `decode`, `count` and `CODEC_ID` of Tasks 2-4
- Produces:

```java
public final class NumericCodecChooser {
  public static byte[] encodeBest(long[] values, int offset, int length); // smallest encodedSize; ties: BitPacked, then Linear, then BlockwiseLinear;
                                                                          // length 0 always BitPacked (Blockwise's 8-byte empty form would win otherwise)
  public static long   get(byte[] encoded, int index);                    // switch on encoded[0]
  public static void   decode(byte[] encoded, long[] out);
  public static int    count(byte[] encoded);
  // IllegalArgumentException("Unknown numeric codec id N") for any other id, and for an empty array
}
```

The three `encodedSize` calls are the only full passes besides the winning `encode`; no candidate is encoded and thrown away.

- [ ] **Step 1: Write the failing tests**

```java
@Test void picksBitPackedForRandomValues()     // Random(11), 4096 values in [0, 1 << 20) -> encoded[0] == BitPackedCodec.CODEC_ID
@Test void picksLinearForArithmeticSequence()  // 5 + 3L * i, n = 10_000 -> LinearCodec.CODEC_ID
@Test void picksBlockwiseForPiecewiseLinear()  // same data as Task 4's piecewise test -> BlockwiseLinearCodec.CODEC_ID
@Test void allEqualAndEmptyPickBitPacked()     // 1000 x 7 (24 bytes vs 48 and 64) and the empty array -> BitPackedCodec.CODEC_ID
@Test void readsDispatchOnTheCodecId()         // for each of the three inputs: get(i) and decode equal the input; count equals length
@Test void unknownIdIsRefused()                // new byte[] {9, 0, 0, 0} and new byte[0] -> IllegalArgumentException
```

- [ ] **Step 2: Run to verify they fail**

Run: `mvn -o -pl engine -am test -Dtest=NumericCodecChooserTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAIL.

- [ ] **Step 3: Implement `NumericCodecChooser`** as specified.

- [ ] **Step 4: Run all codec tests**

Run: `mvn -o -pl engine -am test -Dtest='com/arcadedb/engine/codec/*,com/arcadedb/engine/codec/**/*' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS (moved codec tests, golden test and the four new classes).

- [ ] **Step 5: Stop: changes ready for review**

Files: `NumericCodecChooser.java`, `NumericCodecChooserTest.java`.

---

### Task 6: `BlockHeader`

**Files:**
- Create: `engine/src/main/java/com/arcadedb/engine/block/BlockHeader.java`, `engine/src/main/java/com/arcadedb/engine/block/BlockCorruptionException.java`
- Test: `engine/src/test/java/com/arcadedb/engine/block/BlockHeaderTest.java`

**Interfaces:**
- Consumes: nothing (TS format untouched, D2)
- Produces:

```java
public class BlockCorruptionException extends ArcadeDBException {   // unchecked, like ArcadeDBException
  public BlockCorruptionException(String message);
}
public final class BlockHeader {
  public static final int FIXED_BYTES = 20;                       // headerSize() = FIXED_BYTES + 4 * columnCount
  public BlockHeader(int magic, int version, int rowCount, int[] columnSizes, int payloadCrc32); // copies sizes; IAE: version or column count outside 0..65535, negative size or rowCount, totalSize() > Integer.MAX_VALUE
  public static BlockHeader forColumns(int magic, int version, int rowCount, byte[]... columns); // sizes + CRC32 over the columns in order
  public static BlockHeader read(ByteBuffer buffer);                // from position, advances it by headerSize(); BlockCorruptionException: truncated, header CRC mismatch, invalid sizes
  public static BlockHeader read(ByteBuffer buffer, int expectedMagic); // also BlockCorruptionException on another magic
  public void write(ByteBuffer buffer);                             // at position, advances it by headerSize()
  public int magic(); public int version(); public int rowCount(); public int columnCount(); public int payloadCrc32();
  public int columnOffset(int column);                              // from the first byte of the header: headerSize() + sum of earlier sizes
  public int columnSize(int column);
  public int headerSize(); public int totalSize();
  public void verifyPayload(ByteBuffer buffer, int blockStart);     // CRC32 of [blockStart + headerSize(), blockStart + totalSize()), BlockCorruptionException on mismatch
  public static int crc32(ByteBuffer data, int offset, int length); // absolute region, does not move data's position, works on direct and mapped buffers
}
```

Layout, always big-endian like the TS format (`read`/`write` work on `buffer.duplicate().order(ByteOrder.BIG_ENDIAN)` and then advance the caller's position, so the caller's byte order is irrelevant):

| Offset | Size | Field |
|---|---|---|
| 0 | 4 | magic (chosen by the format using it) |
| 4 | 2 | version, unsigned |
| 6 | 2 | column count `n`, unsigned |
| 8 | 4 | row count |
| 12 | 4n | column sizes |
| 12 + 4n | 4 | payload CRC32 (the column bytes that follow the header, in order) |
| 16 + 4n | 4 | header CRC32 (bytes `[0, 16 + 4n)`) |

`read` checks the header CRC before it builds the size array into a `BlockHeader`, so a corrupted size is refused before any caller allocates from it. Offsets are precomputed into an `int[]` at construction.

- [ ] **Step 1: Write the failing tests**

```java
@Test void roundTripPreservesFieldsAndOffsets()   // forColumns(0x46435331, 1, 42, b10, b0, b7): read(write) equal fields;
                                                   //   headerSize() == 32; columnOffset(0) == 32, (1) == 42, (2) == 42; totalSize() == 49
@Test void writeAndReadAdvanceThePosition()        // buffer position before = 5 -> after write and after read == 5 + headerSize()
@Test void byteOrderOfTheBufferIsIgnored()         // write into a LITTLE_ENDIAN heap buffer, read from a BIG_ENDIAN direct buffer copy -> equal
@Test void corruptedColumnSizeIsRefusedByTheHeaderCrc() // flip one bit at offset 12 -> BlockCorruptionException, message contains "header CRC"
@Test void truncatedHeaderIsRefused()              // only 10 bytes, and only headerSize() - 1 bytes -> BlockCorruptionException (not BufferUnderflowException)
@Test void payloadMismatchIsRefused()              // header + columns written; flip one payload byte -> verifyPayload throws BlockCorruptionException
@Test void wrongMagicIsRefused()                   // read(buffer, 0x12345678) on a 0x46435331 header -> BlockCorruptionException
@Test void crc32MatchesTheJdkAndKeepsThePosition()  // crc32(buf, 3, 100) == CRC32 of the same 100 bytes as byte[]; buf.position() unchanged; same on a direct buffer
@Test void zeroColumnsIsValid()                    // headerSize() == 20, totalSize() == 20
@Test void invalidShapesAreRefusedAtConstruction() // 65536 columns, version 70000, size -1, two sizes of Integer.MAX_VALUE -> IllegalArgumentException
```

- [ ] **Step 2: Run to verify they fail**

Run: `mvn -o -pl engine -am test -Dtest=BlockHeaderTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAIL.

- [ ] **Step 3: Implement `BlockCorruptionException` and `BlockHeader`** as specified; `java.util.zip.CRC32.update(ByteBuffer)` on a sliced duplicate for `crc32`.

- [ ] **Step 4: Run to verify they pass**

Run: same as Step 2. Expected: PASS, 10 tests.

- [ ] **Step 5: Stop: changes ready for review**

Files: `BlockHeader.java`, `BlockCorruptionException.java`, `BlockHeaderTest.java`.

---

### Task 7: `BLOCK_CACHE_MAX_BYTES` and `BlockCache`

**Files:**
- Modify: `engine/src/main/java/com/arcadedb/GlobalConfiguration.java` (new constant right after `TIMESERIES_DECODED_BLOCK_CACHE_RAM`, which ends at `:289`)
- Create: `engine/src/main/java/com/arcadedb/engine/block/BlockCache.java`
- Test: `engine/src/test/java/com/arcadedb/engine/block/BlockCacheTest.java`, `engine/src/test/java/com/arcadedb/engine/block/BlockCacheConfigurationTest.java`

**Interfaces:**
- Consumes: nothing
- Produces:

```java
// GlobalConfiguration (D4)
BLOCK_CACHE_MAX_BYTES("arcadedb.blockCache.maxBytes", SCOPE.JVM, "<description>", Long.class, 16L * 1024 * 1024, null,
    value -> { /* maxHeap == Long.MAX_VALUE ? 16 MB : Math.max(16 MB, maxHeap / 32) */ }),

// com.arcadedb.engine.block
public final class BlockCache {
  public static final int DEFAULT_STRIPES = 16;
  public static BlockCache instance();                              // lazy holder, budget = BLOCK_CACHE_MAX_BYTES read on every admission
  BlockCache(LongSupplier maxBytes, int stripes);                   // package-private, tests; stripes a power of two
  public <T> T get(long fileId, long blockId, int column, Supplier<T> loader, ToLongFunction<T> sizer);
  public void invalidateFile(long fileId);
  public long usedBytes();                                          // sum of the stripes' exact totals
  public long newFileId();                                          // process-unique, from an AtomicLong starting at 1; ids are never persisted
  public long hits(); public long misses();                         // LongAdder
  public void clear();
  long recountBytes();                                              // package-private: sum of held charges walked entry by entry, for tests
}
```

Description text for the setting (no em dash): server-wide memory budget, in bytes, for decoded column blocks read through `BlockCache` (columnar index segments); one budget for the whole JVM whatever the number of databases, types and buckets; split across 16 stripes, so an entry above a sixteenth of it is never held; 0 disables caching; when unset it is heap/32, never below 16 MB; a change applies at the next admission; the time-series decoded column cache keeps its own per-shard budget, `arcadedb.timeSeriesDecodedBlockCacheRAM`.

`get` semantics: key `record Key(long fileId, long blockId, int column)`, stripe `= spread(key.hashCode()) & (stripes - 1)`. Under the stripe monitor: lookup (hit: count, relink, return). On a miss, outside any monitor: `loader.get()`; `null` is returned and not cached; budget `<= 0` or `sizer > maxBytes / stripes` returns the value uncached. Back under the monitor: if another thread admitted the key meanwhile, return **its** value and drop ours (charged once); otherwise insert, add the charge, then evict from the eldest end while the stripe total exceeds `maxBytes / stripes`. Loader exceptions propagate and leave nothing behind. No I/O and no loader call inside a monitor.

- [ ] **Step 1: Write the failing tests**

`BlockCacheTest` (`new BlockCache(() -> budget, stripes)`):

```java
@Test void hitReturnsTheCachedInstanceWithoutLoading()        // 1 stripe: second get returns same instance, loader called once; hits()==1, misses()==1
@Test void evictsLeastRecentlyUsedUnderBudget()                // 1 stripe, budget 100, sizer 40: get a, b; get a; get c -> b reloads next, a does not; usedBytes()==80
@Test void entryLargerThanAStripeIsReturnedButNotHeld()        // 4 stripes, budget 400, sizer 101 -> value returned, usedBytes()==0, loader runs again next time
@Test void zeroBudgetDisablesCaching()                         // budget 0: loader runs on every call, usedBytes()==0
@Test void budgetShrinkAppliesAtTheNextAdmission()             // 1 stripe, budget 1000 holding 10 x 100; budget = 300; one more get -> usedBytes() <= 300
@Test void invalidateFileDropsOnlyThatFile()                   // entries of files 1 and 2; invalidateFile(1) -> file-1 keys reload, file-2 keys hit; usedBytes() == recountBytes()
@Test void nullFromTheLoaderIsNotCached()                      // loader returns null twice -> two loader calls, usedBytes()==0
@Test void loaderExceptionLeavesNothingBehind()                // loader throws IllegalStateException -> propagates; usedBytes()==0; next get loads
@Test void concurrentFirstLoadsOfOneKeyChargeOnce()            // 8 threads, CountDownLatch start, same key, loader returns new long[1000], sizer 8016:
                                                               //   usedBytes()==8016; a later get returns the held instance
@Test void concurrentAccessStaysWithinBudgetAndExact()         // 16 stripes, budget 1 MB, 8 threads on an ExecutorService, 20_000 ops each,
                                                               //   Random(threadIndex) keys over 3 files x 500 blocks x 4 columns, sizes 1..64 KB,
                                                               //   1 op in 500 invalidateFile; after shutdown+awaitTermination:
                                                               //   usedBytes() <= 1 MB, usedBytes() == recountBytes(), no task threw
@Test void newFileIdIsUnique()                                 // 10_000 ids from 4 threads -> all distinct and positive
```

`BlockCacheConfigurationTest`:

```java
@Test void defaultScalesWithTheHeapWithAFloor()   // after reset(): value == max(16 MB, maxMemory / 32), or 16 MB when maxMemory == Long.MAX_VALUE
@Test void instanceIsASingletonReadingTheSetting() // instance() == instance(); setValue(0L) -> a get through instance() is not held (usedBytes() unchanged);
                                                   //   finally BLOCK_CACHE_MAX_BYTES.reset() and instance().clear()
@Test void settingResolvesByKeyAndIsJvmScoped()    // findByKey("arcadedb.blockCache.maxBytes") == BLOCK_CACHE_MAX_BYTES; getScope() == SCOPE.JVM; getType() == Long.class
```

- [ ] **Step 2: Run to verify they fail**

Run: `mvn -o -pl engine -am test -Dtest='BlockCacheTest,BlockCacheConfigurationTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: compilation FAIL.

- [ ] **Step 3: Add `BLOCK_CACHE_MAX_BYTES`** with the 7-argument constructor (`callback` null, `callbackIfNoSet` the heap rule), as `TIMESERIES_DECODED_BLOCK_CACHE_RAM` does.

- [ ] **Step 4: Implement `BlockCache`** as specified. Class javadoc states D5 and that cached values are shared and must be treated as read-only, as `TimeSeriesDecodedColumnCache`'s are.

- [ ] **Step 5: Run the new tests and the configuration sweeps**

Run: `mvn -o -pl engine -am test -Dtest='BlockCacheTest,BlockCacheConfigurationTest,com/arcadedb/*Configuration*,Issue7163DatabaseScopeCallbackTest,Issue7124BooleanSettingStrictCoercionTest,Issue7296AdminCommandAllowListTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS, including `onlyTheAuditedSettingsCarryACallbackTheOverlayHookReaches` and `everySettingResolvesByItsOwnKey`.

- [ ] **Step 6: Stop: changes ready for review**

Files: `GlobalConfiguration.java`, `BlockCache.java`, `BlockCacheTest.java`, `BlockCacheConfigurationTest.java`.

---

### Task 8: Gate - full time-series suite, engine module, downstream modules

**Files:** none new. Fix-ups only, in files this plan already touched.

**Interfaces:**
- Consumes: everything above
- Produces: the roadmap gate "Full time-series suite green"

- [ ] **Step 1: TS suite**

Run the **TS suite**. Expected: PASS; count = Task 1 baseline + every test this plan added; no test red that was green at baseline.

- [ ] **Step 2: Whole engine module**

Run: `mvn -o -pl engine -am verify`
Expected: `BUILD SUCCESS`, `Failures: 0, Errors: 0` in `Results:` (any failure also present at Task 1 Step 0 baseline is reported, not fixed).

- [ ] **Step 3: Downstream modules**

Run the **Downstream compile**, then `mvn -o -pl integration -am test -Dsurefire.failIfNoSpecifiedTests=false -Dtest=Issue9310JsonlRestoredColumnCodecTest`
Expected: compile exit 0; PASS. Under D1 no downstream source changes, so compiling is the real check; this is the one downstream unit test that names `TimeSeriesCodec`. The two downstream ITs naming it (`Issue7032JsonlRoundTripIT`, `Issue7689RemoteTimeSeriesCompleteCreateIT`) are left to CI: running an IT through `-Dit.test` with `-am` fails the parent module.

- [ ] **Step 4: Leftovers**

Run: `grep -rn 'System.out' engine/src/main/java/com/arcadedb/engine/codec engine/src/main/java/com/arcadedb/engine/block` and `grep -n $'\xe2\x80\x94' engine/src/*/java/com/arcadedb/engine/codec/{BitPacked,Linear,BlockwiseLinear,NumericCodecChooser,CodecByteCompatibility}*.java engine/src/*/java/com/arcadedb/engine/block/*.java`
Expected: no output. (Only the new files: the moved codecs already carry em dashes in their comments and stay byte-for-byte as they were.)

- [ ] **Step 5: Stop: changes ready for review**

All files of Tasks 1-7. Report to the user: the gate result, the baseline reds if any, and the follow-up issue to file for D3 (move `TimeSeriesDecodedColumnCache` onto `BlockCache`, deprecating `arcadedb.timeSeriesDecodedBlockCacheRAM`).
