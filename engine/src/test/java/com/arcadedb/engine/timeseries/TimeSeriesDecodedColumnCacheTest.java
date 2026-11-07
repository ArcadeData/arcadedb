/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.engine.timeseries;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.LocalTimeSeriesType;
import com.arcadedb.schema.TimeSeriesTypeBuilder;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The budget arithmetic of {@link TimeSeriesDecodedColumnCache}, driven directly rather than through a shard
 * (code review on PR #8194).
 * <p>
 * {@code TimeSeriesDecodedColumnCacheTest} covers what the cache does for a QUERY, and its budget assertions held
 * because nothing it decodes comes close to the budget - so the two branches that only fire under budget pressure,
 * refusing an oversized column and evicting least-recently-used ones, were never reached by it. Reaching them through
 * the engine would mean sealing blocks near {@code SEALED_BLOCK_SIZE} against a budget sized just under one of their
 * columns, which is a slow and indirect way to assert arithmetic. Synthetic arrays sized around the budget say the
 * same thing exactly.
 * <p>
 * Sizes here are derived from the charge the cache applies - 16 bytes of array header plus 8 per primitive element -
 * rather than hardcoded, so a change to the estimate moves the tests with it instead of breaking them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TimeSeriesDecodedColumnCacheTest {

  private static final int SHAPE = TimeSeriesDecodedColumnCache.SHAPE_RAW;

  /** What the cache charges for a {@code long[]} of this length. */
  private static long charge(final int length) {
    return 16L + 8L * length;
  }

  @Test
  void aColumnLargerThanTheWholeBudgetIsNotAdmitted() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(charge(100));

    cache.put(1L, 0, SHAPE, new long[200]);

    assertThat(cache.get(1L, 0, SHAPE))
        .as("admitting it would evict everything to make room for an entry the next admission evicts in turn")
        .isNull();
    assertThat(cache.getHeldBytes()).isZero();
  }

  @Test
  void aColumnThatExactlyFitsTheBudgetIsAdmitted() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(charge(100));

    cache.put(1L, 0, SHAPE, new long[100]);

    assertThat(cache.get(1L, 0, SHAPE)).isNotNull();
    assertThat(cache.getHeldBytes()).isEqualTo(charge(100));
  }

  @Test
  void admittingPastTheBudgetEvictsUntilItFitsAgain() {
    // Room for two columns of 100 elements, not three.
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(charge(100) * 2 + 8);

    cache.put(1L, 0, SHAPE, new long[100]);
    cache.put(2L, 0, SHAPE, new long[100]);
    assertThat(cache.getHeldBytes()).isEqualTo(charge(100) * 2);

    cache.put(3L, 0, SHAPE, new long[100]);

    assertThat(cache.getHeldBytes())
        .as("the budget is met again after the admission, not merely approached")
        .isLessThanOrEqualTo(charge(100) * 2 + 8);
    assertThat(cache.get(1L, 0, SHAPE)).as("the least recently used column is the one that went").isNull();
    assertThat(cache.get(2L, 0, SHAPE)).isNotNull();
    assertThat(cache.get(3L, 0, SHAPE)).isNotNull();
  }

  /**
   * Eviction order follows USE and not admission, which is the whole difference between an LRU and a queue: the block
   * a dashboard keeps polling has to survive the blocks admitted around it.
   */
  @Test
  void readingAnEntryKeepsItAheadOfAnOlderNeighbour() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(charge(100) * 2 + 8);

    cache.put(1L, 0, SHAPE, new long[100]);
    cache.put(2L, 0, SHAPE, new long[100]);

    // Touch the FIRST one, so the second becomes the least recently used.
    assertThat(cache.get(1L, 0, SHAPE)).isNotNull();

    cache.put(3L, 0, SHAPE, new long[100]);

    assertThat(cache.get(1L, 0, SHAPE)).as("read since it was admitted, so it outlives its neighbour").isNotNull();
    assertThat(cache.get(2L, 0, SHAPE)).isNull();
  }

  @Test
  void aDisabledCacheHoldsNothingAndReportsItself() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(0);

    cache.put(1L, 0, SHAPE, new long[10]);

    assertThat(cache.isEnabled()).isFalse();
    assertThat(cache.get(1L, 0, SHAPE)).isNull();
    assertThat(cache.getHeldBytes()).isZero();
  }

  /**
   * A block written before issue #8043 carries no id, and {@code NO_BLOCK_ID} is not an identity: caching under it
   * would serve one such block's column for another's.
   */
  @Test
  void aBlockWithoutAnIdIsNeverCached() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(charge(1000));

    cache.put(TimeSeriesSealedStore.BlockEntry.NO_BLOCK_ID, 0, SHAPE, new long[10]);

    assertThat(cache.get(TimeSeriesSealedStore.BlockEntry.NO_BLOCK_ID, 0, SHAPE)).isNull();
    assertThat(cache.getHeldBytes()).isZero();
  }

  /**
   * Re-admitting the same key replaces the entry rather than double-charging for it, which a budget kept as a running
   * total gets wrong if the previous charge is not backed out.
   */
  @Test
  void readmittingTheSameColumnDoesNotDoubleCharge() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(charge(1000));

    cache.put(1L, 0, SHAPE, new long[100]);
    cache.put(1L, 0, SHAPE, new long[100]);

    assertThat(cache.getHeldBytes()).isEqualTo(charge(100));
  }

  /**
   * The shape is part of the key, so the same column read raw and boxed is two entries and the budget counts both.
   */
  @Test
  void theSameColumnInTwoShapesIsTwoEntries() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(charge(1000));

    cache.put(1L, 0, TimeSeriesDecodedColumnCache.SHAPE_RAW, new long[100]);
    cache.put(1L, 0, TimeSeriesDecodedColumnCache.SHAPE_DOUBLE, new double[100]);

    assertThat(cache.get(1L, 0, TimeSeriesDecodedColumnCache.SHAPE_RAW)).isNotNull();
    assertThat(cache.get(1L, 0, TimeSeriesDecodedColumnCache.SHAPE_DOUBLE)).isNotNull();
    assertThat(cache.getHeldBytes()).isEqualTo(charge(100) * 2);
  }

  /**
   * A dictionary column is charged for the DISTINCT strings it keeps alive on top of its slots, so the same row count
   * costs more when it retains more of them - the accounting CodeRabbit caught on PR #8194.
   */
  @Test
  void distinctStringsAreChargedOnceEach() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(1024L * 1024L);

    final String shared = "a".repeat(100);
    final String[] repeated = new String[500];
    Arrays.fill(repeated, shared);

    final String[] distinct = new String[500];
    for (int i = 0; i < distinct.length; i++)
      distinct[i] = i + "a".repeat(100);

    cache.put(1L, 0, SHAPE, repeated);
    final long repeatedBytes = cache.getHeldBytes();

    cache.put(2L, 0, SHAPE, distinct);
    final long distinctBytes = cache.getHeldBytes() - repeatedBytes;

    assertThat(distinctBytes)
        .as("500 distinct 100-char strings retain far more than 500 references to one")
        .isGreaterThan(repeatedBytes * 2);
  }

  /**
   * Boxed columns whose elements are shared instances - a STRING column handed back unchanged, a BOOLEAN or BYTE
   * autoboxed to a JVM-cached constant - are charged as references, while a column that really did allocate per row
   * is not.
   * <p>
   * A SHORT is the case that has to keep being charged per row despite looking like the BYTE beside it:
   * {@code Short.valueOf} caches -128..127 too, but that is a sliver of a short's range rather than all of it, so
   * values outside it allocate.
   */
  @Test
  void onlyBoxedColumnsThatAllocatedPerRowAreChargedPerRow() {
    final TimeSeriesDecodedColumnCache cache = new TimeSeriesDecodedColumnCache(1024L * 1024L);

    final Object[] booleans = new Object[500];
    final Object[] bytes = new Object[500];
    final Object[] shorts = new Object[500];
    final Object[] doubles = new Object[500];
    for (int i = 0; i < booleans.length; i++) {
      booleans[i] = i % 2 == 0;
      bytes[i] = (byte) i;
      // Deliberately past Short.valueOf's cache, which is where a short stops behaving like a byte.
      shorts[i] = (short) (1_000 + i);
      doubles[i] = (double) i;
    }

    assertThat(chargeFor(cache, 1L, booleans))
        .as("Boolean.TRUE/FALSE are singletons, so only the references are held")
        .isEqualTo(16L + 8L * booleans.length);
    assertThat(chargeFor(cache, 2L, bytes))
        .as("Byte.valueOf caches every value a byte can hold, so a boxed byte is never allocated")
        .isEqualTo(16L + 8L * bytes.length);
    assertThat(chargeFor(cache, 3L, shorts))
        .as("a short outside -128..127 really was allocated, however much it resembles the byte above")
        .isEqualTo(16L + 24L * shorts.length);
    assertThat(chargeFor(cache, 4L, doubles))
        .as("a boxed Double per row really was allocated per row")
        .isEqualTo(16L + 24L * doubles.length);
  }

  /** What admitting {@code column} under {@code blockId} added to the cache's running total. */
  private static long chargeFor(final TimeSeriesDecodedColumnCache cache, final long blockId, final Object[] column) {
    final long before = cache.getHeldBytes();
    cache.put(blockId, 0, TimeSeriesDecodedColumnCache.SHAPE_BOXED, column);
    return cache.getHeldBytes() - before;
  }

  @Nested
  class DecodedBlockCache extends TestHelper {
    // #8179
    // Issue #8179: reading the same sealed block twice decodes it once.
    // <p>
    // A tag-filtered last-point query - {@code WHERE host = 'host_5' ORDER BY ts DESC LIMIT 1}, what an operational
    // dashboard polls most - was several times slower against a native TIMESERIES type than against an ordinary
    // DOCUMENT type with a {@code (host, ts)} index over the same points. Block-level tag pruning was not the gap:
    // it already works, and a shard holding none of the tag's rows already skips every block on its directory entry
    // alone. The gap was that the ONE block which does hold the tag was re-read and re-decoded on every poll. A block
    // holds up to {@code TimeSeriesShard.SEALED_BLOCK_SIZE} = 65536 samples, so answering with one row decoded 65536
    // timestamps, 65536 tag values and 65536 measurements, and answering again decoded all of them again.
    // <p>
    // <b>What these tests assert is the WORK, not the clock.</b> A wall-clock bound on a full-suite run is a coin flip
    // on the JVM's mood, and the thing the change makes true is exact and countable: the second read decodes nothing.
    // Every test therefore also pins the ANSWER against the answer the same read gives with the cache turned off,
    // because a cache that returns quickly and wrongly is the only outcome worse than a slow one.
    //
    // @author Luca Garulli (l.garulli@arcadedata.com)

    private static final long BASE_TS = 1_700_000_000_000L;
    private static final long STEP_MS = 10L;
    /**
     * Bucket-aligned compaction, so a few thousand samples seal into several blocks rather than one. The cache is
     * keyed per block, so a single-block sealed layer would not show that the right block is the one being reused.
     */
    private static final long BUCKET_MS = 1_000L;

    private static final int[] TS_AND_VALUE = null; // every column, which is what the SQL last-point read projects

    @AfterEach
    void restoreCacheBudget() {
      GlobalConfiguration.TIMESERIES_DECODED_BLOCK_CACHE_RAM.reset();
    }

    // #8179
    /**
     * The issue's own query shape: the second identical last-point read decodes no column at all.
     */
    @Test
    void aRepeatedLastPointQueryDecodesTheBlockOnce() throws Exception {
      final TimeSeriesEngine engine = createMetric("last_point", 1, 8L);
      appendHosts(engine, 0, 4_000, 10);
      engine.compactAll();

      final TimeSeriesDecodedColumnCache cache = cacheOf(engine);
      final TagFilter host5 = TagFilter.eq(0, "host_5");

      final List<Object[]> first = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host5, 1, null);
      final long missesAfterFirst = cache.getMisses();
      assertThat(missesAfterFirst)
          .as("the first read has to decode the columns it needs; nothing is cached yet")
          .isPositive();
      assertThat(first).hasSize(1);

      final long hitsAfterFirst = cache.getHits();
      final List<Object[]> second = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host5, 1, null);

      assertThat(cache.getMisses())
          .as("the second read of the same blocks must decode nothing: every column it wants is already held")
          .isEqualTo(missesAfterFirst);
      assertThat(cache.getHits())
          .as("and it must have been served from the cache rather than having skipped the blocks")
          .isGreaterThan(hitsAfterFirst);
      assertThat(rows(second)).isEqualTo(rows(first));
    }

    // #8179
    /**
     * The answer with a warm cache is the answer with no cache, for each read shape that decodes a block: the
     * descending and ascending bounded scans, the full range scan, and the aggregation push-down.
     */
    @Test
    void theWarmCacheAnswersWhatTheDisabledCacheAnswers() throws Exception {
      final TagFilter host3 = TagFilter.eq(0, "host_3");

      final TimeSeriesEngine cold = createMetric("cold", 1, 0L);
      appendHosts(cold, 0, 4_000, 10);
      cold.compactAll();
      assertThat(cacheOf(cold).isEnabled()).as("a zero budget disables the cache outright").isFalse();

      final List<List<Object>> coldDescending = rows(
          cold.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host3, 5, null));
      final List<List<Object>> coldAscending = rows(
          cold.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host3, 5, null));
      final List<List<Object>> coldRange = rows(cold.query(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host3));
      final double coldSum = sumOfValues(cold, host3);

      final TimeSeriesEngine warm = createMetric("warm", 1, 8L);
      appendHosts(warm, 0, 4_000, 10);
      warm.compactAll();

      // Twice each, so every assertion below compares an answer produced from cached columns rather than from the
      // decode that populated them.
      for (int i = 0; i < 2; i++) {
        assertThat(rows(warm.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host3, 5, null)))
            .isEqualTo(coldDescending);
        assertThat(rows(warm.queryAscending(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host3, 5, null)))
            .isEqualTo(coldAscending);
        assertThat(rows(warm.query(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host3))).isEqualTo(coldRange);
        assertThat(sumOfValues(warm, host3)).isEqualTo(coldSum);
      }
      assertThat(cacheOf(warm).getHits()).as("the second pass has to have been served from the cache").isPositive();
    }

    // #8179
    /**
     * Retention copies every block it retains VERBATIM, which keeps the block's id - the cache key - unchanged. That
     * is sound precisely because the bytes are identical, and this pins it: a read after retention answers the same
     * rows a cold read of the retained store answers, and the dropped rows are gone from both.
     */
    @Test
    void aBlockRetentionCopiedVerbatimKeepsAnsweringCorrectly() throws Exception {
      final TimeSeriesEngine engine = createMetric("retained", 1, 8L);
      final TagFilter host1 = TagFilter.eq(0, "host_1");

      appendHosts(engine, 0, 2_000, 4);
      engine.compactAll();
      // Warm the cache on the blocks retention is about to rewrite around.
      assertThat(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host1)).isNotEmpty();
      assertThat(cacheOf(engine).getMisses()).isPositive();

      final long cutoff = BASE_TS + 10_000L;
      engine.applyRetention(cutoff);

      final List<List<Object>> afterRetention = rows(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host1));
      assertThat(afterRetention)
          .as("no row older than the cutoff may come back, from the cache or from anywhere else")
          .allSatisfy(row -> assertThat((long) row.getFirst()).isGreaterThanOrEqualTo(cutoff));
      assertThat(afterRetention).isEqualTo(coldReadOfTheSameStore(engine, host1));
    }

    // #8179
    /**
     * Downsampling REWRITES the rows of the blocks it touches rather than copying them, so it mints a new block id
     * and a cached column of the old block can never be served for the new one. Averaged values, not the raw ones.
     */
    @Test
    void downsamplingIsNotAnsweredFromTheColumnsItReplaced() throws Exception {
      final TimeSeriesEngine engine = createMetric("downsampled", 1, 8L);
      final TagFilter host0 = TagFilter.eq(0, "host_0");

      appendHosts(engine, 0, 2_000, 2);
      engine.compactAll();

      final List<List<Object>> beforeDownsampling = rows(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host0));
      assertThat(beforeDownsampling).isNotEmpty();

      engine.applyDownsampling(List.of(new DownsamplingTier(1_000L, 60_000L)), BASE_TS + 10_000_000L);

      final List<List<Object>> afterDownsampling = rows(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host0));
      assertThat(afterDownsampling)
          .as("downsampling collapses buckets, so the rows it produces are not the rows that were cached")
          .isNotEqualTo(beforeDownsampling);
      assertThat(afterDownsampling).isEqualTo(coldReadOfTheSameStore(engine, host0));
    }

    // #8179
    /**
     * A budget smaller than a single column admits nothing and the reads still answer - the degradation is to the
     * behaviour the engine had before the cache existed, not to a wrong answer or an exception.
     */
    @Test
    void aBudgetTooSmallToHoldAColumnStillAnswers() throws Exception {
      // 1MB against a 4000-sample block is enough to hold columns, so the too-small case is forced with the
      // smallest budget the setting accepts above "disabled".
      final TimeSeriesEngine engine = createMetric("tiny_budget", 1, 1L);
      final TagFilter host2 = TagFilter.eq(0, "host_2");

      appendHosts(engine, 0, 4_000, 5);
      engine.compactAll();

      final List<List<Object>> first = rows(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host2));
      final List<List<Object>> second = rows(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host2));

      assertThat(first).isNotEmpty().isEqualTo(second);
      assertThat(cacheOf(engine).getHeldBytes())
          .as("whatever it admitted, it never holds more than the budget")
          .isLessThanOrEqualTo(1024L * 1024L);
    }

    // #8179
    /**
     * Every shard keeps its own cache, and a tag absent from a shard is still skipped on the directory entry rather
     * than decoded - the pruning the cache sits behind, unchanged.
     */
    @Test
    void eachShardCachesItsOwnBlocks() throws Exception {
      final TimeSeriesEngine engine = createMetric("sharded", 4, 8L);
      final TagFilter host6 = TagFilter.eq(0, "host_6");

      appendHosts(engine, 0, 4_000, 10);
      engine.compactAll();

      final List<Object[]> first = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host6, 1, null);
      final List<Object[]> second = engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host6, 1, null);
      assertThat(first).hasSize(1);
      assertThat(rows(second)).isEqualTo(rows(first));

      long decodes = 0;
      long reuses = 0;
      for (int s = 0; s < engine.getShardCount(); s++) {
        decodes += engine.getShard(s).getSealedStore().getDecodedColumnCache().getMisses();
        reuses += engine.getShard(s).getSealedStore().getDecodedColumnCache().getHits();
      }
      assertThat(decodes).as("the shards that hold the tag decoded on the first read").isPositive();
      assertThat(reuses).as("and reused those columns on the second").isPositive();
    }

    // #8179
    /**
     * A high-cardinality tag column is charged for the strings it retains, not just for the slots pointing at them
     * (code review on PR #8194).
     * <p>
     * {@code DictionaryCodec.decode} allocates every value with {@code new String(utf8, UTF_8)} per call, so holding
     * the decoded array is what keeps those strings alive, and a block may carry up to 65535 distinct ones. Charging
     * the references alone put megabytes in a cache that believed it held half a megabyte. The comparison is against
     * the SAME row count carrying a handful of repeated values, which is the shape the reference-only charge was right
     * for: the two differ only in how many distinct strings the column retains.
     */
    @Test
    void aHighCardinalityTagIsChargedForTheStringsItRetains() throws Exception {
      final int rows = 4_000;
      final String padding = "x".repeat(200);

      final TimeSeriesEngine repeated = createMetric("few_values", 1, 64L);
      appendTags(repeated, rows, i -> "host_" + (i % 4) + padding);
      repeated.compactAll();
      assertThat(repeated.query(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, null)).isNotEmpty();

      final TimeSeriesEngine distinct = createMetric("all_distinct", 1, 64L);
      appendTags(distinct, rows, i -> "host_" + i + padding);
      distinct.compactAll();
      assertThat(distinct.query(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, null)).isNotEmpty();

      assertThat(cacheOf(distinct).getHeldBytes())
          .as("a column retaining one distinct 200-char string per row costs far more than one retaining four")
          .isGreaterThan(cacheOf(repeated).getHeldBytes() * 2);
    }

    // #8179
    /**
     * Readers racing on the same store get the same rows, whichever of them decoded a column and whichever was handed
     * it back.
     * <p>
     * The cache hands out the codec's OWN array rather than a copy, so a column decoded by one thread is read by every
     * later one. Nothing writes to a decoded array - that is what the audit behind this change established - and this
     * pins the consequence rather than the audit: with readers interleaving against the LRU, and against the eviction
     * a budget this small forces, every answer still equals the single-threaded one. No timing is asserted, so there
     * is nothing here for a loaded machine to make flaky.
     */
    @Test
    void concurrentReadersOfTheSameBlocksAgree() throws Exception {
      final TimeSeriesEngine engine = createMetric("concurrent", 1, 1L);
      final TagFilter host4 = TagFilter.eq(0, "host_4");

      appendHosts(engine, 0, 20_000, 8);
      engine.compactAll();

      final List<List<Object>> expected = rows(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host4));
      assertThat(expected).isNotEmpty();

      final int threads = 8;
      final int readsPerThread = 25;
      final ExecutorService pool = Executors.newFixedThreadPool(threads);
      try {
        final List<Future<?>> running = new ArrayList<>(threads);
        for (int t = 0; t < threads; t++)
          running.add(pool.submit(() -> {
            for (int i = 0; i < readsPerThread; i++) {
              assertThat(rows(engine.queryDescending(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host4, 3, null)))
                  .isEqualTo(expected.subList(expected.size() - 3, expected.size()).reversed());
              assertThat(rows(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, host4))).isEqualTo(expected);
            }
            return null;
          }));

        for (final Future<?> outcome : running)
          // Surfaces an assertion that failed on a pool thread, which would otherwise be swallowed by the Future.
          outcome.get(2, TimeUnit.MINUTES);
      } finally {
        pool.shutdownNow();
      }

      assertThat(cacheOf(engine).getHeldBytes())
          .as("the budget holds across however many threads were admitting columns at once")
          .isLessThanOrEqualTo(1024L * 1024L);
    }

    /**
     * The rows a read of this store returns with its cache dropped, i.e. what the file alone says. Used to check a
     * warm answer against a cold one on the SAME store after a rewrite, which is the comparison a second engine
     * cannot make.
     */
    private List<List<Object>> coldReadOfTheSameStore(final TimeSeriesEngine engine, final TagFilter filter)
        throws IOException {
      for (int s = 0; s < engine.getShardCount(); s++)
        engine.getShard(s).getSealedStore().clearDecodedColumnCache();
      return rows(engine.query(Long.MIN_VALUE, Long.MAX_VALUE, TS_AND_VALUE, filter));
    }

    private double sumOfValues(final TimeSeriesEngine engine, final TagFilter filter) throws IOException {
      // Row position 2 is 'value': the engine row is [ts, host, value], timestamp first and then the non-TIMESTAMP
      // columns in schema order (issue #8140). The single-column aggregate() this used to call was deleted with
      // its second column-index convention (issue #8189).
      final MultiColumnAggregationResult result = engine.aggregateMulti(Long.MIN_VALUE, Long.MAX_VALUE,
          List.of(new MultiColumnAggregationRequest(2, AggregationType.SUM, "sum")), 0, filter);
      double total = 0;
      for (final long bucketTs : result.getBucketTimestamps())
        total += result.getValue(bucketTs, 0);
      return total;
    }

    private static List<List<Object>> rows(final List<Object[]> rows) {
      final List<List<Object>> rendered = new ArrayList<>(rows.size());
      for (final Object[] row : rows)
        rendered.add(Arrays.asList(row));
      return rendered;
    }

    private TimeSeriesDecodedColumnCache cacheOf(final TimeSeriesEngine engine) {
      return engine.getShard(0).getSealedStore().getDecodedColumnCache();
    }

    /**
     * The budget has to be set BEFORE the type is created: a sealed store sizes its cache when it is constructed,
     * which is what makes the setting a per-shard budget rather than a knob a running query can turn.
     */
    private TimeSeriesEngine createMetric(final String typeName, final int shards, final long cacheRamMB) {
      GlobalConfiguration.TIMESERIES_DECODED_BLOCK_CACHE_RAM.setValue(cacheRamMB);
      new TimeSeriesTypeBuilder((DatabaseInternal) database)
          .withName(typeName)
          .withTimestamp("ts")
          .withTag("host", Type.STRING)
          .withField("value", Type.DOUBLE)
          .withShards(shards)
          .withCompactionBucketInterval(BUCKET_MS)
          .create();
      return ((LocalTimeSeriesType) database.getSchema().getType(typeName)).getEngine();
    }

    /** Appends {@code count} samples whose tag value is whatever {@code tag} makes of the row number. */
    private void appendTags(final TimeSeriesEngine engine, final int count, final IntFunction<String> tag)
        throws IOException {
      final long[] timestamps = new long[count];
      final Object[] tags = new Object[count];
      final Object[] values = new Object[count];
      for (int i = 0; i < count; i++) {
        timestamps[i] = BASE_TS + i * STEP_MS;
        tags[i] = tag.apply(i);
        values[i] = (double) i;
      }
      engine.appendBatch(timestamps, new Object[][] { tags, values });
    }

    private void appendHosts(final TimeSeriesEngine engine, final long startOffsetMs, final int count,
        final int hostCount) throws IOException {
      final long[] timestamps = new long[count];
      final Object[] hosts = new Object[count];
      final Object[] values = new Object[count];
      for (int i = 0; i < count; i++) {
        timestamps[i] = BASE_TS + startOffsetMs + i * STEP_MS;
        hosts[i] = "host_" + (i % hostCount);
        values[i] = (double) i;
      }
      engine.appendBatch(timestamps, new Object[][] { hosts, values });
    }
  }

  @Nested
  class LazyIterateQuery extends TestHelper {
    // #9420
    // Issue #9420: {@code iterateQuery} returned an {@code Iterator}, but every matching sealed row of every shard was
    // already in memory when it returned, so a plain {@code SELECT FROM <timeseries type>} with no LIMIT - and the gRPC
    // stream without a row limit - needed heap for the whole range before handing back the first row. A 50M-point type
    // exhausted a 4 GB heap that way (#9416).
    // <p>
    // The sealed layer is now read one block per shard at a time, over the same directory snapshot and block walk
    // {@code forEachRow} uses. These tests pin what that must keep - the same rows, in timestamp order - and what it
    // changes: the blocks are decoded as the iterator advances, not up front, which is observable through the read
    // counters with no heap measurement and no wall clock involved.

    private static final int  TAGS    = 8;
    private static final int  PER_TAG = 50_000;
    private static final int  SHARDS  = 2;
    private static final long BASE_TS = 1_700_000_000_000L;
    private static final long STEP_MS = 1_000L;

    private TimeSeriesEngine engine;

    @BeforeEach
    void populate() throws IOException {
      database.command("sql",
          "CREATE TIMESERIES TYPE Point TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE) SHARDS " + SHARDS);

      engine = ((LocalTimeSeriesType) database.getSchema().getType("Point")).getEngine();

      final int total = TAGS * PER_TAG;
      final long[] timestamps = new long[total];
      final Object[] hosts = new Object[total];
      final Object[] values = new Object[total];

      int i = 0;
      for (int t = 0; t < PER_TAG; t++)
        for (int h = 0; h < TAGS; h++) {
          timestamps[i] = BASE_TS + t * STEP_MS;
          hosts[i] = "host_" + h;
          values[i] = (double) (t * TAGS + h);
          i++;
        }

      engine.appendBatch(timestamps, new Object[][] { hosts, values });
      engine.compactAll();
      // Left in the mutable bucket on purpose: a lazy sealed walk that lost the mutable layer would still pass every
      // ordering assertion below.
      engine.appendBatch(new long[] { BASE_TS + PER_TAG * STEP_MS }, new Object[][] {
          new Object[] { "host_only_in_the_mutable_bucket" }, new Object[] { 1.0d } });
    }

    // #9420
    /** The same rows the materialising {@code query()} returns, sealed and mutable alike, in timestamp order. */
    @Test
    void returnsEveryRowOfBothLayersInTimestampOrder() throws Exception {
      final List<Object[]> reference = engine.query(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      final List<Object[]> iterated = drain(engine.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null));

      assertThat(iterated).hasSameSizeAs(reference).hasSize(TAGS * PER_TAG + 1);
      long previous = Long.MIN_VALUE;
      for (final Object[] row : iterated) {
        assertThat((long) row[0]).as("the shards are still merged by timestamp").isGreaterThanOrEqualTo(previous);
        previous = (long) row[0];
      }
      assertThat(iterated.getLast()[1]).isEqualTo("host_only_in_the_mutable_bucket");
    }

    // #9420
    /** Range bounds and tag filter apply exactly as they do for {@code query()}. */
    @Test
    void rangeAndTagFilterMatchTheMaterialisingReader() throws Exception {
      final long from = BASE_TS + 500 * STEP_MS;
      final long to = BASE_TS + 15_000 * STEP_MS;
      final TagFilter filter = TagFilter.eq(0, "host_3");

      final List<Object[]> reference = engine.query(from, to, null, filter);
      final List<Object[]> iterated = drain(engine.iterateQuery(from, to, null, filter));

      assertThat(iterated).hasSameSizeAs(reference).isNotEmpty();
      for (final Object[] row : iterated) {
        assertThat((long) row[0]).isBetween(from, to);
        assertThat(row[1]).isEqualTo("host_3");
      }
    }

    // #9420
    /**
     * The property the issue is about. Before the fix the call itself decoded every block of the range and built
     * every row; now it decodes at most one block per shard to find each shard's first row, and the rest are read
     * as the iterator reaches them.
     */
    @Test
    void blocksAreDecodedAsTheIteratorAdvancesNotUpFront() throws Exception {
      final int totalBlocks = totalSealedBlocks();
      assertThat(totalBlocks)
          .as("the fixture has to span several blocks per shard, or 'one per shard' says nothing")
          .isGreaterThanOrEqualTo(3 * SHARDS);

      final AggregationMetrics metrics = new AggregationMetrics();
      final Iterator<Object[]> it = engine.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null, metrics);
      assertThat(it.hasNext()).isTrue();
      it.next();

      assertThat(decodedBlocks(metrics))
          .as("one block per shard is enough to know each shard's first row")
          .isLessThanOrEqualTo(SHARDS);
      assertThat(metrics.getMaterializedRows())
          .as("and only those blocks' rows were built, out of %d", TAGS * PER_TAG)
          .isLessThan(TAGS * PER_TAG / 2);

      while (it.hasNext())
        it.next();
      assertThat(decodedBlocks(metrics)).as("draining it still reads every block once").isEqualTo(totalBlocks);
    }

    // #9420
    /**
     * The read now holds nothing of the shard between two blocks, like {@code forEachRow}, so a retention pass can
     * land mid-iteration. It must leave a correct, shorter answer - the truncated rows are gone - and say so through
     * {@code vanishedBlocks}, never fail the read or hand back a row older than the cutoff it has not yet passed.
     */
    @Test
    void aRetentionPassMidIterationSkipsTheBlocksItRemoved() throws Exception {
      final AggregationMetrics metrics = new AggregationMetrics();
      final Iterator<Object[]> it = engine.iterateQuery(Long.MIN_VALUE, Long.MAX_VALUE, null, null, metrics);
      final long firstTs = (long) it.next()[0];

      final long cutoff = BASE_TS + (PER_TAG / 2) * STEP_MS;
      engine.applyRetention(cutoff);

      final List<Object[]> rest = drain(it);
      assertThat(metrics.getVanishedBlocks()).as("the blocks retention removed are counted, not hidden").isPositive();
      assertThat(rest.size() + 1).as("the answer is short by the rows retention removed").isLessThan(TAGS * PER_TAG + 1);
      assertThat(rest.getLast()[1]).isEqualTo("host_only_in_the_mutable_bucket");
      assertThat(firstTs).isEqualTo(BASE_TS);
    }

    // #9420
    /**
     * The other half of {@code forEachRow}'s contract the lazy read now shares (issue #8166): a downsample that
     * replaces blocks the iterator has not reached yet leaves no answer at one resolution, so the read is refused
     * from {@code hasNext()} - mid-stream, after rows were already handed out - rather than finishing silently
     * short or mixing fine rows with coarse ones. Every wire protocol already maps the exception to "retry the read"
     * (HTTP 503, {@code ErrorCategory.RETRY}), and a gRPC stream ends on {@code onError}, never {@code onCompleted}.
     */
    @Test
    void aDownsampleMidIterationRaisesInsteadOfMixingResolutions() throws Exception {
      final TimeSeriesShard shard = engine.getShard(0);
      final Iterator<Object[]> it = shard.iterateRange(Long.MIN_VALUE, Long.MAX_VALUE, null, null);
      assertThat(it.hasNext()).isTrue();
      it.next();

      // Every sealed block of the shard, folded into one-hour buckets per host: the TAG is column 1 and the FIELD
      // column 2 in schema order, the timestamp being column 0.
      shard.getSealedStore().downsampleBlocks(Long.MAX_VALUE, 3_600_000L, 0, List.of(1), List.of(2));

      assertThatThrownBy(() -> drain(it))
          .isInstanceOf(TimeSeriesWalkCoarsenedException.class)
          .hasMessageContaining("downsample");
    }

    private int totalSealedBlocks() {
      int total = 0;
      for (int i = 0; i < engine.getShardCount(); i++)
        total += engine.getShard(i).getSealedStore().getBlockCount();
      return total;
    }

    private static long decodedBlocks(final AggregationMetrics metrics) {
      return metrics.getFastPathBlocks() + metrics.getSlowPathBlocks();
    }

    private static List<Object[]> drain(final Iterator<Object[]> it) {
      final List<Object[]> rows = new ArrayList<>();
      while (it.hasNext())
        rows.add(it.next());
      return rows;
    }
  }
}
