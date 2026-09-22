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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8179: reading the same sealed block twice decodes it once.
 * <p>
 * A tag-filtered last-point query - {@code WHERE host = 'host_5' ORDER BY ts DESC LIMIT 1}, what an operational
 * dashboard polls most - was several times slower against a native TIMESERIES type than against an ordinary
 * DOCUMENT type with a {@code (host, ts)} index over the same points. Block-level tag pruning was not the gap:
 * it already works, and a shard holding none of the tag's rows already skips every block on its directory entry
 * alone. The gap was that the ONE block which does hold the tag was re-read and re-decoded on every poll. A block
 * holds up to {@code TimeSeriesShard.SEALED_BLOCK_SIZE} = 65536 samples, so answering with one row decoded 65536
 * timestamps, 65536 tag values and 65536 measurements, and answering again decoded all of them again.
 * <p>
 * <b>What these tests assert is the WORK, not the clock.</b> A wall-clock bound on a full-suite run is a coin flip
 * on the JVM's mood, and the thing the change makes true is exact and countable: the second read decodes nothing.
 * Every test therefore also pins the ANSWER against the answer the same read gives with the cache turned off,
 * because a cache that returns quickly and wrongly is the only outcome worse than a slow one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8179DecodedBlockCacheTest extends TestHelper {

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
